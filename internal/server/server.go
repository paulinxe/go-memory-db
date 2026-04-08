package server

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"go-memory-db/internal/db"
)

// clientSession holds per-client TCP session state: the server and the active namespace store.
// It becomes useful when switching namespaces.
type clientSession struct {
	server    *Server
	namespace *db.Namespace
}

// Server is the TCP command server.
type Server struct {
	Port           int
	MaxConnections int

	listenerMutex sync.Mutex
	namespaces    *db.NamespaceRegistry
	listener      net.Listener // set while Serve() is running; Close() clears and closes it
	// listenerAddress holds listener.Addr().String() after a successful net.Listen in Serve.
	// It is safe to read via ListenerAddress without the listener mutex as its protected with atomic.Value.
	listenerAddress atomic.Value

	// Shutdown mode control fields
	shutdownOnce  sync.Once
	shutdownMutex sync.RWMutex // apart from protecting the bool, it also protects each new command WaitGroup
	shuttingDown  bool

	// WaitGroup to keep track of in-flight commands.
	inflightCommandsWG sync.WaitGroup

	// Map to keep track of active connections. This is used to close them gracefully when the server is shutting down.
	connections      map[net.Conn]struct{}
	connectionsMutex sync.Mutex
}

func NewServer(port, maxConnections int) *Server {
	if maxConnections < 1 {
		maxConnections = 1
	}

	return &Server{
		Port:           port,
		MaxConnections: maxConnections,
		namespaces:     db.NewNamespaceRegistry(),
		connections:    make(map[net.Conn]struct{}),
	}
}

func (s *Server) Serve() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	s.listenerAddress.Store(listener.Addr().String())

	s.setSignalListeners()

	s.listenerMutex.Lock()
	s.listener = listener
	s.listenerMutex.Unlock()

	err = s.serve(listener)

	return err
}

// Close shuts down the whole server (connections, goroutines, etc). It is idempotent and safe to call
// more than once. Concurrent callers block until the first shutdown finishes; later callers are
// no-ops aside from waiting on the same Once completion.
func (s *Server) Close() error {
	s.shutdownOnce.Do(func() {
		s.listenerMutex.Lock()
		listener := s.listener
		s.listener = nil
		s.listenerMutex.Unlock()

		if listener != nil {
			_ = listener.Close()
		}

		// TODO: set shuttingDown, inflightCommandsWG.Wait(), snapshot and close tracked conns (plan §5)

		s.namespaces.Shutdown()
	})

	return nil
}

// ListenerAddress returns the bound listen address string after Serve has called net.Listen, or "" before that.
// After the first successful listen, the value is not updated (e.g. it is unchanged after Close).
func (s *Server) ListenerAddress() string {
	value := s.listenerAddress.Load()
	if value == nil {
		return ""
	}

	addr, ok := value.(string)
	if !ok {
		return ""
	}

	return addr
}

// setSignalListeners sets up the OS signals listeners for the server.
// At the moment, it only supports SIGINT and SIGTERM for graceful shutdown.
func (s *Server) setSignalListeners() {
	shutdownChannel := make(chan os.Signal, 1)
	signal.Notify(shutdownChannel, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-shutdownChannel
		_ = s.Close()
		signal.Stop(shutdownChannel)
	}()
}

func (s *Server) serve(listener net.Listener) error {
	connections := make(chan struct{}, s.MaxConnections)
	fmt.Printf("server is running on %s. waiting for connections...\n", listener.Addr())

	// TODO: we need support for context cancellation.
	for {
		conn, err := listener.Accept()
		if err != nil {
			return fmt.Errorf("accept: %w", err)
		}

		select {
		case connections <- struct{}{}:
			fmt.Printf("accepted connection from %s\n", conn.RemoteAddr())
			s.connectionsMutex.Lock()
			s.connections[conn] = struct{}{}
			s.connectionsMutex.Unlock()
			go handleConnection(conn, connections, s)
		default:
			printError(conn, "max clients reached")
			conn.Close()
		}
	}
}

func handleConnection(conn net.Conn, connections <-chan struct{}, server *Server) {
	defer closeConnection(conn, connections, server)

	session := &clientSession{
		server:    server,
		namespace: server.namespaces.GetDefault(),
	}

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		server.shutdownMutex.RLock()
		if server.shuttingDown {
			server.shutdownMutex.RUnlock()
			printError(conn, "server shutting down")
			break
		}

		server.inflightCommandsWG.Add(1)
		server.shutdownMutex.RUnlock()
		line := scanner.Text()
		func() {
			// We need an anonymous function so we can use defer to decrement the waitgroup.
			// TODO: most probably this is a code smell and we need a new Connection struct
			defer server.inflightCommandsWG.Done()
			handleCommand(conn, line, session)
		}()
	}
}

func closeConnection(conn net.Conn, connections <-chan struct{}, server *Server) {
	fmt.Printf("closing connection from %s\n", conn.RemoteAddr())

	server.connectionsMutex.Lock()
	delete(server.connections, conn)
	server.connectionsMutex.Unlock()

	conn.Close()
	<-connections
	fmt.Printf("connection from %s closed\n", conn.RemoteAddr())
}

func handleCommand(writer io.Writer, line string, session *clientSession) {
	tokens := strings.Fields(line)
	if len(tokens) == 0 {
		return
	}

	command := tokens[0]

	// Namespace deletion guard. Control-plane commands bypass it.
	switch strings.ToUpper(command) {
	case "PING", "USE", "CNAMESPACE", "DNAMESPACE":
		// These commands are agnostic of the current namespace, they don't care if the namespace is deleted.
	default:
		select {
		case <-session.namespace.IsDeleted():
			printError(writer, "namespace deleted")
			return
		default:
			// As the context is not done/cancelled, we allow the command to proceed.
		}
	}

	switch strings.ToUpper(command) {
	case "PING":
		_, _ = io.WriteString(writer, "+PONG\n")
	case "CNAMESPACE":
		if len(tokens) != 2 {
			printError(writer, "wrong number of arguments for CNAMESPACE. Expecting name")
			return
		}

		name := tokens[1]
		if err := session.server.namespaces.CreateNamespace(name); err != nil {
			printError(writer, err.Error())
			return
		}

		printSuccess(writer, "OK")
	case "DNAMESPACE":
		if len(tokens) != 2 {
			printError(writer, "wrong number of arguments for DNAMESPACE. Expecting name")
			return
		}

		name := tokens[1]
		if err := session.server.namespaces.DeleteNamespace(name); err != nil {
			printError(writer, err.Error())
			return
		}

		printSuccess(writer, "OK")
	case "USE":
		if len(tokens) != 2 {
			printError(writer, "wrong number of arguments for USE. Expecting name")
			return
		}

		name := tokens[1]
		ns, ok := session.server.namespaces.Get(name)
		if !ok {
			printError(writer, db.ErrNamespaceDoesNotExist.Error())
			return
		}

		session.namespace = ns
		printSuccess(writer, "OK")
	case "SET":
		if len(tokens) < 3 {
			printError(writer, "wrong number of arguments for SET. Expecting key value")
			return
		}

		err := session.namespace.GetStore().Set(tokens[1], strings.Join(tokens[2:], " "))
		if err != nil {
			printError(writer, err.Error())
			return
		}

		printSuccess(writer, "OK")
	case "GET":
		if len(tokens) != 2 {
			printError(writer, "wrong number of arguments for GET. Expecting key")
			return
		}

		value, ok := session.namespace.GetStore().Get(tokens[1])
		if !ok {
			printError(writer, "key not found")
			return
		}

		printSuccess(writer, value)
	case "DEL":
		if len(tokens) != 2 {
			printError(writer, "wrong number of arguments for DEL. Expecting key")
			return
		}

		session.namespace.GetStore().Del(tokens[1])
		printSuccess(writer, "OK")
	case "KEYS":
		keys := session.namespace.GetStore().Keys()
		printSuccess(writer, strings.Join(keys, ","))
	case "EXPIRE":
		if len(tokens) != 3 {
			printError(writer, "wrong number of arguments for EXPIRE. Expecting key seconds")
			return
		}

		err := session.namespace.GetStore().Expire(tokens[1], tokens[2])
		if err != nil {
			if err == db.ErrKeyNotFound {
				printSuccess(writer, "0")
				return
			}

			printError(writer, err.Error())
			return
		}

		printSuccess(writer, "OK")
	case "TTL":
		if len(tokens) != 2 {
			printError(writer, "wrong number of arguments for TTL. Expecting key")
			return
		}

		printSuccess(writer, session.namespace.GetStore().TTL(tokens[1]))
	case "LPUSH":
		if len(tokens) < 3 {
			printError(writer, "wrong number of arguments for LPUSH. Expecting key value")
			return
		}

		err := session.namespace.GetStore().LPush(tokens[1], strings.Join(tokens[2:], " "))
		if err != nil {
			printError(writer, err.Error())
			return
		}

		printSuccess(writer, "OK")
	case "LPOP":
		if len(tokens) != 2 {
			printError(writer, "wrong number of arguments for LPOP. Expecting key")
			return
		}

		value, err := session.namespace.GetStore().LPop(tokens[1])
		if err != nil {
			printError(writer, err.Error())
			return
		}

		printSuccess(writer, value)
	case "LGET":
		if len(tokens) != 2 {
			printError(writer, "wrong number of arguments for LGET. Expecting key")
			return
		}

		elements := session.namespace.GetStore().LGet(tokens[1])
		printSuccess(writer, strings.Join(elements, ","))
	case "HSET":
		if len(tokens) < 4 {
			printError(writer, "wrong number of arguments for HSET. Expecting key field value [field value ...]")
			return
		}

		err := session.namespace.GetStore().HSet(tokens[1], tokens[2:])
		if err != nil {
			printError(writer, err.Error())
			return
		}

		printSuccess(writer, "OK")
	case "HSETONE":
		if len(tokens) < 4 {
			printError(writer, "wrong number of arguments for HSETONE. Expecting key field value")
			return
		}

		err := session.namespace.GetStore().HSetOne(tokens[1], tokens[2], strings.Join(tokens[3:], " "))
		if err != nil {
			printError(writer, err.Error())
			return
		}

		printSuccess(writer, "OK")
	case "HGET":
		if len(tokens) != 2 {
			printError(writer, "wrong number of arguments for HGET. Expecting key")
			return
		}

		pairs := session.namespace.GetStore().HGet(tokens[1])
		printSuccess(writer, strings.Join(pairs, ","))
	case "HGETONE":
		if len(tokens) != 3 {
			printError(writer, "wrong number of arguments for HGETONE. Expecting key field")
			return
		}

		value, err := session.namespace.GetStore().HGetOne(tokens[1], tokens[2])
		if err != nil {
			printError(writer, err.Error())
			return
		}

		printSuccess(writer, value)
	case "HDEL":
		if len(tokens) != 3 {
			printError(writer, "wrong number of arguments for HDEL. Expecting key field")
			return
		}

		session.namespace.GetStore().HDel(tokens[1], tokens[2])

		printSuccess(writer, "OK")
	default:
		printError(writer, "unknown command")
	}
}
