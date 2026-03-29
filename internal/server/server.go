package server

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"

	"go-memory-db/internal/store"
)

// Server is the TCP command server.
type Server struct {
	Port           int
	MaxConnections int
	Store          *store.Store

	mutex          sync.Mutex
	listener       net.Listener // set while Serve is running; Close() clears and closes it
}

func NewServer(port, maxConnections int) *Server {
	if maxConnections < 1 {
		maxConnections = 1
	}

	return &Server{
		Port:           port,
		MaxConnections: maxConnections,
		Store:          store.NewStore(),
	}
}

func (s *Server) Serve() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	s.mutex.Lock()
	s.listener = listener
	s.mutex.Unlock()

	err = s.serve(listener)

	// If we reach this point, it means we are shutting down so let's proceed with closing.
	_ = s.Close()

	return err
}

// Close shuts down the listener so Serve's accept loop exits. It is idempotent and safe to call
// more than once.
func (s *Server) Close() error {
	s.mutex.Lock()

	listener := s.listener
	s.listener = nil
	s.mutex.Unlock()

	if listener != nil {
		return listener.Close()
	}

	return nil
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
			go handleConnection(conn, connections, s.Store)
		default:
			printError(conn, "max clients reached")
			conn.Close()
		}
	}
}

func handleConnection(conn net.Conn, connections <-chan struct{}, st *store.Store) {
	defer closeConnection(conn, connections)
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		handleCommand(conn, line, st)
	}
}

func closeConnection(conn net.Conn, connections <-chan struct{}) {
	fmt.Printf("closing connection from %s\n", conn.RemoteAddr())
	conn.Close()
	<-connections
	fmt.Printf("connection from %s closed\n", conn.RemoteAddr())
}

func handleCommand(writer io.Writer, line string, store *store.Store) {
	tokens := strings.Fields(line)
	if len(tokens) == 0 {
		return
	}

	// TODO: to check if worth it introducing a Locator

	command := tokens[0]
	switch strings.ToUpper(command) {
	case "PING":
		io.WriteString(writer, "+PONG\n")
	case "SET":
		if len(tokens) < 3 {
			printError(writer, "wrong number of arguments for SET. Expecting key value")
			return
		}

		store.Set(tokens[1], strings.Join(tokens[2:], " "))
		printSuccess(writer, "OK")
	case "GET":
		if len(tokens) != 2 {
			printError(writer, "wrong number of arguments for GET. Expecting key")
			return
		}

		value, ok := store.Get(tokens[1])
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

		store.Del(tokens[1])
		printSuccess(writer, "OK")
	case "KEYS":
		keys := store.Keys()
		printSuccess(writer, strings.Join(keys, ","))
	case "LPUSH":
		if len(tokens) < 3 {
			printError(writer, "wrong number of arguments for LPUSH. Expecting key value")
			return
		}

		err := store.LPush(tokens[1], strings.Join(tokens[2:], " "))
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

		value, err := store.LPop(tokens[1])
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

		elements := store.LGet(tokens[1])
		printSuccess(writer, strings.Join(elements, ","))
	default:
		printError(writer, "unknown command")
	}
}
