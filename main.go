package main

import (
	"fmt"
	"io"
	"net"
	"bufio"
	"strings"
)

const PORT = ":6379"
const MAX_CONNECTIONS = 1

func main() {
	listener, err := net.Listen("tcp", PORT)
	if err != nil {
		fmt.Printf("failed to listen: %v", err)
		return
	}
	defer listener.Close()

	store := NewStore()

	connections := make(chan struct{}, MAX_CONNECTIONS)
	fmt.Printf("server is running on port %s. waiting for connections...\n", PORT)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("failed to accept: %v", err)
			return
		}

		select {
		case connections <- struct{}{}:
			fmt.Printf("accepted connection from %s\n", conn.RemoteAddr())
			go handleConnection(conn, connections, store)
		default:
			printError(conn, "max clients reached")
			conn.Close()
		}
	}
}

func handleConnection(conn net.Conn, connections <-chan struct{}, store *Store) {
	defer closeConnection(conn, connections)
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		handleCommand(conn, line, store)
	}
}

func closeConnection(conn net.Conn, connections <-chan struct{}) {
	fmt.Printf("closing connection from %s\n", conn.RemoteAddr())
	conn.Close()
	<-connections // Release the connection slot
	fmt.Printf("connection from %s closed\n", conn.RemoteAddr())
}

func handleCommand(conn net.Conn, line string, store *Store) {
	tokens := strings.Fields(line)
	if len(tokens) == 0 {
		// We got a blank line, ignore it
		return
	}

	command := tokens[0]
	switch strings.ToUpper(command) {
	case "PING":
		io.WriteString(conn, "+PONG\n")
	case "SET":
		if len(tokens) < 3 {
			printError(conn, "wrong number of arguments for SET. Expecting key value")
			return
		}

		store.Set(tokens[1], strings.Join(tokens[2:], " "))
		printSuccess(conn, "OK")
	case "GET":
		if len(tokens) != 2 {
			printError(conn, "wrong number of arguments for GET. Expecting key")
			return
		}

		value, ok := store.Get(tokens[1])
		if !ok {
			printError(conn, "key not found")
			return
		}

		printSuccess(conn, value)
	case "DEL":
		if len(tokens) != 2 {
			printError(conn, "wrong number of arguments for DEL. Expecting key")
			return
		}

		store.Del(tokens[1])
		printSuccess(conn, "OK")
	case "KEYS":
		keys := store.Keys()
		printSuccess(conn, strings.Join(keys, ","))
	default:
		printError(conn, "unknown command")
	}
}
