// Package testutil holds integration-test helpers for internal/server.
// Integration tests should use package server_test so they can import testutil without
// an import cycle (testutil imports server).
package testutil

import (
	"bufio"
	"fmt"
	"net"
	"testing"
	"time"

	"go-memory-db/internal/server"
)

// StartTestServer runs the server on an ephemeral port, waits until it is listening,
// registers t.Cleanup to Close it, and returns the server and dial address.
func StartTestServer(t *testing.T, maxConnections int) (*server.Server, string) {
	t.Helper()

	srv := server.NewServer(0, maxConnections)
	go func() { _ = srv.Serve() }()
	t.Cleanup(func() { _ = srv.Close() })

	addr := waitListenAddr(t, srv)
	return srv, addr
}

func waitListenAddr(t *testing.T, srv *server.Server) string {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if addr := srv.ListenerAddress(); addr != "" {
			return addr
		}
		time.Sleep(5 * time.Millisecond)
	}

	t.Fatal("server did not start listening in time")
	return ""
}

func ConnectToServer(t *testing.T, address string) net.Conn {
	t.Helper()

	c, err := DialTCPRetry(address)
	if err != nil {
		t.Fatal(err)
	}

	return c
}

// DialTCPRetry dials addr with a short backoff until success or timeout.
func DialTCPRetry(address string) (net.Conn, error) {
	deadline := time.Now().Add(2 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		connection, err := net.Dial("tcp", address)
		if err == nil {
			return connection, nil
		}

		lastErr = err
		time.Sleep(5 * time.Millisecond)
	}

	if lastErr != nil {
		return nil, fmt.Errorf("dial %s: %w", address, lastErr)
	}

	return nil, fmt.Errorf("dial %s: timeout", address)
}

func SendToServer(t *testing.T, connection net.Conn, command string) {
	t.Helper()

	if _, err := connection.Write([]byte(command)); err != nil {
		t.Fatalf("write: %v", err)
	}
}

// MustReadLine reads until '\n' and asserts the full line matches want (including newline).
func MustReadLine(t *testing.T, r *bufio.Reader, want string) {
	t.Helper()

	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	if line != want {
		t.Fatalf("got line %q, want %q", line, want)
	}
}
