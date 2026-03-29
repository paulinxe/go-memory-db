package server_test

import (
	"bufio"
	"sort"
	"strings"
	"testing"

	"go-memory-db/internal/server/testutil"
)

func Test_we_can_send_the_ping_command(t *testing.T) {
	testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t)
	defer connection.Close()

	reader := bufio.NewReader(connection)
	testutil.SendToServer(t, connection, "PING\n")

	testutil.MustReadLine(t, reader, "+PONG\n")
}

func Test_we_can_send_the_ping_command_in_a_case_insensitive_manner(t *testing.T) {
	testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t)
	defer connection.Close()

	reader := bufio.NewReader(connection)
	testutil.SendToServer(t, connection, "ping\n")

	testutil.MustReadLine(t, reader, "+PONG\n")
}

func Test_blank_lines_are_ignored(t *testing.T) {
	testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t)
	defer connection.Close()

	reader := bufio.NewReader(connection)
	testutil.SendToServer(t, connection, "\n")
	testutil.SendToServer(t, connection, "PING\n")

	testutil.MustReadLine(t, reader, "+PONG\n")
}

func Test_whitespace_only_lines_are_ignored(t *testing.T) {
	testutil.StartTestServer(t, 4)
	c := testutil.ConnectToServer(t)
	defer c.Close()

	r := bufio.NewReader(c)
	testutil.SendToServer(t, c, "   \t  \n")
	testutil.SendToServer(t, c, "PING\n")

	testutil.MustReadLine(t, r, "+PONG\n")
}

func Test_we_can_delete_values(t *testing.T) {
	testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t)
	defer connection.Close()

	reader := bufio.NewReader(connection)

	testutil.SendToServer(t, connection, "SET a 1\n")
	testutil.MustReadLine(t, reader, "+OK\n")

	testutil.SendToServer(t, connection, "LPUSH mylist 1\n")
	testutil.MustReadLine(t, reader, "+OK\n")

	testutil.SendToServer(t, connection, "DEL a\n")
	testutil.MustReadLine(t, reader, "+OK\n")

	testutil.SendToServer(t, connection, "GET a\n")
	testutil.MustReadLine(t, reader, "-key not found\n")

	testutil.SendToServer(t, connection, "DEL mylist\n")
	testutil.MustReadLine(t, reader, "+OK\n")

	testutil.SendToServer(t, connection, "LPOP mylist\n")
	testutil.MustReadLine(t, reader, "-list is empty\n")

	testutil.SendToServer(t, connection, "HSETONE h k v\n")
	testutil.MustReadLine(t, reader, "+OK\n")
	testutil.SendToServer(t, connection, "HGETONE h k\n")
	testutil.MustReadLine(t, reader, "+v\n")

	testutil.SendToServer(t, connection, "DEL h\n")
	testutil.MustReadLine(t, reader, "+OK\n")
	testutil.SendToServer(t, connection, "HGETONE h k\n")
	testutil.MustReadLine(t, reader, "-key not found\n")

	testutil.SendToServer(t, connection, "HSETONE h k2 v2\n")
	testutil.MustReadLine(t, reader, "+OK\n")
	testutil.SendToServer(t, connection, "HGETONE h k2\n")
	testutil.MustReadLine(t, reader, "+v2\n")
}

func Test_delete_fails_when_missing_value(t *testing.T) {
	testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t)
	defer connection.Close()

	reader := bufio.NewReader(connection)
	testutil.SendToServer(t, connection, "DEL\n")

	testutil.MustReadLine(t, reader, "-wrong number of arguments for DEL. Expecting key\n")
}

func Test_we_can_list_keys(t *testing.T) {
	testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t)
	defer connection.Close()

	reader := bufio.NewReader(connection)

	testutil.SendToServer(t, connection, "SET y 1\n")
	testutil.MustReadLine(t, reader, "+OK\n")

	testutil.SendToServer(t, connection, "SET z 2\n")
	testutil.MustReadLine(t, reader, "+OK\n")

	testutil.SendToServer(t, connection, "LPUSH mylist 1\n")
	testutil.MustReadLine(t, reader, "+OK\n")

	testutil.SendToServer(t, connection, "HSETONE hsh f v\n")
	testutil.MustReadLine(t, reader, "+OK\n")
	testutil.SendToServer(t, connection, "HGETONE hsh f\n")
	testutil.MustReadLine(t, reader, "+v\n")

	testutil.SendToServer(t, connection, "KEYS\n")
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}

	if !strings.HasPrefix(line, "+") {
		t.Fatalf("KEYS reply: %q", line)
	}

	body := strings.TrimSuffix(strings.TrimPrefix(line, "+"), "\n")
	parts := strings.Split(body, ",")
	if len(parts) != 4 {
		t.Fatalf("expected 4 keys, got %q", line)
	}

	sort.Strings(parts)
	if parts[0] != "hsh" || parts[1] != "mylist" || parts[2] != "y" || parts[3] != "z" {
		t.Fatalf("KEYS keys %v, want hsh, mylist, y, z", parts)
	}
}

func Test_unknown_commands_are_rejected(t *testing.T) {
	testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t)
	defer connection.Close()

	reader := bufio.NewReader(connection)
	testutil.SendToServer(t, connection, "NOPE\n")

	testutil.MustReadLine(t, reader, "-unknown command\n")
}

func Test_new_connections_are_rejected_when_max_clients_is_reached(t *testing.T) {
	testutil.StartTestServer(t, 1)

	c1 := testutil.ConnectToServer(t)
	defer c1.Close()

	connectionToBeRefused, err := testutil.DialTCPRetry(testutil.GetServerAddress())
	if err != nil {
		t.Fatal(err)
	}
	defer connectionToBeRefused.Close()

	reader := bufio.NewReader(connectionToBeRefused)
	testutil.MustReadLine(t, reader, "-max clients reached\n")
}

