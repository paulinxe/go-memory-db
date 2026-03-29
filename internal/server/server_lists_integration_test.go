package server_test

import (
	"bufio"
	"testing"

	"go-memory-db/internal/server/testutil"
)

func Test_push_fails_when_key_already_exists_for_other_type(t *testing.T) {
	testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t)
	defer connection.Close()

	reader := bufio.NewReader(connection)

	testutil.SendToServer(t, connection, "SET mykey 1\n")
	testutil.MustReadLine(t, reader, "+OK\n")

	testutil.SendToServer(t, connection, "LPUSH mykey 1\n")
	testutil.MustReadLine(t, reader, "-key already exists\n")
}

func Test_pop_fails_when_key_not_exists(t *testing.T) {
	testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t)
	defer connection.Close()

	reader := bufio.NewReader(connection)

	testutil.SendToServer(t, connection, "LPOP mylist\n")
	testutil.MustReadLine(t, reader, "-list is empty\n")
}

func Test_we_can_push_and_pop_values(t *testing.T) {
	testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t)
	defer connection.Close()

	reader := bufio.NewReader(connection)

	testutil.SendToServer(t, connection, "LPUSH mylist 1\n")
	testutil.MustReadLine(t, reader, "+OK\n")

	testutil.SendToServer(t, connection, "LPOP mylist\n")
	testutil.MustReadLine(t, reader, "+1\n")

	testutil.SendToServer(t, connection, "LPOP mylist\n")
	testutil.MustReadLine(t, reader, "-list is empty\n")
}

func Test_we_can_push_and_pop_values_with_spaces(t *testing.T) {
	testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t)
	defer connection.Close()

	reader := bufio.NewReader(connection)

	testutil.SendToServer(t, connection, "LPUSH mylist hello world\n")
	testutil.MustReadLine(t, reader, "+OK\n")

	testutil.SendToServer(t, connection, "LPOP mylist\n")
	testutil.MustReadLine(t, reader, "+hello world\n")
}
