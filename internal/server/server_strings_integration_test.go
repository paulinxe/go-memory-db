package server_test

import (
	"bufio"
	"testing"

	"go-memory-db/internal/server/testutil"
)

func Test_we_can_set_and_get_values(t *testing.T) {
	_, addr := testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t, addr)
	defer connection.Close()

	reader := bufio.NewReader(connection)

	testutil.SendToServer(t, connection, "SET mykey myvalue\n")
	testutil.MustReadLine(t, reader, "+OK\n")

	testutil.SendToServer(t, connection, "GET mykey\n")
	testutil.MustReadLine(t, reader, "+myvalue\n")
}

func Test_we_can_set_values_with_spaces(t *testing.T) {
	_, addr := testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t, addr)
	defer connection.Close()

	reader := bufio.NewReader(connection)

	testutil.SendToServer(t, connection, "SET k hello world\n")
	testutil.MustReadLine(t, reader, "+OK\n")

	testutil.SendToServer(t, connection, "GET k\n")
	testutil.MustReadLine(t, reader, "+hello world\n")
}

func Test_set_fails_when_missing_value(t *testing.T) {
	_, addr := testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t, addr)
	defer connection.Close()

	reader := bufio.NewReader(connection)
	testutil.SendToServer(t, connection, "SET onlykey\n")

	testutil.MustReadLine(t, reader, "-wrong number of arguments for SET. Expecting key value\n")
}

func Test_get_fails_when_key_not_exists(t *testing.T) {
	_, addr := testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t, addr)
	defer connection.Close()

	reader := bufio.NewReader(connection)
	testutil.SendToServer(t, connection, "GET nope\n")

	testutil.MustReadLine(t, reader, "-key not found\n")
}

func Test_get_fails_when_missing_value(t *testing.T) {
	_, addr := testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t, addr)
	defer connection.Close()

	reader := bufio.NewReader(connection)
	testutil.SendToServer(t, connection, "GET\n")

	testutil.MustReadLine(t, reader, "-wrong number of arguments for GET. Expecting key\n")
}
