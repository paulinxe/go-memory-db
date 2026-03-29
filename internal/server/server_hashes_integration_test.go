package server_test

import (
	"bufio"
	"testing"

	"go-memory-db/internal/server/testutil"
)

func Test_hsetone_requires_key_field_and_value(t *testing.T) {
	testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t)
	defer connection.Close()

	reader := bufio.NewReader(connection)

	testutil.SendToServer(t, connection, "HSETONE\n")
	testutil.MustReadLine(t, reader, "-wrong number of arguments for HSETONE. Expecting key field value\n")

	testutil.SendToServer(t, connection, "HSETONE onlykey\n")
	testutil.MustReadLine(t, reader, "-wrong number of arguments for HSETONE. Expecting key field value\n")

	testutil.SendToServer(t, connection, "HSETONE onlykey onlyfield\n")
	testutil.MustReadLine(t, reader, "-wrong number of arguments for HSETONE. Expecting key field value\n")
}

func Test_hsetone_sets_field_and_allows_updates(t *testing.T) {
	testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t)
	defer connection.Close()

	reader := bufio.NewReader(connection)

	testutil.SendToServer(t, connection, "HSETONE user:1 name Jane\n")
	testutil.MustReadLine(t, reader, "+OK\n")
	testutil.SendToServer(t, connection, "HGETONE user:1 name\n")
	testutil.MustReadLine(t, reader, "+Jane\n")

	testutil.SendToServer(t, connection, "HSETONE user:1 email jane@example.com\n")
	testutil.MustReadLine(t, reader, "+OK\n")
	testutil.SendToServer(t, connection, "HGETONE user:1 email\n")
	testutil.MustReadLine(t, reader, "+jane@example.com\n")

	testutil.SendToServer(t, connection, "HSETONE user:1 name Janet\n")
	testutil.MustReadLine(t, reader, "+OK\n")
	testutil.SendToServer(t, connection, "HGETONE user:1 name\n")
	testutil.MustReadLine(t, reader, "+Janet\n")
	testutil.SendToServer(t, connection, "HGETONE user:1 email\n")
	testutil.MustReadLine(t, reader, "+jane@example.com\n")
}

func Test_hsetone_value_preserves_spaces(t *testing.T) {
	testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t)
	defer connection.Close()

	reader := bufio.NewReader(connection)

	testutil.SendToServer(t, connection, "HSETONE doc body first line second line\n")
	testutil.MustReadLine(t, reader, "+OK\n")
	testutil.SendToServer(t, connection, "HGETONE doc body\n")
	testutil.MustReadLine(t, reader, "+first line second line\n")
}

func Test_hsetone_fails_when_key_is_string(t *testing.T) {
	testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t)
	defer connection.Close()

	reader := bufio.NewReader(connection)

	testutil.SendToServer(t, connection, "SET mykey 1\n")
	testutil.MustReadLine(t, reader, "+OK\n")

	testutil.SendToServer(t, connection, "HSETONE mykey field value\n")
	testutil.MustReadLine(t, reader, "-key already exists\n")
}

func Test_hsetone_fails_when_key_is_list(t *testing.T) {
	testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t)
	defer connection.Close()

	reader := bufio.NewReader(connection)

	testutil.SendToServer(t, connection, "LPUSH jobs task\n")
	testutil.MustReadLine(t, reader, "+OK\n")

	testutil.SendToServer(t, connection, "HSETONE jobs field value\n")
	testutil.MustReadLine(t, reader, "-key already exists\n")
}

func Test_hgetone_requires_key_and_field(t *testing.T) {
	testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t)
	defer connection.Close()

	reader := bufio.NewReader(connection)

	testutil.SendToServer(t, connection, "HGETONE\n")
	testutil.MustReadLine(t, reader, "-wrong number of arguments for HGETONE. Expecting key field\n")

	testutil.SendToServer(t, connection, "HGETONE onlykey\n")
	testutil.MustReadLine(t, reader, "-wrong number of arguments for HGETONE. Expecting key field\n")

	testutil.SendToServer(t, connection, "HGETONE onlykey extra field\n")
	testutil.MustReadLine(t, reader, "-wrong number of arguments for HGETONE. Expecting key field\n")
}

func Test_hgetone_returns_error_when_hash_missing(t *testing.T) {
	testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t)
	defer connection.Close()

	reader := bufio.NewReader(connection)

	testutil.SendToServer(t, connection, "HGETONE nosuch name\n")
	testutil.MustReadLine(t, reader, "-key not found\n")
}

func Test_hgetone_returns_error_when_field_missing(t *testing.T) {
	testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t)
	defer connection.Close()

	reader := bufio.NewReader(connection)

	testutil.SendToServer(t, connection, "HSETONE user:1 name Ada\n")
	testutil.MustReadLine(t, reader, "+OK\n")

	testutil.SendToServer(t, connection, "HGETONE user:1 email\n")
	testutil.MustReadLine(t, reader, "-field not found\n")
}

func Test_hgetone_value_can_contain_commas(t *testing.T) {
	testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t)
	defer connection.Close()

	reader := bufio.NewReader(connection)

	testutil.SendToServer(t, connection, "HSETONE cfg csv a,b,c\n")
	testutil.MustReadLine(t, reader, "+OK\n")

	testutil.SendToServer(t, connection, "HGETONE cfg csv\n")
	testutil.MustReadLine(t, reader, "+a,b,c\n")
}
