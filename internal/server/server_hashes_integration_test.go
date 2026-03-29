package server_test

import (
	"bufio"
	"testing"

	"go-memory-db/internal/server/testutil"
)

func Test_hset_requires_key_and_even_field_value_pairs(t *testing.T) {
	testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t)
	defer connection.Close()

	reader := bufio.NewReader(connection)

	testutil.SendToServer(t, connection, "HSET\n")
	testutil.MustReadLine(t, reader, "-wrong number of arguments for HSET. Expecting key field value [field value ...]\n")

	testutil.SendToServer(t, connection, "HSET onlykey\n")
	testutil.MustReadLine(t, reader, "-wrong number of arguments for HSET. Expecting key field value [field value ...]\n")

	testutil.SendToServer(t, connection, "HSET onlykey f1\n")
	testutil.MustReadLine(t, reader, "-wrong number of arguments for HSET. Expecting key field value [field value ...]\n")

	testutil.SendToServer(t, connection, "HSET onlykey a b c\n")
	testutil.MustReadLine(t, reader, "-hset pairs must have even length\n")

	testutil.SendToServer(t, connection, "HSET onlykey f1 v1 extra\n")
	testutil.MustReadLine(t, reader, "-hset pairs must have even length\n")
}

func Test_hset_merges_multiple_pairs_and_preserves_other_fields(t *testing.T) {
	testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t)
	defer connection.Close()

	reader := bufio.NewReader(connection)

	testutil.SendToServer(t, connection, "HSET user:1 id 1 email jane@example.com\n")
	testutil.MustReadLine(t, reader, "+OK\n")
	testutil.SendToServer(t, connection, "HGET user:1\n")
	testutil.MustReadLine(t, reader, "+email,jane@example.com,id,1\n")

	testutil.SendToServer(t, connection, "HSET user:1 email jane2@example.com role admin\n")
	testutil.MustReadLine(t, reader, "+OK\n")
	testutil.SendToServer(t, connection, "HGET user:1\n")
	testutil.MustReadLine(t, reader, "+email,jane2@example.com,id,1,role,admin\n")
}

func Test_hget_requires_key(t *testing.T) {
	testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t)
	defer connection.Close()

	reader := bufio.NewReader(connection)

	testutil.SendToServer(t, connection, "HGET\n")
	testutil.MustReadLine(t, reader, "-wrong number of arguments for HGET. Expecting key\n")

	testutil.SendToServer(t, connection, "HGET k extra\n")
	testutil.MustReadLine(t, reader, "-wrong number of arguments for HGET. Expecting key\n")
}

func Test_hget_returns_empty_when_hash_missing(t *testing.T) {
	testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t)
	defer connection.Close()

	reader := bufio.NewReader(connection)

	testutil.SendToServer(t, connection, "HGET nosuch\n")
	testutil.MustReadLine(t, reader, "+\n")
}

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

func Test_hdel_requires_key_and_field(t *testing.T) {
	testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t)
	defer connection.Close()

	reader := bufio.NewReader(connection)

	testutil.SendToServer(t, connection, "HDEL\n")
	testutil.MustReadLine(t, reader, "-wrong number of arguments for HDEL. Expecting key field\n")

	testutil.SendToServer(t, connection, "HDEL onlykey\n")
	testutil.MustReadLine(t, reader, "-wrong number of arguments for HDEL. Expecting key field\n")

	testutil.SendToServer(t, connection, "HDEL onlykey f extra\n")
	testutil.MustReadLine(t, reader, "-wrong number of arguments for HDEL. Expecting key field\n")
}

func Test_hdel_ok_when_hash_or_field_missing(t *testing.T) {
	testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t)
	defer connection.Close()

	reader := bufio.NewReader(connection)

	testutil.SendToServer(t, connection, "HDEL nosuch f\n")
	testutil.MustReadLine(t, reader, "+OK\n")

	testutil.SendToServer(t, connection, "HSET h a 1 b 2\n")
	testutil.MustReadLine(t, reader, "+OK\n")
	testutil.SendToServer(t, connection, "HDEL h nosuchfield\n")
	testutil.MustReadLine(t, reader, "+OK\n")
	testutil.SendToServer(t, connection, "HGET h\n")
	testutil.MustReadLine(t, reader, "+a,1,b,2\n")
}

func Test_hdel_removes_field_and_drops_empty_hash(t *testing.T) {
	testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t)
	defer connection.Close()

	reader := bufio.NewReader(connection)

	testutil.SendToServer(t, connection, "HSET h x 1 y 2\n")
	testutil.MustReadLine(t, reader, "+OK\n")
	testutil.SendToServer(t, connection, "HDEL h x\n")
	testutil.MustReadLine(t, reader, "+OK\n")
	testutil.SendToServer(t, connection, "HGET h\n")
	testutil.MustReadLine(t, reader, "+y,2\n")

	testutil.SendToServer(t, connection, "HDEL h y\n")
	testutil.MustReadLine(t, reader, "+OK\n")
	testutil.SendToServer(t, connection, "HGET h\n")
	testutil.MustReadLine(t, reader, "+\n")

	testutil.SendToServer(t, connection, "SET h after\n")
	testutil.MustReadLine(t, reader, "+OK\n")
}

func Test_hdel_is_noop_when_key_is_string_or_list(t *testing.T) {
	testutil.StartTestServer(t, 4)
	connection := testutil.ConnectToServer(t)
	defer connection.Close()

	reader := bufio.NewReader(connection)

	testutil.SendToServer(t, connection, "SET mykey 1\n")
	testutil.MustReadLine(t, reader, "+OK\n")
	testutil.SendToServer(t, connection, "HDEL mykey f\n")
	testutil.MustReadLine(t, reader, "+OK\n")
	testutil.SendToServer(t, connection, "GET mykey\n")
	testutil.MustReadLine(t, reader, "+1\n")

	testutil.SendToServer(t, connection, "LPUSH jobs task\n")
	testutil.MustReadLine(t, reader, "+OK\n")
	testutil.SendToServer(t, connection, "HDEL jobs f\n")
	testutil.MustReadLine(t, reader, "+OK\n")
	testutil.SendToServer(t, connection, "LGET jobs\n")
	testutil.MustReadLine(t, reader, "+task\n")
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
