package server_test

import (
	"bufio"
	"strings"
	"testing"

	"go-memory-db/internal/server/testutil"
)

func Test_we_cannot_use_an_unknown_namespace(t *testing.T) {
	testutil.StartTestServer(t, 4)
	c := testutil.ConnectToServer(t)
	defer c.Close()
	r := bufio.NewReader(c)

	testutil.SendToServer(t, c, "USE no_such_ns\n")
	testutil.MustReadLine(t, r, "-namespace does not exist\n")
}

func Test_we_cannot_create_a_namespace_with_a_too_long_name(t *testing.T) {
	testutil.StartTestServer(t, 4)
	c := testutil.ConnectToServer(t)
	defer c.Close()
	r := bufio.NewReader(c)

	longName := strings.Repeat("a", 65)
	testutil.SendToServer(t, c, "CNAMESPACE "+longName+"\n")
	testutil.MustReadLine(t, r, "-namespace name too long\n")
	testutil.SendToServer(t, c, "USE "+longName+"\n")
	testutil.MustReadLine(t, r, "-namespace does not exist\n")
}

func Test_we_get_an_error_when_using_the_namespace_commands_wrongly(t *testing.T) {
	testutil.StartTestServer(t, 4)
	c := testutil.ConnectToServer(t)
	defer c.Close()
	r := bufio.NewReader(c)

	testutil.SendToServer(t, c, "USE\n")
	testutil.MustReadLine(t, r, "-wrong number of arguments for USE. Expecting name\n")
	testutil.SendToServer(t, c, "CNAMESPACE\n")
	testutil.MustReadLine(t, r, "-wrong number of arguments for CNAMESPACE. Expecting name\n")
	testutil.SendToServer(t, c, "DNAMESPACE\n")
	testutil.MustReadLine(t, r, "-wrong number of arguments for DNAMESPACE. Expecting name\n")
}

func Test_we_can_switch_between_namespaces(t *testing.T) {
	testutil.StartTestServer(t, 4)
	c := testutil.ConnectToServer(t)
	defer c.Close()
	r := bufio.NewReader(c)

	testutil.SendToServer(t, c, "CNAMESPACE app\n")
	testutil.MustReadLine(t, r, "+OK\n")
	testutil.SendToServer(t, c, "SET k default-val\n")
	testutil.MustReadLine(t, r, "+OK\n")
	testutil.SendToServer(t, c, "USE app\n")
	testutil.MustReadLine(t, r, "+OK\n")
	testutil.SendToServer(t, c, "SET k app-val\n")
	testutil.MustReadLine(t, r, "+OK\n")
	testutil.SendToServer(t, c, "GET k\n")
	testutil.MustReadLine(t, r, "+app-val\n")
	testutil.SendToServer(t, c, "USE default\n")
	testutil.MustReadLine(t, r, "+OK\n")
	testutil.SendToServer(t, c, "GET k\n")
	testutil.MustReadLine(t, r, "+default-val\n")
}

func Test_a_new_connection_lands_on_the_default_namespace(t *testing.T) {
	testutil.StartTestServer(t, 4)
	c := testutil.ConnectToServer(t)
	defer c.Close()
	r := bufio.NewReader(c)

	testutil.SendToServer(t, c, "CNAMESPACE app\n")
	testutil.MustReadLine(t, r, "+OK\n")
	testutil.SendToServer(t, c, "SET x only-on-default\n")
	testutil.MustReadLine(t, r, "+OK\n")
	testutil.SendToServer(t, c, "USE app\n")
	testutil.MustReadLine(t, r, "+OK\n")
	testutil.SendToServer(t, c, "GET x\n")
	testutil.MustReadLine(t, r, "-key not found\n")
	testutil.SendToServer(t, c, "USE default\n")
	testutil.MustReadLine(t, r, "+OK\n")
	testutil.SendToServer(t, c, "GET x\n")
	testutil.MustReadLine(t, r, "+only-on-default\n")
}

func Test_we_dont_get_an_error_when_creating_a_namespace_multiple_times(t *testing.T) {
	testutil.StartTestServer(t, 4)
	c := testutil.ConnectToServer(t)
	defer c.Close()
	r := bufio.NewReader(c)

	testutil.SendToServer(t, c, "CNAMESPACE app\n")
	testutil.MustReadLine(t, r, "+OK\n")
	testutil.SendToServer(t, c, "USE app\n")
	testutil.MustReadLine(t, r, "+OK\n")
	testutil.SendToServer(t, c, "SET counter 1\n")
	testutil.MustReadLine(t, r, "+OK\n")
	testutil.SendToServer(t, c, "CNAMESPACE app\n")
	testutil.MustReadLine(t, r, "+OK\n")
	testutil.SendToServer(t, c, "GET counter\n")
	testutil.MustReadLine(t, r, "+1\n")
}

func Test_we_dont_get_an_error_when_creating_the_default_namespace_multiple_times(t *testing.T) {
	testutil.StartTestServer(t, 4)
	c := testutil.ConnectToServer(t)
	defer c.Close()
	r := bufio.NewReader(c)

	testutil.SendToServer(t, c, "SET k v\n")
	testutil.MustReadLine(t, r, "+OK\n")
	testutil.SendToServer(t, c, "CNAMESPACE default\n")
	testutil.MustReadLine(t, r, "+OK\n")
	testutil.SendToServer(t, c, "GET k\n")
	testutil.MustReadLine(t, r, "+v\n")
}

func Test_namespace_isolation_across_connections(t *testing.T) {
	testutil.StartTestServer(t, 4)
	connection1 := testutil.ConnectToServer(t)
	defer connection1.Close()
	connection2 := testutil.ConnectToServer(t)
	defer connection2.Close()
	ra := bufio.NewReader(connection1)
	rb := bufio.NewReader(connection2)

	testutil.SendToServer(t, connection1, "CNAMESPACE ns_a\n")
	testutil.MustReadLine(t, ra, "+OK\n")
	testutil.SendToServer(t, connection1, "USE ns_a\n")
	testutil.MustReadLine(t, ra, "+OK\n")
	testutil.SendToServer(t, connection1, "SET shared 1\n")
	testutil.MustReadLine(t, ra, "+OK\n")

	testutil.SendToServer(t, connection2, "CNAMESPACE ns_b\n")
	testutil.MustReadLine(t, rb, "+OK\n")
	testutil.SendToServer(t, connection2, "USE ns_b\n")
	testutil.MustReadLine(t, rb, "+OK\n")
	testutil.SendToServer(t, connection2, "SET shared 2\n")
	testutil.MustReadLine(t, rb, "+OK\n")

	testutil.SendToServer(t, connection1, "GET shared\n")
	testutil.MustReadLine(t, ra, "+1\n")
	testutil.SendToServer(t, connection2, "GET shared\n")
	testutil.MustReadLine(t, rb, "+2\n")
}

func Test_we_can_delete_a_namespace_and_then_use_fails(t *testing.T) {
	testutil.StartTestServer(t, 4)
	c := testutil.ConnectToServer(t)
	defer c.Close()
	r := bufio.NewReader(c)

	testutil.SendToServer(t, c, "CNAMESPACE db1\n")
	testutil.MustReadLine(t, r, "+OK\n")
	testutil.SendToServer(t, c, "USE db1\n")
	testutil.MustReadLine(t, r, "+OK\n")
	testutil.SendToServer(t, c, "DNAMESPACE db1\n")
	testutil.MustReadLine(t, r, "+OK\n")
	testutil.SendToServer(t, c, "USE db1\n")
	testutil.MustReadLine(t, r, "-namespace does not exist\n")
}

func Test_we_cannot_delete_default_namespace(t *testing.T) {
	testutil.StartTestServer(t, 4)
	c := testutil.ConnectToServer(t)
	defer c.Close()
	r := bufio.NewReader(c)

	testutil.SendToServer(t, c, "DNAMESPACE default\n")
	testutil.MustReadLine(t, r, "-cannot delete default namespace\n")
}

func Test_deleting_current_namespace_blocks_data_commands_until_use(t *testing.T) {
	testutil.StartTestServer(t, 4)
	c := testutil.ConnectToServer(t)
	defer c.Close()
	r := bufio.NewReader(c)

	testutil.SendToServer(t, c, "CNAMESPACE db1\n")
	testutil.MustReadLine(t, r, "+OK\n")
	testutil.SendToServer(t, c, "USE db1\n")
	testutil.MustReadLine(t, r, "+OK\n")
	testutil.SendToServer(t, c, "DNAMESPACE db1\n")
	testutil.MustReadLine(t, r, "+OK\n")

	testutil.SendToServer(t, c, "SET a b\n")
	testutil.MustReadLine(t, r, "-namespace deleted\n")

	testutil.SendToServer(t, c, "USE default\n")
	testutil.MustReadLine(t, r, "+OK\n")
	testutil.SendToServer(t, c, "SET a b\n")
	testutil.MustReadLine(t, r, "+OK\n")
}

func Test_deleting_namespace_invalidates_other_attached_connections(t *testing.T) {
	testutil.StartTestServer(t, 4)
	connection1 := testutil.ConnectToServer(t)
	defer connection1.Close()
	connection2 := testutil.ConnectToServer(t)
	defer connection2.Close()
	ra := bufio.NewReader(connection1)
	rb := bufio.NewReader(connection2)

	testutil.SendToServer(t, connection1, "CNAMESPACE db1\n")
	testutil.MustReadLine(t, ra, "+OK\n")
	testutil.SendToServer(t, connection1, "USE db1\n")
	testutil.MustReadLine(t, ra, "+OK\n")

	testutil.SendToServer(t, connection2, "DNAMESPACE db1\n")
	testutil.MustReadLine(t, rb, "+OK\n")

	testutil.SendToServer(t, connection1, "SET x 1\n")
	testutil.MustReadLine(t, ra, "-namespace deleted\n")
}
