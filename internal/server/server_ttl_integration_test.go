package server_test

import (
	"bufio"
	"testing"
	"time"

	"go-memory-db/internal/server/testutil"
)

func Test_expire_missing_key_is_noop(t *testing.T) {
	testutil.StartTestServer(t, 4)
	c := testutil.ConnectToServer(t)
	defer c.Close()
	r := bufio.NewReader(c)

	testutil.SendToServer(t, c, "EXPIRE missing 10\n")
	testutil.MustReadLine(t, r, "+0\n")
}

func Test_expire_rejects_non_integer_seconds(t *testing.T) {
	testutil.StartTestServer(t, 4)
	c := testutil.ConnectToServer(t)
	defer c.Close()
	r := bufio.NewReader(c)

	testutil.SendToServer(t, c, "SET k v\n")
	testutil.MustReadLine(t, r, "+OK\n")
	testutil.SendToServer(t, c, "EXPIRE k 1.5\n")
	testutil.MustReadLine(t, r, "-invalid integer\n")
}

func Test_expire_rejects_zero_or_negative_seconds(t *testing.T) {
	testutil.StartTestServer(t, 4)
	c := testutil.ConnectToServer(t)
	defer c.Close()
	r := bufio.NewReader(c)

	testutil.SendToServer(t, c, "SET k v\n")
	testutil.MustReadLine(t, r, "+OK\n")
	testutil.SendToServer(t, c, "EXPIRE k 0\n")
	testutil.MustReadLine(t, r, "-expiry too low\n")
	testutil.SendToServer(t, c, "EXPIRE k -1\n")
	testutil.MustReadLine(t, r, "-expiry too low\n")
}

func Test_ttl_uses_expiry_map_only(t *testing.T) {
	testutil.StartTestServer(t, 4)
	c := testutil.ConnectToServer(t)
	defer c.Close()
	r := bufio.NewReader(c)

	testutil.SendToServer(t, c, "TTL missing\n")
	testutil.MustReadLine(t, r, "+-1\n")
	testutil.SendToServer(t, c, "SET k v\n")
	testutil.MustReadLine(t, r, "+OK\n")
	testutil.SendToServer(t, c, "TTL k\n")
	testutil.MustReadLine(t, r, "+-1\n")
}

func Test_expired_key_is_eventually_removed_by_sweeper(t *testing.T) {
	testutil.StartTestServer(t, 4)
	c := testutil.ConnectToServer(t)
	defer c.Close()
	r := bufio.NewReader(c)

	testutil.SendToServer(t, c, "SET k v\n")
	testutil.MustReadLine(t, r, "+OK\n")
	testutil.SendToServer(t, c, "EXPIRE k 1\n")
	testutil.MustReadLine(t, r, "+OK\n")
	testutil.SendToServer(t, c, "GET k\n")
	testutil.MustReadLine(t, r, "+v\n")

	time.Sleep(1500 * time.Millisecond)

	testutil.SendToServer(t, c, "GET k\n")
	testutil.MustReadLine(t, r, "-key not found\n")
	testutil.SendToServer(t, c, "TTL k\n")
	testutil.MustReadLine(t, r, "+-1\n")
}

