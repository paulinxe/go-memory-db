package main

import (
	"fmt"
	"io"
	"net"
)

func printSuccess(conn net.Conn, message string) {
	io.WriteString(conn, fmt.Sprintf("+%s\n", message))
}

func printError(conn net.Conn, message string) {
	io.WriteString(conn, fmt.Sprintf("-%s\n", message))
}
