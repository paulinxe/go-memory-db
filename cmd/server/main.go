package main

import (
	"flag"
	"fmt"
	"os"

	"go-memory-db/internal/server"
)

func main() {
	port := flag.Int("port", 6379, "TCP listen port")
	maxConn := flag.Int("max-connections", 1, "maximum concurrent clients")
	flag.Parse()

	srv := server.NewServer(*port, *maxConn)
	if err := srv.Serve(); err != nil {
		fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		os.Exit(1)
	}
}
