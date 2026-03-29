package server

import (
	"fmt"
	"io"
)

func printSuccess(w io.Writer, message string) {
	_, _ = io.WriteString(w, fmt.Sprintf("+%s\n", message))
}

func printError(w io.Writer, message string) {
	_, _ = io.WriteString(w, fmt.Sprintf("-%s\n", message))
}
