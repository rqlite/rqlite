package main

import (
	"fmt"
	"os"

	"github.com/rqlite/rqlite/v10/internal/rsum"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s <file> [file ...]\n", os.Args[0])
		os.Exit(1)
	}

	exitCode := 0
	for _, path := range os.Args[1:] {
		sum, err := rsum.CRC32(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %s\n", path, err)
			exitCode = 1
			continue
		}
		fmt.Printf("%08x\n", sum)
	}
	os.Exit(exitCode)
}
