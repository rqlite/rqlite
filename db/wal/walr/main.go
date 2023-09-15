package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/rqlite/rqlite/db/wal"
)

const name = `walr`
const desc = `walr is a tool for displaying information about WAL files.`

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "\n%s\n\n", desc)
		fmt.Fprintf(os.Stderr, "Usage: %s <Path to WAL file>\n", name)
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(1)
	}
	walPath := flag.Args()[0]

	walFD, err := os.Open(walPath)
	if err != nil {
		fmt.Println("failed to open WAL file:", err)
		os.Exit(1)
	}

	r := wal.NewReader(walFD)
	if err := r.ReadHeader(); err != nil {
		fmt.Println("failed to read WAL header:", err)
		os.Exit(1)
	}
	fmt.Println("WAL page size:", r.PageSize())

	nFrames := 0
	nCommits := 0
	uniquePgs := make(map[uint32]struct{})
	buf := make([]byte, r.PageSize())
	for {
		pgno, commit, err := r.ReadFrame(buf)
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("failed to read WAL frame:", err)
			os.Exit(1)
		}
		uniquePgs[pgno] = struct{}{}
		nFrames++

		if commit != 0 {
			nCommits++
		}
	}

	fmt.Printf("Found %d WAL frames, %d unique pages, %d commit frames\n", nFrames, len(uniquePgs), nCommits)
}
