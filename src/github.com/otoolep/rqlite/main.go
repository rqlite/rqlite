package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/otoolep/raft"
	"github.com/otoolep/rqlite/command"
	"github.com/otoolep/rqlite/server"
)

var host string
var port int
var join string
var dbfile string

func init() {
	flag.StringVar(&host, "h", "localhost", "hostname")
	flag.IntVar(&port, "p", 4001, "port")
	flag.StringVar(&join, "join", "", "host:port of leader to join")
	flag.StringVar(&dbfile, "dbfile", "db.sqlite", "sqlite filename")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [arguments] <data-path> \n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	rand.Seed(time.Now().UnixNano())

	// Setup commands.
	raft.RegisterCommand(&command.WriteCommand{})

	// Set the data directory.
	if flag.NArg() == 0 {
		flag.Usage()
		log.Fatal("Data path argument required")
	}
	path := flag.Arg(0)
	if err := os.MkdirAll(path, 0744); err != nil {
		log.Fatalf("Unable to create path: %v", err)
	}

	s := server.New(path, dbfile, host, port)
	log.Fatal(s.ListenAndServe(join))
}
