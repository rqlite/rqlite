package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"runtime/pprof"
	"time"

	"github.com/otoolep/raft"
	"github.com/otoolep/rqlite/command"
	"github.com/otoolep/rqlite/server"

	log "code.google.com/p/log4go"
)

var host string
var port int
var join string
var dbfile string
var cpuprofile string

func init() {
	flag.StringVar(&host, "h", "localhost", "hostname")
	flag.IntVar(&port, "p", 4001, "port")
	flag.StringVar(&join, "join", "", "host:port of leader to join")
	flag.StringVar(&dbfile, "dbfile", "db.sqlite", "sqlite filename")
	flag.StringVar(&cpuprofile, "cpuprofile", "", "write CPU profile to file")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [arguments] <data-path> \n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	// Set up profiling, if requested.
	if cpuprofile != "" {
		log.Info("Profiling enabled")
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Error("Unable to create path: %w", err)
		}
		defer f.Close()

		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	// Raft requires randomness.
	rand.Seed(time.Now().UnixNano())

	// Setup commands.
	raft.RegisterCommand(&command.WriteCommand{})
	raft.RegisterCommand(&command.TransactionWriteCommandSet{})

	// Set the data directory.
	if flag.NArg() == 0 {
		flag.Usage()
		log.Error("Data path argument required")
	}
	path := flag.Arg(0)
	if err := os.MkdirAll(path, 0744); err != nil {
		log.Error("Unable to create path: %v", err)
	}

	s := server.New(path, dbfile, host, port)
	go func() {
		log.Error(s.ListenAndServe(join))
	}()

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	log.Info("rqlite server stopped")
}
