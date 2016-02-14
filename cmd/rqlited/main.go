/*
rqlite -- a replicated SQLite database.

rqlite is a distributed system that provides a replicated SQLite database.
rqlite is written in Go and uses Raft to achieve consensus across all the
instances of the SQLite databases. rqlite ensures that every change made to
the database is made to a majority of underlying SQLite files, or none-at-all.
*/

package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"time"

	httpd "github.com/otoolep/rqlite/http"
	"github.com/otoolep/rqlite/store"
)

var httpAddr string
var tcpAddr string
var raftAddr string
var joinAddr string
var cpuprofile string
var disableReporting bool

func init() {
	flag.StringVar(&httpAddr, "http", "localhost:4001", "HTTP query server bind address")
	flag.StringVar(&tcpAddr, "tcp", "localhost:4002", "TCP query server bind address")
	flag.StringVar(&raftAddr, "raft", "localhost:4003", "Raft communication bind address")
	flag.StringVar(&joinAddr, "join", "", "host:port of leader to join")
	flag.StringVar(&cpuprofile, "cpuprofile", "", "write CPU profile to file")
	flag.BoolVar(&disableReporting, "noreport", false, "Disable anonymised launch reporting")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [arguments] <data-path> \n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	// Ensure the data path was set.
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(1)
	}

	dataPath := flag.Arg(0)

	// Set up profiling, if requested.
	if cpuprofile != "" {
		log.Println("profiling enabled")
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Printf("unable to create path: %s", err.Error())
		}
		defer f.Close()

		err = pprof.StartCPUProfile(f)
		if err != nil {
			log.Printf("unable to start CPU Profile: %s", err.Error())
		}

		defer pprof.StopCPUProfile()
	}

	// Create the store.
	store := store.New(dataPath, raftAddr)

	// Create the HTTP query server.
	s := httpd.New(httpAddr, store)
	if err := s.Start(); err != nil {
		log.Printf("failed to start HTTP server: %s", err.Error())

	}

	if !disableReporting {
		reportLaunch()
	}

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	log.Println("rqlite server stopped")
}

func reportLaunch() {
	json := fmt.Sprintf(`{"os": "%s", "arch": "%s", "app": "rqlite"}`, runtime.GOOS, runtime.GOARCH)
	data := bytes.NewBufferString(json)
	client := http.Client{Timeout: time.Duration(5 * time.Second)}
	go func() {
		_, err := client.Post("https://logs-01.loggly.com/inputs/8a0edd84-92ba-46e4-ada8-c529d0f105af/tag/reporting/",
			"application/json", data)
		if err != nil {
			log.Printf("Report launch failed: %s", err.Error())
		}
	}()
}
