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
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/pprof"
	"strings"

	sql "github.com/otoolep/rqlite/db"
	httpd "github.com/otoolep/rqlite/http"
	"github.com/otoolep/rqlite/store"
)

const sqliteDSN = "db.sqlite"

// These variables are populated via the Go linker.
var (
	version = "2.1"
	commit  string
	branch  string
)

var httpAddr string
var raftAddr string
var joinAddr string
var expvar bool
var dsn string
var inMem bool
var disRedirect bool
var showVersion bool
var cpuprofile string

const desc = `rqlite is a distributed system that provides a replicated SQLite database.`

func init() {
	flag.StringVar(&httpAddr, "http", "localhost:4001", "HTTP query server bind address")
	flag.StringVar(&raftAddr, "raft", "localhost:4002", "Raft communication bind address")
	flag.StringVar(&joinAddr, "join", "", "host:port of leader to join")
	flag.BoolVar(&expvar, "expvar", true, "Serve expvar data on HTTP server")
	flag.StringVar(&dsn, "dsn", "", `SQLite DSN parameters. E.g. "cache=shared&mode=memory"`)
	flag.BoolVar(&inMem, "mem", false, "Use an in-memory database")
	flag.BoolVar(&disRedirect, "noredir", true, "Disable leader-redirect")
	flag.BoolVar(&showVersion, "version", false, "Show version information and exit")
	flag.StringVar(&cpuprofile, "cpuprofile", "", "Write CPU profile to file")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "\n%s\n\n", desc)
		fmt.Fprintf(os.Stderr, "Usage: %s [arguments] <data directory>\n", os.Args[0])
		flag.PrintDefaults()
	}

	// If commit, branch, or build time are not set, make that clear.
	if commit == "" {
		commit = "unknown"
	}
	if branch == "" {
		branch = "unknown"
	}

}

func main() {
	flag.Parse()

	if showVersion {
		fmt.Printf("rqlited version %s (commit %s)\n", version, commit)
		os.Exit(0)
	}

	// Ensure the data path is set.
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(1)
	}

	dataPath := flag.Arg(0)

	// Configure logging and pump out initial message.
	log.SetFlags(log.LstdFlags)
	log.SetPrefix("[rqlited] ")
	log.Printf("rqlited starting, version %s, commit %s, branch %s", version, commit, branch)

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

	// Create and open the store.
	dataPath, err := filepath.Abs(dataPath)
	if err != nil {
		log.Fatalf("failed to determine absolute data path: %s", err.Error())
	}
	dbConf := sql.NewConfig()
	dbConf.DSN = dsn
	dbConf.Memory = inMem
	store := store.New(dbConf, dataPath, raftAddr)
	if err := store.Open(joinAddr == ""); err != nil {
		log.Fatalf("failed to open store: %s", err.Error())
	}

	// If join was specified, make the join request.
	if joinAddr != "" {
		if err := join(joinAddr, raftAddr); err != nil {
			log.Fatalf("failed to join node at %s: %s", joinAddr, err.Error())
		}
	}

	// Create the HTTP query server.
	s := httpd.New(httpAddr, store)
	s.DisableRedirect = disRedirect
	s.Expvar = expvar
	s.Version = version
	s.Commit = commit
	s.Branch = branch
	if err := s.Start(); err != nil {
		log.Fatalf("failed to start HTTP server: %s", err.Error())

	}

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	if err := store.Close(); err != nil {
		log.Printf("failed to close store: %s", err.Error())
	}
	log.Println("rqlite server stopped")
}

func join(joinAddr, raftAddr string) error {
	b, err := json.Marshal(map[string]string{"addr": raftAddr})
	if err != nil {
		return err
	}

	// Check for protocol scheme, and insert default if necessary.
	fullAddr := fmt.Sprintf("%s/join", joinAddr)
	if !strings.HasPrefix(joinAddr, "http://") && !strings.HasPrefix(joinAddr, "https://") {
		fullAddr = fmt.Sprintf("http://%s", joinAddr)
	}

	resp, err := http.Post(fullAddr, "application-type/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}
