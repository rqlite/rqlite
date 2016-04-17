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
	"crypto/tls"
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

	"github.com/otoolep/rqlite/auth"
	sql "github.com/otoolep/rqlite/db"
	httpd "github.com/otoolep/rqlite/http"
	"github.com/otoolep/rqlite/store"
)

const sqliteDSN = "db.sqlite"

const logo = `
            _ _ _           _
           | (_) |         | |
  _ __ __ _| |_| |_ ___  __| |
 | '__/ _  | | | __/ _ \/ _  |
 | | | (_| | | | ||  __/ (_| |
 |_|  \__, |_|_|\__\___|\__,_|
         | |
         |_|
`

// These variables are populated via the Go linker.
var (
	version = "2"
	commit  string
	branch  string
)

var httpAddr string
var authFile string
var x509Cert string
var x509Key string
var raftAddr string
var joinAddr string
var noVerify bool
var expvar bool
var dsn string
var inMem bool
var disRedirect bool
var showVersion bool
var cpuprofile string

const desc = `rqlite is a distributed system that provides a replicated SQLite database.`

func init() {
	flag.StringVar(&httpAddr, "http", "localhost:4001", "HTTP query server address. Set X.509 cert and key for HTTPS.")
	flag.StringVar(&x509Cert, "x509cert", "", "Path to X.509 certificate")
	flag.StringVar(&x509Key, "x509key", "", "Path to X.509 private key for certificate")
	flag.StringVar(&authFile, "auth", "", "Path to authentication and authorization file. If not set, not enabled.")
	flag.StringVar(&raftAddr, "raft", "localhost:4002", "Raft communication bind address")
	flag.StringVar(&joinAddr, "join", "", "protocol://host:port of leader to join")
	flag.BoolVar(&noVerify, "noverify", false, "Skip verification of any HTTPS cert when joining")
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

	// Display logo.
	fmt.Println(logo)

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
			log.Fatalf("unable to start CPU Profile: %s", err.Error())
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
		if err := join(joinAddr, noVerify, raftAddr); err != nil {
			log.Fatalf("failed to join node at %s: %s", joinAddr, err.Error())
		}
		log.Println("successfully joined node at", joinAddr)
	}

	// Load authentication information, if supplied.
	var credentialStore *auth.CredentialsStore
	if authFile != "" {
		f, err := os.Open(authFile)
		if err != nil {
			log.Fatalf("failed to open authentication file %s: %s", authFile, err.Error())
		}
		credentialStore = auth.NewCredentialsStore()
		if err := credentialStore.Load(f); err != nil {
			log.Fatalf("failed to load authentication file: %s", err.Error())
		}
	}

	// Create the HTTP query server.
	s := httpd.New(httpAddr, store, credentialStore)
	s.CertFile = x509Cert
	s.KeyFile = x509Key
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

func join(joinAddr string, skipVerify bool, raftAddr string) error {
	b, err := json.Marshal(map[string]string{"addr": raftAddr})
	if err != nil {
		return err
	}

	// Check for protocol scheme, and insert default if necessary.
	fullAddr := fmt.Sprintf("%s/join", joinAddr)
	if !strings.HasPrefix(joinAddr, "http://") && !strings.HasPrefix(joinAddr, "https://") {
		fullAddr = fmt.Sprintf("http://%s", joinAddr)
	}

	// Enable skipVerify as requested.
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: skipVerify},
	}
	client := &http.Client{Transport: tr}

	// Attempt to join.
	resp, err := client.Post(fullAddr, "application-type/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("failed to join, node returned: %s", resp.Status)
	}
	defer resp.Body.Close()

	return nil
}
