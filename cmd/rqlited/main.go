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
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/pprof"
	"time"

	"github.com/rqlite/rqlite/auth"
	"github.com/rqlite/rqlite/cluster"
	httpd "github.com/rqlite/rqlite/http"
	"github.com/rqlite/rqlite/store"
	"github.com/rqlite/rqlite/tcp"
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
	version   = "3"
	commit    string
	branch    string
	buildtime string
)

const (
	muxRaftHeader = 1 // Raft consensus communications
	muxMetaHeader = 2 // Cluster meta communications
)

const (
	publishPeerDelay   = 1 * time.Second
	publishPeerTimeout = 30 * time.Second
)

var httpAddr string
var httpAdv string
var authFile string
var x509Cert string
var x509Key string
var raftAddr string
var raftAdv string
var joinAddr string
var noVerify bool
var expvar bool
var dsn string
var onDisk bool
var noVerifySelect bool
var showVersion bool
var cpuprofile string

const desc = `rqlite is a distributed system that provides a replicated SQLite database.`

func init() {
	flag.StringVar(&httpAddr, "http", "localhost:4001", "HTTP query server address. Set X.509 cert and key for HTTPS.")
	flag.StringVar(&httpAdv, "httpadv", "", "HTTP redirection advertise address. If not set, same as query server.")
	flag.StringVar(&x509Cert, "x509cert", "", "Path to X.509 certificate")
	flag.StringVar(&x509Key, "x509key", "", "Path to X.509 private key for certificate")
	flag.StringVar(&authFile, "auth", "", "Path to authentication and authorization file. If not set, not enabled.")
	flag.StringVar(&raftAddr, "raft", "localhost:4002", "Raft communication bind address")
	flag.StringVar(&raftAdv, "raftadv", "", "Raft advertise address. If not set, same as bind")
	flag.StringVar(&joinAddr, "join", "", "protocol://host:port of leader to join")
	flag.BoolVar(&noVerify, "noverify", false, "Skip verification of any HTTPS cert when joining")
	flag.BoolVar(&expvar, "expvar", true, "Serve expvar data on HTTP server")
	flag.StringVar(&dsn, "dsn", "", `SQLite DSN parameters. E.g. "cache=shared&mode=memory"`)
	flag.BoolVar(&onDisk, "ondisk", false, "Use an on-disk SQLite database")
	flag.BoolVar(&noVerifySelect, "nosel", false, "Don't verify that all queries begin with SELECT")
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
	if buildtime == "" {
		buildtime = "unknown"
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

	// Set up TCP communication between nodes.
	ln, err := net.Listen("tcp", raftAddr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %s", raftAddr, err.Error())
	}
	var adv net.Addr
	if raftAdv != "" {
		adv, err = net.ResolveTCPAddr("tcp", raftAdv)
		if err != nil {
			log.Fatalf("failed to resolve advertise address %s: %s", raftAdv, err.Error())
		}
	}
	mux := tcp.NewMux(ln, adv)
	go mux.Serve()

	// Start up mux and get transports for cluster.
	raftTn := mux.Listen(muxRaftHeader)

	// Create and open the store.
	dataPath, err = filepath.Abs(dataPath)
	if err != nil {
		log.Fatalf("failed to determine absolute data path: %s", err.Error())
	}
	dbConf := store.NewDBConfig(dsn, !onDisk)
	store := store.New(dbConf, dataPath, raftTn)
	if err := store.Open(joinAddr == ""); err != nil {
		log.Fatalf("failed to open store: %s", err.Error())
	}

	// Create and configure cluster service.
	tn := mux.Listen(muxMetaHeader)
	cs := cluster.NewService(tn, store)
	if err := cs.Open(); err != nil {
		log.Fatalf("failed to open cluster service: %s", err.Error())
	}

	// If join was specified, make the join request.
	if joinAddr != "" {
		if !store.JoinRequired() {
			log.Println("node is already member of cluster, ignoring join request")
		} else {
			if err := join(joinAddr, noVerify, raftAddr, raftAdv); err != nil {
				log.Fatalf("failed to join node at %s: %s", joinAddr, err.Error())
			}
			log.Println("successfully joined node at", joinAddr)
		}
	}

	// Publish to the cluster the mapping between this Raft address and API address.
	// The Raft layer broadcasts the resolved address, so use that as the key. But
	// only set different HTTP advertise address if set.
	apiAdv := httpAddr
	if httpAdv != "" {
		apiAdv = httpAdv
	}

	if err := publishAPIAddr(cs, raftTn.Addr().String(), apiAdv, publishPeerTimeout); err != nil {
		log.Fatalf("failed to set peer for %s to %s: %s", raftAddr, httpAddr, err.Error())
	}
	log.Printf("set peer for %s to %s", raftTn.Addr().String(), apiAdv)

	// Create HTTP server and load authentication information, if supplied.
	var s *httpd.Service
	if authFile != "" {
		f, err := os.Open(authFile)
		if err != nil {
			log.Fatalf("failed to open authentication file %s: %s", authFile, err.Error())
		}
		credentialStore := auth.NewCredentialsStore()
		if err := credentialStore.Load(f); err != nil {
			log.Fatalf("failed to load authentication file: %s", err.Error())
		}
		s = httpd.New(httpAddr, store, credentialStore)
	} else {
		s = httpd.New(httpAddr, store, nil)
	}

	s.CertFile = x509Cert
	s.KeyFile = x509Key
	s.NoVerifySelect = noVerifySelect
	s.Expvar = expvar
	s.BuildInfo = map[string]interface{}{
		"commit":     commit,
		"branch":     branch,
		"version":    version,
		"build_time": buildtime,
	}
	if err := s.Start(); err != nil {
		log.Fatalf("failed to start HTTP server: %s", err.Error())
	}

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	if err := store.Close(true); err != nil {
		log.Printf("failed to close store: %s", err.Error())
	}
	log.Println("rqlite server stopped")
}

func join(joinAddr string, skipVerify bool, raftAddr, raftAdv string) error {
	addr := raftAddr
	if raftAdv != "" {
		addr = raftAdv
	}
	b, err := json.Marshal(map[string]string{"addr": addr})
	if err != nil {
		return err
	}

	// Check for protocol scheme, and insert default if necessary.
	fullAddr := httpd.NormalizeAddr(fmt.Sprintf("%s/join", joinAddr))

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
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("failed to join, node returned: %s", resp.Status)
	}

	return nil
}

func publishAPIAddr(c *cluster.Service, raftAddr, apiAddr string, t time.Duration) error {
	tck := time.NewTicker(publishPeerDelay)
	defer tck.Stop()
	tmr := time.NewTimer(t)
	defer tmr.Stop()

	for {
		select {
		case <-tck.C:
			if err := c.SetPeer(raftAddr, apiAddr); err != nil {
				log.Printf("failed to set peer for %s to %s: %s (retrying)",
					raftAddr, apiAddr, err.Error())
				continue
			}
			return nil
		case <-tmr.C:
			return fmt.Errorf("set peer timeout expired")
		}
	}
}
