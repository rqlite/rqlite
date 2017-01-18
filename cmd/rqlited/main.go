/*
rqlite -- replicating SQLite via the Raft consensus protocol..

rqlite is a distributed system that provides a replicated relational database,
using SQLite as the storage engine.

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
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
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
 | '__/ _  | | | __/ _ \/ _  |  The lightweight, distributed
 | | | (_| | | | ||  __/ (_| |  relational database.
 |_|  \__, |_|_|\__\___|\__,_|
         | |
         |_|
`

// These variables are populated via the Go linker.
var (
	version   = "3"
	commit    = "unknown"
	branch    = "unknown"
	buildtime = "unknown"
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
var pprofEnabled bool
var dsn string
var onDisk bool
var raftSnapThreshold uint64
var raftHeartbeatTimeout string
var raftApplyTimeout string
var showVersion bool
var cpuProfile string
var memProfile string

const desc = `rqlite is a distributed system that provides a replicated relational database.`

func init() {
	flag.StringVar(&httpAddr, "http", "localhost:4001", "HTTP query server address. For HTTPS, set X.509 cert and key.")
	flag.StringVar(&httpAdv, "httpadv", "", "HTTP redirection advertise address. If not set, same as query server.")
	flag.StringVar(&x509Cert, "x509cert", "", "Path to X.509 certificate")
	flag.StringVar(&x509Key, "x509key", "", "Path to X.509 private key for certificate")
	flag.StringVar(&authFile, "auth", "", "Path to authentication and authorization file. If not set, not enabled.")
	flag.StringVar(&raftAddr, "raft", "localhost:4002", "Raft communication bind address")
	flag.StringVar(&raftAdv, "raftadv", "", "Raft advertise address. If not set, same as bind")
	flag.StringVar(&joinAddr, "join", "", "Join a cluster via node at protocol://host:port")
	flag.BoolVar(&noVerify, "noverify", false, "Skip verification of remote HTTPS cert when joining cluster")
	flag.BoolVar(&expvar, "expvar", true, "Serve expvar data on HTTP server")
	flag.BoolVar(&pprofEnabled, "pprof", true, "Serve pprof data on HTTP server")
	flag.StringVar(&dsn, "dsn", "", `SQLite DSN parameters. E.g. "cache=shared&mode=memory"`)
	flag.BoolVar(&onDisk, "ondisk", false, "Use an on-disk SQLite database")
	flag.BoolVar(&showVersion, "version", false, "Show version information and exit")
	flag.StringVar(&raftHeartbeatTimeout, "rafttimeout", "1s", "Raft heartbeat timeout")
	flag.StringVar(&raftApplyTimeout, "raftapplytimeout", "10s", "Raft apply timeout")
	flag.Uint64Var(&raftSnapThreshold, "raftsnap", 8192, "Number of outstanding log entries that trigger snapshot")
	flag.StringVar(&cpuProfile, "cpuprofile", "", "Path to file for CPU profiling information")
	flag.StringVar(&memProfile, "memprofile", "", "Path to file for memory profiling information")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "\n%s\n\n", desc)
		fmt.Fprintf(os.Stderr, "Usage: %s [arguments] <data directory>\n", os.Args[0])
		flag.PrintDefaults()
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

	// Start requested profiling.
	startProfile(cpuProfile, memProfile)

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

	store := store.New(&store.StoreConfig{
		DBConf: dbConf,
		Dir:    dataPath,
		Tn:     raftTn,
	})
	if err := store.Open(joinAddr == ""); err != nil {
		log.Fatalf("failed to open store: %s", err.Error())
	}
	store.SnapshotThreshold = raftSnapThreshold
	store.HeartbeatTimeout, err = time.ParseDuration(raftHeartbeatTimeout)
	if err != nil {
		log.Fatalf("failed to parse Raft heartbeat timeout %s: %s", raftHeartbeatTimeout, err.Error())
	}
	store.ApplyTimeout, err = time.ParseDuration(raftApplyTimeout)
	if err != nil {
		log.Fatalf("failed to parse Raft apply timeout %s: %s", raftApplyTimeout, err.Error())
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
				log.Fatalf("failed to join cluster at %s: %s", joinAddr, err.Error())
			}
			log.Println("successfully joined cluster at", joinAddr)
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
	s.Expvar = expvar
	s.Pprof = pprofEnabled
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
	stopProfile()
	log.Println("rqlite server stopped")
}

func join(joinAddr string, skipVerify bool, raftAddr, raftAdv string) error {
	addr := raftAddr
	if raftAdv != "" {
		addr = raftAdv
	}

	// Join using IP address, as that is what Hashicorp Raft works in.
	resv, err := net.ResolveTCPAddr("tcp", addr)
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

	for {
		b, err := json.Marshal(map[string]string{"addr": resv.String()})
		if err != nil {
			return err
		}

		// Attempt to join.
		resp, err := client.Post(fullAddr, "application-type/json", bytes.NewReader(b))
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		b, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		switch resp.StatusCode {
		case http.StatusOK:
			return nil
		case http.StatusMovedPermanently:
			fullAddr = resp.Header.Get("location")
			if fullAddr == "" {
				return fmt.Errorf("failed to join, invalid redirect received")
			}
			log.Println("join request redirecting to", fullAddr)
			continue
		default:
			return fmt.Errorf("failed to join, node returned: %s: (%s)", resp.Status, string(b))
		}
	}
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

// prof stores the file locations of active profiles.
var prof struct {
	cpu *os.File
	mem *os.File
}

// startProfile initializes the CPU and memory profile, if specified.
func startProfile(cpuprofile, memprofile string) {
	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Fatalf("failed to create CPU profile file at %s: %s", cpuprofile, err.Error())
		}
		log.Printf("writing CPU profile to: %s\n", cpuprofile)
		prof.cpu = f
		pprof.StartCPUProfile(prof.cpu)
	}

	if memprofile != "" {
		f, err := os.Create(memprofile)
		if err != nil {
			log.Fatalf("failed to create memory profile file at %s: %s", cpuprofile, err.Error())
		}
		log.Printf("writing memory profile to: %s\n", memprofile)
		prof.mem = f
		runtime.MemProfileRate = 4096
	}
}

// stopProfile closes the CPU and memory profiles if they are running.
func stopProfile() {
	if prof.cpu != nil {
		pprof.StopCPUProfile()
		prof.cpu.Close()
		log.Println("CPU profiling stopped")
	}
	if prof.mem != nil {
		pprof.Lookup("heap").WriteTo(prof.mem, 0)
		prof.mem.Close()
		log.Println("memory profiling stopped")
	}
}
