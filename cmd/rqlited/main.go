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
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/rqlite/rqlite/auth"
	"github.com/rqlite/rqlite/cluster"
	"github.com/rqlite/rqlite/disco"
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
var discoURL string
var discoID string
var expvar bool
var pprofEnabled bool
var dsn string
var onDisk bool
var raftSnapThreshold uint64
var raftHeartbeatTimeout string
var raftApplyTimeout string
var raftOpenTimeout string
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
	flag.StringVar(&discoURL, "disco", "http://discovery.rqlite.com", "Set Discovery Service URL")
	flag.StringVar(&discoID, "discoid", "", "Set Discovery ID. If not set, Discovery Service not used")
	flag.BoolVar(&expvar, "expvar", true, "Serve expvar data on HTTP server")
	flag.BoolVar(&pprofEnabled, "pprof", true, "Serve pprof data on HTTP server")
	flag.StringVar(&dsn, "dsn", "", `SQLite DSN parameters. E.g. "cache=shared&mode=memory"`)
	flag.BoolVar(&onDisk, "ondisk", false, "Use an on-disk SQLite database")
	flag.BoolVar(&showVersion, "version", false, "Show version information and exit")
	flag.StringVar(&raftHeartbeatTimeout, "rafttimeout", "1s", "Raft heartbeat timeout")
	flag.StringVar(&raftApplyTimeout, "raftapplytimeout", "10s", "Raft apply timeout")
	flag.StringVar(&raftOpenTimeout, "raftopentimeout", "120s", "Time for initial Raft logs to be applied. Use 0s duration to skip wait.")
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
		fmt.Printf("rqlited %s %s %s (commit %s, branch %s)\n",
			version, runtime.GOOS, runtime.GOARCH, commit, branch)
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
	log.SetOutput(os.Stderr)
	log.SetPrefix("[rqlited] ")
	log.Printf("rqlited starting, version %s, commit %s, branch %s", version, commit, branch)
	log.Printf("target architecture is %s, operating system target is %s", runtime.GOARCH, runtime.GOOS)

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

	// Set optional parameters on store.
	store.SnapshotThreshold = raftSnapThreshold
	store.HeartbeatTimeout, err = time.ParseDuration(raftHeartbeatTimeout)
	if err != nil {
		log.Fatalf("failed to parse Raft heartbeat timeout %s: %s", raftHeartbeatTimeout, err.Error())
	}
	store.ApplyTimeout, err = time.ParseDuration(raftApplyTimeout)
	if err != nil {
		log.Fatalf("failed to parse Raft apply timeout %s: %s", raftApplyTimeout, err.Error())
	}
	store.OpenTimeout, err = time.ParseDuration(raftOpenTimeout)
	if err != nil {
		log.Fatalf("failed to parse Raft open timeout %s: %s", raftOpenTimeout, err.Error())
	}

	// Determine join addresses.
	joins, err := determineJoinAddresses()
	if err != nil {
		log.Fatalf("unable to determine join addresses: %s", err.Error())
	}

	// Now, open it.
	if err := store.Open(len(joins) == 0); err != nil {
		log.Fatalf("failed to open store: %s", err.Error())
	}

	// Create and configure cluster service.
	tn := mux.Listen(muxMetaHeader)
	cs := cluster.NewService(tn, store)
	if err := cs.Open(); err != nil {
		log.Fatalf("failed to open cluster service: %s", err.Error())
	}

	// Execute any requested join operation.
	if len(joins) > 0 {
		log.Println("join addresses are:", joins)
		if store.JoinRequired() {
			advAddr := raftAddr
			if raftAdv != "" {
				advAddr = raftAdv
			}
			if j, err := cluster.Join(joins, advAddr, noVerify); err != nil {
				log.Fatalf("failed to join cluster at %s: %s", joins, err.Error())
			} else {
				log.Println("successfully joined cluster at", j)
			}
		} else {
			log.Println("node is already member of cluster, ignoring any join requests")
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

func determineJoinAddresses() ([]string, error) {
	apiAdv := httpAddr
	if httpAdv != "" {
		apiAdv = httpAdv
	}

	var addrs []string
	if joinAddr != "" {
		// An explicit join address is first priority.
		addrs = append(addrs, joinAddr)
	}

	if discoID != "" {
		log.Printf("registering with Discovery Service at %s with ID %s", discoURL, discoID)
		c := disco.New(discoURL)
		r, err := c.Register(discoID, apiAdv)
		if err != nil {
			return nil, err
		}
		log.Println("Discovery service responded with nodes:", r.Nodes)
		for _, a := range r.Nodes {
			if a != apiAdv {
				// Only other nodes can be joined.
				addrs = append(addrs, a)
			}
		}
	}

	return addrs, nil
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
