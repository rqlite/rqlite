/*
Command rqlited is the rqlite server.

rqlite is a lightweight, distributed system that provides a replicated
relational database, using SQLite as the storage engine. rqlite is written
in Go and uses Raft to achieve consensus across all the instances of the
SQLite databases. rqlite ensures that every change made to the database is
made to a majority of underlying SQLite files, or none-at-all.
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
	"strings"
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
            _ _ _
           | (_) |
  _ __ __ _| |_| |_ ___
 | '__/ _  | | | __/ _ \   The lightweight, distributed
 | | | (_| | | | ||  __/   relational database.
 |_|  \__, |_|_|\__\___|
         | |
         |_|
`

// These variables are populated via the Go linker.
var (
	version   = "4"
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
var nodeEncrypt bool
var nodeX509Cert string
var nodeX509Key string
var raftAddr string
var raftAdv string
var joinAddr string
var noVerify bool
var noNodeVerify bool
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

const name = `rqlited`
const desc = `rqlite is a lightweight, distributed relational database, which uses SQLite as its
storage engine. It provides an easy-to-use, fault-tolerant store for relational data.`

func init() {
	flag.StringVar(&httpAddr, "http-addr", "localhost:4001", "HTTP server bind address. For HTTPS, set X.509 cert and key")
	flag.StringVar(&httpAdv, "http-adv-addr", "", "Advertised HTTP address. If not set, same as HTTP server")
	flag.StringVar(&x509Cert, "http-cert", "", "Path to X.509 certificate for HTTP endpoint")
	flag.StringVar(&x509Key, "http-key", "", "Path to X.509 private key for HTTP endpoint")
	flag.BoolVar(&noVerify, "http-no-verify", false, "Skip verification of remote HTTPS cert when joining cluster")
	flag.BoolVar(&nodeEncrypt, "node-encrypt", false, "Enable node-to-node encryption")
	flag.StringVar(&nodeX509Cert, "node-cert", "cert.pem", "Path to X.509 certificate for node-to-node encryption")
	flag.StringVar(&nodeX509Key, "node-key", "key.pem", "Path to X.509 private key for node-to-node encryption")
	flag.BoolVar(&noNodeVerify, "node-no-verify", false, "Skip verification of a remote node cert")
	flag.StringVar(&authFile, "auth", "", "Path to authentication and authorization file. If not set, not enabled")
	flag.StringVar(&raftAddr, "raft-addr", "localhost:4002", "Raft communication bind address")
	flag.StringVar(&raftAdv, "raft-adv-addr", "", "Advertised Raft communication address. If not set, same as Raft bind")
	flag.StringVar(&joinAddr, "join", "", "Comma-delimited list of nodes, through which a cluster can be joined (proto://host:port)")
	flag.StringVar(&discoURL, "disco-url", "http://discovery.rqlite.com", "Set Discovery Service URL")
	flag.StringVar(&discoID, "disco-id", "", "Set Discovery ID. If not set, Discovery Service not used")
	flag.BoolVar(&expvar, "expvar", true, "Serve expvar data on HTTP server")
	flag.BoolVar(&pprofEnabled, "pprof", true, "Serve pprof data on HTTP server")
	flag.StringVar(&dsn, "dsn", "", `SQLite DSN parameters. E.g. "cache=shared&mode=memory"`)
	flag.BoolVar(&onDisk, "on-disk", false, "Use an on-disk SQLite database")
	flag.BoolVar(&showVersion, "version", false, "Show version information and exit")
	flag.StringVar(&raftHeartbeatTimeout, "raft-timeout", "1s", "Raft heartbeat timeout")
	flag.StringVar(&raftApplyTimeout, "raft-apply-timeout", "10s", "Raft apply timeout")
	flag.StringVar(&raftOpenTimeout, "raft-open-timeout", "120s", "Time for initial Raft logs to be applied. Use 0s duration to skip wait")
	flag.Uint64Var(&raftSnapThreshold, "raft-snap", 8192, "Number of outstanding log entries that trigger snapshot")
	flag.StringVar(&cpuProfile, "cpu-profile", "", "Path to file for CPU profiling information")
	flag.StringVar(&memProfile, "mem-profile", "", "Path to file for memory profiling information")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "\n%s\n\n", desc)
		fmt.Fprintf(os.Stderr, "Usage: %s [arguments] <data directory>\n", name)
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	if showVersion {
		fmt.Printf("%s %s %s %s %s (commit %s, branch %s)\n",
			name, version, runtime.GOOS, runtime.GOARCH, runtime.Version(), commit, branch)
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
	log.SetPrefix(fmt.Sprintf("[%s] ", name))
	log.Printf("%s starting, version %s, commit %s, branch %s", name, version, commit, branch)
	log.Printf("%s, target architecture is %s, operating system target is %s", runtime.Version(), runtime.GOARCH, runtime.GOOS)

	// Start requested profiling.
	startProfile(cpuProfile, memProfile)

	// Set up node-to-node TCP communication.
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

	// Start up node-to-node network mux.
	var mux *tcp.Mux
	if nodeEncrypt {
		log.Printf("enabling node-to-node encryption with cert: %s, key: %s", nodeX509Cert, nodeX509Key)
		mux, err = tcp.NewTLSMux(ln, adv, nodeX509Cert, nodeX509Key)
	} else {
		mux, err = tcp.NewMux(ln, adv)
	}
	if err != nil {
		log.Fatalf("failed to create node-to-node mux: %s", err.Error())
	}
	mux.InsecureSkipVerify = noNodeVerify
	go mux.Serve()

	// Get transport for Raft communications.
	raftTn := mux.Listen(muxRaftHeader)

	// Create and open the store.
	dataPath, err = filepath.Abs(dataPath)
	if err != nil {
		log.Fatalf("failed to determine absolute data path: %s", err.Error())
	}
	dbConf := store.NewDBConfig(dsn, !onDisk)

	str := store.New(&store.StoreConfig{
		DBConf: dbConf,
		Dir:    dataPath,
		Tn:     raftTn,
	})

	// Set optional parameters on store.
	str.SnapshotThreshold = raftSnapThreshold
	str.HeartbeatTimeout, err = time.ParseDuration(raftHeartbeatTimeout)
	if err != nil {
		log.Fatalf("failed to parse Raft heartbeat timeout %s: %s", raftHeartbeatTimeout, err.Error())
	}
	str.ApplyTimeout, err = time.ParseDuration(raftApplyTimeout)
	if err != nil {
		log.Fatalf("failed to parse Raft apply timeout %s: %s", raftApplyTimeout, err.Error())
	}
	str.OpenTimeout, err = time.ParseDuration(raftOpenTimeout)
	if err != nil {
		log.Fatalf("failed to parse Raft open timeout %s: %s", raftOpenTimeout, err.Error())
	}

	// Determine join addresses, if necessary.
	ja, err := store.JoinAllowed(dataPath)
	if err != nil {
		log.Fatalf("unable to determine if join permitted: %s", err.Error())
	}

	var joins []string
	if ja {
		joins, err = determineJoinAddresses()
		if err != nil {
			log.Fatalf("unable to determine join addresses: %s", err.Error())
		}
	} else {
		log.Println("node is already member of cluster, skip determining join addresses")
	}

	// Now, open it.
	if err := str.Open(len(joins) == 0); err != nil {
		log.Fatalf("failed to open store: %s", err.Error())
	}

	// Create and configure cluster service.
	tn := mux.Listen(muxMetaHeader)
	cs := cluster.NewService(tn, str)
	if err := cs.Open(); err != nil {
		log.Fatalf("failed to open cluster service: %s", err.Error())
	}

	// Execute any requested join operation.
	if len(joins) > 0 {
		log.Println("join addresses are:", joins)
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
		log.Println("no join addresses set")
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
		s = httpd.New(httpAddr, str, credentialStore)
	} else {
		s = httpd.New(httpAddr, str, nil)
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

	// Register cross-component statuses.
	if err := s.RegisterStatus("mux", mux); err != nil {
		log.Fatalf("failed to register mux status: %s", err.Error())
	}

	// Block until signalled.
	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	if err := str.Close(true); err != nil {
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
		// Explicit join addresses are first priority.
		addrs = strings.Split(joinAddr, ",")
	}

	if discoID != "" {
		log.Printf("registering with Discovery Service at %s with ID %s", discoURL, discoID)
		c := disco.New(discoURL)
		r, err := c.Register(discoID, apiAdv)
		if err != nil {
			return nil, err
		}
		log.Println("Discovery Service responded with nodes:", r.Nodes)
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
