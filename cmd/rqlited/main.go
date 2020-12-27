// Command rqlited is the rqlite server.
package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
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
	version   = "5"
	commit    = "unknown"
	branch    = "unknown"
	buildtime = "unknown"
	features  = []string{}
)

var httpAddr string
var httpAdv string
var authFile string
var x509CACert string
var x509Cert string
var x509Key string
var nodeEncrypt bool
var nodeX509CACert string
var nodeX509Cert string
var nodeX509Key string
var nodeID string
var raftAddr string
var raftAdv string
var joinAddr string
var joinAttempts int
var joinInterval string
var noVerify bool
var noNodeVerify bool
var discoURL string
var discoID string
var expvar bool
var pprofEnabled bool
var dsn string
var onDisk bool
var raftLogLevel string
var raftNonVoter bool
var raftSnapThreshold uint64
var raftSnapInterval string
var raftLeaderLeaseTimeout string
var raftHeartbeatTimeout string
var raftElectionTimeout string
var raftApplyTimeout string
var raftOpenTimeout string
var raftShutdownOnRemove bool
var compressionSize int
var compressionBatch int
var showVersion bool
var cpuProfile string
var memProfile string

const name = `rqlited`
const desc = `rqlite is a lightweight, distributed relational database, which uses SQLite as its
storage engine. It provides an easy-to-use, fault-tolerant store for relational data.`

func init() {
	flag.StringVar(&nodeID, "node-id", "", "Unique name for node. If not set, set to hostname")
	flag.StringVar(&httpAddr, "http-addr", "localhost:4001", "HTTP server bind address. For HTTPS, set X.509 cert and key")
	flag.StringVar(&httpAdv, "http-adv-addr", "", "Advertised HTTP address. If not set, same as HTTP server")
	flag.StringVar(&x509CACert, "http-ca-cert", "", "Path to root X.509 certificate for HTTP endpoint")
	flag.StringVar(&x509Cert, "http-cert", "", "Path to X.509 certificate for HTTP endpoint")
	flag.StringVar(&x509Key, "http-key", "", "Path to X.509 private key for HTTP endpoint")
	flag.BoolVar(&noVerify, "http-no-verify", false, "Skip verification of remote HTTPS cert when joining cluster")
	flag.BoolVar(&nodeEncrypt, "node-encrypt", false, "Enable node-to-node encryption")
	flag.StringVar(&nodeX509CACert, "node-ca-cert", "", "Path to root X.509 certificate for node-to-node encryption")
	flag.StringVar(&nodeX509Cert, "node-cert", "cert.pem", "Path to X.509 certificate for node-to-node encryption")
	flag.StringVar(&nodeX509Key, "node-key", "key.pem", "Path to X.509 private key for node-to-node encryption")
	flag.BoolVar(&noNodeVerify, "node-no-verify", false, "Skip verification of a remote node cert")
	flag.StringVar(&authFile, "auth", "", "Path to authentication and authorization file. If not set, not enabled")
	flag.StringVar(&raftAddr, "raft-addr", "localhost:4002", "Raft communication bind address")
	flag.StringVar(&raftAdv, "raft-adv-addr", "", "Advertised Raft communication address. If not set, same as Raft bind")
	flag.StringVar(&joinAddr, "join", "", "Comma-delimited list of nodes, through which a cluster can be joined (proto://host:port)")
	flag.IntVar(&joinAttempts, "join-attempts", 5, "Number of join attempts to make")
	flag.StringVar(&joinInterval, "join-interval", "5s", "Period between join attempts")
	flag.StringVar(&discoURL, "disco-url", "http://discovery.rqlite.com", "Set Discovery Service URL")
	flag.StringVar(&discoID, "disco-id", "", "Set Discovery ID. If not set, Discovery Service not used")
	flag.BoolVar(&expvar, "expvar", true, "Serve expvar data on HTTP server")
	flag.BoolVar(&pprofEnabled, "pprof", true, "Serve pprof data on HTTP server")
	flag.StringVar(&dsn, "dsn", "", `SQLite DSN parameters. E.g. "cache=shared&mode=memory"`)
	flag.BoolVar(&onDisk, "on-disk", false, "Use an on-disk SQLite database")
	flag.BoolVar(&showVersion, "version", false, "Show version information and exit")
	flag.BoolVar(&raftNonVoter, "raft-non-voter", false, "Configure as non-voting node")
	flag.StringVar(&raftHeartbeatTimeout, "raft-timeout", "1s", "Raft heartbeat timeout")
	flag.StringVar(&raftElectionTimeout, "raft-election-timeout", "1s", "Raft election timeout")
	flag.StringVar(&raftApplyTimeout, "raft-apply-timeout", "10s", "Raft apply timeout")
	flag.StringVar(&raftOpenTimeout, "raft-open-timeout", "120s", "Time for initial Raft logs to be applied. Use 0s duration to skip wait")
	flag.Uint64Var(&raftSnapThreshold, "raft-snap", 8192, "Number of outstanding log entries that trigger snapshot")
	flag.StringVar(&raftSnapInterval, "raft-snap-int", "30s", "Snapshot threshold check interval")
	flag.StringVar(&raftLeaderLeaseTimeout, "raft-leader-lease-timeout", "0s", "Raft leader lease timeout. Use 0s for Raft default")
	flag.BoolVar(&raftShutdownOnRemove, "raft-remove-shutdown", false, "Shutdown Raft if node removed")
	flag.StringVar(&raftLogLevel, "raft-log-level", "INFO", "Minimum log level for Raft module")
	flag.IntVar(&compressionSize, "compression-size", 150, "Request query size for compression attempt")
	flag.IntVar(&compressionBatch, "compression-batch", 5, "Request batch threshold for compression attempt")
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

	// Create internode network layer.
	var tn *tcp.Transport
	if nodeEncrypt {
		log.Printf("enabling node-to-node encryption with cert: %s, key: %s", nodeX509Cert, nodeX509Key)
		tn = tcp.NewTLSTransport(nodeX509Cert, nodeX509Key, noVerify)
	} else {
		tn = tcp.NewTransport()
	}
	if err := tn.Open(raftAddr); err != nil {
		log.Fatalf("failed to open internode network layer: %s", err.Error())
	}

	// Create and open the store.
	dataPath, err := filepath.Abs(dataPath)
	if err != nil {
		log.Fatalf("failed to determine absolute data path: %s", err.Error())
	}
	dbConf := store.NewDBConfig(dsn, !onDisk)

	str := store.New(tn, &store.StoreConfig{
		DBConf: dbConf,
		Dir:    dataPath,
		ID:     idOrRaftAddr(),
	})

	// Set optional parameters on store.
	str.SetRequestCompression(compressionBatch, compressionSize)
	str.RaftLogLevel = raftLogLevel
	str.ShutdownOnRemove = raftShutdownOnRemove
	str.SnapshotThreshold = raftSnapThreshold
	str.SnapshotInterval, err = time.ParseDuration(raftSnapInterval)
	if err != nil {
		log.Fatalf("failed to parse Raft Snapsnot interval %s: %s", raftSnapInterval, err.Error())
	}
	str.LeaderLeaseTimeout, err = time.ParseDuration(raftLeaderLeaseTimeout)
	if err != nil {
		log.Fatalf("failed to parse Raft Leader lease timeout %s: %s", raftLeaderLeaseTimeout, err.Error())
	}
	str.HeartbeatTimeout, err = time.ParseDuration(raftHeartbeatTimeout)
	if err != nil {
		log.Fatalf("failed to parse Raft heartbeat timeout %s: %s", raftHeartbeatTimeout, err.Error())
	}
	str.ElectionTimeout, err = time.ParseDuration(raftElectionTimeout)
	if err != nil {
		log.Fatalf("failed to parse Raft election timeout %s: %s", raftElectionTimeout, err.Error())
	}
	str.ApplyTimeout, err = time.ParseDuration(raftApplyTimeout)
	if err != nil {
		log.Fatalf("failed to parse Raft apply timeout %s: %s", raftApplyTimeout, err.Error())
	}

	// Any prexisting node state?
	var enableBootstrap bool
	isNew := store.IsNewNode(dataPath)
	if isNew {
		log.Printf("no preexisting node state detected in %s, node may be bootstrapping", dataPath)
		enableBootstrap = true // New node, so we may be bootstrapping
	} else {
		log.Printf("preexisting node state detected in %s", dataPath)
	}

	// Determine join addresses
	var joins []string
	joins, err = determineJoinAddresses()
	if err != nil {
		log.Fatalf("unable to determine join addresses: %s", err.Error())
	}

	// Supplying join addresses means bootstrapping a new cluster won't
	// be required.
	if len(joins) > 0 {
		enableBootstrap = false
		log.Println("join addresses specified, node is not bootstrapping")
	} else {
		log.Println("no join addresses set")
	}

	// Join address supplied, but we don't need them!
	if !isNew && len(joins) > 0 {
		log.Println("node is already member of cluster, ignoring join addresses")
	}

	// Now, open store.
	if err := str.Open(enableBootstrap); err != nil {
		log.Fatalf("failed to open store: %s", err.Error())
	}

	// Prepare metadata for join command.
	apiAdv := httpAddr
	if httpAdv != "" {
		apiAdv = httpAdv
	}
	apiProto := "http"
	if x509Cert != "" {
		apiProto = "https"
	}
	meta := map[string]string{
		"api_addr":  apiAdv,
		"api_proto": apiProto,
	}

	// Execute any requested join operation.
	if len(joins) > 0 && isNew {
		log.Println("join addresses are:", joins)
		advAddr := raftAddr
		if raftAdv != "" {
			advAddr = raftAdv
		}

		joinDur, err := time.ParseDuration(joinInterval)
		if err != nil {
			log.Fatalf("failed to parse Join interval %s: %s", joinInterval, err.Error())
		}

		tlsConfig := tls.Config{InsecureSkipVerify: noVerify}
		if x509CACert != "" {
			asn1Data, err := ioutil.ReadFile(x509CACert)
			if err != nil {
				log.Fatalf("ioutil.ReadFile failed: %s", err.Error())
			}
			tlsConfig.RootCAs = x509.NewCertPool()
			ok := tlsConfig.RootCAs.AppendCertsFromPEM([]byte(asn1Data))
			if !ok {
				log.Fatalf("failed to parse root CA certificate(s) in %q", x509CACert)
			}
		}

		if j, err := cluster.Join(joins, str.ID(), advAddr, !raftNonVoter, meta,
			joinAttempts, joinDur, &tlsConfig); err != nil {
			log.Fatalf("failed to join cluster at %s: %s", joins, err.Error())
		} else {
			log.Println("successfully joined cluster at", j)
		}

	}

	// Wait until the store is in full consensus.
	openTimeout, err := time.ParseDuration(raftOpenTimeout)
	if err != nil {
		log.Fatalf("failed to parse Raft open timeout %s: %s", raftOpenTimeout, err.Error())
	}
	str.WaitForLeader(openTimeout)
	str.WaitForApplied(openTimeout)

	// This may be a standalone server. In that case set its own metadata.
	if err := str.SetMetadata(meta); err != nil && err != store.ErrNotLeader {
		// Non-leader errors are OK, since metadata will then be set through
		// consensus as a result of a join. All other errors indicate a problem.
		log.Fatalf("failed to set store metadata: %s", err.Error())
	}

	// Start the HTTP API server.
	if err := startHTTPService(str); err != nil {
		log.Fatalf("failed to start HTTP server: %s", err.Error())
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

func startHTTPService(str *store.Store) error {
	// Get the credential store.
	credStr, err := credentialStore()
	if err != nil {
		return err
	}

	// Create HTTP server and load authentication information if required.
	var s *httpd.Service
	if credStr != nil {
		s = httpd.New(httpAddr, str, credStr)
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
	return s.Start()
}

func credentialStore() (*auth.CredentialsStore, error) {
	if authFile == "" {
		return nil, nil
	}

	f, err := os.Open(authFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open authentication file %s: %s", authFile, err.Error())
	}

	cs := auth.NewCredentialsStore()
	if cs.Load(f); err != nil {
		return nil, err
	}
	return cs, nil
}

func idOrRaftAddr() string {
	if nodeID != "" {
		return nodeID
	}
	if raftAdv == "" {
		return raftAddr
	}
	return raftAdv
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
