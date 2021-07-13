package server

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"

	"github.com/rqlite/rqlite/auth"
	"github.com/rqlite/rqlite/cluster"
	"github.com/rqlite/rqlite/cmd"
	"github.com/rqlite/rqlite/disco"
	httpd "github.com/rqlite/rqlite/http"
	"github.com/rqlite/rqlite/store"
	"github.com/rqlite/rqlite/tcp"
)

type Server struct {
	// waits for a termination signal
	terminate chan os.Signal
	// notifies that server was gracefully shutted down
	done  chan bool
	cfg   *Config
	store *store.Store
	ready bool
	log   *log.Logger
}

func New(cfg *Config, opts ...Option) *Server {
	for _, o := range opts {
		o(cfg)
	}
	return &Server{cfg: cfg}
}

func (s *Server) Start() error {
	log := log.New(os.Stderr, "[server] ", log.LstdFlags)
	cfg := s.cfg
	s.log = log

	if s.cfg.DataPath == "" {
		return errors.New("fatal: no data directory set")
	}

	// Start requested profiling.
	startProfile(s.cfg.CPUProfile, s.cfg.MemoryProfile)

	// Create internode network mux and configure.
	muxLn, err := net.Listen("tcp", s.cfg.RaftAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %s", s.cfg.RaftAddr, err.Error())
	}
	mux, err := startNodeMux(cfg, muxLn)
	if err != nil {
		return fmt.Errorf("failed to start node mux: %s", err.Error())
	}
	raftTn := mux.Listen(cluster.MuxRaftHeader)

	// Create cluster service, so nodes can learn information about each other. This can be started
	// now since it doesn't require a functioning Store yet.
	clstr, err := clusterService(cfg, mux.Listen(cluster.MuxClusterHeader))
	if err != nil {
		return fmt.Errorf("failed to create cluster service: %s", err.Error())
	}

	// Create and open the store.
	dataPath, err := filepath.Abs(s.cfg.DataPath)
	if err != nil {
		return fmt.Errorf("failed to determine absolute data path: %s", err.Error())
	}
	dbConf := store.NewDBConfig(s.cfg.DSN, !s.cfg.OnDisk)

	str := store.New(raftTn, &store.StoreConfig{
		DBConf: dbConf,
		Dir:    dataPath,
		ID:     idOrRaftAddr(cfg),
	})
	s.store = str

	// Set optional parameters on store.
	str.SetRequestCompression(s.cfg.CompressionBatch, s.cfg.CompressionSize)
	str.RaftLogLevel = s.cfg.RaftLogLevel
	str.ShutdownOnRemove = s.cfg.RaftShutdownOnRemove
	str.SnapshotThreshold = s.cfg.RaftSnapThreshold
	str.SnapshotInterval = cfg.RaftSnapInterval
	str.LeaderLeaseTimeout = cfg.RaftLeaderLeaseTimeout
	str.HeartbeatTimeout = cfg.RaftHeartbeatTimeout
	str.ElectionTimeout = cfg.RaftElectionTimeout
	str.ApplyTimeout = cfg.RaftApplyTimeout

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
	joins, err = determineJoinAddresses(cfg)
	if err != nil {
		return fmt.Errorf("unable to determine join addresses: %s", err.Error())
	}

	// Supplying join addresses means bootstrapping a new cluster won't
	// be required.
	if len(joins) > 0 {
		enableBootstrap = false
		log.Println("join addresses specified, node is not bootstrapping")
	} else {
		log.Println("no join addresses set")
	}

	// Now, open store.
	if err := str.Open(enableBootstrap); err != nil {
		return fmt.Errorf("failed to open store: %s", err.Error())
	}

	if err = s.requestJoin(joins); err != nil {
		return err
	}

	s.terminate = make(chan os.Signal, 1)
	s.done = make(chan bool, 1)
	go func() {
		// This defer is placed first to be executed last, it signals
		// that all resources where closed gracefully
		defer func() {
			log.Println("bye bye!")
			s.done <- true
		}()
		defer stopProfile()
		defer muxLn.Close()
		defer clstr.Close()

		// Wait until the store is in full consensus.
		if err := waitForConsensus(cfg, str); err != nil {
			log.Fatal(err.Error())
		}
		log.Println("store has reached consensus")

		// Start the HTTP API server.
		if err := startHTTPService(cfg, str, clstr); err != nil {
			log.Fatalf("failed to start HTTP server: %s", err.Error())
		}
		signal.Notify(s.terminate, os.Interrupt)
		log.Println("node is ready")
		s.ready = true

		// Block until signalled.
		<-s.terminate
		s.ready = false
		s.log.Println("waiting for graceful termination...")
		if err := str.Close(true); err != nil {
			log.Printf("failed to close store: %s", err.Error())
		}
		log.Println("rqlite server stopped")
	}()

	return nil
}

func (s *Server) IsReady() bool {
	return s.ready
}

func (s *Server) Done() chan bool {
	return s.done
}

func (s *Server) Stop() {
	s.terminate <- os.Interrupt
	<-s.done
}

// requestJoin executes any requested join operation.
func (s *Server) requestJoin(joins []string) error {
	if len(joins) == 0 {
		return nil
	}

	var (
		store = s.store
		cfg   = s.cfg
		log   = s.log
	)

	log.Println("join addresses are:", joins)
	advAddr := cfg.RaftAddr
	if cfg.RaftAdv != "" {
		advAddr = cfg.RaftAdv
	}

	tlsConfig := tls.Config{InsecureSkipVerify: cfg.NoVerify}
	if cfg.X509CACert != "" {
		asn1Data, err := ioutil.ReadFile(cfg.X509CACert)
		if err != nil {
			return fmt.Errorf("ioutil.ReadFile failed: %s", err.Error())
		}
		tlsConfig.RootCAs = x509.NewCertPool()
		ok := tlsConfig.RootCAs.AppendCertsFromPEM([]byte(asn1Data))
		if !ok {
			return fmt.Errorf("failed to parse root CA certificate(s) in %q", cfg.X509CACert)
		}
	}

	if j, err := cluster.Join(cfg.JoinSrcIP, joins, store.ID(), advAddr, !cfg.RaftNonVoter,
		cfg.JoinAttempts, cfg.JoinInterval, &tlsConfig); err != nil {
		return fmt.Errorf("failed to join cluster at %s: %s", joins, err.Error())
	} else {
		log.Println("successfully joined cluster at", j)
	}
	return nil
}

func determineJoinAddresses(cfg *Config) ([]string, error) {
	apiAdv := cfg.HTTPAddr
	if cfg.HTTPAdv != "" {
		apiAdv = cfg.HTTPAdv
	}

	var addrs []string
	if cfg.JoinAddr != "" {
		// Explicit join addresses are first priority.
		addrs = strings.Split(cfg.JoinAddr, ",")
	}

	if cfg.DiscoID != "" {
		log.Printf("registering with Discovery Service at %s with ID %s", cfg.DiscoURL, cfg.DiscoID)
		c := disco.New(cfg.DiscoURL)
		r, err := c.Register(cfg.DiscoID, apiAdv)
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

func waitForConsensus(cfg *Config, str *store.Store) error {
	openTimeout := cfg.RaftOpenTimeout
	if _, err := str.WaitForLeader(openTimeout); err != nil {
		if cfg.RaftWaitForLeader {
			return fmt.Errorf("leader did not appear within timeout: %s", err.Error())
		}
		log.Println("ignoring error while waiting for leader")
	}
	if openTimeout != 0 {
		if err := str.WaitForApplied(openTimeout); err != nil {
			return fmt.Errorf("log was not fully applied within timeout: %s", err.Error())
		}
	} else {
		log.Println("not waiting for logs to be applied")
	}
	return nil
}

func startHTTPService(cfg *Config, str *store.Store, cltr *cluster.Service) error {
	// Get the credential store.
	credStr, err := credentialStore(cfg)
	if err != nil {
		return err
	}

	// Create HTTP server and load authentication information if required.
	var s *httpd.Service
	if credStr != nil {
		s = httpd.New(cfg.HTTPAddr, str, cltr, credStr)
	} else {
		s = httpd.New(cfg.HTTPAddr, str, cltr, nil)
	}

	s.CertFile = cfg.X509Cert
	s.KeyFile = cfg.X509Key
	s.TLS1011 = cfg.TLS1011
	s.Expvar = cfg.Expvar
	s.Pprof = cfg.PPROFEnabled
	s.BuildInfo = map[string]interface{}{
		"commit":     cmd.Commit,
		"branch":     cmd.Branch,
		"version":    cmd.Version,
		"build_time": cmd.Buildtime,
	}
	return s.Start()
}

func startNodeMux(cfg *Config, ln net.Listener) (*tcp.Mux, error) {
	var adv net.Addr
	var err error
	if cfg.RaftAdv != "" {
		adv, err = net.ResolveTCPAddr("tcp", cfg.RaftAdv)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve advertise address %s: %s", cfg.RaftAdv, err.Error())
		}
	}

	var mux *tcp.Mux
	if cfg.NodeEncrypt {
		log.Printf("enabling node-to-node encryption with cert: %s, key: %s", cfg.NodeX509Cert, cfg.NodeX509Key)
		mux, err = tcp.NewTLSMux(ln, adv, cfg.NodeX509Cert, cfg.NodeX509Key, cfg.NodeX509CACert)
	} else {
		mux, err = tcp.NewMux(ln, adv)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create node-to-node mux: %s", err.Error())
	}
	mux.InsecureSkipVerify = cfg.NoNodeVerify
	go mux.Serve()

	return mux, nil
}

func credentialStore(cfg *Config) (*auth.CredentialsStore, error) {
	if cfg.AuthFile == "" {
		return nil, nil
	}

	f, err := os.Open(cfg.AuthFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open authentication file %s: %s", cfg.AuthFile, err.Error())
	}

	cs := auth.NewCredentialsStore()
	if cs.Load(f); err != nil {
		return nil, err
	}
	return cs, nil
}

func clusterService(cfg *Config, tn cluster.Transport) (*cluster.Service, error) {
	c := cluster.New(tn)
	apiAddr := cfg.HTTPAddr
	if cfg.HTTPAdv != "" {
		apiAddr = cfg.HTTPAdv
	}
	c.SetAPIAddr(apiAddr)
	c.EnableHTTPS(cfg.X509Cert != "" && cfg.X509Key != "") // Conditions met for a HTTPS API

	if err := c.Open(); err != nil {
		return nil, err
	}
	return c, nil
}

func idOrRaftAddr(cfg *Config) string {
	if cfg.NodeID != "" {
		return cfg.NodeID
	}
	if cfg.RaftAdv == "" {
		return cfg.RaftAddr
	}
	return cfg.RaftAdv
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
