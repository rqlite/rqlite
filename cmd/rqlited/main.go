// Command rqlited is the rqlite server.
package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	consul "github.com/rqlite/rqlite-disco-clients/consul"
	"github.com/rqlite/rqlite-disco-clients/dns"
	"github.com/rqlite/rqlite-disco-clients/dnssrv"
	etcd "github.com/rqlite/rqlite-disco-clients/etcd"
	"github.com/rqlite/rqlite/auth"
	"github.com/rqlite/rqlite/cluster"
	"github.com/rqlite/rqlite/cmd"
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
         | |               www.rqlite.io
         |_|
`

const name = `rqlited`
const desc = `rqlite is a lightweight, distributed relational database, which uses SQLite as its
storage engine. It provides an easy-to-use, fault-tolerant store for relational data.`

func init() {
	log.SetFlags(log.LstdFlags)
	log.SetOutput(os.Stderr)
	log.SetPrefix(fmt.Sprintf("[%s] ", name))
}

func main() {
	cfg, err := ParseFlags(name, desc, &BuildInfo{
		Version: cmd.Version,
		Commit:  cmd.Commit,
		Branch:  cmd.Branch,
	})
	if err != nil {
		log.Fatalf("failed to parse command-line flags: %s", err.Error())
	}

	// Display logo.
	fmt.Println(logo)

	// Configure logging and pump out initial message.
	log.Printf("%s starting, version %s, commit %s, branch %s, compiler %s", name, cmd.Version, cmd.Commit, cmd.Branch, runtime.Compiler)
	log.Printf("%s, target architecture is %s, operating system target is %s", runtime.Version(), runtime.GOARCH, runtime.GOOS)
	log.Printf("launch command: %s", strings.Join(os.Args, " "))

	// Start requested profiling.
	startProfile(cfg.CPUProfile, cfg.MemProfile)

	// Create internode network mux and configure.
	muxLn, err := net.Listen("tcp", cfg.RaftAddr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %s", cfg.RaftAddr, err.Error())
	}
	mux, err := startNodeMux(cfg, muxLn)
	if err != nil {
		log.Fatalf("failed to start node mux: %s", err.Error())
	}
	raftTn := mux.Listen(cluster.MuxRaftHeader)
	log.Printf("Raft TCP mux Listener registered with %d", cluster.MuxRaftHeader)

	// Create the store.
	str, err := createStore(cfg, raftTn)
	if err != nil {
		log.Fatalf("failed to create store: %s", err.Error())
	}

	// Now, open store.
	if err := str.Open(); err != nil {
		log.Fatalf("failed to open store: %s", err.Error())
	}

	// Get any credential store.
	credStr, err := credentialStore(cfg)
	if err != nil {
		log.Fatalf("failed to get credential store: %s", err.Error())
	}

	// Create cluster service now, so nodes will be able to learn information about each other.
	clstr, err := clusterService(cfg, mux.Listen(cluster.MuxClusterHeader), str)
	if err != nil {
		log.Fatalf("failed to create cluster service: %s", err.Error())
	}
	log.Printf("Cluster TCP mux Listener registered with %d", cluster.MuxClusterHeader)

	// Start the HTTP API server.
	clstrDialer := tcp.NewDialer(cluster.MuxClusterHeader, cfg.NodeEncrypt, cfg.NoNodeVerify)
	clstrClient := cluster.NewClient(clstrDialer)
	if err := clstrClient.SetLocal(cfg.RaftAdv, clstr); err != nil {
		log.Fatalf("failed to set cluster client local parameters: %s", err.Error())
	}
	httpServ, err := startHTTPService(cfg, str, clstrClient, credStr)
	if err != nil {
		log.Fatalf("failed to start HTTP server: %s", err.Error())
	}

	// Register remaining status providers.
	httpServ.RegisterStatus("cluster", clstr)

	tlsConfig := tls.Config{InsecureSkipVerify: cfg.NoHTTPVerify}
	if cfg.X509CACert != "" {
		asn1Data, err := ioutil.ReadFile(cfg.X509CACert)
		if err != nil {
			log.Fatalf("ioutil.ReadFile failed: %s", err.Error())
		}
		tlsConfig.RootCAs = x509.NewCertPool()
		ok := tlsConfig.RootCAs.AppendCertsFromPEM(asn1Data)
		if !ok {
			log.Fatalf("failed to parse root CA certificate(s) in %q", cfg.X509CACert)
		}
	}

	// Create the cluster!
	nodes, err := str.Nodes()
	if err != nil {
		log.Fatalf("failed to get nodes %s", err.Error())
	}
	if err := createCluster(cfg, &tlsConfig, len(nodes) > 0, str, httpServ, credStr); err != nil {
		log.Fatalf("clustering failure: %s", err.Error())
	}

	// Tell the user the node is ready for HTTP, giving some advice on how to connect.
	log.Printf("node HTTP API available at %s", cfg.HTTPURL())
	h, p, _ := net.SplitHostPort(cfg.HTTPAdv)
	log.Printf("connect using the command-line tool via 'rqlite -H %s -p %s'", h, p)

	// Block until signalled.
	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	if err := str.Close(true); err != nil {
		log.Printf("failed to close store: %s", err.Error())
	}
	clstr.Close()
	muxLn.Close()
	stopProfile()
	log.Println("rqlite server stopped")
}

func createStore(cfg *Config, ln *tcp.Layer) (*store.Store, error) {
	dataPath, err := filepath.Abs(cfg.DataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to determine absolute data path: %s", err.Error())
	}
	dbConf := store.NewDBConfig(!cfg.OnDisk)
	dbConf.OnDiskPath = cfg.OnDiskPath
	dbConf.FKConstraints = cfg.FKConstraints

	str := store.New(ln, &store.Config{
		DBConf: dbConf,
		Dir:    cfg.DataPath,
		ID:     cfg.NodeID,
	})

	// Set optional parameters on store.
	str.StartupOnDisk = cfg.OnDiskStartup
	str.SetRequestCompression(cfg.CompressionBatch, cfg.CompressionSize)
	str.RaftLogLevel = cfg.RaftLogLevel
	str.NoFreeListSync = cfg.RaftNoFreelistSync
	str.ShutdownOnRemove = cfg.RaftShutdownOnRemove
	str.SnapshotThreshold = cfg.RaftSnapThreshold
	str.SnapshotInterval = cfg.RaftSnapInterval
	str.LeaderLeaseTimeout = cfg.RaftLeaderLeaseTimeout
	str.HeartbeatTimeout = cfg.RaftHeartbeatTimeout
	str.ElectionTimeout = cfg.RaftElectionTimeout
	str.ApplyTimeout = cfg.RaftApplyTimeout
	str.BootstrapExpect = cfg.BootstrapExpect

	isNew := store.IsNewNode(dataPath)
	if isNew {
		log.Printf("no preexisting node state detected in %s, node may be bootstrapping", dataPath)
	} else {
		log.Printf("preexisting node state detected in %s", dataPath)
	}

	return str, nil
}

func createDiscoService(cfg *Config, str *store.Store) (*disco.Service, error) {
	var c disco.Client
	var err error
	var rc io.ReadCloser

	rc = cfg.DiscoConfigReader()
	defer func() {
		if rc != nil {
			rc.Close()
		}
	}()
	if cfg.DiscoMode == DiscoModeConsulKV {
		var consulCfg *consul.Config
		consulCfg, err = consul.NewConfigFromReader(rc)
		if err != nil {
			return nil, fmt.Errorf("create Consul config: %s", err.Error())
		}

		c, err = consul.New(cfg.DiscoKey, consulCfg)
		if err != nil {
			return nil, fmt.Errorf("create Consul client: %s", err.Error())
		}
	} else if cfg.DiscoMode == DiscoModeEtcdKV {
		var etcdCfg *etcd.Config
		etcdCfg, err = etcd.NewConfigFromReader(rc)
		if err != nil {
			return nil, fmt.Errorf("create etcd config: %s", err.Error())
		}

		c, err = etcd.New(cfg.DiscoKey, etcdCfg)
		if err != nil {
			return nil, fmt.Errorf("create etcd client: %s", err.Error())
		}
	} else {
		return nil, fmt.Errorf("invalid disco service: %s", cfg.DiscoMode)
	}

	return disco.NewService(c, str), nil
}

func startHTTPService(cfg *Config, str *store.Store, cltr *cluster.Client, credStr *auth.CredentialsStore) (*httpd.Service, error) {
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
	s.Pprof = cfg.PprofEnabled
	s.BuildInfo = map[string]interface{}{
		"commit":     cmd.Commit,
		"branch":     cmd.Branch,
		"version":    cmd.Version,
		"compiler":   runtime.Compiler,
		"build_time": cmd.Buildtime,
	}
	return s, s.Start()
}

// startNodeMux starts the TCP mux on the given listener, which should be already
// bound to the relevant interface.
func startNodeMux(cfg *Config, ln net.Listener) (*tcp.Mux, error) {
	var adv net.Addr
	var err error

	adv, err = net.ResolveTCPAddr("tcp", cfg.RaftAdv)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve advertise address %s: %s", cfg.RaftAdv, err.Error())
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

func clusterService(cfg *Config, tn cluster.Transport, db cluster.Database) (*cluster.Service, error) {
	c := cluster.New(tn, db)
	c.SetAPIAddr(cfg.HTTPAdv)
	c.EnableHTTPS(cfg.X509Cert != "" && cfg.X509Key != "") // Conditions met for an HTTPS API

	if err := c.Open(); err != nil {
		return nil, err
	}
	return c, nil
}

func createCluster(cfg *Config, tlsConfig *tls.Config, hasPeers bool, str *store.Store,
	httpServ *httpd.Service, credStr *auth.CredentialsStore) error {
	joins := cfg.JoinAddresses()
	if joins == nil && cfg.DiscoMode == "" && !hasPeers {
		// Brand new node, told to bootstrap itself. So do it.
		log.Println("bootstraping single new node")
		if err := str.Bootstrap(store.NewServer(str.ID(), str.Addr(), true)); err != nil {
			return fmt.Errorf("failed to bootstrap single new node: %s", err.Error())
		}
		return nil
	}

	// Prepare the Joiner
	joiner := cluster.NewJoiner(cfg.JoinSrcIP, cfg.JoinAttempts, cfg.JoinInterval, tlsConfig)
	if cfg.JoinAs != "" {
		pw, ok := credStr.Password(cfg.JoinAs)
		if !ok {
			return fmt.Errorf("user %s does not exist in credential store", cfg.JoinAs)
		}
		joiner.SetBasicAuth(cfg.JoinAs, pw)
	}

	// Prepare defintion of being part of a cluster.
	isClustered := func() bool {
		leader, _ := str.LeaderAddr()
		return leader != ""
	}

	if joins != nil && cfg.BootstrapExpect == 0 {
		// Explicit join operation requested, so do it.
		j, err := joiner.Do(joins, str.ID(), cfg.RaftAdv, !cfg.RaftNonVoter)
		if err != nil {
			return fmt.Errorf("failed to join cluster: %s", err.Error())
		}
		log.Println("successfully joined cluster at", j)
		return nil
	}

	if joins != nil && cfg.BootstrapExpect > 0 {
		// Bootstrap with explicit join addresses requests.
		if hasPeers {
			log.Println("preexisting node configuration detected, ignoring bootstrap request")
			return nil
		}

		bs := cluster.NewBootstrapper(cluster.NewAddressProviderString(joins),
			cfg.BootstrapExpect, tlsConfig)
		if cfg.JoinAs != "" {
			pw, ok := credStr.Password(cfg.JoinAs)
			if !ok {
				return fmt.Errorf("user %s does not exist in credential store", cfg.JoinAs)
			}
			bs.SetBasicAuth(cfg.JoinAs, pw)
		}
		return bs.Boot(str.ID(), cfg.RaftAdv, isClustered, cfg.BootstrapExpectTimeout)
	}

	if cfg.DiscoMode == "" {
		// No more clustering techniques to try. Node will just sit, probably using
		// existing Raft state.
		return nil
	}

	log.Printf("discovery mode: %s", cfg.DiscoMode)
	switch cfg.DiscoMode {
	case DiscoModeDNS, DiscoModeDNSSRV:
		if hasPeers {
			log.Printf("preexisting node configuration detected, ignoring %s", cfg.DiscoMode)
			return nil
		}
		rc := cfg.DiscoConfigReader()
		defer func() {
			if rc != nil {
				rc.Close()
			}
		}()

		var provider interface {
			cluster.AddressProvider
			httpd.StatusReporter
		}
		if cfg.DiscoMode == DiscoModeDNS {
			dnsCfg, err := dns.NewConfigFromReader(rc)
			if err != nil {
				return fmt.Errorf("error reading DNS configuration: %s", err.Error())
			}
			provider = dns.New(dnsCfg)

		} else {
			dnssrvCfg, err := dnssrv.NewConfigFromReader(rc)
			if err != nil {
				return fmt.Errorf("error reading DNS configuration: %s", err.Error())
			}
			provider = dnssrv.New(dnssrvCfg)
		}

		bs := cluster.NewBootstrapper(provider, cfg.BootstrapExpect, tlsConfig)
		if cfg.JoinAs != "" {
			pw, ok := credStr.Password(cfg.JoinAs)
			if !ok {
				return fmt.Errorf("user %s does not exist in credential store", cfg.JoinAs)
			}
			bs.SetBasicAuth(cfg.JoinAs, pw)
		}
		httpServ.RegisterStatus("disco", provider)
		return bs.Boot(str.ID(), cfg.RaftAdv, isClustered, cfg.BootstrapExpectTimeout)

	case DiscoModeEtcdKV, DiscoModeConsulKV:
		discoService, err := createDiscoService(cfg, str)
		if err != nil {
			return fmt.Errorf("failed to start discovery service: %s", err.Error())
		}

		if !hasPeers {
			log.Println("no preexisting nodes, registering with discovery service")

			leader, addr, err := discoService.Register(str.ID(), cfg.HTTPURL(), cfg.RaftAdv)
			if err != nil {
				return fmt.Errorf("failed to register with discovery service: %s", err.Error())
			}
			if leader {
				log.Println("node registered as leader using discovery service")
				if err := str.Bootstrap(store.NewServer(str.ID(), str.Addr(), true)); err != nil {
					return fmt.Errorf("failed to bootstrap single new node: %s", err.Error())
				}
			} else {
				for {
					log.Printf("discovery service returned %s as join address", addr)
					if j, err := joiner.Do([]string{addr}, str.ID(), cfg.RaftAdv, !cfg.RaftNonVoter); err != nil {
						log.Printf("failed to join cluster at %s: %s", addr, err.Error())

						time.Sleep(time.Second)
						_, addr, err = discoService.Register(str.ID(), cfg.HTTPURL(), cfg.RaftAdv)
						if err != nil {
							log.Printf("failed to get updated leader: %s", err.Error())
						}
						continue
					} else {
						log.Println("successfully joined cluster at", j)
						break
					}
				}
			}
		} else {
			log.Println("preexisting node configuration detected, not registering with discovery service")
		}
		go discoService.StartReporting(cfg.NodeID, cfg.HTTPURL(), cfg.RaftAdv)
		httpServ.RegisterStatus("disco", discoService)

	default:
		return fmt.Errorf("invalid disco mode %s", cfg.DiscoMode)
	}
	return nil
}
