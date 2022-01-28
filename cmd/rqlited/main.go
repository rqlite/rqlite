// Command rqlited is the rqlite server.
package main

import (
	"bytes"
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

	// Determine join addresses
	var joins []string
	joins, err = determineJoinAddresses(cfg)
	if err != nil {
		log.Fatalf("unable to determine join addresses: %s", err.Error())
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
	if err := createCluster(cfg, joins, &tlsConfig, len(nodes) > 0, str, httpServ, credStr); err != nil {
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

// determineJoinAddresses returns the join addresses supplied at the command-line.
// removing any occurence of this nodes HTTP address.
func determineJoinAddresses(cfg *Config) ([]string, error) {
	var addrs []string
	if cfg.JoinAddr != "" {
		addrs = strings.Split(cfg.JoinAddr, ",")
	}

	// It won't work to attempt an explicit self-join, so remove any such address.
	var validAddrs []string
	for i := range addrs {
		if addrs[i] == cfg.HTTPAdv || addrs[i] == cfg.HTTPAddr {
			log.Printf("ignoring join address %s equal to this node's address", addrs[i])
			continue
		}
		validAddrs = append(validAddrs, addrs[i])
	}

	return validAddrs, nil
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
	var reader io.Reader

	if cfg.DiscoConfig != "" {
		// Open config file. If opening fails, assume the config is a JSON string.
		cfgFile, err := os.Open(cfg.DiscoConfig)
		if err != nil {
			reader = bytes.NewReader([]byte(cfg.DiscoConfig))
		} else {
			reader = cfgFile
			defer cfgFile.Close()
		}
	}

	if cfg.DiscoMode == DiscoModeConsulKV {
		var consulCfg *consul.Config
		if reader != nil {
			consulCfg, err = consul.NewConfigFromReader(reader)
			if err != nil {
				return nil, err
			}
		}

		c, err = consul.New(cfg.DiscoKey, consulCfg)
		if err != nil {
			return nil, err
		}
	} else if cfg.DiscoMode == DiscoModeEtcdKV {
		var etcdCfg *etcd.Config
		if reader != nil {
			etcdCfg, err = etcd.NewConfigFromReader(reader)
			if err != nil {
				return nil, err
			}
		}

		c, err = etcd.New(cfg.DiscoKey, etcdCfg)
		if err != nil {
			return nil, err
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

func createCluster(cfg *Config, joins []string, tlsConfig *tls.Config, hasPeers bool, str *store.Store,
	httpServ *httpd.Service, credStr *auth.CredentialsStore) error {
	if len(joins) == 0 && cfg.DiscoMode == "" && !hasPeers {
		// Brand new node, told to bootstrap itself. So do it.
		log.Println("bootstraping single new node")
		if err := str.Bootstrap(store.NewServer(str.ID(), str.Addr(), true)); err != nil {
			return fmt.Errorf("failed to bootstrap single new node: %s", err.Error())
		}
		return nil
	}

	if len(joins) > 0 {
		if cfg.BootstrapExpect == 0 {
			// Explicit join operation requested, so do it.
			log.Println("explicit join addresses are:", joins)

			if err := addJoinCreds(joins, cfg.JoinAs, credStr); err != nil {
				return fmt.Errorf("failed too add auth creds: %s", err.Error())
			}
			j, err := cluster.Join(cfg.JoinSrcIP, joins, str.ID(), cfg.RaftAdv, !cfg.RaftNonVoter,
				cfg.JoinAttempts, cfg.JoinInterval, tlsConfig)
			if err != nil {
				return fmt.Errorf("failed to join cluster at %s: %s", joins, err.Error())
			}
			log.Println("successfully joined cluster at", j, cfg.RaftAdv, cfg.JoinAttempts,
				cfg.JoinInterval, tlsConfig)
			return nil
		}

		if hasPeers {
			log.Println("preexisting node configuration detected, ignoring bootstrap request")
			return nil
		}

		// Must self-notify when bootstrapping
		targets := append(joins, cfg.HTTPAdv)
		log.Println("bootstrap addresses are:", targets)
		if err := addJoinCreds(targets, cfg.JoinAs, credStr); err != nil {
			return fmt.Errorf("failed too add auth creds: %s", err.Error())
		}
		bs := cluster.NewBootstrapper(cluster.NewAddressProviderString(targets),
			cfg.BootstrapExpect, tlsConfig)

		done := func() bool {
			leader, _ := str.LeaderAddr()
			return leader != ""
		}
		return bs.Boot(str.ID(), cfg.RaftAdv, done, cfg.BootstrapExpectTimeout)
	}

	if cfg.DiscoMode == "" {
		// No more clustering techniques to try. Node will just sit, probably using
		// existing Raft state.
		return nil
	}

	log.Printf("discovery mode: %s", cfg.DiscoMode)
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
				if err := addJoinCreds([]string{addr}, cfg.JoinAs, credStr); err != nil {
					return fmt.Errorf("failed too add auth creds: %s", err.Error())
				}

				if j, err := cluster.Join(cfg.JoinSrcIP, []string{addr}, str.ID(), cfg.RaftAdv, !cfg.RaftNonVoter,
					cfg.JoinAttempts, cfg.JoinInterval, tlsConfig); err != nil {
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
	return nil
}

// addJoinCreds adds credentials to any join addresses, if necessary.
func addJoinCreds(joins []string, joinAs string, credStr *auth.CredentialsStore) error {
	if credStr == nil || joinAs == "" {
		return nil
	}

	pw, ok := credStr.Password(joinAs)
	if !ok {
		return fmt.Errorf("user %s does not exist in credential store", joinAs)
	}

	var err error
	for i := range joins {
		joins[i], err = httpd.AddBasicAuth(joins[i], joinAs, pw)
		if err != nil {
			return fmt.Errorf("failed to use credential store join_as: %s", err.Error())
		}
	}
	return nil
}
