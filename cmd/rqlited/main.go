// Command rqlited is the rqlite server.
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"strings"
	"syscall"
	"time"

	consul "github.com/rqlite/rqlite-disco-clients/consul"
	"github.com/rqlite/rqlite-disco-clients/dns"
	"github.com/rqlite/rqlite-disco-clients/dnssrv"
	etcd "github.com/rqlite/rqlite-disco-clients/etcd"
	"github.com/rqlite/rqlite/v8/auth"
	"github.com/rqlite/rqlite/v8/auto/backup"
	"github.com/rqlite/rqlite/v8/auto/restore"
	"github.com/rqlite/rqlite/v8/aws"
	"github.com/rqlite/rqlite/v8/cluster"
	"github.com/rqlite/rqlite/v8/cmd"
	"github.com/rqlite/rqlite/v8/db"
	"github.com/rqlite/rqlite/v8/disco"
	httpd "github.com/rqlite/rqlite/v8/http"
	"github.com/rqlite/rqlite/v8/otel"
	"github.com/rqlite/rqlite/v8/rtls"
	"github.com/rqlite/rqlite/v8/store"
	"github.com/rqlite/rqlite/v8/tcp"
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
storage engine. It provides an easy-to-use, fault-tolerant store for relational data.

Visit https://www.rqlite.io to learn more.`

func init() {
	log.SetFlags(log.LstdFlags)
	log.SetOutput(os.Stderr)
	log.SetPrefix(fmt.Sprintf("[%s] ", name))
}

func main() {
	// Handle signals first, so signal handling is established before anything else.
	sigCh := HandleSignals(syscall.SIGINT, syscall.SIGTERM, os.Interrupt)

	cfg, err := ParseFlags(name, desc, &BuildInfo{
		Version:       cmd.Version,
		Commit:        cmd.Commit,
		Branch:        cmd.Branch,
		SQLiteVersion: db.DBVersion,
	})
	if err != nil {
		log.Fatalf("failed to parse command-line flags: %s", err.Error())
	}
	fmt.Print(logo)

	mainCtx, mainCancel := context.WithCancel(context.Background())
	defer mainCancel()

	// Configure logging and pump out initial message.
	log.Printf("%s starting, version %s, SQLite %s, commit %s, branch %s, compiler %s", name, cmd.Version,
		db.DBVersion, cmd.Commit, cmd.Branch, runtime.Compiler)
	log.Printf("%s, target architecture is %s, operating system target is %s", runtime.Version(),
		runtime.GOARCH, runtime.GOOS)
	log.Printf("launch command: %s", strings.Join(os.Args, " "))

	// Configure span export
	if cfg.OTLPDest != "" {
		cleanup, err := otel.Setup(mainCtx, cfg.OTLPDest)
		if err != nil {
			log.Printf("%v", err)
		}
		defer func() {
			if err := cleanup(); err != nil {
				log.Printf("%v", err)
			}
		}()
	}

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

	// Raft internode layer
	raftLn := mux.Listen(cluster.MuxRaftHeader)
	log.Printf("Raft TCP mux Listener registered with byte header %d", cluster.MuxRaftHeader)
	raftDialer, err := cluster.CreateRaftDialer(cfg.NodeX509Cert, cfg.NodeX509Key, cfg.NodeX509CACert,
		cfg.NodeVerifyServerName, cfg.NoNodeVerify)
	if err != nil {
		log.Fatalf("failed to create Raft dialer: %s", err.Error())
	}
	raftTn := tcp.NewLayer(raftLn, raftDialer)

	// Create the store.
	str, err := createStore(cfg, raftTn)
	if err != nil {
		log.Fatalf("failed to create store: %s", err.Error())
	}

	// Install the auto-restore data, if necessary.
	if cfg.AutoRestoreFile != "" {
		hd, err := store.HasData(str.Path())
		if err != nil {
			log.Fatalf("failed to check for existing data: %s", err.Error())
		}
		if hd {
			log.Printf("auto-restore requested, but data already exists in %s, skipping", str.Path())
		} else {
			log.Printf("auto-restore requested, initiating download")
			start := time.Now()
			path, errOK, err := restore.DownloadFile(mainCtx, cfg.AutoRestoreFile)
			if err != nil {
				var b strings.Builder
				b.WriteString(fmt.Sprintf("failed to download auto-restore file: %s", err.Error()))
				if errOK {
					b.WriteString(", continuing with node startup anyway")
					log.Print(b.String())
				} else {
					log.Fatal(b.String())
				}
			} else {
				log.Printf("auto-restore file downloaded in %s", time.Since(start))
				if err := str.SetRestorePath(path); err != nil {
					log.Fatalf("failed to preload auto-restore data: %s", err.Error())
				}
			}
		}
	}

	// Get any credential store.
	credStr, err := credentialStore(cfg)
	if err != nil {
		log.Fatalf("failed to get credential store: %s", err.Error())
	}

	// Create cluster service now, so nodes will be able to learn information about each other.
	clstrServ, err := clusterService(cfg, mux.Listen(cluster.MuxClusterHeader), str, str, credStr)
	if err != nil {
		log.Fatalf("failed to create cluster service: %s", err.Error())
	}
	log.Printf("cluster TCP mux Listener registered with byte header %d", cluster.MuxClusterHeader)

	// Create the HTTP service.
	//
	// We want to start the HTTP server as soon as possible, so the node is responsive and external
	// systems can see that it's running. We still have to open the Store though, so the node won't
	// be able to do much until that happens however.
	clstrClient, err := createClusterClient(cfg, clstrServ)
	if err != nil {
		log.Fatalf("failed to create cluster client: %s", err.Error())
	}
	httpServ, err := startHTTPService(cfg, str, clstrClient, credStr)
	if err != nil {
		log.Fatalf("failed to start HTTP server: %s", err.Error())
	}

	// Now, open store. How long this takes does depend on how much data is being stored by rqlite.
	if err := str.Open(); err != nil {
		log.Fatalf("failed to open store: %s", err.Error())
	}

	// Register remaining status providers.
	httpServ.RegisterStatus("cluster", clstrServ)
	httpServ.RegisterStatus("network", tcp.NetworkReporter{})

	// Create the cluster!
	nodes, err := str.Nodes()
	if err != nil {
		log.Fatalf("failed to get nodes %s", err.Error())
	}
	if err := createCluster(cfg, len(nodes) > 0, clstrClient, str, httpServ, credStr); err != nil {
		log.Fatalf("clustering failure: %s", err.Error())
	}

	// Tell the user the node is ready for HTTP, giving some advice on how to connect.
	log.Printf("node HTTP API available at %s", cfg.HTTPURL())
	h, p, _ := net.SplitHostPort(cfg.HTTPAdv)
	log.Printf("connect using the command-line tool via 'rqlite -H %s -p %s'", h, p)

	// Start any requested auto-backups
	backupSrvStx, backupSrvCancel := context.WithCancel(mainCtx)
	backupSrv, err := startAutoBackups(backupSrvStx, cfg, str)
	if err != nil {
		log.Fatalf("failed to start auto-backups: %s", err.Error())
	}
	if backupSrv != nil {
		httpServ.RegisterStatus("auto_backups", backupSrv)
	}

	// Block until signalled.
	<-sigCh

	// Stop the HTTP server first, so clients get notification as soon as
	// possible that the node is going away.
	httpServ.Close()

	if cfg.RaftClusterRemoveOnShutdown {
		remover := cluster.NewRemover(clstrClient, 5*time.Second, str)
		remover.SetCredentials(cluster.CredentialsFor(credStr, cfg.JoinAs))
		log.Printf("initiating removal of this node from cluster before shutdown")
		if err := remover.Do(cfg.NodeID, true); err != nil {
			log.Fatalf("failed to remove this node from cluster before shutdown: %s", err.Error())
		}
		log.Printf("removed this node successfully from cluster before shutdown")
	}

	if cfg.RaftStepdownOnShutdown {
		if str.IsLeader() {
			// Don't log a confusing message if (probably) not Leader
			log.Printf("stepping down as Leader before shutdown")
		}
		// Perform a stepdown, ignore any errors.
		str.Stepdown(true)
	}

	backupSrvCancel()
	if err := str.Close(true); err != nil {
		log.Printf("failed to close store: %s", err.Error())
	}
	clstrServ.Close()
	muxLn.Close()
	stopProfile()
	log.Println("rqlite server stopped")
}

func startAutoBackups(ctx context.Context, cfg *Config, str *store.Store) (*backup.Uploader, error) {
	if cfg.AutoBackupFile == "" {
		return nil, nil
	}

	b, err := backup.ReadConfigFile(cfg.AutoBackupFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read auto-backup file: %s", err.Error())
	}

	uCfg, s3cfg, err := backup.Unmarshal(b)
	if err != nil {
		return nil, fmt.Errorf("failed to parse auto-backup file: %s", err.Error())
	}
	provider := store.NewProvider(str, uCfg.Vacuum, !uCfg.NoCompress)
	sc, err := aws.NewS3Client(s3cfg.Endpoint, s3cfg.Region, s3cfg.AccessKeyID, s3cfg.SecretAccessKey,
		s3cfg.Bucket, s3cfg.Path, s3cfg.ForcePathStyle)
	if err != nil {
		return nil, fmt.Errorf("failed to create aws S3 client: %s", err.Error())
	}
	u := backup.NewUploader(sc, provider, time.Duration(uCfg.Interval), !uCfg.NoCompress)
	u.Start(ctx, str.IsLeader)
	return u, nil
}

func createStore(cfg *Config, ln *tcp.Layer) (*store.Store, error) {
	dbConf := store.NewDBConfig()
	dbConf.OnDiskPath = cfg.OnDiskPath
	dbConf.FKConstraints = cfg.FKConstraints

	str := store.New(ln, &store.Config{
		DBConf: dbConf,
		Dir:    cfg.DataPath,
		ID:     cfg.NodeID,
	})

	// Set optional parameters on store.
	str.RaftLogLevel = cfg.RaftLogLevel
	str.ShutdownOnRemove = cfg.RaftShutdownOnRemove
	str.SnapshotThreshold = cfg.RaftSnapThreshold
	str.SnapshotThresholdWALSize = cfg.RaftSnapThresholdWALSize
	str.SnapshotInterval = cfg.RaftSnapInterval
	str.LeaderLeaseTimeout = cfg.RaftLeaderLeaseTimeout
	str.HeartbeatTimeout = cfg.RaftHeartbeatTimeout
	str.ElectionTimeout = cfg.RaftElectionTimeout
	str.ApplyTimeout = cfg.RaftApplyTimeout
	str.BootstrapExpect = cfg.BootstrapExpect
	str.ReapTimeout = cfg.RaftReapNodeTimeout
	str.ReapReadOnlyTimeout = cfg.RaftReapReadOnlyNodeTimeout
	str.AutoVacInterval = cfg.AutoVacInterval

	if store.IsNewNode(cfg.DataPath) {
		log.Printf("no preexisting node state detected in %s, node may be bootstrapping", cfg.DataPath)
	} else {
		log.Printf("preexisting node state detected in %s", cfg.DataPath)
	}

	return str, nil
}

func createDiscoService(cfg *Config, str *store.Store) (*disco.Service, error) {
	var c disco.Client
	var err error

	rc := cfg.DiscoConfigReader()
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
	return disco.NewService(c, str, disco.VoterSuffrage(!cfg.RaftNonVoter)), nil
}

func startHTTPService(cfg *Config, str *store.Store, cltr *cluster.Client, credStr *auth.CredentialsStore) (*httpd.Service, error) {
	// Create HTTP server and load authentication information.
	s := httpd.New(cfg.HTTPAddr, str, cltr, credStr)

	s.CACertFile = cfg.HTTPx509CACert
	s.CertFile = cfg.HTTPx509Cert
	s.KeyFile = cfg.HTTPx509Key
	s.ClientVerify = cfg.HTTPVerifyClient
	s.DefaultQueueCap = cfg.WriteQueueCap
	s.DefaultQueueBatchSz = cfg.WriteQueueBatchSz
	s.DefaultQueueTimeout = cfg.WriteQueueTimeout
	s.DefaultQueueTx = cfg.WriteQueueTx
	s.AllowOrigin = cfg.HTTPAllowOrigin
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
	var err error
	adv := tcp.NameAddress{
		Address: cfg.RaftAdv,
	}

	var mux *tcp.Mux
	if cfg.NodeX509Cert != "" {
		var b strings.Builder
		b.WriteString(fmt.Sprintf("enabling node-to-node encryption with cert: %s, key: %s",
			cfg.NodeX509Cert, cfg.NodeX509Key))
		if cfg.NodeX509CACert != "" {
			b.WriteString(fmt.Sprintf(", CA cert %s", cfg.NodeX509CACert))
		}
		if cfg.NodeVerifyClient {
			b.WriteString(", mutual TLS enabled")
		} else {
			b.WriteString(", mutual TLS disabled")
		}
		log.Println(b.String())
		mux, err = tcp.NewTLSMux(ln, adv, cfg.NodeX509Cert, cfg.NodeX509Key, cfg.NodeX509CACert,
			cfg.NoNodeVerify, cfg.NodeVerifyClient)
	} else {
		mux, err = tcp.NewMux(ln, adv)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create node-to-node mux: %s", err.Error())
	}
	go mux.Serve()
	return mux, nil
}

func credentialStore(cfg *Config) (*auth.CredentialsStore, error) {
	if cfg.AuthFile == "" {
		return nil, nil
	}
	return auth.NewCredentialsStoreFromFile(cfg.AuthFile)
}

func clusterService(cfg *Config, ln net.Listener, db cluster.Database, mgr cluster.Manager, credStr *auth.CredentialsStore) (*cluster.Service, error) {
	c := cluster.New(ln, db, mgr, credStr)
	c.SetAPIAddr(cfg.HTTPAdv)
	c.EnableHTTPS(cfg.HTTPx509Cert != "" && cfg.HTTPx509Key != "") // Conditions met for an HTTPS API
	if err := c.Open(); err != nil {
		return nil, err
	}
	return c, nil
}

func createClusterClient(cfg *Config, clstr *cluster.Service) (*cluster.Client, error) {
	var dialerTLSConfig *tls.Config
	var err error
	if cfg.NodeX509Cert != "" || cfg.NodeX509CACert != "" {
		dialerTLSConfig, err = rtls.CreateClientConfig(cfg.NodeX509Cert, cfg.NodeX509Key,
			cfg.NodeX509CACert, cfg.NodeVerifyServerName, cfg.NoNodeVerify)
		if err != nil {
			return nil, fmt.Errorf("failed to create TLS config for cluster dialer: %s", err.Error())
		}
	}
	clstrDialer := tcp.NewDialer(cluster.MuxClusterHeader, dialerTLSConfig)
	clstrClient := cluster.NewClient(clstrDialer, cfg.ClusterConnectTimeout)
	if err := clstrClient.SetLocal(cfg.RaftAdv, clstr); err != nil {
		return nil, fmt.Errorf("failed to set cluster client local parameters: %s", err.Error())
	}
	return clstrClient, nil
}

func createCluster(cfg *Config, hasPeers bool, client *cluster.Client, str *store.Store, httpServ *httpd.Service, credStr *auth.CredentialsStore) error {
	joins := cfg.JoinAddresses()
	if err := networkCheckJoinAddrs(joins); err != nil {
		return err
	}
	if joins == nil && cfg.DiscoMode == "" && !hasPeers {
		if cfg.RaftNonVoter {
			return fmt.Errorf("cannot create a new non-voting node without joining it to an existing cluster")
		}

		// Brand new node, told to bootstrap itself. So do it.
		log.Println("bootstraping single new node")
		if err := str.Bootstrap(store.NewServer(str.ID(), cfg.RaftAdv, true)); err != nil {
			return fmt.Errorf("failed to bootstrap single new node: %s", err.Error())
		}
		return nil
	}

	// Prepare definition of being part of a cluster.
	bootDoneFn := func() bool {
		leader, _ := str.LeaderAddr()
		return leader != ""
	}
	clusterSuf := cluster.VoterSuffrage(!cfg.RaftNonVoter)

	joiner := cluster.NewJoiner(client, cfg.JoinAttempts, cfg.JoinInterval)
	joiner.SetCredentials(cluster.CredentialsFor(credStr, cfg.JoinAs))
	if joins != nil && cfg.BootstrapExpect == 0 {
		// Explicit join operation requested, so do it.
		j, err := joiner.Do(joins, str.ID(), cfg.RaftAdv, clusterSuf)
		if err != nil {
			return fmt.Errorf("failed to join cluster: %s", err.Error())
		}
		log.Println("successfully joined cluster at", j)
		return nil
	}

	if joins != nil && cfg.BootstrapExpect > 0 {
		// Bootstrap with explicit join addresses requests.
		bs := cluster.NewBootstrapper(cluster.NewAddressProviderString(joins), client)
		bs.SetCredentials(cluster.CredentialsFor(credStr, cfg.JoinAs))
		return bs.Boot(str.ID(), cfg.RaftAdv, clusterSuf, bootDoneFn, cfg.BootstrapExpectTimeout)
	}

	if cfg.DiscoMode == "" {
		// No more clustering techniques to try. Node will just sit, probably using
		// existing Raft state.
		return nil
	}

	// DNS-based discovery requested. It's OK to proceed with this even if this node
	// is already part of a cluster. Re-joining and re-notifying other nodes will be
	// ignored when the node is already part of the cluster.
	log.Printf("discovery mode: %s", cfg.DiscoMode)
	switch cfg.DiscoMode {
	case DiscoModeDNS, DiscoModeDNSSRV:
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
			provider = dns.NewWithPort(dnsCfg, cfg.RaftPort())

		} else {
			dnssrvCfg, err := dnssrv.NewConfigFromReader(rc)
			if err != nil {
				return fmt.Errorf("error reading DNS configuration: %s", err.Error())
			}
			provider = dnssrv.New(dnssrvCfg)
		}

		bs := cluster.NewBootstrapper(provider, client)
		bs.SetCredentials(cluster.CredentialsFor(credStr, cfg.JoinAs))
		httpServ.RegisterStatus("disco", provider)
		return bs.Boot(str.ID(), cfg.RaftAdv, clusterSuf, bootDoneFn, cfg.BootstrapExpectTimeout)

	case DiscoModeEtcdKV, DiscoModeConsulKV:
		discoService, err := createDiscoService(cfg, str)
		if err != nil {
			return fmt.Errorf("failed to start discovery service: %s", err.Error())
		}
		// Safe to start reporting before doing registration. If the node hasn't bootstrapped
		// yet, or isn't leader, reporting will just be a no-op until something changes.
		go discoService.StartReporting(cfg.NodeID, cfg.HTTPURL(), cfg.RaftAdv)
		httpServ.RegisterStatus("disco", discoService)

		if hasPeers {
			log.Printf("preexisting node configuration detected, not registering with discovery service")
			return nil
		}
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
				if j, err := joiner.Do([]string{addr}, str.ID(), cfg.RaftAdv, clusterSuf); err != nil {
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

	default:
		return fmt.Errorf("invalid disco mode %s", cfg.DiscoMode)
	}
	return nil
}

func networkCheckJoinAddrs(joinAddrs []string) error {
	log.Println("checking that any supplied join addresses don't serve HTTP(S)")
	if addr, ok := httpd.AnyServingHTTP(joinAddrs); ok {
		return fmt.Errorf("join address %s appears to be serving HTTP when it should be Raft", addr)
	}
	return nil
}
