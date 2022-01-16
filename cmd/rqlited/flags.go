package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"time"
)

type Config struct {
	DataPath               string
	HTTPAddr               string
	HTTPAdv                string
	TLS1011                bool
	AuthFile               string
	X509CACert             string
	X509Cert               string
	X509Key                string
	NodeEncrypt            bool
	NodeX509CACert         string
	NodeX509Cert           string
	NodeX509Key            string
	NodeID                 string
	RaftAddr               string
	RaftAdv                string
	JoinSrcIP              string
	JoinAddr               string
	JoinAs                 string
	JoinAttempts           int
	JoinInterval           time.Duration
	NoVerify               bool
	NoNodeVerify           bool
	DiscoMode              string
	DiscoKey               string
	DiscoConfig            string
	Expvar                 bool
	PprofEnabled           bool
	OnDisk                 bool
	OnDiskPath             string
	OnDiskStartup          bool
	FKConstraints          bool
	RaftLogLevel           string
	RaftNonVoter           bool
	RaftSnapThreshold      uint64
	RaftSnapInterval       time.Duration
	RaftLeaderLeaseTimeout time.Duration
	RaftHeartbeatTimeout   time.Duration
	RaftElectionTimeout    time.Duration
	RaftApplyTimeout       time.Duration
	RaftOpenTimeout        time.Duration
	RaftWaitForLeader      bool
	RaftShutdownOnRemove   bool
	CompressionSize        int
	CompressionBatch       int
	ShowVersion            bool
	CPUProfile             string
	MemProfile             string
}

type BuildInfo struct {
	Version string
	Commit  string
	Branch  string
}

func ParseFlags(name, desc string, build *BuildInfo) (*Config, error) {
	if flag.Parsed() {
		return nil, fmt.Errorf("command-line flags already parsed")
	}
	config := Config{}

	flag.StringVar(&config.NodeID, "node-id", "", "Unique name for node. If not set, set to advertised Raft address")
	flag.StringVar(&config.HTTPAddr, "http-addr", "localhost:4001", "HTTP server bind address. For HTTPS, set X.509 cert and key")
	flag.StringVar(&config.HTTPAdv, "http-adv-addr", "", "Advertised HTTP address. If not set, same as HTTP server bind")
	flag.BoolVar(&config.TLS1011, "tls1011", false, "Support deprecated TLS versions 1.0 and 1.1")
	flag.StringVar(&config.X509CACert, "http-ca-cert", "", "Path to root X.509 certificate for HTTP endpoint")
	flag.StringVar(&config.X509Cert, "http-cert", "", "Path to X.509 certificate for HTTP endpoint")
	flag.StringVar(&config.X509Key, "http-key", "", "Path to X.509 private key for HTTP endpoint")
	flag.BoolVar(&config.NoVerify, "http-no-verify", false, "Skip verification of remote HTTPS cert when joining cluster")
	flag.BoolVar(&config.NodeEncrypt, "node-encrypt", false, "Enable node-to-node encryption")
	flag.StringVar(&config.NodeX509CACert, "node-ca-cert", "", "Path to root X.509 certificate for node-to-node encryption")
	flag.StringVar(&config.NodeX509Cert, "node-cert", "cert.pem", "Path to X.509 certificate for node-to-node encryption")
	flag.StringVar(&config.NodeX509Key, "node-key", "key.pem", "Path to X.509 private key for node-to-node encryption")
	flag.BoolVar(&config.NoNodeVerify, "node-no-verify", false, "Skip verification of a remote node cert")
	flag.StringVar(&config.AuthFile, "auth", "", "Path to authentication and authorization file. If not set, not enabled")
	flag.StringVar(&config.RaftAddr, "raft-addr", "localhost:4002", "Raft communication bind address")
	flag.StringVar(&config.RaftAdv, "raft-adv-addr", "", "Advertised Raft communication address. If not set, same as Raft bind")
	flag.StringVar(&config.JoinSrcIP, "join-source-ip", "", "Set source IP address during Join request")
	flag.StringVar(&config.JoinAddr, "join", "", "Comma-delimited list of nodes, through which a cluster can be joined (proto://host:port)")
	flag.StringVar(&config.JoinAs, "join-as", "", "Username in authentication file to join as. If not set, joins anonymously")
	flag.IntVar(&config.JoinAttempts, "join-attempts", 5, "Number of join attempts to make")
	flag.DurationVar(&config.JoinInterval, "join-interval", 5*time.Second, "Period between join attempts")
	flag.StringVar(&config.DiscoMode, "disco-mode", "", "Choose cluster discovery service. If not set, not used")
	flag.StringVar(&config.DiscoKey, "disco-key", "rqlite", "Key prefix for cluster discovery service")
	flag.StringVar(&config.DiscoConfig, "disco-config", "", "Set path to cluster discovery config file")
	flag.BoolVar(&config.Expvar, "expvar", true, "Serve expvar data on HTTP server")
	flag.BoolVar(&config.PprofEnabled, "pprof", true, "Serve pprof data on HTTP server")
	flag.BoolVar(&config.OnDisk, "on-disk", false, "Use an on-disk SQLite database")
	flag.StringVar(&config.OnDiskPath, "on-disk-path", "", "Path for SQLite on-disk database file. If not set, use file in data directory")
	flag.BoolVar(&config.OnDiskStartup, "on-disk-startup", false, "Do not initialize on-disk database in memory first at startup")
	flag.BoolVar(&config.FKConstraints, "fk", false, "Enable SQLite foreign key constraints")
	flag.BoolVar(&config.ShowVersion, "version", false, "Show version information and exit")
	flag.BoolVar(&config.RaftNonVoter, "raft-non-voter", false, "Configure as non-voting node")
	flag.DurationVar(&config.RaftHeartbeatTimeout, "raft-timeout", time.Second, "Raft heartbeat timeout")
	flag.DurationVar(&config.RaftElectionTimeout, "raft-election-timeout", time.Second, "Raft election timeout")
	flag.DurationVar(&config.RaftApplyTimeout, "raft-apply-timeout", 10*time.Second, "Raft apply timeout")
	flag.DurationVar(&config.RaftOpenTimeout, "raft-open-timeout", 120*time.Second, "Time for initial Raft logs to be applied. Use 0s duration to skip wait")
	flag.BoolVar(&config.RaftWaitForLeader, "raft-leader-wait", true, "Node waits for a leader before answering requests")
	flag.Uint64Var(&config.RaftSnapThreshold, "raft-snap", 8192, "Number of outstanding log entries that trigger snapshot")
	flag.DurationVar(&config.RaftSnapInterval, "raft-snap-int", 30*time.Second, "Snapshot threshold check interval")
	flag.DurationVar(&config.RaftLeaderLeaseTimeout, "raft-leader-lease-timeout", 0, "Raft leader lease timeout. Use 0s for Raft default")
	flag.BoolVar(&config.RaftShutdownOnRemove, "raft-remove-shutdown", false, "Shutdown Raft if node removed")
	flag.StringVar(&config.RaftLogLevel, "raft-log-level", "INFO", "Minimum log level for Raft module")
	flag.IntVar(&config.CompressionSize, "compression-size", 150, "Request query size for compression attempt")
	flag.IntVar(&config.CompressionBatch, "compression-batch", 5, "Request batch threshold for compression attempt")
	flag.StringVar(&config.CPUProfile, "cpu-profile", "", "Path to file for CPU profiling information")
	flag.StringVar(&config.MemProfile, "mem-profile", "", "Path to file for memory profiling information")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "\n%s\n\n", desc)
		fmt.Fprintf(os.Stderr, "Usage: %s [flags] <data directory>\n", name)
		flag.PrintDefaults()
	}

	if config.ShowVersion {
		fmt.Printf("%s %s %s %s %s (commit %s, branch %s, compiler %s)\n",
			name, build.Version, runtime.GOOS, runtime.GOARCH, runtime.Version(), build.Commit, build.Branch, runtime.Compiler)
		os.Exit(0)
	}

	if config.OnDiskPath != "" && !config.OnDisk {
		fmt.Fprintf(os.Stderr, "fatal: on-disk-path is set, but on-disk is not\n")
		os.Exit(1)
	}

	// Ensure the data path is set.
	if flag.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "fatal: no data directory set\n")
		os.Exit(1)
	}
	config.DataPath = flag.Arg(0)

	// Ensure no args come after the data directory.
	if flag.NArg() > 1 {
		fmt.Fprintf(os.Stderr, "fatal: arguments after data directory are not accepted\n")
		os.Exit(1)
	}

	// Enforce policies regarding addresses
	if config.RaftAdv == "" {
		config.RaftAdv = config.RaftAddr
	}
	if config.HTTPAdv == "" {
		config.HTTPAdv = config.HTTPAddr
	}

	// Node ID policy
	if config.NodeID == "" {
		config.NodeID = config.RaftAdv
	}

	flag.Parse()
	return &config, nil
}
