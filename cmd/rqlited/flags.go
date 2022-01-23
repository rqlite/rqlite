package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"
	"time"
)

const (
	DiscoModeNone     = ""
	DiscoModeConsulKV = "consul-kv"
	DiscoModeEtcdKV   = "etcd-kv"
)

// Config represents the configuration as set by command-line flags.
// All variables will be set, unless explicit noted.
type Config struct {
	// DataPath is path to node data. Always set.
	DataPath string

	// HTTPAddr is the bind network address for the HTTP Server.
	// It never includes a trailing HTTP or HTTPS.
	HTTPAddr string

	// HTTPAdv is the advertised HTTP server network.
	HTTPAdv string

	// TLS1011 indicates whether the node should support deprecated
	// encryption standards.
	TLS1011 bool

	// AuthFile is the path to the authentication file. May not be set.
	AuthFile string

	// X509CACert is the path the root-CA certficate file for when this
	// node contacts other nodes' HTTP servers. May not be set.
	X509CACert string

	// X509Cert is the path to the X509 cert for the HTTP server. May not be set.
	X509Cert string

	// X509Key is the path to the private key for the HTTP server. May not be set.
	X509Key string

	// NodeEncrypt indicates whether node encryption should be enabled.
	NodeEncrypt bool

	// NodeX509CACert is the path the root-CA certficate file for when this
	// node contacts other nodes' Raft servers. May not be set.
	NodeX509CACert string

	// NodeX509Cert is the path to the X509 cert for the Raft server. May not be set.
	NodeX509Cert string

	// NodeX509Key is the path to the X509 key for the Raft server. May not be set.
	NodeX509Key string

	// NodeID is the Raft ID for the node.
	NodeID string

	// RaftAddr is the bind network address for the Raft server.
	RaftAddr string

	// RaftAdv is the advertised Raft server address.
	RaftAdv string

	// JoinSrcIP sets the source IP address during Join request. May not be set.
	JoinSrcIP string

	// JoinAddr is the list addresses to use for a join attempt. Each address
	// will include the proto (HTTP or HTTPS) and will never include the node's
	// own HTTP server address. May not be set.
	JoinAddr string

	// JoinAs sets the user join attempts should be performed as. May not be set.
	JoinAs string

	// JoinAttempts is the number of times a node should attempt to join using a
	// given address.
	JoinAttempts int

	// JoinInterval is the time between retrying failed join operations.
	JoinInterval time.Duration

	// NoHTTPVerify disables checking other nodes' HTTP X509 certs for validity.
	NoHTTPVerify bool

	// NoNodeVerify disables checking other nodes' Node X509 certs for validity.
	NoNodeVerify bool

	// DisoMode sets the discovery mode. May not be set.
	DiscoMode string

	// DiscoKey sets the discovery prefix key.
	DiscoKey string

	// DiscoConfig sets the path to any discovery configuration file. May not be set.
	DiscoConfig string

	// Expvar enables go/expvar information. Defaults to true.
	Expvar bool

	// PprofEnabled enables Go PProf information. Defaults to true.
	PprofEnabled bool

	// OnDisk enables on-disk mode.
	OnDisk bool

	// OnDiskPath sets the path to the SQLite file. May not be set.
	OnDiskPath string

	// OnDiskStartup disables the in-memory on-disk startup optimization.
	OnDiskStartup bool

	// FKConstraints enables SQLite foreign key constraints.
	FKConstraints bool

	// RaftLogLevel sets the minimum logging level for the Raft subsystem.
	RaftLogLevel string

	// RaftNonVoter controls whether this node is a voting, read-only node.
	RaftNonVoter bool

	// RaftSnapThreshold is the number of outstanding log entries that trigger snapshot.
	RaftSnapThreshold uint64

	// RaftSnapInterval sets the threshold check interval.
	RaftSnapInterval time.Duration

	// RaftLeaderLeaseTimeout sets the leader lease timeout.
	RaftLeaderLeaseTimeout time.Duration

	// RaftHeartbeatTimeout sets the heartbeast timeout.
	RaftHeartbeatTimeout time.Duration

	// RaftElectionTimeout sets the election timeout.
	RaftElectionTimeout time.Duration

	// RaftApplyTimeout sets the Log-apply timeout.
	RaftApplyTimeout time.Duration

	// RaftOpenTimeout sets the Raft store open timeout.
	RaftOpenTimeout time.Duration

	// RaftShutdownOnRemove sets whether Raft should be shutdown if the node is removed
	RaftShutdownOnRemove bool

	// RaftNoFreelistSync disables syncing Raft database freelist to disk. When true,
	// it improves the database write performance under normal operation, but requires
	// a full database re-sync during recovery.
	RaftNoFreelistSync bool

	// CompressionSize sets request query size for compression attempt
	CompressionSize int

	// CompressionBatch sets request batch threshold for compression attempt.
	CompressionBatch int

	// ShowVersion instructs the node to show version information and exit.
	ShowVersion bool

	// CPUProfile enables CPU profiling.
	CPUProfile string

	// MemProfile enables memory profiling.
	MemProfile string
}

// HTTPURL returns the fully-formed, advertised HTTP API address for this config, including
// protocol, host and port.
func (c *Config) HTTPURL() string {
	apiProto := "http"
	if c.X509Cert != "" {
		apiProto = "https"
	}
	return fmt.Sprintf("%s://%s", apiProto, c.HTTPAdv)
}

// BuildInfo is build information for display at command line.
type BuildInfo struct {
	Version string
	Commit  string
	Branch  string
}

// ParseFlags parses the command line, and returns the configuration.
func ParseFlags(name, desc string, build *BuildInfo) (*Config, error) {
	if flag.Parsed() {
		return nil, fmt.Errorf("command-line flags already parsed")
	}
	config := Config{}

	flag.StringVar(&config.NodeID, "node-id", "", "Unique name for node. If not set, set to advertised Raft address")
	flag.StringVar(&config.HTTPAddr, "http-addr", "localhost:4001", "HTTP server bind address. To enable HTTPS, set X.509 cert and key")
	flag.StringVar(&config.HTTPAdv, "http-adv-addr", "", "Advertised HTTP address. If not set, same as HTTP server bind")
	flag.BoolVar(&config.TLS1011, "tls1011", false, "Support deprecated TLS versions 1.0 and 1.1")
	flag.StringVar(&config.X509CACert, "http-ca-cert", "", "Path to root X.509 certificate for HTTP endpoint")
	flag.StringVar(&config.X509Cert, "http-cert", "", "Path to X.509 certificate for HTTP endpoint")
	flag.StringVar(&config.X509Key, "http-key", "", "Path to X.509 private key for HTTP endpoint")
	flag.BoolVar(&config.NoHTTPVerify, "http-no-verify", false, "Skip verification of remote HTTPS cert when joining cluster")
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
	flag.Uint64Var(&config.RaftSnapThreshold, "raft-snap", 8192, "Number of outstanding log entries that trigger snapshot")
	flag.DurationVar(&config.RaftSnapInterval, "raft-snap-int", 30*time.Second, "Snapshot threshold check interval")
	flag.DurationVar(&config.RaftLeaderLeaseTimeout, "raft-leader-lease-timeout", 0, "Raft leader lease timeout. Use 0s for Raft default")
	flag.BoolVar(&config.RaftShutdownOnRemove, "raft-remove-shutdown", false, "Shutdown Raft if node removed")
	flag.BoolVar(&config.RaftNoFreelistSync, "raft-no-freelist-sync", false, "Do not sync Raft log database freelist to disk")
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
	flag.Parse()

	if config.ShowVersion {
		msg := fmt.Sprintf("%s %s %s %s %s (commit %s, branch %s, compiler %s)\n",
			name, build.Version, runtime.GOOS, runtime.GOARCH, runtime.Version(), build.Commit, build.Branch, runtime.Compiler)
		errorExit(0, msg)
	}

	if config.OnDiskPath != "" && !config.OnDisk {
		errorExit(1, "fatal: on-disk-path is set, but on-disk is not\n")
	}

	// Ensure the data path is set.
	if flag.NArg() < 1 {
		errorExit(1, "fatal: no data directory set\n")
	}
	config.DataPath = flag.Arg(0)

	// Ensure no args come after the data directory.
	if flag.NArg() > 1 {
		errorExit(1, "fatal: arguments after data directory are not accepted\n")
	}

	// Enforce policies regarding addresses
	if config.RaftAdv == "" {
		config.RaftAdv = config.RaftAddr
	}
	if config.HTTPAdv == "" {
		config.HTTPAdv = config.HTTPAddr
	}

	// Perfom some address validity checks.
	if strings.HasPrefix(strings.ToLower(config.HTTPAddr), "http") ||
		strings.HasPrefix(strings.ToLower(config.HTTPAdv), "http") {
		errorExit(1, "fatal: HTTP options should not include protocol (http:// or https://)\n")
	}
	if _, _, err := net.SplitHostPort(config.HTTPAddr); err != nil {
		errorExit(1, "HTTP bind address not valid")
	}
	if _, _, err := net.SplitHostPort(config.HTTPAdv); err != nil {
		errorExit(1, "HTTP advertised address not valid")
	}
	if _, _, err := net.SplitHostPort(config.RaftAddr); err != nil {
		errorExit(1, "Raft bind address not valid")
	}
	if _, _, err := net.SplitHostPort(config.RaftAdv); err != nil {
		errorExit(1, "Raft advertised address not valid")
	}

	// Valid disco mode?
	switch config.DiscoMode {
	case "":
	case DiscoModeConsulKV:
	case DiscoModeEtcdKV:
		break
	default:
		errorExit(1, fmt.Sprintf("fatal: invalid disco mode, choose %s or %s\n",
			DiscoModeConsulKV, DiscoModeEtcdKV))
	}

	// Node ID policy
	if config.NodeID == "" {
		config.NodeID = config.RaftAdv
	}

	return &config, nil
}

func errorExit(code int, msg string) {
	fmt.Fprintf(os.Stderr, msg)
	os.Exit(code)
}
