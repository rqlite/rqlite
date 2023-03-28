package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"runtime"
	"strings"
	"time"
)

const (
	DiscoModeNone     = ""
	DiscoModeConsulKV = "consul-kv"
	DiscoModeEtcdKV   = "etcd-kv"
	DiscoModeDNS      = "dns"
	DiscoModeDNSSRV   = "dns-srv"

	HTTPAddrFlag    = "http-addr"
	HTTPAdvAddrFlag = "http-adv-addr"
	RaftAddrFlag    = "raft-addr"
	RaftAdvAddrFlag = "raft-adv-addr"

	HTTPx509CertFlag = "http-cert"
	HTTPx509KeyFlag  = "http-key"
	NodeX509CertFlag = "node-cert"
	NodeX509KeyFlag  = "node-key"
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

	// HTTPx509CACert is the path to the CA certficate file for when this node verifies
	// other certificates for any HTTP communications. May not be set.
	HTTPx509CACert string

	// HTTPx509Cert is the path to the X509 cert for the HTTP server. May not be set.
	HTTPx509Cert string

	// HTTPx509Key is the path to the private key for the HTTP server. May not be set.
	HTTPx509Key string

	// NoHTTPVerify disables checking other nodes' server HTTP X509 certs for validity.
	NoHTTPVerify bool

	// HTTPVerifyClient indicates whether the HTTP server should verify client certificates.
	HTTPVerifyClient bool

	// NodeEncrypt indicates whether node encryption should be enabled.
	NodeEncrypt bool

	// NodeX509CACert is the path to the CA certficate file for when this node verifies
	// other certificates for any inter-node communications. May not be set.
	NodeX509CACert string

	// NodeX509Cert is the path to the X509 cert for the Raft server. May not be set.
	NodeX509Cert string

	// NodeX509Key is the path to the X509 key for the Raft server. May not be set.
	NodeX509Key string

	// NoNodeVerify disables checking other nodes' Node X509 certs for validity.
	NoNodeVerify bool

	// NodeVerifyClient indicates whether a node should verify client certificates from
	// other nodes.
	NodeVerifyClient bool

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

	// BootstrapExpect is the minimum number of nodes required for a bootstrap.
	BootstrapExpect int

	// BootstrapExpectTimeout is the maximum time a bootstrap operation can take.
	BootstrapExpectTimeout time.Duration

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

	// RaftHeartbeatTimeout sets the heartbeat timeout.
	RaftHeartbeatTimeout time.Duration

	// RaftElectionTimeout sets the election timeout.
	RaftElectionTimeout time.Duration

	// RaftApplyTimeout sets the Log-apply timeout.
	RaftApplyTimeout time.Duration

	// RaftShutdownOnRemove sets whether Raft should be shutdown if the node is removed
	RaftShutdownOnRemove bool

	// RaftStepdownOnShutdown sets whether Leadership should be relinquished on shutdown
	RaftStepdownOnShutdown bool

	// RaftNoFreelistSync disables syncing Raft database freelist to disk. When true,
	// it improves the database write performance under normal operation, but requires
	// a full database re-sync during recovery.
	RaftNoFreelistSync bool

	// RaftReapNodeTimeout sets the duration after which a non-reachable voting node is
	// reaped i.e. removed from the cluster.
	RaftReapNodeTimeout time.Duration

	// RaftReapReadOnlyNodeTimeout sets the duration after which a non-reachable non-voting node is
	// reaped i.e. removed from the cluster.
	RaftReapReadOnlyNodeTimeout time.Duration

	// ClusterConnectTimeout sets the timeout when initially connecting to another node in
	// the cluster, for non-Raft communications.
	ClusterConnectTimeout time.Duration

	// WriteQueueCap is the default capacity of Execute queues
	WriteQueueCap int

	// WriteQueueBatchSz is the default batch size for Execute queues
	WriteQueueBatchSz int

	// WriteQueueTimeout is the default time after which any data will be sent on
	// Execute queues, if a batch size has not been reached.
	WriteQueueTimeout time.Duration

	// WriteQueueTx controls whether writes from the queue are done within a transaction.
	WriteQueueTx bool

	// CompressionSize sets request query size for compression attempt
	CompressionSize int

	// CompressionBatch sets request batch threshold for compression attempt.
	CompressionBatch int

	// CPUProfile enables CPU profiling.
	CPUProfile string

	// MemProfile enables memory profiling.
	MemProfile string
}

// Validate checks the configuration for internal consistency, and activates
// important rqlite policies. It must be called at least once on a Config
// object before the Config object is used. It is OK to call more than
// once.
func (c *Config) Validate() error {
	if c.OnDiskPath != "" && !c.OnDisk {
		return errors.New("-on-disk-path is set, but -on-disk is not")
	}

	if !bothUnsetSet(c.HTTPx509Cert, c.HTTPx509Key) {
		return fmt.Errorf("either both -%s and -%s must be set, or neither", HTTPx509CertFlag, HTTPx509KeyFlag)
	}
	if !bothUnsetSet(c.NodeX509Cert, c.NodeX509Key) {
		return fmt.Errorf("either both -%s and -%s must be set, or neither", NodeX509CertFlag, NodeX509KeyFlag)

	}

	// Enforce policies regarding addresses
	if c.RaftAdv == "" {
		c.RaftAdv = c.RaftAddr
	}
	if c.HTTPAdv == "" {
		c.HTTPAdv = c.HTTPAddr
	}

	// Node ID policy
	if c.NodeID == "" {
		c.NodeID = c.RaftAdv
	}

	// Perfom some address validity checks.
	if strings.HasPrefix(strings.ToLower(c.HTTPAddr), "http") ||
		strings.HasPrefix(strings.ToLower(c.HTTPAdv), "http") {
		return errors.New("HTTP options should not include protocol (http:// or https://)")
	}
	if _, _, err := net.SplitHostPort(c.HTTPAddr); err != nil {
		return errors.New("HTTP bind address not valid")
	}

	hadv, _, err := net.SplitHostPort(c.HTTPAdv)
	if err != nil {
		return errors.New("HTTP advertised HTTP address not valid")
	}
	if addr := net.ParseIP(hadv); addr != nil && addr.IsUnspecified() {
		return fmt.Errorf("advertised HTTP address is not routable (%s), specify it via -%s or -%s",
			hadv, HTTPAddrFlag, HTTPAdvAddrFlag)
	}

	if _, _, err := net.SplitHostPort(c.RaftAddr); err != nil {
		return errors.New("raft bind address not valid")
	}

	radv, _, err := net.SplitHostPort(c.RaftAdv)
	if err != nil {
		return errors.New("raft advertised address not valid")
	}
	if addr := net.ParseIP(radv); addr != nil && addr.IsUnspecified() {
		return fmt.Errorf("advertised Raft address is not routable (%s), specify it via -%s or -%s",
			radv, RaftAddrFlag, RaftAdvAddrFlag)
	}

	// Enforce bootstrapping policies
	if c.BootstrapExpect > 0 && c.RaftNonVoter {
		return errors.New("bootstrapping only applicable to voting nodes")
	}

	// Join addresses OK?
	if c.JoinAddr != "" {
		addrs := strings.Split(c.JoinAddr, ",")
		for i := range addrs {
			u, err := url.Parse(addrs[i])
			if err != nil {
				return fmt.Errorf("%s is an invalid join adddress", addrs[i])
			}
			if c.BootstrapExpect == 0 {
				if u.Host == c.HTTPAdv || addrs[i] == c.HTTPAddr {
					return errors.New("node cannot join with itself unless bootstrapping")
				}
			}
		}
	}

	// Valid disco mode?
	switch c.DiscoMode {
	case "":
	case DiscoModeEtcdKV, DiscoModeConsulKV:
		if c.BootstrapExpect > 0 {
			return fmt.Errorf("bootstrapping not applicable when using %s", c.DiscoMode)
		}
	case DiscoModeDNS, DiscoModeDNSSRV:
		if c.BootstrapExpect == 0 {
			return fmt.Errorf("bootstrap-expect value required when using %s", c.DiscoMode)
		}
	default:
		return fmt.Errorf("disco mode must be one of %s, %s, %s, or %s",
			DiscoModeConsulKV, DiscoModeEtcdKV, DiscoModeDNS, DiscoModeDNSSRV)
	}

	return nil
}

// JoinAddresses returns the join addresses set at the command line. Returns nil
// if no join addresses were set.
func (c *Config) JoinAddresses() []string {
	if c.JoinAddr == "" {
		return nil
	}
	return strings.Split(c.JoinAddr, ",")
}

// HTTPURL returns the fully-formed, advertised HTTP API address for this config, including
// protocol, host and port.
func (c *Config) HTTPURL() string {
	apiProto := "http"
	if c.HTTPx509Cert != "" {
		apiProto = "https"
	}
	return fmt.Sprintf("%s://%s", apiProto, c.HTTPAdv)
}

// DiscoConfigReader returns a ReadCloser providing access to the Disco config.
// The caller must call close on the ReadCloser when finished with it. If no
// config was supplied, it returns nil.
func (c *Config) DiscoConfigReader() io.ReadCloser {
	var rc io.ReadCloser
	if c.DiscoConfig == "" {
		return nil
	}

	// Open config file. If opening fails, assume string is the literal config.
	cfgFile, err := os.Open(c.DiscoConfig)
	if err != nil {
		rc = io.NopCloser(bytes.NewReader([]byte(c.DiscoConfig)))
	} else {
		rc = cfgFile
	}
	return rc
}

// BuildInfo is build information for display at command line.
type BuildInfo struct {
	Version       string
	Commit        string
	Branch        string
	SQLiteVersion string
}

// ParseFlags parses the command line, and returns the configuration.
func ParseFlags(name, desc string, build *BuildInfo) (*Config, error) {
	if flag.Parsed() {
		return nil, fmt.Errorf("command-line flags already parsed")
	}
	config := &Config{}
	showVersion := false

	flag.StringVar(&config.NodeID, "node-id", "", "Unique name for node. If not set, set to advertised Raft address")
	flag.StringVar(&config.HTTPAddr, HTTPAddrFlag, "localhost:4001", "HTTP server bind address. To enable HTTPS, set X.509 certificate and key")
	flag.StringVar(&config.HTTPAdv, HTTPAdvAddrFlag, "", "Advertised HTTP address. If not set, same as HTTP server bind")
	flag.BoolVar(&config.TLS1011, "tls1011", false, "Support deprecated TLS versions 1.0 and 1.1")
	flag.StringVar(&config.HTTPx509CACert, "http-ca-cert", "", "Path to X.509 CA certificate for HTTPS")
	flag.StringVar(&config.HTTPx509Cert, HTTPx509CertFlag, "", "Path to HTTPS X.509 certificate")
	flag.StringVar(&config.HTTPx509Key, HTTPx509KeyFlag, "", "Path to HTTPS X.509 private key")
	flag.BoolVar(&config.NoHTTPVerify, "http-no-verify", false, "Skip verification of remote node's HTTPS certificate when joining a cluster")
	flag.BoolVar(&config.HTTPVerifyClient, "http-verify-client", false, "Enable mutual TLS for HTTPS")
	flag.BoolVar(&config.NodeEncrypt, "node-encrypt", false, "Ignored, control node-to-node encryption by setting node certificate and key")
	flag.StringVar(&config.NodeX509CACert, "node-ca-cert", "", "Path to X.509 CA certificate for node-to-node encryption")
	flag.StringVar(&config.NodeX509Cert, NodeX509CertFlag, "", "Path to X.509 certificate for node-to-node mutual authentication and encryption")
	flag.StringVar(&config.NodeX509Key, NodeX509KeyFlag, "", "Path to X.509 private key for node-to-node mutual authentication and encryption")
	flag.BoolVar(&config.NoNodeVerify, "node-no-verify", false, "Skip verification of any node-node certificate")
	flag.BoolVar(&config.NodeVerifyClient, "node-verify-client", false, "Enable mutual TLS for node-to-node communication")
	flag.StringVar(&config.AuthFile, "auth", "", "Path to authentication and authorization file. If not set, not enabled")
	flag.StringVar(&config.RaftAddr, RaftAddrFlag, "localhost:4002", "Raft communication bind address")
	flag.StringVar(&config.RaftAdv, RaftAdvAddrFlag, "", "Advertised Raft communication address. If not set, same as Raft bind")
	flag.StringVar(&config.JoinSrcIP, "join-source-ip", "", "Set source IP address during Join request")
	flag.StringVar(&config.JoinAddr, "join", "", "Comma-delimited list of nodes, through which a cluster can be joined (proto://host:port)")
	flag.StringVar(&config.JoinAs, "join-as", "", "Username in authentication file to join as. If not set, joins anonymously")
	flag.IntVar(&config.JoinAttempts, "join-attempts", 5, "Number of join attempts to make")
	flag.DurationVar(&config.JoinInterval, "join-interval", 3*time.Second, "Period between join attempts")
	flag.IntVar(&config.BootstrapExpect, "bootstrap-expect", 0, "Minimum number of nodes required for a bootstrap")
	flag.DurationVar(&config.BootstrapExpectTimeout, "bootstrap-expect-timeout", 120*time.Second, "Maximum time for bootstrap process")
	flag.StringVar(&config.DiscoMode, "disco-mode", "", "Choose clustering discovery mode. If not set, no node discovery is performed")
	flag.StringVar(&config.DiscoKey, "disco-key", "rqlite", "Key prefix for cluster discovery service")
	flag.StringVar(&config.DiscoConfig, "disco-config", "", "Set discovery config, or path to cluster discovery config file")
	flag.BoolVar(&config.Expvar, "expvar", true, "Serve expvar data on HTTP server")
	flag.BoolVar(&config.PprofEnabled, "pprof", true, "Serve pprof data on HTTP server")
	flag.BoolVar(&config.OnDisk, "on-disk", false, "Use an on-disk SQLite database")
	flag.StringVar(&config.OnDiskPath, "on-disk-path", "", "Path for SQLite on-disk database file. If not set, use file in data directory")
	flag.BoolVar(&config.OnDiskStartup, "on-disk-startup", false, "Do not initialize on-disk database in memory first at startup")
	flag.BoolVar(&config.FKConstraints, "fk", false, "Enable SQLite foreign key constraints")
	flag.BoolVar(&showVersion, "version", false, "Show version information and exit")
	flag.BoolVar(&config.RaftNonVoter, "raft-non-voter", false, "Configure as non-voting node")
	flag.DurationVar(&config.RaftHeartbeatTimeout, "raft-timeout", time.Second, "Raft heartbeat timeout")
	flag.DurationVar(&config.RaftElectionTimeout, "raft-election-timeout", time.Second, "Raft election timeout")
	flag.DurationVar(&config.RaftApplyTimeout, "raft-apply-timeout", 10*time.Second, "Raft apply timeout")
	flag.Uint64Var(&config.RaftSnapThreshold, "raft-snap", 8192, "Number of outstanding log entries that trigger snapshot")
	flag.DurationVar(&config.RaftSnapInterval, "raft-snap-int", 30*time.Second, "Snapshot threshold check interval")
	flag.DurationVar(&config.RaftLeaderLeaseTimeout, "raft-leader-lease-timeout", 0, "Raft leader lease timeout. Use 0s for Raft default")
	flag.BoolVar(&config.RaftStepdownOnShutdown, "raft-shutdown-stepdown", true, "Stepdown as leader before shutting down. Enabled by default")
	flag.BoolVar(&config.RaftShutdownOnRemove, "raft-remove-shutdown", false, "Shutdown Raft if node removed")
	flag.BoolVar(&config.RaftNoFreelistSync, "raft-no-freelist-sync", false, "Do not sync Raft log database freelist to disk")
	flag.StringVar(&config.RaftLogLevel, "raft-log-level", "INFO", "Minimum log level for Raft module")
	flag.DurationVar(&config.RaftReapNodeTimeout, "raft-reap-node-timeout", 0*time.Hour, "Time after which a non-reachable voting node will be reaped. If not set, no reaping takes place")
	flag.DurationVar(&config.RaftReapReadOnlyNodeTimeout, "raft-reap-read-only-node-timeout", 0*time.Hour, "Time after which a non-reachable non-voting node will be reaped. If not set, no reaping takes place")
	flag.DurationVar(&config.ClusterConnectTimeout, "cluster-connect-timeout", 30*time.Second, "Timeout for initial connection to other nodes")
	flag.IntVar(&config.WriteQueueCap, "write-queue-capacity", 1024, "Write queue capacity")
	flag.IntVar(&config.WriteQueueBatchSz, "write-queue-batch-size", 128, "Write queue batch size")
	flag.DurationVar(&config.WriteQueueTimeout, "write-queue-timeout", 50*time.Millisecond, "Write queue timeout")
	flag.BoolVar(&config.WriteQueueTx, "write-queue-tx", false, "Use a transaction when writing from queue")
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

	if showVersion {
		msg := fmt.Sprintf("%s %s %s %s %s sqlite%s (commit %s, branch %s, compiler %s)",
			name, build.Version, runtime.GOOS, runtime.GOARCH, runtime.Version(), build.SQLiteVersion,
			build.Commit, build.Branch, runtime.Compiler)
		errorExit(0, msg)
	}

	// Ensure, if set explicitly, that reap times are not too low.
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "raft-reap-node-timeout" || f.Name == "raft-reap-read-only-node-timeout" {
			d, err := time.ParseDuration(f.Value.String())
			if err != nil {
				errorExit(1, fmt.Sprintf("failed to parse duration: %s", err.Error()))
			}
			if d <= 0 {
				errorExit(1, fmt.Sprintf("-%s must be greater than 0", f.Name))
			}
		}
	})

	// Ensure the data path is set.
	if flag.NArg() < 1 {
		errorExit(1, "no data directory set")
	}
	config.DataPath = flag.Arg(0)

	// Ensure no args come after the data directory.
	if flag.NArg() > 1 {
		errorExit(1, "arguments after data directory are not accepted")
	}

	if err := config.Validate(); err != nil {
		errorExit(1, err.Error())
	}
	return config, nil
}

func errorExit(code int, msg string) {
	if code != 0 {
		fmt.Fprintf(os.Stderr, "fatal: ")
	}
	fmt.Fprintf(os.Stderr, "%s\n", msg)
	os.Exit(code)
}

// bothUnsetSet returns true if both a and b are unset, or both are set.
func bothUnsetSet(a, b string) bool {
	return (a == "" && b == "") || (a != "" && b != "")
}
