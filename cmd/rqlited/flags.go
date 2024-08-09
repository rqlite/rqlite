package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/rqlite/rqlite/v8/rarchive"
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

	// ExtensionsPath is the path to the directory or Zipfile containing SQLite extensions.
	ExtensionsPath string

	// HTTPAddr is the bind network address for the HTTP Server.
	// It never includes a trailing HTTP or HTTPS.
	HTTPAddr string

	// HTTPAdv is the advertised HTTP server network.
	HTTPAdv string

	// HTTPAllowOrigin is the value to set for Access-Control-Allow-Origin HTTP header.
	HTTPAllowOrigin string

	// AuthFile is the path to the authentication file. May not be set.
	AuthFile string `filepath:"true"`

	// AutoBackupFile is the path to the auto-backup file. May not be set.
	AutoBackupFile string `filepath:"true"`

	// AutoRestoreFile is the path to the auto-restore file. May not be set.
	AutoRestoreFile string `filepath:"true"`

	// HTTPx509CACert is the path to the CA certificate file for when this node verifies
	// other certificates for any HTTP communications. May not be set.
	HTTPx509CACert string `filepath:"true"`

	// HTTPx509Cert is the path to the X509 cert for the HTTP server. May not be set.
	HTTPx509Cert string `filepath:"true"`

	// HTTPx509Key is the path to the private key for the HTTP server. May not be set.
	HTTPx509Key string `filepath:"true"`

	// HTTPVerifyClient indicates whether the HTTP server should verify client certificates.
	HTTPVerifyClient bool

	// NodeX509CACert is the path to the CA certificate file for when this node verifies
	// other certificates for any inter-node communications. May not be set.
	NodeX509CACert string `filepath:"true"`

	// NodeX509Cert is the path to the X509 cert for the Raft server. May not be set.
	NodeX509Cert string `filepath:"true"`

	// NodeX509Key is the path to the X509 key for the Raft server. May not be set.
	NodeX509Key string `filepath:"true"`

	// NoNodeVerify disables checking other nodes' Node X509 certs for validity.
	NoNodeVerify bool

	// NodeVerifyClient enable mutual TLS for node-to-node communication.
	NodeVerifyClient bool

	// NodeVerifyServerName is the hostname to verify on the certificates returned by nodes.
	// If NoNodeVerify is true this field is ignored.
	NodeVerifyServerName string

	// NodeID is the Raft ID for the node.
	NodeID string

	// RaftAddr is the bind network address for the Raft server.
	RaftAddr string

	// RaftAdv is the advertised Raft server address.
	RaftAdv string

	// JoinAddrs is the list of Raft addresses to use for a join attempt.
	JoinAddrs string

	// JoinAttempts is the number of times a node should attempt to join using a
	// given address.
	JoinAttempts int

	// JoinInterval is the time between retrying failed join operations.
	JoinInterval time.Duration

	// JoinAs sets the user join attempts should be performed as. May not be set.
	JoinAs string

	// BootstrapExpect is the minimum number of nodes required for a bootstrap.
	BootstrapExpect int

	// BootstrapExpectTimeout is the maximum time a bootstrap operation can take.
	BootstrapExpectTimeout time.Duration

	// DiscoMode sets the discovery mode. May not be set.
	DiscoMode string

	// DiscoKey sets the discovery prefix key.
	DiscoKey string

	// DiscoConfig sets the path to any discovery configuration file. May not be set.
	DiscoConfig string

	// OnDiskPath sets the path to the SQLite file. May not be set.
	OnDiskPath string

	// FKConstraints enables SQLite foreign key constraints.
	FKConstraints bool

	// AutoVacInterval sets the automatic VACUUM interval. Use 0s to disable.
	AutoVacInterval time.Duration

	// RaftLogLevel sets the minimum logging level for the Raft subsystem.
	RaftLogLevel string

	// RaftNonVoter controls whether this node is a voting, read-only node.
	RaftNonVoter bool

	// RaftSnapThreshold is the number of outstanding log entries that trigger snapshot.
	RaftSnapThreshold uint64

	// RaftSnapThreshold is the size of a SQLite WAL file which will trigger a snapshot.
	RaftSnapThresholdWALSize uint64

	// RaftSnapInterval sets the threshold check interval.
	RaftSnapInterval time.Duration

	// RaftLeaderLeaseTimeout sets the leader lease timeout.
	RaftLeaderLeaseTimeout time.Duration

	// RaftHeartbeatTimeout specifies the time in follower state without contact
	// from a Leader before the node attempts an election.
	RaftHeartbeatTimeout time.Duration

	// RaftElectionTimeout specifies the time in candidate state without contact
	// from a Leader before the node attempts an election.
	RaftElectionTimeout time.Duration

	// RaftApplyTimeout sets the Log-apply timeout.
	RaftApplyTimeout time.Duration

	// RaftShutdownOnRemove sets whether Raft should be shutdown if the node is removed
	RaftShutdownOnRemove bool

	// RaftClusterRemoveOnShutdown sets whether the node should remove itself from the cluster on shutdown
	RaftClusterRemoveOnShutdown bool

	// RaftStepdownOnShutdown sets whether Leadership should be relinquished on shutdown
	RaftStepdownOnShutdown bool

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

	// CPUProfile enables CPU profiling.
	CPUProfile string

	// MemProfile enables memory profiling.
	MemProfile string

	// TraceProfile enables trace profiling.
	TraceProfile string
}

// Validate checks the configuration for internal consistency, and activates
// important rqlite policies. It must be called at least once on a Config
// object before the Config object is used. It is OK to call more than
// once.
func (c *Config) Validate() error {
	dataPath, err := filepath.Abs(c.DataPath)
	if err != nil {
		return fmt.Errorf("failed to determine absolute data path: %s", err.Error())
	}
	c.DataPath = dataPath

	err = c.CheckFilePaths()
	if err != nil {
		return err
	}

	err = c.CheckDirPaths()
	if err != nil {
		return err
	}

	if c.ExtensionsPath != "" {
		if !fileExists(c.ExtensionsPath) {
			return fmt.Errorf("extensions path does not exist: %s", c.ExtensionsPath)
		}
		if !isDir(c.ExtensionsPath) && !rarchive.IsZipFile(c.ExtensionsPath) {
			return fmt.Errorf("extensions path is not a valid zip file: %s", c.ExtensionsPath)
		}
	}

	if !bothUnsetSet(c.HTTPx509Cert, c.HTTPx509Key) {
		return fmt.Errorf("either both -%s and -%s must be set, or neither", HTTPx509CertFlag, HTTPx509KeyFlag)
	}
	if !bothUnsetSet(c.NodeX509Cert, c.NodeX509Key) {
		return fmt.Errorf("either both -%s and -%s must be set, or neither", NodeX509CertFlag, NodeX509KeyFlag)

	}

	if c.RaftAddr == c.HTTPAddr {
		return errors.New("HTTP and Raft addresses must differ")
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

	// Perform some address validity checks.
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

	if _, rp, err := net.SplitHostPort(c.RaftAddr); err != nil {
		return errors.New("raft bind address not valid")
	} else if _, err := strconv.Atoi(rp); err != nil {
		return errors.New("raft bind port not valid")
	}

	radv, rp, err := net.SplitHostPort(c.RaftAdv)
	if err != nil {
		return errors.New("raft advertised address not valid")
	}
	if addr := net.ParseIP(radv); addr != nil && addr.IsUnspecified() {
		return fmt.Errorf("advertised Raft address is not routable (%s), specify it via -%s or -%s",
			radv, RaftAddrFlag, RaftAdvAddrFlag)
	}
	if _, err := strconv.Atoi(rp); err != nil {
		return errors.New("raft advertised port is not valid")
	}

	if c.RaftAdv == c.HTTPAdv {
		return errors.New("advertised HTTP and Raft addresses must differ")
	}

	// Enforce bootstrapping policies
	if c.BootstrapExpect > 0 && c.RaftNonVoter {
		return errors.New("bootstrapping only applicable to voting nodes")
	}

	// Join parameters OK?
	if c.JoinAddrs != "" {
		addrs := strings.Split(c.JoinAddrs, ",")
		for i := range addrs {
			if _, _, err := net.SplitHostPort(addrs[i]); err != nil {
				return fmt.Errorf("%s is an invalid join address", addrs[i])
			}

			if c.BootstrapExpect == 0 {
				if addrs[i] == c.RaftAdv || addrs[i] == c.RaftAddr {
					return errors.New("node cannot join with itself unless bootstrapping")
				}
				if c.AutoRestoreFile != "" {
					return errors.New("auto-restoring cannot be used when joining a cluster")
				}
			}
		}

		if c.DiscoMode != "" {
			return errors.New("disco mode cannot be used when also explicitly joining a cluster")
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
		if c.BootstrapExpect == 0 && !c.RaftNonVoter {
			return fmt.Errorf("bootstrap-expect value required when using %s with a voting node", c.DiscoMode)
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
	if c.JoinAddrs == "" {
		return nil
	}
	return strings.Split(c.JoinAddrs, ",")
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

// RaftPort returns the port on which the Raft system is listening. Validate must
// have been called before calling this method.
func (c *Config) RaftPort() int {
	_, port, err := net.SplitHostPort(c.RaftAddr)
	if err != nil {
		panic("RaftAddr not valid")
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		panic("RaftAddr port not valid")
	}
	return p
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

// CheckFilePaths checks that all file paths in the config exist.
// Empty filepaths are ignored.
func (c *Config) CheckFilePaths() error {
	v := reflect.ValueOf(c).Elem()

	// Iterate through the fields of the struct
	for i := 0; i < v.NumField(); i++ {
		field := v.Type().Field(i)
		fieldValue := v.Field(i)

		if fieldValue.Kind() != reflect.String {
			continue
		}

		if tagValue, ok := field.Tag.Lookup("filepath"); ok && tagValue == "true" {
			filePath := fieldValue.String()
			if filePath == "" {
				continue
			}
			if !fileExists(filePath) {
				return fmt.Errorf("%s does not exist", filePath)
			}
		}
	}
	return nil
}

// CheckDirPaths checks that all directory paths in the config exist and are directories.
// Empty directory paths are ignored.
func (c *Config) CheckDirPaths() error {
	v := reflect.ValueOf(c).Elem()

	// Iterate through the fields of the struct
	for i := 0; i < v.NumField(); i++ {
		field := v.Type().Field(i)
		fieldValue := v.Field(i)

		if fieldValue.Kind() != reflect.String {
			continue
		}

		if tagValue, ok := field.Tag.Lookup("dirpath"); ok && tagValue == "true" {
			dirPath := fieldValue.String()
			if dirPath == "" {
				continue
			}
			if !fileExists(dirPath) {
				return fmt.Errorf("%s does not exist", dirPath)
			}
			if !isDir(dirPath) {
				return fmt.Errorf("%s is not a directory", dirPath)
			}
		}
	}
	return nil
}

// ExtensionsAreDir returns true if the extensions are stored in a directory.
func (c *Config) ExtensionsAreDir() bool {
	return isDir(c.ExtensionsPath)
}

// ExtensionsAreZip returns true if the extensions are stored in a zipfile.
func (c *Config) ExtensionsAreZip() bool {
	return rarchive.IsZipFile(c.ExtensionsPath)
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

	fs := flag.NewFlagSet(name, flag.ExitOnError)

	fs.StringVar(&config.NodeID, "node-id", "", "Unique ID for node. If not set, set to advertised Raft address")
	fs.StringVar(&config.ExtensionsPath, "extensions-path", "", "Path to directory or zipfile containing SQLite extensions to be loaded")
	fs.StringVar(&config.HTTPAddr, HTTPAddrFlag, "localhost:4001", "HTTP server bind address. To enable HTTPS, set X.509 certificate and key")
	fs.StringVar(&config.HTTPAdv, HTTPAdvAddrFlag, "", "Advertised HTTP address. If not set, same as HTTP server bind address")
	fs.StringVar(&config.HTTPAllowOrigin, "http-allow-origin", "", "Value to set for Access-Control-Allow-Origin HTTP header")
	fs.StringVar(&config.HTTPx509CACert, "http-ca-cert", "", "Path to X.509 CA certificate for HTTPS")
	fs.StringVar(&config.HTTPx509Cert, HTTPx509CertFlag, "", "Path to HTTPS X.509 certificate")
	fs.StringVar(&config.HTTPx509Key, HTTPx509KeyFlag, "", "Path to HTTPS X.509 private key")
	fs.BoolVar(&config.HTTPVerifyClient, "http-verify-client", false, "Enable mutual TLS for HTTPS")
	fs.StringVar(&config.NodeX509CACert, "node-ca-cert", "", "Path to X.509 CA certificate for node-to-node encryption")
	fs.StringVar(&config.NodeX509Cert, NodeX509CertFlag, "", "Path to X.509 certificate for node-to-node mutual authentication and encryption")
	fs.StringVar(&config.NodeX509Key, NodeX509KeyFlag, "", "Path to X.509 private key for node-to-node mutual authentication and encryption")
	fs.BoolVar(&config.NoNodeVerify, "node-no-verify", false, "Skip verification of any node-node certificate")
	fs.BoolVar(&config.NodeVerifyClient, "node-verify-client", false, "Enable mutual TLS for node-to-node communication")
	fs.StringVar(&config.NodeVerifyServerName, "node-verify-server-name", "", "Hostname to verify on certificate returned by a node")
	fs.StringVar(&config.AuthFile, "auth", "", "Path to authentication and authorization file. If not set, not enabled")
	fs.StringVar(&config.AutoBackupFile, "auto-backup", "", "Path to automatic backup configuration file. If not set, not enabled")
	fs.StringVar(&config.AutoRestoreFile, "auto-restore", "", "Path to automatic restore configuration file. If not set, not enabled")
	fs.StringVar(&config.RaftAddr, RaftAddrFlag, "localhost:4002", "Raft communication bind address")
	fs.StringVar(&config.RaftAdv, RaftAdvAddrFlag, "", "Advertised Raft communication address. If not set, same as Raft bind address")
	fs.StringVar(&config.JoinAddrs, "join", "", "Comma-delimited list of nodes, in host:port form, through which a cluster can be joined")
	fs.IntVar(&config.JoinAttempts, "join-attempts", 5, "Number of join attempts to make")
	fs.DurationVar(&config.JoinInterval, "join-interval", 3*time.Second, "Period between join attempts")
	fs.StringVar(&config.JoinAs, "join-as", "", "Username in authentication file to join as. If not set, joins anonymously")
	fs.IntVar(&config.BootstrapExpect, "bootstrap-expect", 0, "Minimum number of nodes required for a bootstrap")
	fs.DurationVar(&config.BootstrapExpectTimeout, "bootstrap-expect-timeout", 120*time.Second, "Maximum time for bootstrap process")
	fs.StringVar(&config.DiscoMode, "disco-mode", "", "Choose clustering discovery mode. If not set, no node discovery is performed")
	fs.StringVar(&config.DiscoKey, "disco-key", "rqlite", "Key prefix for cluster discovery service")
	fs.StringVar(&config.DiscoConfig, "disco-config", "", "Set discovery config, or path to cluster discovery config file")
	fs.StringVar(&config.OnDiskPath, "on-disk-path", "", "Path for SQLite on-disk database file. If not set, use a file in data directory")
	fs.BoolVar(&config.FKConstraints, "fk", false, "Enable SQLite foreign key constraints")
	fs.BoolVar(&showVersion, "version", false, "Show version information and exit")
	fs.DurationVar(&config.AutoVacInterval, "auto-vacuum-int", 0, "Period between automatic VACUUMs. It not set, not enabled")
	fs.BoolVar(&config.RaftNonVoter, "raft-non-voter", false, "Configure as non-voting node")
	fs.DurationVar(&config.RaftHeartbeatTimeout, "raft-timeout", time.Second, "Raft heartbeat timeout")
	fs.DurationVar(&config.RaftElectionTimeout, "raft-election-timeout", time.Second, "Raft election timeout")
	fs.DurationVar(&config.RaftApplyTimeout, "raft-apply-timeout", 10*time.Second, "Raft apply timeout")
	fs.Uint64Var(&config.RaftSnapThreshold, "raft-snap", 8192, "Number of outstanding log entries which triggers Raft snapshot")
	fs.Uint64Var(&config.RaftSnapThresholdWALSize, "raft-snap-wal-size", 4*1024*1024, "SQLite WAL file size in bytes which triggers Raft snapshot. Set to 0 to disable")
	fs.DurationVar(&config.RaftSnapInterval, "raft-snap-int", 10*time.Second, "Snapshot threshold check interval")
	fs.DurationVar(&config.RaftLeaderLeaseTimeout, "raft-leader-lease-timeout", 0, "Raft leader lease timeout. Use 0s for Raft default")
	fs.BoolVar(&config.RaftStepdownOnShutdown, "raft-shutdown-stepdown", true, "If leader, stepdown before shutting down. Enabled by default")
	fs.BoolVar(&config.RaftShutdownOnRemove, "raft-remove-shutdown", false, "Shutdown Raft if node removed from cluster")
	fs.BoolVar(&config.RaftClusterRemoveOnShutdown, "raft-cluster-remove-shutdown", false, "Node removes itself from cluster on graceful shutdown")
	fs.StringVar(&config.RaftLogLevel, "raft-log-level", "WARN", "Minimum log level for Raft module")
	fs.DurationVar(&config.RaftReapNodeTimeout, "raft-reap-node-timeout", 0*time.Hour, "Time after which a non-reachable voting node will be reaped. If not set, no reaping takes place")
	fs.DurationVar(&config.RaftReapReadOnlyNodeTimeout, "raft-reap-read-only-node-timeout", 0*time.Hour, "Time after which a non-reachable non-voting node will be reaped. If not set, no reaping takes place")
	fs.DurationVar(&config.ClusterConnectTimeout, "cluster-connect-timeout", 30*time.Second, "Timeout for initial connection to other nodes")
	fs.IntVar(&config.WriteQueueCap, "write-queue-capacity", 1024, "QueuedWrites queue capacity")
	fs.IntVar(&config.WriteQueueBatchSz, "write-queue-batch-size", 128, "QueuedWrites queue batch size")
	fs.DurationVar(&config.WriteQueueTimeout, "write-queue-timeout", 50*time.Millisecond, "QueuedWrites queue timeout")
	fs.BoolVar(&config.WriteQueueTx, "write-queue-tx", false, "Use a transaction when processing a queued write")
	fs.StringVar(&config.CPUProfile, "cpu-profile", "", "Path to file for CPU profiling information")
	fs.StringVar(&config.MemProfile, "mem-profile", "", "Path to file for memory profiling information")
	fs.StringVar(&config.TraceProfile, "trace-profile", "", "Path to file for trace profiling information")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "\n%s\n\n", desc)
		fmt.Fprintf(os.Stderr, "Usage: %s [flags] <data directory>\n", name)
		fs.PrintDefaults()
	}
	fs.Parse(os.Args[1:])

	if showVersion {
		msg := fmt.Sprintf("%s %s %s %s %s sqlite%s (commit %s, branch %s, compiler %s)",
			name, build.Version, runtime.GOOS, runtime.GOARCH, runtime.Version(), build.SQLiteVersion,
			build.Commit, build.Branch, runtime.Compiler)
		errorExit(0, msg)
	}

	// Ensure, if set explicitly, that reap times are not too low.
	fs.Visit(func(f *flag.Flag) {
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
	if fs.NArg() < 1 {
		errorExit(1, "no data directory set")
	}
	config.DataPath = fs.Arg(0)

	// Ensure no args come after the data directory.
	if fs.NArg() > 1 {
		fmt.Fprintf(os.Stderr, "arguments after data directory (%s) are not accepted (%s)\n",
			config.DataPath, fs.Args()[1:])
		os.Exit(1)
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

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func isDir(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	return fi.IsDir()
}
