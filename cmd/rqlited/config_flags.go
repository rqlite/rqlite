// Code generated by go generate; DO NOT EDIT.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

// Config represents all configuration options.
type Config struct {
	// DataPath is path to node data. Always set
	DataPath string
	// Show version information and exit
	ShowVersion bool
	// Comma-delimited list of paths to directories, zipfiles, or tar.gz files containing SQLite extensions
	ExtensionPaths []string
	// HTTP server bind address. To enable HTTPS, set X.509 certificate and key
	HTTPAddr string
	// Advertised HTTP address. If not set, same as HTTP server bind address
	HTTPAdv string
	// Value to set for Access-Control-Allow-Origin HTTP header
	HTTPAllowOrigin string
	// Path to authentication and authorization file. If not set, not enabled
	AuthFile string
	// Path to automatic backup configuration file. If not set, not enabled
	AutoBackupFile string
	// Path to automatic restore configuration file. If not set, not enabled
	AutoRestoreFile string
	// Path to X.509 CA certificate for HTTPS
	HTTPx509CACert string
	// Path to HTTPS X.509 certificate
	HTTPx509Cert string
	// Path to HTTPS X.509 private key
	HTTPx509Key string
	// Enable mutual TLS for HTTPS
	HTTPVerifyClient bool
	// Path to X.509 CA certificate for node-to-node encryption
	NodeX509CACert string
	// Path to X.509 certificate for node-to-node mutual authentication and encryption
	NodeX509Cert string
	// Path to X.509 private key for node-to-node mutual authentication and encryption
	NodeX509Key string
	// Skip verification of any node-node certificate
	NoNodeVerify bool
	// Enable mutual TLS for node-to-node communication
	NodeVerifyClient bool
	// Hostname to verify on certificate returned by a node
	NodeVerifyServerName string
	// Unique ID for node. If not set, set to advertised Raft address
	NodeID string
	// Raft communication bind address
	RaftAddr string
	// Advertised Raft communication address. If not set, same as Raft bind address
	RaftAdv string
	// Comma-delimited list of nodes, in host:port form, through which a cluster can be joined
	JoinAddrs string
	// Number of join attempts to make
	JoinAttempts int
	// Period between join attempts
	JoinInterval time.Duration
	// Username in authentication file to join as. If not set, joins anonymously
	JoinAs string
	// Minimum number of nodes required for a bootstrap
	BootstrapExpect int
	// Maximum time for bootstrap process
	BootstrapExpectTimeout time.Duration
	// Choose clustering discovery mode. If not set, no node discovery is performed
	DiscoMode string
	// Key prefix for cluster discovery service
	DiscoKey string
	// Set discovery config, or path to cluster discovery config file
	DiscoConfig string
	// Path for SQLite on-disk database file. If not set, use a file in data directory
	OnDiskPath string
	// Enable SQLite foreign key constraints
	FKConstraints bool
	// Period between automatic VACUUMs. It not set, not enabled
	AutoVacInterval time.Duration
	// Period between automatic 'PRAGMA optimize'. Set to 0h to disable
	AutoOptimizeInterval time.Duration
	// Minimum log level for Raft module
	RaftLogLevel string
	// Configure as non-voting node
	RaftNonVoter bool
	// Number of outstanding log entries which triggers Raft snapshot
	RaftSnapThreshold uint64
	// SQLite WAL file size in bytes which triggers Raft snapshot. Set to 0 to disable
	RaftSnapThresholdWALSize uint64
	// Snapshot threshold check interval
	RaftSnapInterval time.Duration
	// Raft leader lease timeout. Use 0s for Raft default
	RaftLeaderLeaseTimeout time.Duration
	// Raft heartbeat timeout
	RaftHeartbeatTimeout time.Duration
	// Raft election timeout
	RaftElectionTimeout time.Duration
	// Raft apply timeout
	RaftApplyTimeout time.Duration
	// Shutdown Raft if node removed from cluster
	RaftShutdownOnRemove bool
	// Node removes itself from cluster on graceful shutdown
	RaftClusterRemoveOnShutdown bool
	// If leader, stepdown before shutting down. Enabled by default
	RaftStepdownOnShutdown bool
	// Time after which a non-reachable voting node will be reaped. If not set, no reaping takes place
	RaftReapNodeTimeout time.Duration
	// Time after which a non-reachable non-voting node will be reaped. If not set, no reaping takes place
	RaftReapReadOnlyNodeTimeout time.Duration
	// Timeout for initial connection to other nodes
	ClusterConnectTimeout time.Duration
	// QueuedWrites queue capacity
	WriteQueueCap int
	// QueuedWrites queue batch size
	WriteQueueBatchSz int
	// QueuedWrites queue timeout
	WriteQueueTimeout time.Duration
	// Use a transaction when processing a queued write
	WriteQueueTx bool
	// Path to file for CPU profiling information
	CPUProfile string
	// Path to file for memory profiling information
	MemProfile string
	// Path to file for trace profiling information
	TraceProfile string
}

// Forge sets up and parses command-line flags.
func Forge(arguments []string) (*flag.FlagSet, *Config, error) {
	config := &Config{}
	fs := flag.NewFlagSet("rqlited", flag.ExitOnError)
	if len(arguments) <= 0 {
		return nil, nil, fmtError("missing required argument: DataPath")
	}
	fs.BoolVar(&config.ShowVersion, "version", false, "Show version information and exit")
	var tmpExtensionPaths string
	fs.StringVar(&tmpExtensionPaths, "extensions-path", "", "Comma-delimited list of paths to directories, zipfiles, or tar.gz files containing SQLite extensions")
	fs.StringVar(&config.HTTPAddr, "http-addr", "localhost:4001", "HTTP server bind address. To enable HTTPS, set X.509 certificate and key")
	fs.StringVar(&config.HTTPAdv, "http-adv-addr", "", "Advertised HTTP address. If not set, same as HTTP server bind address")
	fs.StringVar(&config.HTTPAllowOrigin, "http-allow-origin", "", "Value to set for Access-Control-Allow-Origin HTTP header")
	fs.StringVar(&config.AuthFile, "auth", "", "Path to authentication and authorization file. If not set, not enabled")
	fs.StringVar(&config.AutoBackupFile, "auto-backup", "", "Path to automatic backup configuration file. If not set, not enabled")
	fs.StringVar(&config.AutoRestoreFile, "auto-restore", "", "Path to automatic restore configuration file. If not set, not enabled")
	fs.StringVar(&config.HTTPx509CACert, "http-ca-cert", "", "Path to X.509 CA certificate for HTTPS")
	fs.StringVar(&config.HTTPx509Cert, "http-cert", "", "Path to HTTPS X.509 certificate")
	fs.StringVar(&config.HTTPx509Key, "http-key", "", "Path to HTTPS X.509 private key")
	fs.BoolVar(&config.HTTPVerifyClient, "http-verify-client", false, "Enable mutual TLS for HTTPS")
	fs.StringVar(&config.NodeX509CACert, "node-ca-cert", "", "Path to X.509 CA certificate for node-to-node encryption")
	fs.StringVar(&config.NodeX509Cert, "node-cert", "", "Path to X.509 certificate for node-to-node mutual authentication and encryption")
	fs.StringVar(&config.NodeX509Key, "node-key", "", "Path to X.509 private key for node-to-node mutual authentication and encryption")
	fs.BoolVar(&config.NoNodeVerify, "node-no-verify", false, "Skip verification of any node-node certificate")
	fs.BoolVar(&config.NodeVerifyClient, "node-verify-client", false, "Enable mutual TLS for node-to-node communication")
	fs.StringVar(&config.NodeVerifyServerName, "node-verify-server-name", "", "Hostname to verify on certificate returned by a node")
	fs.StringVar(&config.NodeID, "node-id", "", "Unique ID for node. If not set, set to advertised Raft address")
	fs.StringVar(&config.RaftAddr, "raft-addr", "localhost:4002", "Raft communication bind address")
	fs.StringVar(&config.RaftAdv, "raft-adv-addr", "", "Advertised Raft communication address. If not set, same as Raft bind address")
	fs.StringVar(&config.JoinAddrs, "join", "", "Comma-delimited list of nodes, in host:port form, through which a cluster can be joined")
	fs.IntVar(&config.JoinAttempts, "join-attempts", 5, "Number of join attempts to make")
	fs.DurationVar(&config.JoinInterval, "join-interval", mustParseDuration("3s"), "Period between join attempts")
	fs.StringVar(&config.JoinAs, "join-as", "", "Username in authentication file to join as. If not set, joins anonymously")
	fs.IntVar(&config.BootstrapExpect, "bootstrap-expect", 0, "Minimum number of nodes required for a bootstrap")
	fs.DurationVar(&config.BootstrapExpectTimeout, "bootstrap-expect-timeout", mustParseDuration("120s"), "Maximum time for bootstrap process")
	fs.StringVar(&config.DiscoMode, "disco-mode", "", "Choose clustering discovery mode. If not set, no node discovery is performed")
	fs.StringVar(&config.DiscoKey, "disco-key", "rqlite", "Key prefix for cluster discovery service")
	fs.StringVar(&config.DiscoConfig, "disco-config", "", "Set discovery config, or path to cluster discovery config file")
	fs.StringVar(&config.OnDiskPath, "on-disk-path", "", "Path for SQLite on-disk database file. If not set, use a file in data directory")
	fs.BoolVar(&config.FKConstraints, "fk", false, "Enable SQLite foreign key constraints")
	fs.DurationVar(&config.AutoVacInterval, "auto-vacuum-int", mustParseDuration("0s"), "Period between automatic VACUUMs. It not set, not enabled")
	fs.DurationVar(&config.AutoOptimizeInterval, "auto-optimize-int", mustParseDuration("24h"), "Period between automatic 'PRAGMA optimize'. Set to 0h to disable")
	fs.StringVar(&config.RaftLogLevel, "raft-log-level", "WARN", "Minimum log level for Raft module")
	fs.BoolVar(&config.RaftNonVoter, "raft-non-voter", false, "Configure as non-voting node")
	fs.Uint64Var(&config.RaftSnapThreshold, "raft-snap", 8192, "Number of outstanding log entries which triggers Raft snapshot")
	fs.Uint64Var(&config.RaftSnapThresholdWALSize, "raft-snap-wal-size", 4194304, "SQLite WAL file size in bytes which triggers Raft snapshot. Set to 0 to disable")
	fs.DurationVar(&config.RaftSnapInterval, "raft-snap-int", mustParseDuration("10s"), "Snapshot threshold check interval")
	fs.DurationVar(&config.RaftLeaderLeaseTimeout, "raft-leader-lease-timeout", mustParseDuration("0s"), "Raft leader lease timeout. Use 0s for Raft default")
	fs.DurationVar(&config.RaftHeartbeatTimeout, "raft-timeout", mustParseDuration("1s"), "Raft heartbeat timeout")
	fs.DurationVar(&config.RaftElectionTimeout, "raft-election-timeout", mustParseDuration("1s"), "Raft election timeout")
	fs.DurationVar(&config.RaftApplyTimeout, "raft-apply-timeout", mustParseDuration("10s"), "Raft apply timeout")
	fs.BoolVar(&config.RaftShutdownOnRemove, "raft-remove-shutdown", false, "Shutdown Raft if node removed from cluster")
	fs.BoolVar(&config.RaftClusterRemoveOnShutdown, "raft-cluster-remove-shutdown", false, "Node removes itself from cluster on graceful shutdown")
	fs.BoolVar(&config.RaftStepdownOnShutdown, "raft-shutdown-stepdown", true, "If leader, stepdown before shutting down. Enabled by default")
	fs.DurationVar(&config.RaftReapNodeTimeout, "raft-reap-node-timeout", mustParseDuration("0h"), "Time after which a non-reachable voting node will be reaped. If not set, no reaping takes place")
	fs.DurationVar(&config.RaftReapReadOnlyNodeTimeout, "raft-reap-read-only-node-timeout", mustParseDuration("0h"), "Time after which a non-reachable non-voting node will be reaped. If not set, no reaping takes place")
	fs.DurationVar(&config.ClusterConnectTimeout, "cluster-connect-timeout", mustParseDuration("30s"), "Timeout for initial connection to other nodes")
	fs.IntVar(&config.WriteQueueCap, "write-queue-capacity", 1024, "QueuedWrites queue capacity")
	fs.IntVar(&config.WriteQueueBatchSz, "write-queue-batch-size", 128, "QueuedWrites queue batch size")
	fs.DurationVar(&config.WriteQueueTimeout, "write-queue-timeout", mustParseDuration("50ms"), "QueuedWrites queue timeout")
	fs.BoolVar(&config.WriteQueueTx, "write-queue-tx", false, "Use a transaction when processing a queued write")
	fs.StringVar(&config.CPUProfile, "cpu-profile", "", "Path to file for CPU profiling information")
	fs.StringVar(&config.MemProfile, "mem-profile", "", "Path to file for memory profiling information")
	fs.StringVar(&config.TraceProfile, "trace-profile", "", "Path to file for trace profiling information")
	fs.Usage = func() {
		usage("\nrqlite is a lightweight, distributed relational database, which uses SQLite as its\nstorage engine. It provides an easy-to-use, fault-tolerant store for relational data.\n\nVisit https://www.rqlite.io to learn more.\n\nUsage: rqlited [flags] <data directory>\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(arguments); err != nil {
		return nil, nil, err
	}
	config.DataPath = fs.Arg(0)
	config.ExtensionPaths = splitString(tmpExtensionPaths, ",")
	return fs, config, nil
}

func mustParseDuration(d string) time.Duration {
	td, err := time.ParseDuration(d)
	if err != nil {
		panic(err)
	}
	return td
}

func splitString(s, sep string) []string {
	return strings.Split(s, sep)
}

func fmtError(msg string) error {
	return fmt.Errorf(msg)
}

func usage(msg string) {
	fmt.Fprintf(os.Stderr, msg)
}
