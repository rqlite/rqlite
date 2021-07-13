// Command rqlited is the rqlite server.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/rqlite/rqlite/cmd"
	"github.com/rqlite/rqlite/server"
)

const logo = `
            _ _ _
           | (_) |
  _ __ __ _| |_| |_ ___
 | '__/ _  | | | __/ _ \   The lightweight, distributed
 | | | (_| | | | ||  __/   relational database.
 |_|  \__, |_|_|\__\___|
         | |               www.rqlite.com
         |_|
`

var cfg *server.Config
var joinInterval string
var raftSnapInterval string
var raftLeaderLeaseTimeout string
var raftHeartbeatTimeout string
var raftElectionTimeout string
var raftApplyTimeout string
var raftOpenTimeout string
var showVersion bool

const name = `rqlited`
const desc = `rqlite is a lightweight, distributed relational database, which uses SQLite as its
storage engine. It provides an easy-to-use, fault-tolerant store for relational data.`

func init() {
	cfg = &server.Config{}
	flag.StringVar(&cfg.NodeID, "node-id", "", "Unique name for node. If not set, set to Raft address")
	flag.StringVar(&cfg.HTTPAddr, "http-addr", "localhost:4001", "HTTP server bind address. For HTTPS, set X.509 cert and key")
	flag.StringVar(&cfg.HTTPAdv, "http-adv-addr", "", "Advertised HTTP address. If not set, same as HTTP server")
	flag.StringVar(&cfg.JoinSrcIP, "join-source-ip", "", "Set source IP address during Join request")
	flag.BoolVar(&cfg.TLS1011, "tls1011", false, "Support deprecated TLS versions 1.0 and 1.1")
	flag.StringVar(&cfg.X509CACert, "http-ca-cert", "", "Path to root X.509 certificate for HTTP endpoint")
	flag.StringVar(&cfg.X509Cert, "http-cert", "", "Path to X.509 certificate for HTTP endpoint")
	flag.StringVar(&cfg.X509Key, "http-key", "", "Path to X.509 private key for HTTP endpoint")
	flag.BoolVar(&cfg.NoVerify, "http-no-verify", false, "Skip verification of remote HTTPS cert when joining cluster")
	flag.BoolVar(&cfg.NodeEncrypt, "node-encrypt", false, "Enable node-to-node encryption")
	flag.StringVar(&cfg.NodeX509CACert, "node-ca-cert", "", "Path to root X.509 certificate for node-to-node encryption")
	flag.StringVar(&cfg.NodeX509Cert, "node-cert", "cert.pem", "Path to X.509 certificate for node-to-node encryption")
	flag.StringVar(&cfg.NodeX509Key, "node-key", "key.pem", "Path to X.509 private key for node-to-node encryption")
	flag.BoolVar(&cfg.NoNodeVerify, "node-no-verify", false, "Skip verification of a remote node cert")
	flag.StringVar(&cfg.AuthFile, "auth", "", "Path to authentication and authorization file. If not set, not enabled")
	flag.StringVar(&cfg.RaftAddr, "raft-addr", "localhost:4002", "Raft communication bind address")
	flag.StringVar(&cfg.RaftAdv, "raft-adv-addr", "", "Advertised Raft communication address. If not set, same as Raft bind")
	flag.StringVar(&cfg.JoinAddr, "join", "", "Comma-delimited list of nodes, through which a cluster can be joined (proto://host:port)")
	flag.IntVar(&cfg.JoinAttempts, "join-attempts", 5, "Number of join attempts to make")
	flag.StringVar(&joinInterval, "join-interval", "5s", "Period between join attempts")
	flag.StringVar(&cfg.DiscoURL, "disco-url", "http://discovery.rqlite.com", "Set Discovery Service URL")
	flag.StringVar(&cfg.DiscoID, "disco-id", "", "Set Discovery ID. If not set, Discovery Service not used")
	flag.BoolVar(&cfg.Expvar, "expvar", true, "Serve expvar data on HTTP server")
	flag.BoolVar(&cfg.PPROFEnabled, "pprof", true, "Serve pprof data on HTTP server")
	flag.StringVar(&cfg.DSN, "dsn", "", `SQLite DSN parameters. E.g. "cache=shared&mode=memory"`)
	flag.BoolVar(&cfg.OnDisk, "on-disk", false, "Use an on-disk SQLite database")
	flag.BoolVar(&showVersion, "version", false, "Show version information and exit")
	flag.BoolVar(&cfg.RaftNonVoter, "raft-non-voter", false, "Configure as non-voting node")
	flag.StringVar(&raftHeartbeatTimeout, "raft-timeout", "1s", "Raft heartbeat timeout")
	flag.StringVar(&raftElectionTimeout, "raft-election-timeout", "1s", "Raft election timeout")
	flag.StringVar(&raftApplyTimeout, "raft-apply-timeout", "10s", "Raft apply timeout")
	flag.StringVar(&raftOpenTimeout, "raft-open-timeout", "120s", "Time for initial Raft logs to be applied. Use 0s duration to skip wait")
	flag.BoolVar(&cfg.RaftWaitForLeader, "raft-leader-wait", true, "Node waits for a leader before answering requests")
	flag.Uint64Var(&cfg.RaftSnapThreshold, "raft-snap", 8192, "Number of outstanding log entries that trigger snapshot")
	flag.StringVar(&raftSnapInterval, "raft-snap-int", "30s", "Snapshot threshold check interval")
	flag.StringVar(&raftLeaderLeaseTimeout, "raft-leader-lease-timeout", "0s", "Raft leader lease timeout. Use 0s for Raft default")
	flag.BoolVar(&cfg.RaftShutdownOnRemove, "raft-remove-shutdown", false, "Shutdown Raft if node removed")
	flag.StringVar(&cfg.RaftLogLevel, "raft-log-level", "INFO", "Minimum log level for Raft module")
	flag.IntVar(&cfg.CompressionSize, "compression-size", 150, "Request query size for compression attempt")
	flag.IntVar(&cfg.CompressionBatch, "compression-batch", 5, "Request batch threshold for compression attempt")
	flag.StringVar(&cfg.CPUProfile, "cpu-profile", "", "Path to file for CPU profiling information")
	flag.StringVar(&cfg.MemoryProfile, "mem-profile", "", "Path to file for memory profiling information")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "\n%s\n\n", desc)
		fmt.Fprintf(os.Stderr, "Usage: %s [flags] <data directory>\n", name)
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	if showVersion {
		fmt.Printf("%s %s %s %s %s (commit %s, branch %s)\n",
			name, cmd.Version, runtime.GOOS, runtime.GOARCH, runtime.Version(), cmd.Commit, cmd.Branch)
		os.Exit(0)
	}

	// Ensure the data path is set.
	if flag.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "fatal: no data directory set\n")
		os.Exit(1)
	}

	// Ensure no args come after the data directory.
	if flag.NArg() > 1 {
		fmt.Fprintf(os.Stderr, "fatal: arguments after data directory are not accepted\n")
		os.Exit(1)
	}

	cfg.DataPath = flag.Arg(0)

	// Display logo.
	fmt.Println(logo)

	// Configure logging and pump out initial message.
	log.SetFlags(log.LstdFlags)
	log.SetOutput(os.Stderr)
	log.SetPrefix(fmt.Sprintf("[%s] ", name))
	log.Printf("%s starting, version %s, commit %s, branch %s", name, cmd.Version, cmd.Commit, cmd.Branch)
	log.Printf("%s, target architecture is %s, operating system target is %s", runtime.Version(), runtime.GOARCH, runtime.GOOS)
	log.Printf("launch command: %s", strings.Join(os.Args, " "))

	cfg.RaftSnapInterval = parseDuration(raftSnapInterval, "failed to parse Raft Snapsnot interval")
	cfg.RaftLeaderLeaseTimeout = parseDuration(raftLeaderLeaseTimeout, "failed to parse Raft Leader lease timeout")
	cfg.RaftHeartbeatTimeout = parseDuration(raftHeartbeatTimeout, "failed to parse Raft heartbeat timeout")
	cfg.RaftElectionTimeout = parseDuration(raftElectionTimeout, "failed to parse Raft election timeout")
	cfg.RaftApplyTimeout = parseDuration(raftApplyTimeout, "failed to parse Raft apply timeout")
	cfg.JoinInterval = parseDuration(joinInterval, "failed to parse Join interval")
	cfg.RaftOpenTimeout = parseDuration(raftOpenTimeout, "failed to parse Raft open timeout")

	s := server.New(cfg)
	if err := s.Start(); err != nil {
		log.Fatalf("failed to start server: %s", err.Error())
	}

	<-s.Done()
}

func parseDuration(v string, errMsg string) time.Duration {
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Fatalf("%v %s: %s", errMsg, v, err.Error())
	}
	return d
}
