package server

import "time"

type Config struct {
	DataPath               string
	HTTPAddr               string
	HTTPAdv                string
	JoinSrcIP              string
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
	JoinAddr               string
	JoinAttempts           int
	JoinInterval           time.Duration
	NoVerify               bool
	NoNodeVerify           bool
	DiscoURL               string
	DiscoID                string
	Expvar                 bool
	PPROFEnabled           bool
	DSN                    string
	OnDisk                 bool
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
	CPUProfile             string
	MemoryProfile          string
}

func Defaults(dataPath string) Option {
	return func(c *Config) {
		c.DataPath = dataPath
		c.HTTPAddr = "localhost:4001"
		c.NodeX509Cert = "cert.pem"
		c.NodeX509Key = "key.pem"
		c.RaftAddr = "localhost:4002"
		c.JoinAttempts = 5
		c.JoinInterval = 5 * time.Second
		c.DiscoURL = "http://discovery.rqlite.com"
		c.Expvar = true
		c.PPROFEnabled = true
		c.RaftHeartbeatTimeout = 1 * time.Second
		c.RaftElectionTimeout = 1 * time.Second
		c.RaftApplyTimeout = 10 * time.Second
		c.RaftOpenTimeout = 120 * time.Second
		c.RaftWaitForLeader = true
		c.RaftSnapThreshold = 8192
		c.RaftSnapInterval = 30 * time.Second
		c.RaftLeaderLeaseTimeout = 0
		c.RaftLogLevel = "INFO"
		c.CompressionSize = 150
		c.CompressionBatch = 5
	}
}

func (c *Config) With(ops ...Option) *Config {
	for _, o := range ops {
		o(c)
	}
	return c
}

type Option func(*Config)

func HTTPAddr(a string) Option {
	return func(c *Config) {
		c.HTTPAddr = a
	}
}

func RaftAddr(a string) Option {
	return func(c *Config) {
		c.RaftAddr = a
	}
}

func JoinAddr(a string) Option {
	return func(c *Config) {
		c.JoinAddr = a
	}
}
