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
	"strconv"
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

	if len(c.ExtensionPaths) > 0 {
		for _, p := range c.ExtensionPaths {
			if !fileExists(p) {
				return fmt.Errorf("extension path does not exist: %s", p)
			}
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

// BuildInfo is build information for display at command line.
type BuildInfo struct {
	Version       string
	Commit        string
	Branch        string
	SQLiteVersion string
}

// ParseFlags parses the command line, and returns the configuration.
func ParseFlags(name, desc string, build *BuildInfo) (*Config, error) {
	fs, config, err := Forge(os.Args[1:])
	if err != nil {
		return nil, err
	}

	// if showVersion {
	// 	msg := fmt.Sprintf("%s %s %s %s %s sqlite%s (commit %s, branch %s, compiler %s)",
	// 		name, build.Version, runtime.GOOS, runtime.GOARCH, runtime.Version(), build.SQLiteVersion,
	// 		build.Commit, build.Branch, runtime.Compiler)
	// 	errorExit(0, msg)
	// }

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
