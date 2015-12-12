/*
rqlite -- a replicated SQLite database.

rqlite is a distributed system that provides a replicated SQLite database.
rqlite is written in Go and uses Raft to achieve consensus across all the
instances of the SQLite databases. rqlite ensures that every change made to
the database is made to a majority of underlying SQLite files, or none-at-all.
*/

package main

import (
	"bytes"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/jsrCorp/rqlite/log"
	"github.com/jsrCorp/rqlite/server"
)

var host string
var port int
var join string
var dbfile string
var cpuprofile string
var logFile string
var logLevel string
var snapAfter int
var disableReporting bool

func init() {
	flag.StringVar(&host, "h", "localhost", "hostname")
	flag.IntVar(&port, "p", 4001, "port")
	flag.StringVar(&join, "join", "", "host:port of leader to join")
	flag.StringVar(&dbfile, "dbfile", "db.sqlite", "sqlite filename")
	flag.StringVar(&cpuprofile, "cpuprofile", "", "write CPU profile to file")
	flag.StringVar(&logFile, "logfile", "stdout", "log file path")
	flag.StringVar(&logLevel, "loglevel", "INFO", "log level (ERROR|WARN|INFO|DEBUG|TRACE)")
	flag.IntVar(&snapAfter, "s", 100, "Snapshot and compact after this number of new log entries")
	flag.BoolVar(&disableReporting, "noreport", false, "Disable anonymised launch reporting")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [arguments] <data-path> \n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	// Set up profiling, if requested.
	if cpuprofile != "" {
		log.Info("Profiling enabled")
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Errorf("Unable to create path: %s", err.Error())
		}
		defer closeFile(f)

		err = pprof.StartCPUProfile(f)
		if err != nil {
			log.Errorf("Unable to start CPU Profile: %s", err.Error())
		}

		defer pprof.StopCPUProfile()
	}

	// Set logging
	log.SetLevel(logLevel)
	if logFile != "stdout" {
		f := createFile(logFile)
		defer closeFile(f)

		log.Infof("Redirecting logging to %s", logFile)
		log.SetOutput(f)
	}

	// Set the data directory.
	if flag.NArg() == 0 {
		flag.Usage()
		println("Data path argument required")
		log.Error("No data path supplied -- aborting")
		os.Exit(1)
	}

	dataPath := flag.Arg(0)
	createDir(dataPath)

	s := server.NewServer(dataPath, dbfile, snapAfter, host, port)
	go func() {
		log.Error(s.ListenAndServe(join).Error())
	}()

	if !disableReporting {
		reportLaunch()
	}

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	log.Info("rqlite server stopped")
}

func closeFile(f *os.File) {
	if err := f.Close(); err != nil {
		log.Errorf("Unable to close file: %s", err.Error())
		os.Exit(1)
	}
}

func createFile(path string) *os.File {
	usr, _ := user.Current()
	dir := usr.HomeDir

	// Check in case of paths like "/something/~/something/"
	if path[:2] == "~/" {
		path = strings.Replace(path, "~/", dir+"/", 1)
	}

	if err := os.MkdirAll(filepath.Dir(path), 0744); err != nil {
		log.Errorf("Unable to create path: %s", err.Error())
		os.Exit(1)
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil && !strings.Contains(err.Error(), "is a directory") {
		log.Errorf("Unable to open file: %s", err.Error())
		os.Exit(1)
	}

	return f
}

func createDir(path string) {
	if err := os.MkdirAll(path, 0744); err != nil {
		log.Errorf("Unable to create path: %s", err.Error())
		os.Exit(1)
	}
}

func reportLaunch() {
	json := fmt.Sprintf(`{"os": "%s", "arch": "%s", "app": "rqlite"}`, runtime.GOOS, runtime.GOARCH)
	data := bytes.NewBufferString(json)
	client := http.Client{Timeout: time.Duration(5 * time.Second)}
	go func() {
		_, err := client.Post("https://logs-01.loggly.com/inputs/8a0edd84-92ba-46e4-ada8-c529d0f105af/tag/reporting/",
			"application/json", data)
		if err != nil {
			log.Errorf("Report launch failed: %s", err.Error())
		}
	}()
}
