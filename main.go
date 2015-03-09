/*
rqlite -- a replicated SQLite database.

rqlite is a distributed system that provides a replicated SQLite database.
rqlite is written in Go and uses Raft to achieve consensus across all the
instances of the SQLite databases. rqlite ensures that every change made to
the database is made to a majority of underlying SQLite files, or none-at-all.
*/

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/pprof"

	"github.com/otoolep/rqlite/server"

	log "code.google.com/p/log4go"
)

var host string
var port int
var join string
var dbfile string
var cpuprofile string
var logFile string
var logLevel string
var snapAfter int

func init() {
	flag.StringVar(&host, "h", "localhost", "hostname")
	flag.IntVar(&port, "p", 4001, "port")
	flag.StringVar(&join, "join", "", "host:port of leader to join")
	flag.StringVar(&dbfile, "dbfile", "db.sqlite", "sqlite filename")
	flag.StringVar(&cpuprofile, "cpuprofile", "", "write CPU profile to file")
	flag.StringVar(&logFile, "logfile", "stdout", "log file path")
	flag.StringVar(&logLevel, "loglevel", "INFO", "log level (ERROR|WARN|INFO|DEBUG|TRACE)")
	flag.IntVar(&snapAfter, "s", 1000, "Snapshot and compact after this number of new log entries")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [arguments] <data-path> \n", os.Args[0])
		flag.PrintDefaults()
	}
}

func setupLogging(loggingLevel, logFile string) {
	level := log.DEBUG
	switch loggingLevel {
	case "TRACE":
		level = log.TRACE
	case "DEBUG":
		level = log.DEBUG
	case "INFO":
		level = log.INFO
	case "WARN":
		level = log.WARNING
	case "ERROR":
		level = log.ERROR
	}

	log.Global = make(map[string]*log.Filter)

	if logFile == "stdout" {
		flw := log.NewConsoleLogWriter()
		log.AddFilter("stdout", level, flw)

	} else {
		logFileDir := filepath.Dir(logFile)
		os.MkdirAll(logFileDir, 0744)

		flw := log.NewFileLogWriter(logFile, false)
		log.AddFilter("file", level, flw)

		flw.SetFormat("[%D %T] [%L] (%S) %M")
		flw.SetRotate(true)
		flw.SetRotateSize(0)
		flw.SetRotateLines(0)
		flw.SetRotateDaily(true)
	}

	log.Info("Redirectoring logging to %s", logFile)
}

func main() {
	flag.Parse()

	// Set up profiling, if requested.
	if cpuprofile != "" {
		log.Info("Profiling enabled")
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Error("Unable to create path: %s", err.Error())
		}
		defer f.Close()

		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	setupLogging(logLevel, logFile)

	// Set the data directory.
	if flag.NArg() == 0 {
		flag.Usage()
		println("Data path argument required")
		log.Error("No data path supplied -- aborting")
		os.Exit(1)
	}
	path := flag.Arg(0)
	if err := os.MkdirAll(path, 0744); err != nil {
		log.Error("Unable to create path: %s", err.Error())
	}

	s := server.NewServer(path, dbfile, snapAfter, host, port)
	go func() {
		log.Error(s.ListenAndServe(join))
	}()

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	log.Info("rqlite server stopped")
}
