package main

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"

	"github.com/hashicorp/go-hclog"
)

// prof stores the file locations of active profiles.
var prof struct {
	cpu   *os.File
	mem   *os.File
	trace *os.File
}

// startProfile starts any requested profiling.
func startProfile(cpuprofile, memprofile, traceprofile string) {
	log := hclog.Default()
	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Error(fmt.Sprintf("failed to create CPU profile file at %s", cpuprofile), "error", err)
			os.Exit(1)
		}
		log.Info(fmt.Sprintf("writing CPU profile to: %s\n", cpuprofile))
		prof.cpu = f
		if err := pprof.StartCPUProfile(prof.cpu); err != nil {
			log.Error("failed to start CPU profiling", "error", err)
			os.Exit(1)
		}
	}

	if memprofile != "" {
		f, err := os.Create(memprofile)
		if err != nil {
			log.Error(fmt.Sprintf("failed to create memory profile file at %s", cpuprofile), "error", err.Error())
			os.Exit(1)
		}
		log.Info(fmt.Sprintf("writing memory profile to: %s\n", memprofile))
		prof.mem = f
		runtime.MemProfileRate = 4096
	}

	if traceprofile != "" {
		f, err := os.Create(traceprofile)
		if err != nil {
			log.Error(fmt.Sprintf("failed to create trace profile file at %s", cpuprofile), "error", err.Error())
			os.Exit(1)
		}
		prof.trace = f
		log.Info(fmt.Sprintf("writing trace profile to: %s\n", traceprofile))
		if err := trace.Start(prof.trace); err != nil {
			log.Error("failed to start trace profiling", "error", err)
			os.Exit(1)
		}
	}
}

// stopProfile stops any active profiling.
func stopProfile() {
	log := hclog.Default()
	if prof.cpu != nil {
		pprof.StopCPUProfile()
		if err := prof.cpu.Close(); err != nil {
			log.Error("failed to close CPU profile file", "error", err)
			os.Exit(1)
		}
		log.Info("CPU profiling stopped")
	}
	if prof.mem != nil {
		if err := pprof.Lookup("heap").WriteTo(prof.mem, 0); err != nil {
			log.Error("failed to write memory profile", "error", err)
			os.Exit(1)
		}
		if err := prof.mem.Close(); err != nil {
			log.Error("failed to close memory profile file", "error", err)
			os.Exit(1)
		}
		log.Info("memory profiling stopped")
	}
	if prof.trace != nil {
		trace.Stop()
		if err := prof.trace.Close(); err != nil {
			log.Error("failed to close trace profile file", "error", err)
			os.Exit(1)
		}
		log.Info("trace profiling stopped")
	}
}
