package main

import (
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
)

// prof stores the file locations of active profiles.
var prof struct {
	cpu   *os.File
	mem   *os.File
	trace *os.File
}

// startProfile starts any requested profiling.
func startProfile(cpuprofile, memprofile, traceprofile string) {
	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Fatalf("failed to create CPU profile file at %s: %s", cpuprofile, err.Error())
		}
		log.Printf("writing CPU profile to: %s\n", cpuprofile)
		prof.cpu = f
		if err := pprof.StartCPUProfile(prof.cpu); err != nil {
			log.Fatalf("failed to start CPU profiling: %s", err.Error())
		}
	}

	if memprofile != "" {
		f, err := os.Create(memprofile)
		if err != nil {
			log.Fatalf("failed to create memory profile file at %s: %s", cpuprofile, err.Error())
		}
		log.Printf("writing memory profile to: %s\n", memprofile)
		prof.mem = f
		runtime.MemProfileRate = 4096
	}

	if traceprofile != "" {
		f, err := os.Create(traceprofile)
		if err != nil {
			log.Fatalf("failed to create trace profile file at %s: %s", cpuprofile, err.Error())
		}
		prof.trace = f
		log.Printf("writing trace profile to: %s\n", traceprofile)
		if err := trace.Start(prof.trace); err != nil {
			log.Fatalf("failed to start trace profiling: %s", err.Error())
		}
	}
}

// stopProfile stops any active profiling.
func stopProfile() {
	if prof.cpu != nil {
		pprof.StopCPUProfile()
		if err := prof.cpu.Close(); err != nil {
			log.Fatalf("failed to close CPU profile file: %s", err.Error())
		}
		log.Println("CPU profiling stopped")
	}
	if prof.mem != nil {
		if err := pprof.Lookup("heap").WriteTo(prof.mem, 0); err != nil {
			log.Fatalf("failed to write memory profile: %s", err.Error())
		}
		if err := prof.mem.Close(); err != nil {
			log.Fatalf("failed to close memory profile file: %s", err.Error())
		}
		log.Println("memory profiling stopped")
	}
	if prof.trace != nil {
		trace.Stop()
		if err := prof.trace.Close(); err != nil {
			log.Fatalf("failed to close trace profile file: %s", err.Error())
		}
		log.Println("trace profiling stopped")
	}
}
