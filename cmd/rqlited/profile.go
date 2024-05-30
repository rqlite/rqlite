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
		pprof.StartCPUProfile(prof.cpu)
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
		trace.Start(prof.trace)
	}
}

// stopProfile stops any active profiling.
func stopProfile() {
	if prof.cpu != nil {
		pprof.StopCPUProfile()
		prof.cpu.Close()
		log.Println("CPU profiling stopped")
	}
	if prof.mem != nil {
		pprof.Lookup("heap").WriteTo(prof.mem, 0)
		prof.mem.Close()
		log.Println("memory profiling stopped")
	}
	if prof.trace != nil {
		trace.Stop()
		prof.trace.Close()
		log.Println("trace profiling stopped")
	}
}
