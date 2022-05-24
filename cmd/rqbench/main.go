// Command rqbench is a simple rqlite load test utility.

package main

import (
	"flag"
	"fmt"
	"os"
	"time"
)

var addr string
var numReqs int
var batchSz int
var modPrint int
var tx bool
var qw bool
var tp string
var path string
var oneshot string

const name = `rqbench`
const desc = `rqbench is a simple load testing utility for rqlite.`

func init() {
	flag.StringVar(&addr, "a", "localhost:4001", "Node address")
	flag.IntVar(&numReqs, "n", 100, "Number of requests")
	flag.IntVar(&batchSz, "b", 1, "Statements per request")
	flag.IntVar(&modPrint, "m", 0, "Print progress every m requests")
	flag.BoolVar(&tx, "x", false, "Use explicit transaction per request")
	flag.BoolVar(&qw, "q", false, "Use queued writes")
	flag.StringVar(&tp, "t", "http", "Transport to use")
	flag.StringVar(&path, "p", "/db/execute", "Endpoint to use")
	flag.StringVar(&oneshot, "o", "", "One-shot execute statement to preload")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "\n%s\n\n", desc)
		fmt.Fprintf(os.Stderr, "Usage: %s [arguments] <SQL statement>\n", name)
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	// Ensure the SQL statement is set
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(1)
	}
	stmt := flag.Args()[0]

	if tp != "http" {
		fmt.Fprintf(os.Stderr, "not a valid transport: %s\n", tp)
	}

	if oneshot != "" {
		o := NewHTTPTester(addr, "/db/execute")
		if err := o.Prepare(oneshot, 1, false); err != nil {
			fmt.Println("failed to prepare oneshot:", err.Error())
			os.Exit(1)
		}
		_, err := run(o, 1)
		if err != nil {
			fmt.Println("failed to execute oneshot:", err.Error())
			os.Exit(1)
		}
	}

	var tester Tester
	tester = NewHTTPTester(addr, path)
	if qw {
		fmt.Println("using queued write tester")
		tester = NewQueuedHTTPTester(addr, path)
	}
	if err := tester.Prepare(stmt, batchSz, tx); err != nil {
		fmt.Println("failed to prepare test:", err.Error())
		os.Exit(1)
	}
	fmt.Println("Test target:", tester.String())

	d, err := run(tester, numReqs)
	if err != nil {
		fmt.Println("failed to run test:", err.Error())
		os.Exit(1)
	}
	fmt.Println("Total duration:", d)
	fmt.Printf("Requests/sec: %.2f\n", float64((numReqs))/d.Seconds())
	fmt.Printf("Statements/sec: %.2f\n", float64((numReqs*batchSz))/d.Seconds())
}

// Tester is the interface test executors must implement.
type Tester interface {
	fmt.Stringer
	Prepare(stmt string, bSz int, tx bool) error
	Once() (time.Duration, error)
	Close() error
}

func run(t Tester, n int) (time.Duration, error) {
	var dur time.Duration

	for i := 0; i < n; i++ {
		d, err := t.Once()
		if err != nil {
			return 0, err
		}
		dur += d

		if modPrint != 0 && i != 0 && i%modPrint == 0 {
			fmt.Printf("%d requests completed in %s\n", i, d)
		}
	}
	return dur, t.Close()
}
