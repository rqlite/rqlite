package main

import (
	"flag"
	"fmt"
	"os"
)

const name = `discoctl`
const desc = `Command-line tool for rqlite Discovery Service control.`
const commands = `
        create            create a new Discovery Service ID
        list              list nodes for a Discovery Service ID
        remove            remove a node from the list, for a given Discovery Service ID
`

var discoURL string

func init() {
	flag.StringVar(&discoURL, "disco-url", "http://discovery.rqlite.com", "Set Discovery Service URL")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "\nUsage: %s [arguments] <command>\n", name)
		fmt.Fprintf(os.Stderr, "\n%s\n\n", desc)
		fmt.Fprintf(os.Stderr, "Available commands:\n%s\n", commands)
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()
}
