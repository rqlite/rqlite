package main

import (
	"flag"
	"fmt"
	"os"
)

const name = `rqlite-disco`
const desc = `Command-line tool for rqlite Discovery Service control.`
const commands = `
        create            create a new Discovery Service ID
        list              list nodes for a Discovery Service ID
        remove            remove a node from the list, for a given Discovery Service ID
`

var discoURL string
var createFlags *flag.FlagSet
var listFlags *flag.FlagSet
var removeFlags *flag.FlagSet

func init() {
	flag.StringVar(&discoURL, "disco-url", "http://discovery.rqlite.com", "Set Discovery Service URL")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "\nUsage: %s [arguments] <command>\n", name)
		fmt.Fprintf(os.Stderr, "\n%s\n\n", desc)
		fmt.Fprintf(os.Stderr, "Available commands:\n%s\n", commands)
		flag.PrintDefaults()
	}

	createFlags = flag.NewFlagSet("create", flag.ExitOnError)
	createFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, "\nUsage: %s \n", name)
		fmt.Fprintf(os.Stderr, "Create a new Discovery Service ID")
	}
}

func main() {
	flag.Parse()
	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(1)
	}

	switch flag.Arg(0) {
	case "create":
		createFlags.Parse(flag.Args()[2:])
		os.Exit(0)
	case "list":
		fmt.Println("list")
		os.Exit(0)
	case "remove":
		fmt.Println("remove")
		os.Exit(0)
	default:
		flag.Usage()
		os.Exit(1)
	}
}
