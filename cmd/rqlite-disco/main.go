package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/rqlite/rqlite/disco"
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
		id, err := create()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create discovery ID: %s", err.Error())
			os.Exit(1)
		}
		fmt.Println(id)
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

func create() (string, error) {
	url := discoURL

	resp, err := http.Post(discoURL, "", nil)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	fmt.Println(string(b))

	r := &disco.Response{}
	if err := json.Unmarshal(b, r); err != nil {
		return "", err
	}

	return r.DiscoID, nil
}
