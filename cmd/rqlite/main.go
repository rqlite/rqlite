// Command rqlite is the command-line interface for rqlite.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/mkideal/cli"
	clix "github.com/mkideal/cli/ext"
	"github.com/peterh/liner"
	"github.com/rqlite/rqlite/v10/cmd"
	"github.com/rqlite/rqlite/v10/cmd/rqlite/history"
	httpcl "github.com/rqlite/rqlite/v10/cmd/rqlite/http"
	"github.com/rqlite/rqlite/v10/internal/rtls"
)

const HOST_ENV_VAR = "RQLITE_HOST"

type Node struct {
	ApiAddr string `json:"api_addr"`
	_       json.RawMessage
}

type Nodes map[string]Node

type argT struct {
	cli.Helper
	Alternatives string        `cli:"a,alternatives" usage:"comma separated list of 'host:port' pairs to use as fallback"`
	Protocol     string        `cli:"s,scheme" usage:"protocol scheme (http or https)" dft:"http"`
	Host         string        `cli:"H,host" usage:"rqlited host address" dft:"127.0.0.1"`
	Port         uint16        `cli:"p,port" usage:"rqlited host port" dft:"4001"`
	Prefix       string        `cli:"P,prefix" usage:"rqlited HTTP URL prefix" dft:"/"`
	Insecure     bool          `cli:"i,insecure" usage:"do not verify rqlited HTTPS certificate" dft:"false"`
	CACert       string        `cli:"c,ca-cert" usage:"path to trusted X.509 root CA certificate"`
	ClientCert   string        `cli:"d,client-cert" usage:"path to client X.509 certificate for mutual TLS"`
	ClientKey    string        `cli:"k,client-key" usage:"path to client X.509 key for mutual TLS"`
	Credentials  string        `cli:"u,user" usage:"set basic auth credentials in form username:password"`
	Version      bool          `cli:"v,version" usage:"display CLI version"`
	HTTPTimeout  clix.Duration `cli:"t,http-timeout" usage:"set timeout on HTTP requests" dft:"30s"`
}

var cliHelp []string

func init() {
	cliHelp = []string{
		`.backup FILE                                  Write database backup to FILE`,
		`.blobarray [on|off]                           Show setting, or set BLOB data display as byte arrays`,
		`.boot FILE                                    Boot the node using a SQLite file read from FILE`,
		`.changes [on|off]                             Show setting, or set display of last insert ID after writes`,
		`.consistency [none|weak|linearizable|strong]  Show or set read consistency level`,
		`.dump FILE [TABLE,TABLE...]                   Dump the database in SQL text to FILE, optionally limited to TABLEs`,
		`.exit                                         Exit this program`,
		`.expvar                                       Show expvar (Go runtime) information for connected node`,
		`.extensions                                   Show loaded SQLite extensions`,
		`.forcewrites [on|off]                         Show setting, or set all statements to be executed via /db/execute`,
		`.help                                         Show this message`,
		`.indexes                                      Show names of all indexes`,
		`.leader                                       Show the current cluster leader`,
		`.mode [column|csv|json|line]                  Show or set output mode for query results`,
		`.nodes [all]                                  Show connection status of voting nodes. 'all' to show all nodes`,
		`.output FILE                                  Send output to FILE, or stdout if FILE is omitted`,
		`.quit                                         Exit this program`,
		`.read FILE                                    Read and execute SQL statements from FILE`,
		`.ready                                        Show ready status for connected node`,
		`.remove NODEID                                Remove node NODEID from the cluster`,
		`.restore FILE                                 Load using SQLite file or SQL dump contained in FILE`,
		`.schema                                       Show CREATE statements for all tables`,
		`.reap                                         Request a snapshot reap on connected node`,
		`.show                                         Show the current values for various settings`,
		`.snapshot [TRAILINGLOGS]                      Request a Raft snapshot and log truncation on connected node`,
		`.status                                       Show status and diagnostic information for connected node`,
		`.stepdown [NODEID]                            Instruct the leader to stepdown, optionally specifying new Leader node`,
		`.sysdump FILE                                 Dump system diagnostics to FILE`,
		`.tables                                       List names of tables`,
		`.timer [on|off]                               Show setting, or set query timings on or off`,
	}
	sort.Strings(cliHelp)
}

func main() {
	cli.SetUsageStyle(cli.ManualStyle)
	cli.Run(new(argT), func(ctx *cli.Context) error {
		argv := ctx.Argv().(*argT)
		if argv.Help {
			ctx.WriteUsage()
			return nil
		}

		if argv.Version {
			ctx.String("Version %s, commit %s, built on %s\n", cmd.Version,
				cmd.Commit, cmd.Buildtime)
			return nil
		}

		_, hostEnvSet := os.LookupEnv(HOST_ENV_VAR)
		if hostEnvSet && !(ctx.IsSet("--host", "-H") || ctx.IsSet("--port", "-p") || ctx.IsSet("--scheme", "-s")) {
			protocol, host, port, err := httpcl.ParseHostEnv(HOST_ENV_VAR)
			if err != nil {
				return err
			}
			if protocol != "" {
				argv.Protocol = protocol
			}
			if host != "" {
				argv.Host = host
			}
			if port != 0 {
				argv.Port = port
			}
		}

		httpClient, err := getHTTPClient(argv)
		if err != nil {
			ctx.String("%s %v\n", ctx.Color().Red("ERR!"), err)
			return nil
		}

		hosts := createHostList(argv)
		client := httpcl.NewClient(httpClient, hosts,
			httpcl.WithScheme(argv.Protocol),
			httpcl.WithBasicAuth(argv.Credentials),
			httpcl.WithPrefix(argv.Prefix))

		connectionStr := fmt.Sprintf("%s://%s", argv.Protocol, address6(argv))
		version, err := getVersion(client)
		if err != nil {
			msg := err.Error()
			if errors.Is(err, syscall.ECONNREFUSED) {
				msg = fmt.Sprintf("Unable to connect to rqlited at %s - is it running?",
					connectionStr)
			}
			ctx.String("%s %v\n", ctx.Color().Red("ERR!"), msg)
			return nil
		}

		fmt.Println("Welcome to the rqlite CLI.")
		fmt.Printf("Enter \".help\" for usage hints.\n")
		fmt.Printf("Connected to %s running version %s\n", connectionStr, version)

		blobArray := false
		timer := false
		forceWrites := false
		changes := false
		consistency := "weak"
		mode := "column"
		prefix := fmt.Sprintf("%s>", address6(argv))

		var output io.Writer = os.Stdout
		var outputFile *os.File
		outputName := "stdout"
		defer func() {
			if outputFile != nil {
				outputFile.Close()
			}
		}()

		// Set up line editing with liner
		line := liner.NewLiner()
		defer line.Close()

		// Set up command history.
		hr := history.Reader()
		if hr != nil {
			_, err := line.ReadHistory(hr)
			if err != nil {
				// If reading history fails, it's not critical - just continue
			}
			hr.Close()
		}

	FOR_READ:
		for {
			input, err := line.Prompt(prefix + " ")
			if err != nil {
				if err == liner.ErrPromptAborted {
					break FOR_READ
				}
				if err == io.EOF {
					break FOR_READ
				}
				return err
			}

			input = strings.TrimSpace(input)
			if input == "" {
				continue
			}

			// Add command to history for up/down arrow navigation
			line.AppendHistory(input)

			var (
				index = strings.Index(input, " ")
				cmd   = input
			)
			if index >= 0 {
				cmd = input[:index]
			}
			cmd = strings.ToUpper(cmd)
			switch cmd {
			case ".CHANGES":
				err = handleToggle(ctx, input, index, &changes)
			case ".CONSISTENCY":
				if index == -1 || index == len(input)-1 {
					ctx.String("%s\n", consistency)
					break
				}
				err = setConsistency(input[index+1:], &consistency)
			case ".EXTENSIONS":
				err = extensions(ctx, client)
			case ".FORCEWRITES":
				err = handleToggle(ctx, input, index, &forceWrites)
			case ".TABLES":
				err = queryWithClient(output, client, timer, blobArray, consistency, mode, `SELECT name FROM sqlite_master WHERE type="table" ORDER BY name ASC`)
			case ".INDEXES":
				err = queryWithClient(output, client, timer, blobArray, consistency, mode, `SELECT sql FROM sqlite_master WHERE type="index"`)
			case ".SCHEMA":
				err = queryWithClient(output, client, timer, blobArray, consistency, mode, `SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY rowid ASC`)
			case ".TIMER":
				err = handleToggle(ctx, input, index, &timer)
			case ".BLOBARRAY":
				err = handleToggle(ctx, input, index, &blobArray)
			case ".STATUS":
				err = status(ctx, client, input)
			case ".READY":
				err = ready(ctx, client)
			case ".LEADER":
				err = leader(ctx, client)
			case ".MODE":
				if index == -1 || index == len(input)-1 {
					ctx.String("%s\n", mode)
					break
				}
				err = setMode(input[index+1:], &mode)
			case ".NODES":
				err = nodes(ctx, client, index != -1 && index < len(input)-1)
			case ".EXPVAR":
				err = expvar(ctx, client, input)
			case ".REMOVE":
				err = removeNode(client, input[index+1:])
			case ".BACKUP":
				arg, argErr := requireArg(input, index, "please specify an output file for the backup")
				if argErr != nil {
					err = argErr
				} else {
					err = backup(ctx, client, arg)
				}
			case ".RESTORE":
				arg, argErr := requireArg(input, index, "please specify an input file to restore from")
				if argErr != nil {
					err = argErr
				} else {
					err = restore(ctx, client, arg)
				}
			case ".BOOT":
				arg, argErr := requireArg(input, index, "please specify an input file to boot with")
				if argErr != nil {
					err = argErr
				} else {
					err = boot(ctx, client, arg)
				}
			case ".SYSDUMP":
				arg, argErr := requireArg(input, index, "please specify an output file for the sysdump")
				if argErr != nil {
					err = argErr
				} else {
					err = sysdump(client, arg)
				}
			case ".DUMP":
				arg, argErr := requireArg(input, index, "please specify an output file for the SQL text")
				if argErr != nil {
					err = argErr
				} else {
					filename, tables := parseDumpArgs(arg)
					err = dump(ctx, client, filename, tables)
				}
			case ".HELP":
				ctx.String("%s\n", strings.Join(cliHelp, "\n"))
			case ".QUIT", "QUIT", "EXIT", ".EXIT":
				break FOR_READ
			case ".REAP":
				err = reap(ctx, client)
			case ".READ":
				arg, argErr := requireArg(input, index, "please specify an input file to read from")
				if argErr != nil {
					err = argErr
				} else {
					err = readFile(ctx, output, client, timer, forceWrites, changes, mode, arg)
				}
			case ".OUTPUT":
				arg := ""
				if index != -1 && index < len(input)-1 {
					arg = input[index+1:]
				}
				if arg == "" || arg == "stdout" {
					if outputFile != nil {
						outputFile.Close()
						outputFile = nil
					}
					output = os.Stdout
					outputName = "stdout"
				} else {
					f, openErr := os.Create(arg)
					if openErr != nil {
						err = openErr
						break
					}
					if outputFile != nil {
						outputFile.Close()
					}
					outputFile = f
					output = f
					outputName = arg
				}
			case ".SHOW":
				ctx.String("  blobarray: %s\n", onOff(blobArray))
				ctx.String("  changes: %s\n", onOff(changes))
				ctx.String("  consistency: %s\n", consistency)
				ctx.String("  forcewrites: %s\n", onOff(forceWrites))
				ctx.String("  mode: %s\n", mode)
				ctx.String("  output: %s\n", outputName)
				ctx.String("  timer: %s\n", onOff(timer))
			case ".SNAPSHOT":
				trailingLogs := 0
				if index != -1 && index < len(input)-1 {
					trailingLogsStr := strings.TrimSpace(input[index+1:])
					if trailingLogsStr != "" {
						trailingLogs, err = strconv.Atoi(trailingLogsStr)
						if err != nil || trailingLogs < 0 {
							err = fmt.Errorf("invalid trailing logs value: %s", trailingLogsStr)
							break
						}
					}
				}
				err = snapshot(client, trailingLogs)
			case ".STEPDOWN":
				nodeID := ""
				if index != -1 && index < len(input)-1 {
					nodeID = strings.TrimSpace(input[index+1:])
				}
				err = stepdown(client, nodeID)
			default:
				err = requestWithClient(output, client, timer, forceWrites, changes, mode, input)
			}
			if hcerr, ok := err.(*httpcl.HostChangedError); ok {
				// If a previous request was executed on a different host, make that change
				// visible to the user.
				if hcerr != nil {
					prefix = fmt.Sprintf("%s>", hcerr.NewHost)
				}
			} else if err != nil {
				ctx.String("%s %v\n", ctx.Color().Red("ERR!"), err)
			}
		}

		hw := history.Writer()
		if hw != nil {
			sz := history.Size()
			// Write history using liner's WriteHistory method
			_, err := line.WriteHistory(hw)
			hw.Close()
			if err != nil || sz <= 0 {
				history.Delete()
			}
		}
		ctx.String("bye~\n")
		return nil
	})
}

func onOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

func handleToggle(ctx *cli.Context, input string, index int, flag *bool) error {
	if index == -1 || index == len(input)-1 {
		if *flag {
			ctx.String("on\n")
		} else {
			ctx.String("off\n")
		}
		return nil
	}
	return toggleFlag(input[index+1:], flag)
}

func toggleFlag(op string, flag *bool) error {
	if op != "on" && op != "off" {
		return fmt.Errorf("invalid option '%s'. Use 'on' or 'off' (default)", op)
	}
	*flag = (op == "on")
	return nil
}

func requireArg(input string, index int, msg string) (string, error) {
	if index == -1 || index == len(input)-1 {
		return "", errors.New(msg)
	}
	return input[index+1:], nil
}

func parseDumpArgs(arg string) (string, []string) {
	args := strings.Fields(arg)
	if len(args) == 0 {
		return arg, nil
	}
	filename := args[0]
	if len(args) <= 1 {
		return filename, nil
	}
	// Join all remaining arguments as table specification and split by comma
	tablesStr := strings.Join(args[1:], " ")
	parts := strings.Split(tablesStr, ",")
	var tables []string
	for _, table := range parts {
		table = strings.TrimSpace(table)
		if table != "" {
			tables = append(tables, table)
		}
	}
	return filename, tables
}

func setMode(m string, mode *string) error {
	switch m {
	case "column", "csv", "json", "line":
		*mode = m
		return nil
	default:
		return fmt.Errorf("invalid mode '%s'. Use 'column', 'csv', 'json', or 'line'", m)
	}
}

func setConsistency(r string, c *string) error {
	if r != "strong" && r != "weak" && r != "linearizable" && r != "none" {
		return fmt.Errorf("invalid consistency '%s'. Use 'none', 'weak', 'linearizable', or 'strong'", r)
	}
	*c = r
	return nil
}

func makeJSONBody(line string) string {
	data, err := json.Marshal([]string{line})
	if err != nil {
		return ""
	}
	return string(data)
}

func status(ctx *cli.Context, client *httpcl.Client, line string) error {
	u := fmt.Sprintf("%sstatus", client.Prefix)
	return cliJSON(ctx, client, line, u)
}

func ready(ctx *cli.Context, client *httpcl.Client) error {
	u := fmt.Sprintf("%sreadyz", client.Prefix)
	resp, err := client.Get(u)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		ctx.String("ready\n")
	} else {
		ctx.String("not ready\n")
	}
	return nil
}

func leader(ctx *cli.Context, client *httpcl.Client) error {
	u := fmt.Sprintf("%snodes", client.Prefix)
	resp, err := client.Get(u)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("unauthorized")
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server responded with %s: %s", resp.Status, body)
	}

	allNodes := make(map[string]any)
	if err := parseResponse(body, &allNodes); err != nil {
		return err
	}

	for id, v := range allNodes {
		node, ok := v.(map[string]any)
		if !ok {
			continue
		}
		if isLeader, ok := node["leader"].(bool); ok && isLeader {
			pprintJSON(0, map[string]any{id: node})
			return nil
		}
	}
	return fmt.Errorf("no leader found")
}

func nodes(ctx *cli.Context, client *httpcl.Client, all bool) error {
	u := fmt.Sprintf("%snodes", client.Prefix)
	if all {
		u += "?nonvoters"
	}
	return cliJSON(ctx, client, "", u)
}

func expvar(ctx *cli.Context, client *httpcl.Client, line string) error {
	u := fmt.Sprintf("%sdebug/vars", client.Prefix)
	return cliJSON(ctx, client, line, u)
}

func reap(ctx *cli.Context, client *httpcl.Client) error {
	u := fmt.Sprintf("%sreap", client.Prefix)
	resp, err := client.Post(u, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("unauthorized")
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server responded with %s", resp.Status)
	}

	var result map[string]int
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("failed to decode response: %s", err)
	}
	ctx.String("snapshots reaped: %d, WALs checkpointed: %d\n",
		result["snapshots_reaped"], result["wals_checkpointed"])
	return nil
}

func snapshot(client *httpcl.Client, trailingLogs int) error {
	u := fmt.Sprintf("%ssnapshot?trailing_logs=%d", client.Prefix, trailingLogs)
	resp, err := client.Post(u, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("unauthorized")
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("server responded with %s", resp.Status)
	}
	return nil
}

func stepdown(client *httpcl.Client, nodeID string) error {
	u := fmt.Sprintf("%sleader?wait=true", client.Prefix)

	var body io.Reader
	var headers http.Header
	if nodeID != "" {
		jsonBody, err := json.Marshal(map[string]string{"id": nodeID})
		if err != nil {
			return err
		}
		body = bytes.NewReader(jsonBody)
		headers = http.Header{"Content-Type": {"application/json"}}
	}

	resp, err := client.PostWithHeaders(u, body, headers)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("unauthorized")
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server responded with %s", resp.Status)
	}
	return nil
}

func readFile(ctx *cli.Context, output io.Writer, client *httpcl.Client, timer, forceWrites, changes bool, mode, filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	var lastHostChanged error
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}
		err := requestWithClient(output, client, timer, forceWrites, changes, mode, line)
		if hcerr, ok := err.(*httpcl.HostChangedError); ok {
			lastHostChanged = hcerr
		} else if err != nil {
			ctx.String("%s %v\n", ctx.Color().Red("ERR!"), err)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return lastHostChanged
}

func sysdump(client *httpcl.Client, filename string) error {
	nodes, err := getNodes(client)
	if err != nil {
		return err
	}

	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, n := range nodes {
		if n.ApiAddr == "" {
			continue
		}
		urls := []string{
			fmt.Sprintf("%s/status?pretty", n.ApiAddr),
			fmt.Sprintf("%s/nodes?pretty", n.ApiAddr),
			fmt.Sprintf("%s/readyz", n.ApiAddr),
			fmt.Sprintf("%s/debug/vars", n.ApiAddr),
		}
		if err := urlsToWriter(client, urls, f); err != nil {
			f.WriteString(fmt.Sprintf("Error sysdumping %s: %s\n", n.ApiAddr, err.Error()))
		}
	}
	return nil
}

func getNodes(client *httpcl.Client) (Nodes, error) {
	u := fmt.Sprintf("%snodes", client.Prefix)
	resp, err := client.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	response, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var nodes Nodes
	if err := parseResponse(response, &nodes); err != nil {
		return nil, err
	}
	return nodes, nil
}

func getHTTPClient(argv *argT) (*http.Client, error) {
	tlsConfig, err := rtls.CreateClientConfig(argv.ClientCert, argv.ClientKey, argv.CACert, rtls.NoServerName, argv.Insecure)
	if err != nil {
		return nil, err
	}
	tlsConfig.NextProtos = nil // CLI refuses to connect otherwise.

	client := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
			Proxy:           http.ProxyFromEnvironment,
		},
		Timeout: argv.HTTPTimeout.Duration,
	}

	// Explicitly handle redirects.
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	return &client, nil
}

func getVersion(client *httpcl.Client) (string, error) {
	u := fmt.Sprintf("%sstatus", client.Prefix)
	resp, err := client.Get(u)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	version, ok := resp.Header["X-Rqlite-Version"]
	if !ok || len(version) != 1 {
		return "unknown", nil
	}
	return version[0], nil
}

func parseResponse(response []byte, ret any) error {
	decoder := json.NewDecoder(bytes.NewReader(response))
	decoder.UseNumber()
	return decoder.Decode(ret)
}

func extensions(ctx *cli.Context, client *httpcl.Client) error {
	u := fmt.Sprintf("%sstatus?key=extensions", client.Prefix)
	resp, err := client.Get(u)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("unauthorized")
	} else if resp.StatusCode == http.StatusNotFound {
		return nil
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	ret := make(map[string]any)
	if err := parseResponse(body, &ret); err != nil {
		return err
	}

	exts, ok := ret["names"]
	if !ok {
		return nil
	}
	for _, ext := range exts.([]any) {
		ctx.String("%s\n", ext.(string))
	}

	return nil
}

// cliJSON fetches JSON from a URL, and displays it at the CLI. If line contains more
// than one word, then the JSON is filtered to only show the key specified in the
// second word.
func cliJSON(ctx *cli.Context, client *httpcl.Client, line, u string) error {
	resp, err := client.Get(u)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("unauthorized")
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	ret := make(map[string]any)
	if err := parseResponse(body, &ret); err != nil {
		return err
	}

	// Specific key requested?
	parts := strings.Split(line, " ")
	if len(parts) >= 2 {
		ret = map[string]any{parts[1]: ret[parts[1]]}
	}
	pprintJSON(0, ret)

	return nil
}

func pprintJSON(indent int, m map[string]any) {
	const indentation = "  "
	for _, k := range slices.Sorted(maps.Keys(m)) {
		v := m[k]
		if v == nil {
			continue
		}
		switch w := v.(type) {
		case map[string]any:
			for i := 0; i < indent; i++ {
				fmt.Print(indentation)
			}
			fmt.Printf("%s:\n", k)
			pprintJSON(indent+1, w)
		default:
			for i := 0; i < indent; i++ {
				fmt.Print(indentation)
			}
			fmt.Printf("%s: %v\n", k, v)
		}
	}
}

func urlsToWriter(client *httpcl.Client, urls []string, w io.Writer) error {
	for _, u := range urls {
		err := func() error {
			w.Write([]byte("\n=========================================\n"))
			w.Write([]byte(fmt.Sprintf("URL: %s\n", u)))

			resp, err := client.GetDirect(u)
			if err != nil {
				if _, err := w.Write([]byte(fmt.Sprintf("Status: %s\n\n", err))); err != nil {
					return err
				}
				return nil
			}
			defer resp.Body.Close()

			if _, err := w.Write([]byte(fmt.Sprintf("Status: %s\n\n", resp.Status))); err != nil {
				return err
			}

			if resp.StatusCode != http.StatusOK {
				return nil
			}

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return err
			}

			if _, err := w.Write(body); err != nil {
				return err
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}

	return nil
}

func createHostList(argv *argT) []string {
	var hosts = make([]string, 0)
	hosts = append(hosts, address6(argv))
	hosts = append(hosts, strings.Split(argv.Alternatives, ",")...)
	return hosts
}

// address6 returns a string representation of the given address and port,
// which is compatible with IPv6 addresses.
func address6(argv *argT) string {
	return net.JoinHostPort(argv.Host, fmt.Sprintf("%d", argv.Port))
}
