// Command rqlite is the command-line interface for rqlite.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"syscall"

	"github.com/Bowery/prompt"
	"github.com/mkideal/cli"
	clix "github.com/mkideal/cli/ext"
	"github.com/rqlite/rqlite/v8/cmd"
	"github.com/rqlite/rqlite/v8/cmd/rqlite/history"
	httpcl "github.com/rqlite/rqlite/v8/cmd/rqlite/http"
	"github.com/rqlite/rqlite/v8/internal/rtls"
)

const HOST_ENV_VAR = "RQLITE_HOST"

const maxRedirect = 21

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
	ClientCert   string        `cli:"d,client-cert" usage:"path to client X.509 certificate for mTLS"`
	ClientKey    string        `cli:"k,client-key" usage:"path to client X.509 key for mTLS"`
	Credentials  string        `cli:"u,user" usage:"set basic auth credentials in form username:password"`
	Version      bool          `cli:"v,version" usage:"display CLI version"`
	HTTPTimeout  clix.Duration `cli:"t,http-timeout" usage:"set timeout on HTTP requests" dft:"30s"`
}

var cliHelp []string

func init() {
	cliHelp = []string{
		`.backup FILE                                  Write database backup to FILE`,
		`.blobarray on|off                             Display BLOB data as byte arrays`,
		`.boot FILE                                    Boot the node using a SQLite file read from FILE`,
		`.consistency [none|weak|linearizable|strong]  Show or set read consistency level`,
		`.dump FILE                                    Dump the database in SQL text format to FILE`,
		`.exit                                         Exit this program`,
		`.expvar                                       Show expvar (Go runtime) information for connected node`,
		`.extensions                                   Show loaded SQLite extensions`,
		`.help                                         Show this message`,
		`.indexes                                      Show names of all indexes`,
		`.quit                                         Exit this program`,
		`.ready                                        Show ready status for connected node`,
		`.remove NODEID                                Remove node NODEID from the cluster`,
		`.restore FILE                                 Load using SQLite file or SQL dump contained in FILE`,
		`.nodes [all]                                  Show connection status of voting nodes. 'all' to show all nodes`,
		`.schema                                       Show CREATE statements for all tables`,
		`.snapshot                                     Request a Raft snapshot and log truncation on connected node`,
		`.status                                       Show status and diagnostic information for connected node`,
		`.stepdown [NODEID]                            Instruct the leader to stepdown, optionally specifying new Leader node`,
		`.sysdump FILE                                 Dump system diagnostics to FILE`,
		`.tables                                       List names of tables`,
		`.timer on|off                                 Turn query timings on or off`,
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
			ctx.String("Version %s, commit %s, branch %s, built on %s\n", cmd.Version,
				cmd.Commit, cmd.Branch, cmd.Buildtime)
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

		connectionStr := fmt.Sprintf("%s://%s", argv.Protocol, address6(argv))
		version, err := getVersionWithClient(httpClient, argv)
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
		consistency := "weak"
		prefix := fmt.Sprintf("%s>", address6(argv))
		term, err := prompt.NewTerminal()
		if err != nil {
			ctx.String("%s %v\n", ctx.Color().Red("ERR!"), err)
			return nil
		}
		term.Close()

		// Set up command history.
		hr := history.Reader()
		if hr != nil {
			histCmds, err := history.Read(hr)
			if err == nil {
				term.History = histCmds
			}
			hr.Close()
		}

		hosts := createHostList(argv)
		client := httpcl.NewClient(httpClient, hosts,
			httpcl.WithScheme(argv.Protocol),
			httpcl.WithBasicAuth(argv.Credentials),
			httpcl.WithPrefix(argv.Prefix))

	FOR_READ:
		for {
			term.Reopen()
			line, err := term.Basic(prefix, false)
			term.Close()
			if err != nil {
				if errors.Is(err, prompt.ErrEOF) {
					break FOR_READ
				}
				return err
			}

			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			var (
				index = strings.Index(line, " ")
				cmd   = line
			)
			if index >= 0 {
				cmd = line[:index]
			}
			cmd = strings.ToUpper(cmd)
			switch cmd {
			case ".CONSISTENCY":
				if index == -1 || index == len(line)-1 {
					ctx.String("%s\n", consistency)
					break
				}
				err = setConsistency(line[index+1:], &consistency)
			case ".EXTENSIONS":
				err = extensions(ctx, client, cmd, argv)
			case ".TABLES":
				err = queryWithClient(ctx, client, timer, blobArray, consistency, `SELECT name FROM sqlite_master WHERE type="table"`)
			case ".INDEXES":
				err = queryWithClient(ctx, client, timer, blobArray, consistency, `SELECT sql FROM sqlite_master WHERE type="index"`)
			case ".SCHEMA":
				err = queryWithClient(ctx, client, timer, blobArray, consistency, `SELECT sql FROM sqlite_master`)
			case ".TIMER":
				err = toggleFlag(line[index+1:], &timer)
			case ".BLOBARRAY":
				err = toggleFlag(line[index+1:], &blobArray)
			case ".STATUS":
				err = status(ctx, client, cmd, argv)
			case ".READY":
				err = ready(ctx, client, argv)
			case ".NODES":
				if index == -1 || index == len(line)-1 {
					err = nodes(ctx, client, cmd, line, argv, false)
					break
				}
				err = nodes(ctx, client, cmd, line, argv, true)
			case ".EXPVAR":
				err = expvar(ctx, client, cmd, line, argv)
			case ".REMOVE":
				err = removeNode(client, line[index+1:], argv)
			case ".BACKUP":
				if index == -1 || index == len(line)-1 {
					err = fmt.Errorf("please specify an output file for the backup")
					break
				}
				err = backup(ctx, line[index+1:], argv)
			case ".RESTORE":
				if index == -1 || index == len(line)-1 {
					err = fmt.Errorf("please specify an input file to restore from")
					break
				}
				err = restore(ctx, line[index+1:], argv)
			case ".BOOT":
				if index == -1 || index == len(line)-1 {
					err = fmt.Errorf("please specify an input file to boot with")
					break
				}
				err = boot(ctx, line[index+1:], argv)
			case ".SYSDUMP":
				if index == -1 || index == len(line)-1 {
					err = fmt.Errorf("please specify an output file for the sysdump")
					break
				}
				err = sysdump(ctx, httpClient, line[index+1:], argv)
			case ".DUMP":
				if index == -1 || index == len(line)-1 {
					err = fmt.Errorf("please specify an output file for the SQL text")
					break
				}
				err = dump(ctx, line[index+1:], argv)
			case ".HELP":
				ctx.String("%s", strings.Join(cliHelp, "\n"))
			case ".QUIT", "QUIT", "EXIT", ".EXIT":
				break FOR_READ
			case ".SNAPSHOT":
				err = snapshot(client, argv)
			case ".STEPDOWN":
				nodeID := ""
				if index != -1 && index < len(line)-1 {
					nodeID = strings.TrimSpace(line[index+1:])
				}
				err = stepdown(client, nodeID, argv)
			case "SELECT", "PRAGMA":
				err = queryWithClient(ctx, client, timer, blobArray, consistency, line)
			default:
				err = executeWithClient(ctx, client, timer, line)
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
			history.Write(term.History, sz, hw)
			hw.Close()
			if sz <= 0 {
				history.Delete()
			}
		}
		ctx.String("bye~\n")
		return nil
	})
}

func toggleFlag(op string, flag *bool) error {
	if op != "on" && op != "off" {
		return fmt.Errorf("invalid option '%s'. Use 'on' or 'off' (default)", op)
	}
	*flag = (op == "on")
	return nil
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

func status(ctx *cli.Context, client *httpcl.Client, line string, argv *argT) error {
	url := fmt.Sprintf("%s://%s/status", argv.Protocol, address6(argv))
	return cliJSON(ctx, client, line, url)
}

func ready(ctx *cli.Context, client *httpcl.Client, argv *argT) error {
	u := url.URL{
		Scheme: argv.Protocol,
		Host:   address6(argv),
		Path:   fmt.Sprintf("%sreadyz", argv.Prefix),
	}
	urlStr := u.String()

	resp, err := client.Get(urlStr)
	if err != nil {
		return err
	}
	if resp.StatusCode == 200 {
		ctx.String("ready\n")
	} else {
		ctx.String("not ready\n")
	}

	return nil
}

// nodes returns the status of nodes in the cluster. If all is true, then
// non-voting nodes are included in the response.
func nodes(ctx *cli.Context, client *httpcl.Client, cmd, line string, argv *argT, all bool) error {
	path := "nodes"
	if all {
		path = "nodes?nonvoters"
	}
	url := fmt.Sprintf("%s://%s/%s", argv.Protocol, address6(argv), path)
	return cliJSON(ctx, client, "", url)
}

func expvar(ctx *cli.Context, client *httpcl.Client, cmd, line string, argv *argT) error {
	url := fmt.Sprintf("%s://%s/debug/vars", argv.Protocol, address6(argv))
	return cliJSON(ctx, client, line, url)
}

func snapshot(client *httpcl.Client, argv *argT) error {
	url := fmt.Sprintf("%s://%s/snapshot", argv.Protocol, address6(argv))
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return err
	}
	if argv.Credentials != "" {
		creds := strings.Split(argv.Credentials, ":")
		if len(creds) != 2 {
			return fmt.Errorf("invalid Basic Auth credentials format")
		}
		req.SetBasicAuth(creds[0], creds[1])
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("server responded with %s", resp.Status)
	}
	return nil
}

func stepdown(client *httpcl.Client, nodeID string, argv *argT) error {
	url := fmt.Sprintf("%s://%s/leader?wait=true", argv.Protocol, address6(argv))

	var body io.Reader
	if nodeID != "" {
		// Create JSON body with node ID
		reqBody := map[string]string{"id": nodeID}
		jsonBody, err := json.Marshal(reqBody)
		if err != nil {
			return err
		}
		body = bytes.NewReader(jsonBody)
	}

	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return err
	}

	if nodeID != "" {
		req.Header.Set("Content-Type", "application/json")
	}

	if argv.Credentials != "" {
		creds := strings.Split(argv.Credentials, ":")
		if len(creds) != 2 {
			return fmt.Errorf("invalid Basic Auth credentials format")
		}
		req.SetBasicAuth(creds[0], creds[1])
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server responded with %s", resp.Status)
	}
	return nil
}

func sysdump(ctx *cli.Context, client *http.Client, filename string, argv *argT) error {
	_ = ctx
	nodes, err := getNodes(client, argv)
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
		if err := urlsToWriter(client, urls, f, argv); err != nil {
			f.WriteString(fmt.Sprintf("Error sysdumping %s: %s\n", n.ApiAddr, err.Error()))
		}
	}
	return nil
}

func getNodes(client *http.Client, argv *argT) (Nodes, error) {
	u := url.URL{
		Scheme: argv.Protocol,
		Host:   address6(argv),
		Path:   fmt.Sprintf("%snodes", argv.Prefix),
	}
	urlStr := u.String()

	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		return nil, err
	}
	if argv.Credentials != "" {
		creds := strings.Split(argv.Credentials, ":")
		if len(creds) != 2 {
			return nil, fmt.Errorf("invalid Basic Auth credentials format")
		}
		req.SetBasicAuth(creds[0], creds[1])
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	response, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var nodes Nodes
	if err := parseResponse(&response, &nodes); err != nil {
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

func getVersionWithClient(client *http.Client, argv *argT) (string, error) {
	u := url.URL{
		Scheme: argv.Protocol,
		Host:   address6(argv),
		Path:   fmt.Sprintf("%s/status", argv.Prefix),
	}
	urlStr := u.String()

	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		return "", err
	}
	if argv.Credentials != "" {
		creds := strings.Split(argv.Credentials, ":")
		if len(creds) != 2 {
			return "", fmt.Errorf("invalid Basic Auth credentials format")
		}
		req.SetBasicAuth(creds[0], creds[1])
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}

	version, ok := resp.Header["X-Rqlite-Version"]
	if !ok || len(version) != 1 {
		return "unknown", nil
	}
	return version[0], nil
}

func sendRequest(ctx *cli.Context, makeNewRequest func(string) (*http.Request, error), urlStr string, argv *argT) (*[]byte, error) {
	// create a byte-based buffer that implements io.Writer
	var buf []byte
	w := bytes.NewBuffer(buf)
	_, err := sendRequestW(ctx, makeNewRequest, urlStr, argv, w)
	if err != nil {
		return nil, err
	}
	return &buf, nil
}

func sendRequestW(ctx *cli.Context, makeNewRequest func(string) (*http.Request, error), urlStr string, argv *argT, w io.Writer) (int64, error) {
	_ = ctx
	url := urlStr
	tlsConfig, err := rtls.CreateClientConfig(argv.ClientCert, argv.ClientKey, argv.CACert, rtls.NoServerName, argv.Insecure)
	if err != nil {
		return 0, err
	}
	tlsConfig.NextProtos = nil // CLI refuses to connect otherwise.
	client := http.Client{Transport: &http.Transport{
		TLSClientConfig: tlsConfig,
		Proxy:           http.ProxyFromEnvironment,
	}}

	// Explicitly handle redirects.
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	nRedirect := 0
	for {
		req, err := makeNewRequest(url)
		if err != nil {
			return 0, err
		}

		if argv.Credentials != "" {
			creds := strings.Split(argv.Credentials, ":")
			if len(creds) != 2 {
				return 0, fmt.Errorf("invalid Basic Auth credentials format")
			}
			req.SetBasicAuth(creds[0], creds[1])
		}

		resp, err := client.Do(req)
		if err != nil {
			return 0, err
		}

		n, err := io.Copy(w, resp.Body)
		if err != nil {
			return n, err
		}
		resp.Body.Close()

		if resp.StatusCode == http.StatusUnauthorized {
			return 0, fmt.Errorf("unauthorized")
		}

		if resp.StatusCode == http.StatusMovedPermanently {
			nRedirect++
			if nRedirect > maxRedirect {
				return 0, fmt.Errorf("maximum leader redirect limit exceeded")
			}
			url = resp.Header["Location"][0]
			continue
		}

		if resp.StatusCode != http.StatusOK {
			return 0, fmt.Errorf("server responded with: %s", resp.Status)
		}

		return n, nil
	}
}

func parseResponse(response *[]byte, ret any) error {
	decoder := json.NewDecoder(strings.NewReader(string(*response)))
	decoder.UseNumber()
	return decoder.Decode(ret)
}

func extensions(ctx *cli.Context, client *httpcl.Client, cmd string, argv *argT) error {
	_ = cmd
	url := fmt.Sprintf("%s://%s/status?key=extensions", argv.Protocol, address6(argv))
	resp, err := client.Get(url)
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
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.UseNumber()
	if err := decoder.Decode(&ret); err != nil {
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
func cliJSON(ctx *cli.Context, client *httpcl.Client, line, url string) error {
	_ = ctx
	// Recursive JSON printer.
	var pprint func(indent int, m map[string]any)
	pprint = func(indent int, m map[string]any) {
		indentation := "  "
		for k, v := range m {
			if v == nil {
				continue
			}
			switch w := v.(type) {
			case map[string]any:
				for i := 0; i < indent; i++ {
					fmt.Print(indentation)
				}
				fmt.Printf("%s:\n", k)
				pprint(indent+1, w)
			default:
				for i := 0; i < indent; i++ {
					fmt.Print(indentation)
				}
				fmt.Printf("%s: %v\n", k, v)
			}
		}
	}

	resp, err := client.Get(url)
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
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.UseNumber()
	if err := decoder.Decode(&ret); err != nil {
		return err
	}

	// Specific key requested?
	parts := strings.Split(line, " ")
	if len(parts) >= 2 {
		ret = map[string]any{parts[1]: ret[parts[1]]}
	}
	pprint(0, ret)

	return nil
}

func urlsToWriter(client *http.Client, urls []string, w io.Writer, argv *argT) error {
	for i := range urls {
		err := func() error {
			w.Write([]byte("\n=========================================\n"))
			w.Write([]byte(fmt.Sprintf("URL: %s\n", urls[i])))

			req, err := http.NewRequest("GET", urls[i], nil)
			if err != nil {
				return err
			}
			if argv.Credentials != "" {
				creds := strings.Split(argv.Credentials, ":")
				if len(creds) != 2 {
					return fmt.Errorf("invalid Basic Auth credentials format")
				}
				req.SetBasicAuth(creds[0], creds[1])
			}

			resp, err := client.Do(req)
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
