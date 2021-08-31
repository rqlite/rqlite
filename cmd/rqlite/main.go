// Command rqlite is the command-line interface for rqlite.
package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/Bowery/prompt"
	"github.com/mkideal/cli"
	"github.com/rqlite/rqlite/cmd"
)

const maxRedirect = 21

type argT struct {
	cli.Helper
	Protocol    string `cli:"s,scheme" usage:"protocol scheme (http or https)" dft:"http"`
	Host        string `cli:"H,host" usage:"rqlited host address" dft:"127.0.0.1"`
	Port        uint16 `cli:"p,port" usage:"rqlited host port" dft:"4001"`
	Prefix      string `cli:"P,prefix" usage:"rqlited HTTP URL prefix" dft:"/"`
	Insecure    bool   `cli:"i,insecure" usage:"do not verify rqlited HTTPS certificate" dft:"false"`
	CACert      string `cli:"c,ca-cert" usage:"path to trusted X.509 root CA certificate"`
	Credentials string `cli:"u,user" usage:"set basic auth credentials in form username:password"`
	Version     bool   `cli:"v,version" usage:"display CLI version"`
}

var cliHelp = []string{
	`.backup <file>                      Write database backup to SQLite file`,
	`.consistency [none|weak|strong]     Show or set read consistency level`,
	`.dump <file>                        Dump the database in SQL text format to a file`,
	`.expvar                             Show expvar (Go runtime) information for connected node`,
	`.help                               Show this message`,
	`.indexes                            Show names of all indexes`,
	`.restore <file>                     Restore the database from a SQLite dump file`,
	`.nodes                              Show connection status of all nodes in cluster`,
	`.schema                             Show CREATE statements for all tables`,
	`.status                             Show status and diagnostic information for connected node`,
	`.sysdump <file>                     Dump system diagnostics to a file for offline analysis`,
	`.tables                             List names of tables`,
	`.timer on|off                       Turn query timer on or off`,
	`.remove <raft ID>                   Remove a node from the cluster`,
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
			ctx.String("Version %s, commmit %s, branch %s, built on %s\n", cmd.Version,
				cmd.Commit, cmd.Branch, cmd.Buildtime)
			return nil
		}

		client, err := getHTTPClient(argv)
		if err != nil {
			ctx.String("%s %v\n", ctx.Color().Red("ERR!"), err)
			return nil
		}

		version, err := getVersionWithClient(client, argv)
		if err != nil {
			ctx.String("%s %v\n", ctx.Color().Red("ERR!"), err)
			return nil
		}

		fmt.Println("Welcome to the rqlite CLI. Enter \".help\" for usage hints.")
		fmt.Printf("Version %s, commit %s, branch %s\n", cmd.Version, cmd.Commit, cmd.Branch)
		fmt.Printf("Connected to rqlited version %s\n", version)

		timer := false
		consistency := "weak"
		prefix := fmt.Sprintf("%s:%d>", argv.Host, argv.Port)
		term, err := prompt.NewTerminal()
		if err != nil {
			ctx.String("%s %v\n", ctx.Color().Red("ERR!"), err)
			return nil
		}
		term.Close()

	FOR_READ:
		for {
			term.Reopen()
			line, err := term.Basic(prefix, false)
			term.Close()
			if err != nil {
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
			case ".TABLES":
				err = queryWithClient(ctx, client, argv, timer, consistency, `SELECT name FROM sqlite_master WHERE type="table"`)
			case ".INDEXES":
				err = queryWithClient(ctx, client, argv, timer, consistency, `SELECT sql FROM sqlite_master WHERE type="index"`)
			case ".SCHEMA":
				err = queryWithClient(ctx, client, argv, timer, consistency, `SELECT sql FROM sqlite_master`)
			case ".TIMER":
				err = toggleTimer(line[index+1:], &timer)
			case ".STATUS":
				err = status(ctx, cmd, line, argv)
			case ".NODES":
				err = nodes(ctx, cmd, line, argv)
			case ".EXPVAR":
				err = expvar(ctx, cmd, line, argv)
			case ".REMOVE":
				err = removeNode(client, line[index+1:], argv, timer)
			case ".BACKUP":
				if index == -1 || index == len(line)-1 {
					err = fmt.Errorf("Please specify an output file for the backup")
					break
				}
				err = backup(ctx, line[index+1:], argv)
			case ".RESTORE":
				if index == -1 || index == len(line)-1 {
					err = fmt.Errorf("Please specify an input file to restore from")
					break
				}
				err = restore(ctx, line[index+1:], argv)
			case ".SYSDUMP":
				if index == -1 || index == len(line)-1 {
					err = fmt.Errorf("Please specify an output file for the sysdump")
					break
				}
				err = sysdump(ctx, line[index+1:], argv)
			case ".DUMP":
				if index == -1 || index == len(line)-1 {
					err = fmt.Errorf("Please specify an output file for the SQL text")
					break
				}
				err = dump(ctx, line[index+1:], argv)
			case ".HELP":
				err = help(ctx, cmd, line, argv)
			case ".QUIT", "QUIT", "EXIT":
				break FOR_READ
			case "SELECT", "PRAGMA":
				err = queryWithClient(ctx, client, argv, timer, consistency, line)
			default:
				err = executeWithClient(ctx, client, argv, timer, line)
			}
			if err != nil {
				ctx.String("%s %v\n", ctx.Color().Red("ERR!"), err)
			}
		}
		ctx.String("bye~\n")
		return nil
	})
}

func toggleTimer(op string, flag *bool) error {
	if op != "on" && op != "off" {
		return fmt.Errorf("invalid option '%s'. Use 'on' or 'off' (default)", op)
	}
	*flag = (op == "on")
	return nil
}

func setConsistency(r string, c *string) error {
	if r != "strong" && r != "weak" && r != "none" {
		return fmt.Errorf("invalid consistency '%s'. Use 'none', 'weak', or 'strong'", r)
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

func help(ctx *cli.Context, cmd, line string, argv *argT) error {
	sort.Strings(cliHelp)
	fmt.Printf(strings.Join(cliHelp, "\n"))
	return nil
}

func status(ctx *cli.Context, cmd, line string, argv *argT) error {
	url := fmt.Sprintf("%s://%s:%d/status", argv.Protocol, argv.Host, argv.Port)
	return cliJSON(ctx, cmd, line, url, argv)
}

func nodes(ctx *cli.Context, cmd, line string, argv *argT) error {
	url := fmt.Sprintf("%s://%s:%d/nodes", argv.Protocol, argv.Host, argv.Port)
	return cliJSON(ctx, cmd, line, url, argv)
}

func expvar(ctx *cli.Context, cmd, line string, argv *argT) error {
	url := fmt.Sprintf("%s://%s:%d/debug/vars", argv.Protocol, argv.Host, argv.Port)
	return cliJSON(ctx, cmd, line, url, argv)
}

func sysdump(ctx *cli.Context, filename string, argv *argT) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}

	urls := []string{
		fmt.Sprintf("%s://%s:%d/status?pretty", argv.Protocol, argv.Host, argv.Port),
		fmt.Sprintf("%s://%s:%d/nodes?pretty", argv.Protocol, argv.Host, argv.Port),
		fmt.Sprintf("%s://%s:%d/debug/vars", argv.Protocol, argv.Host, argv.Port),
	}

	if err := urlsToWriter(urls, f, argv); err != nil {
		return err
	}
	return f.Close()
}

func getHTTPClient(argv *argT) (*http.Client, error) {
	var rootCAs *x509.CertPool

	if argv.CACert != "" {
		pemCerts, err := ioutil.ReadFile(argv.CACert)
		if err != nil {
			return nil, err
		}

		rootCAs = x509.NewCertPool()

		ok := rootCAs.AppendCertsFromPEM(pemCerts)
		if !ok {
			return nil, fmt.Errorf("failed to parse root CA certificate(s)")
		}
	}

	client := http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: argv.Insecure, RootCAs: rootCAs},
		Proxy:           http.ProxyFromEnvironment,
	}}

	// Explicitly handle redirects.
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	return &client, nil
}

func getVersionWithClient(client *http.Client, argv *argT) (string, error) {
	u := url.URL{
		Scheme: argv.Protocol,
		Host:   fmt.Sprintf("%s:%d", argv.Host, argv.Port),
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
	url := urlStr
	var rootCAs *x509.CertPool

	if argv.CACert != "" {
		pemCerts, err := ioutil.ReadFile(argv.CACert)
		if err != nil {
			return nil, err
		}

		rootCAs = x509.NewCertPool()

		ok := rootCAs.AppendCertsFromPEM(pemCerts)
		if !ok {
			return nil, fmt.Errorf("failed to parse root CA certificate(s)")
		}
	}

	client := http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: argv.Insecure, RootCAs: rootCAs},
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
		response, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		resp.Body.Close()

		if resp.StatusCode == http.StatusUnauthorized {
			return nil, fmt.Errorf("unauthorized")
		}

		if resp.StatusCode == http.StatusMovedPermanently {
			nRedirect++
			if nRedirect > maxRedirect {
				return nil, fmt.Errorf("maximum leader redirect limit exceeded")
			}
			url = resp.Header["Location"][0]
			continue
		}

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("server responded with: %s", resp.Status)
		}

		return &response, nil
	}
}

func parseResponse(response *[]byte, ret interface{}) error {
	return json.Unmarshal(*response, ret)
}

// cliJSON fetches JSON from a URL, and displays it at the CLI.
func cliJSON(ctx *cli.Context, cmd, line, url string, argv *argT) error {
	// Recursive JSON printer.
	var pprint func(indent int, m map[string]interface{})
	pprint = func(indent int, m map[string]interface{}) {
		indentation := "  "
		for k, v := range m {
			if v == nil {
				continue
			}
			switch v.(type) {
			case map[string]interface{}:
				for i := 0; i < indent; i++ {
					fmt.Print(indentation)
				}
				fmt.Printf("%s:\n", k)
				pprint(indent+1, v.(map[string]interface{}))
			default:
				for i := 0; i < indent; i++ {
					fmt.Print(indentation)
				}
				fmt.Printf("%s: %v\n", k, v)
			}
		}
	}

	client := http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: argv.Insecure},
		Proxy:           http.ProxyFromEnvironment,
	}}

	req, err := http.NewRequest("GET", url, nil)
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

	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("unauthorized")
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	ret := make(map[string]interface{})
	if err := json.Unmarshal(body, &ret); err != nil {
		return err
	}

	// Specific key requested?
	parts := strings.Split(line, " ")
	if len(parts) >= 2 {
		ret = map[string]interface{}{parts[1]: ret[parts[1]]}
	}
	pprint(0, ret)

	return nil
}

func urlsToWriter(urls []string, w io.Writer, argv *argT) error {
	client := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: argv.Insecure},
			Proxy:           http.ProxyFromEnvironment,
		},
		Timeout: 10 * time.Second,
	}

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

			body, err := ioutil.ReadAll(resp.Body)
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
