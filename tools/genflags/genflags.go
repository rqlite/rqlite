package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"text/template"

	"github.com/spf13/viper"
)

// Template for generating flags.go
const flagTemplate = `
// Code generated by go generate; DO NOT EDIT.
package {{ .Pkg }}

import (
	"fmt"
	"flag"
	"os"
	"strings"
	"time"
)

// StringSlice is a slice of strings which implements the flag.Value interface.
type StringSliceValue []string

// String returns a string representation of the slice.
func (s *StringSliceValue) String() string {
	return fmt.Sprintf("%v", *s)
}

// Set sets the value of the slice.
func (s *StringSliceValue) Set(value string) error {
	*s = strings.Split(value, ",")
	var r []string
	for _, v := range *s {
		if v != "" {
			r = append(r, v)
		}
	}
	*s = r
	return nil
}

// Config represents all configuration options.
type Config struct {
{{- range .Flags }}
	// {{ .ShortHelp }}
	{{ .Name }} {{ .GoType }}
{{- end }}
}

// ParseFlags sets up and parses command-line flags.
func ParseFlags() *Config {
	config := &Config{}
	fs := flag.NewFlagSet("rqlite", flag.ExitOnError)
{{- range .Flags }}
	{{- if eq .Type "string" }}
	fs.StringVar(&config.{{ .Name }}, "{{ .CLI }}", "{{ .Default }}", "{{ .ShortHelp }}")
	{{- else if eq .Type "bool" }}
	fs.BoolVar(&config.{{ .Name }}, "{{ .CLI }}", {{ .Default }}, "{{ .ShortHelp }}")
	{{- else if eq .Type "int" }}
	fs.IntVar(&config.{{ .Name }}, "{{ .CLI }}", {{ .Default }}, "{{ .ShortHelp }}")
	{{- else if eq .Type "duration" }}
	fs.DurationVar(&config.{{ .Name }}, "{{ .CLI }}", mustParseDuration("{{ .Default }}"), "{{ .ShortHelp }}")
	{{- end }}
{{- end }}
	fs.Parse(os.Args[1:])
	return config
}

func mustParseDuration(d string) time.Duration {
	td, err := time.ParseDuration(d)
	if err != nil {
		panic(err)
	}
	return td
}
`

// Flag represents a single flag configuration.
type Flag struct {
	Name      string      `mapstructure:"name"`
	CLI       string      `mapstructure:"cli"`
	Type      string      `mapstructure:"type"`
	Default   interface{} `mapstructure:"default"`
	ShortHelp string      `mapstructure:"short_help"`
	LongHelp  string      `mapstructure:"long_help"`
}

// GoType converts the flag type to Go type.
func (f Flag) GoType() string {
	switch f.Type {
	case "string":
		return "string"
	case "stringslicevalue":
		return "StringSliceValue"
	case "bool":
		return "bool"
	case "int":
		return "int"
	case "uint64":
		return "uint64"
	case "duration":
		return "time.Duration"
	default:
		panic(fmt.Sprintf("unknown type: %s", f.Type))
	}
}

func generateFlagsFile(flags []Flag, pkg, out string) {
	// Parse the template.
	tmpl, err := template.New("flags").Funcs(template.FuncMap{
		"GoType": Flag.GoType,
	}).Parse(flagTemplate)
	if err != nil {
		log.Fatalf("Error parsing template: %v", err)
	}

	// Execute the template with the flags data.
	var output bytes.Buffer
	if err := tmpl.Execute(&output, struct {
		Pkg   string
		Flags []Flag
	}{Pkg: pkg, Flags: flags}); err != nil {
		log.Fatalf("Error executing template: %v", err)
	}

	// Write the output to flags.go.
	if err := os.WriteFile(out, output.Bytes(), 0644); err != nil {
		log.Fatalf("Error writing flags.go: %v", err)
	}
}

func generateMarkdownTable(flags []Flag, out string) {
	var output bytes.Buffer

	// Write the markdown table header.
	output.WriteString("| Flag | Default | Purpose | Usage notes |\n")
	output.WriteString("|------|---------|---------|-------------|\n")

	// Write each flag as a row in the table.
	for _, flag := range flags {
		cli := escapeMarkdown(flag.CLI)
		shortHelp := escapeMarkdown(flag.ShortHelp)
		longHelp := escapeMarkdown(flag.LongHelp)
		defaultVal := fmt.Sprintf("%v", flag.Default)
		defaultVal = escapeMarkdown(defaultVal)
		output.WriteString(fmt.Sprintf("| `%s` | %s | %s | %s |\n", cli, defaultVal, shortHelp, longHelp))
	}

	// Write the output to the specified file.
	if err := os.WriteFile(out, output.Bytes(), 0644); err != nil {
		log.Fatalf("Error writing markdown file: %v", err)
	}
}

// escapeMarkdown escapes markdown special characters.
func escapeMarkdown(text string) string {
	text = strings.ReplaceAll(text, "|", "\\|")
	text = strings.ReplaceAll(text, "\n", "<br>")
	return text
}

func generateHTMLTable(flags []Flag, out string) {
	// Define the HTML template with precise control over column widths.
	const htmlTemplate = `
<!DOCTYPE html>
<html>
<head>
<style>
table {
	width: 100%;
	border-collapse: collapse;
}
th, td {
	border: 1px solid #ddd;
	padding: 8px;
}
th {
	background-color: #f2f2f2;
	text-align: left;
}
.col-cli { width: 30%; }
.col-usage { width: 70%; }
</style>
</head>
<body>

<table>
	<tr>
		<th class="col-cli">Flag</th>
		<th class="col-usage">Usage</th>
	</tr>
	{{- range .Flags }}
	<tr>
		<td><code>{{ .CLI | html }}</code></td>
		<td>{{ .ShortHelp | html }}.
		{{- if .LongHelp }}
		    <br><br>{{ .LongHelp | html }}
		{{- end }}</td>
	</tr>
	{{- end }}
</table>

</body>
</html>
`

	// Parse the template.
	tmpl, err := template.New("htmlTable").Funcs(template.FuncMap{
		"html": func(s string) string {
			return template.HTMLEscapeString(s)
		},
	}).Parse(htmlTemplate)
	if err != nil {
		log.Fatalf("Error parsing HTML template: %v", err)
	}

	// Execute the template with the flags data.
	var output bytes.Buffer
	if err := tmpl.Execute(&output, struct {
		Flags []Flag
	}{Flags: flags}); err != nil {
		log.Fatalf("Error executing HTML template: %v", err)
	}

	// Write the output to the specified file.
	if err := os.WriteFile(out, output.Bytes(), 0644); err != nil {
		log.Fatalf("Error writing HTML file: %v", err)
	}
}

func main() {
	var pkg = flag.String("pkg", "rflags", "Package name")
	var format = flag.String("format", "go", "Output format (go, markdown, html)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [--format] <input> <output>\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	if flag.NArg() != 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s [--format] <input> <output>\n", os.Args[0])
		os.Exit(1)
	}
	inPath := flag.Arg(0)
	outPath := flag.Arg(1)

	viper.SetConfigFile(inPath)
	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Error reading config file: %v", err)
	}

	var flags []Flag
	if err := viper.UnmarshalKey("flags", &flags); err != nil {
		log.Fatalf("Error unmarshaling config: %v", err)
	}

	switch *format {
	case "go":
		generateFlagsFile(flags, *pkg, outPath)
	case "markdown":
		generateMarkdownTable(flags, outPath)
	case "html":
		generateHTMLTable(flags, outPath)
	default:
		log.Fatalf("Unknown format: %s", *format)
	}
}
