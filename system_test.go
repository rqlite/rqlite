package main

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/otoolep/rqlite/server"
)

type stringCloser struct {
	io.Reader
}

func (stringCloser) Close() error { return nil }

type querySet struct {
	Time     stringCloser  `json:"time,omitempty"`
	Rows     []interface{} `json:"rows,omitempty"`
	Failures []interface{} `json:"failures,omitempty"`
}

func TestSystem_EndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping for short testing")
	}

	tests := []struct {
		name     string // Name of test, printed during testing.
		post     string // The query to POST.
		get      string // The query to GET. Ignored if post is set.
		rows     string // Expected rows, as a string. Ignored if not set.
		failures string // Expected failures, as a string. Ignored if not set.
	}{
		{
			name: "create table",
			post: "CREATE TABLE foo (id integer not null primary key, name text)",
		},
		{
			name: "select from empty table",
			get:  "SELECT * FROM foo",
		},
		{
			name: "insert one record",
			get:  `INSERT INTO foo(name) VALUES("fiona")`,
		},
		{
			name: "select after 1 record inserted",
			get:  "SELECT * FROM foo",
		},
	}

	// Start the single rqlite server
	path := tempfile()
	os.MkdirAll(path, 0744)
	defer os.RemoveAll(path)
	s := server.NewServer(path, "test.db", 20000, "localhost", 8086)
	go func() {
		t.Fatal(s.ListenAndServe(join))
	}()

	// Give the server time to start-up
	time.Sleep(1 * time.Second)

	for _, tt := range tests {
		t.Logf("executing test %s", tt.name)

		var resp *http.Response
		var err error
		if tt.post != "" {
			resp, err = http.Post("http://localhost:8086/db", "text/plain", bytes.NewReader([]byte(tt.post)))
		} else {
			client := &http.Client{}
			u, _ := url.Parse("http://localhost:8086/db")
			body := stringCloser{bytes.NewBufferString(tt.get)}

			req := &http.Request{
				Method:        "GET",
				URL:           u,
				Body:          body,
				ContentLength: int64(len(tt.get)),
			}
			resp, err = client.Do(req)
		}
		if err != nil {
			t.Fatalf("test %s: HTTP request failed: %s", tt.name, err.Error())
		}
		if resp.StatusCode != 200 {
			t.Fatalf("test %s: bad status code returned: %d", tt.name, resp.StatusCode)
		}

		body, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		t.Log(string(body))

		q := querySet{}
		if err := json.Unmarshal(body, &q); err != nil {
			t.Fatalf("test %s: failed to unmarshal response: %s", tt.name, err.Error())
		}
		t.Logf(">>%s", q.Rows)

	}

}

// tempfile returns a temporary path.
func tempfile() string {
	f, _ := ioutil.TempFile("", "rqlite-")
	path := f.Name()
	f.Close()
	os.Remove(path)
	return path
}
