package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"
)

// HTTPTester represents an HTTP transport tester.
type HTTPTester struct {
	client http.Client
	url    string
	br     *bytes.Reader
}

// NewHTTPTester returns an instantiated HTTP tester.
func NewHTTPTester(addr, path string) *HTTPTester {
	return &HTTPTester{
		client: http.Client{},
		url:    fmt.Sprintf("http://%s%s", addr, path),
	}
}

// String returns a string representation of the tester.
func (h *HTTPTester) String() string {
	return h.url
}

// Prepare prepares the tester for execution.
func (h *HTTPTester) Prepare(stmt string, bSz int, tx bool) error {
	s := make([]string, bSz)
	for i := 0; i < len(s); i++ {
		s[i] = stmt
	}

	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	h.br = bytes.NewReader(b)

	if tx {
		h.url = h.url + "?transaction"
	}

	return nil
}

// Once executes a single test request.
func (h *HTTPTester) Once() (time.Duration, error) {
	h.br.Seek(0, io.SeekStart)

	start := time.Now()
	resp, err := h.client.Post(h.url, "application/json", h.br)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("received %s", resp.Status)
	}
	dur := time.Since(start)

	return dur, nil
}
