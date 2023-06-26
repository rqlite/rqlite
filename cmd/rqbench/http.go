package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Result represents a single result from a test request.
type Result struct {
	Error string `json:"error,omitempty"`
	Rest  json.RawMessage
}

// Response represents a response from a test request.
type Response struct {
	Results []Result `json:"results"`
}

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

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("received %s", resp.Status)
	}

	r := Response{}
	err = json.NewDecoder(resp.Body).Decode(&r)
	if err != nil {
		return 0, err
	}
	if len(r.Results) == 0 {
		return 0, fmt.Errorf("expected at least 1 result, got %d", len(r.Results))
	}
	for _, res := range r.Results {
		if res.Error != "" {
			return 0, fmt.Errorf("received error: %s", res.Error)
		}
	}
	dur := time.Since(start)

	return dur, nil
}

// Close closes the tester
func (h *HTTPTester) Close() error {
	return nil
}
