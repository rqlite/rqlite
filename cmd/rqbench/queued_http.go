package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// QueuedHTTPTester represents an HTTP transport tester that uses
// queued writes
type QueuedHTTPTester struct {
	client  http.Client
	url     string
	waitURL string
	br      *bytes.Reader
}

// NewHTTPTester returns an instantiated HTTP tester.
func NewQueuedHTTPTester(addr, path string) *QueuedHTTPTester {
	return &QueuedHTTPTester{
		client:  http.Client{},
		url:     fmt.Sprintf("http://%s%s?queue", addr, path),
		waitURL: fmt.Sprintf("http://%s%s?queue&wait", addr, path),
	}
}

// String returns a string representation of the tester.
func (h *QueuedHTTPTester) String() string {
	return h.url
}

// Prepare prepares the tester for execution.
func (h *QueuedHTTPTester) Prepare(stmt string, bSz int, _ bool) error {
	s := make([]string, bSz)
	for i := 0; i < len(s); i++ {
		s[i] = stmt
	}

	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	h.br = bytes.NewReader(b)

	return nil
}

// Once executes a single test request.
func (h *QueuedHTTPTester) Once() (time.Duration, error) {
	h.br.Seek(0, io.SeekStart)

	start := time.Now()
	resp, err := h.client.Post(h.url, "application/json", h.br)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	_, err = io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("received %s", resp.Status)
	}
	dur := time.Since(start)

	return dur, nil
}

// Close closes the tester
func (h *QueuedHTTPTester) Close() error {
	start := time.Now()
	resp, err := h.client.Post(h.waitURL, "application/json", h.br)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Close() received %s", resp.Status)
	}
	fmt.Println("Queued tester wait request took", time.Since(start))
	return nil
}
