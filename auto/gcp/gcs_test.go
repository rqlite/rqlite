package gcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func Test_NewGCSClient(t *testing.T) {
	credsPath := createCredFile(t)
	cfg := &GCSConfig{
		Endpoint:        "http://localhost:8080",
		ProjectID:       "proj",
		Bucket:          "mybucket",
		Name:            "object.txt",
		CredentialsPath: credsPath,
	}

	cli, err := NewGCSClient(cfg, nil)
	if err != nil {
		t.Fatalf("NewGCSClient: %v", err)
	}

	if cli.cfg.ProjectID != "proj" {
		t.Errorf("ProjectID = %s, want proj", cli.cfg.ProjectID)
	}
	if cli.cfg.Bucket != "mybucket" {
		t.Errorf("Bucket = %s, want mybucket", cli.cfg.Bucket)
	}
	if cli.cfg.Name != "object.txt" {
		t.Errorf("Name = %s, want object.txt", cli.cfg.Name)
	}
	if cli.cfg.Endpoint != "http://localhost:8080" {
		t.Errorf("Endpoint = %s, want http://localhost:8080", cli.cfg.Endpoint)
	}
	if cli.cfg.CredentialsPath != credsPath {
		t.Errorf("CredentialsPath = %s, want %s", cli.cfg.CredentialsPath, credsPath)
	}
}

func Test_EnsureBucketExists(t *testing.T) {
	var gotPath string
	handler := func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		if r.Method != http.MethodGet {
			t.Errorf("unexpected method %s", r.Method)
		}
		w.WriteHeader(http.StatusOK) // bucket already there
	}
	cli, shutdown := newTestClient(t, handler)
	defer shutdown()

	if err := cli.EnsureBucket(context.Background()); err != nil {
		t.Fatalf("EnsureBucket: %v", err)
	}
	want := "/storage/v1/b/mybucket"
	if gotPath != want {
		t.Errorf("path = %s, want %s", gotPath, want)
	}
}

func Test_EnsureBucketCreate(t *testing.T) {
	var step int32
	handler := func(w http.ResponseWriter, r *http.Request) {
		switch atomic.AddInt32(&step, 1) {
		case 1: // initial GET -> 404
			if r.Method != http.MethodGet {
				t.Fatalf("step1 method %s", r.Method)
			}
			w.WriteHeader(http.StatusNotFound)
		case 2: // POST create
			if r.Method != http.MethodPost {
				t.Fatalf("step2 method %s", r.Method)
			}
			if !strings.Contains(r.URL.RawQuery, "project=proj") {
				t.Fatalf("missing project param")
			}
			var body bytes.Buffer
			io.Copy(&body, r.Body)
			if !strings.Contains(body.String(), `"name":"mybucket"`) {
				t.Fatalf("create body = %s", body.String())
			}
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected extra request")
		}
	}
	cli, shutdown := newTestClient(t, handler)
	defer shutdown()

	if err := cli.EnsureBucket(context.Background()); err != nil {
		t.Fatalf("EnsureBucket create: %v", err)
	}
	if step != 2 {
		t.Fatalf("expected 2 requests, got %d", step)
	}
}

func Test_Upload(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method %s", r.Method)
		}
		if !strings.HasPrefix(r.URL.Path, "/upload/storage/v1/b/mybucket/o") {
			t.Fatalf("received path does not have correct prefix: %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer TESTTOKEN" {
			t.Fatalf("auth header missing")
		}
		mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || !strings.HasPrefix(mediaType, "multipart/related") {
			t.Fatalf("content-type: %s", r.Header.Get("Content-Type"))
		}
		mr := multipart.NewReader(r.Body, params["boundary"])
		// part 1: metadata
		p1, _ := mr.NextPart()
		var meta struct {
			Name     string `json:"name"`
			Metadata struct {
				ID string `json:"rqlite-auto-backup-id"`
			} `json:"metadata"`
		}
		if err := json.NewDecoder(p1).Decode(&meta); err != nil {
			t.Fatalf("metadata decode: %v", err)
		}
		if meta.Name != "object.txt" || meta.Metadata.ID != "v123" {
			t.Fatalf("metadata %+v", meta)
		}
		// part 2: data
		p2, _ := mr.NextPart()
		body, _ := io.ReadAll(p2)
		if string(body) != "hello" {
			t.Fatalf("payload %q", body)
		}
		w.WriteHeader(http.StatusOK)
	}
	cli, shutdown := newTestClient(t, handler)
	defer shutdown()

	if err := cli.Upload(context.Background(),
		strings.NewReader("hello"), "v123"); err != nil {
		t.Fatalf("Upload: %v", err)
	}
}

func Test_Download(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method %s", r.Method)
		}
		if !strings.Contains(r.URL.RawQuery, "alt=media") {
			t.Fatalf("missing alt=media")
		}
		if r.URL.Path != "/storage/v1/b/mybucket/o/object.txt" {
			t.Fatalf("path %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("world"))
	}
	cli, shutdown := newTestClient(t, handler)
	defer shutdown()

	var buf writerAtBuffer
	if err := cli.Download(context.Background(), &buf); err != nil {
		t.Fatalf("Download: %v", err)
	}
	if buf.String() != "world" {
		t.Fatalf("got %q, want %q", buf.String(), "world")
	}
}

func Test_Delete(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Fatalf("method %s", r.Method)
		}
		if r.URL.Path != "/storage/v1/b/mybucket/o/object.txt" {
			t.Fatalf("path %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}
	cli, shutdown := newTestClient(t, handler)
	defer shutdown()

	if err := cli.Delete(context.Background()); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}

func Test_CurrentID(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method %s", r.Method)
		}
		resp := map[string]any{
			"metadata": map[string]string{GCPGCSIDKey: "xyz"},
		}
		enc := json.NewEncoder(w)
		enc.Encode(resp)
	}
	cli, shutdown := newTestClient(t, handler)
	defer shutdown()

	id, err := cli.CurrentID(context.Background())
	if err != nil {
		t.Fatalf("CurrentID: %v", err)
	}
	if id != "xyz" {
		t.Errorf("id = %s, want xyz", id)
	}
}

type writerAtBuffer struct {
	buf bytes.Buffer
}

func (w *writerAtBuffer) WriteAt(p []byte, off int64) (int, error) {
	if off != int64(w.buf.Len()) {
		return 0, fmt.Errorf("unexpected offset %d", off)
	}
	return w.buf.Write(p)
}

func (w *writerAtBuffer) String() string { return w.buf.String() }

// createCredFile returns a temp-file pathname that satisfies loadServiceAccount.
func createCredFile(t *testing.T) string {
	t.Helper()
	f, err := os.CreateTemp("", "cred-*.json")
	if err != nil {
		t.Fatalf("temp file: %v", err)
	}
	cred := `{"client_email":"test@example.com","private_key":"-----BEGIN PRIVATE KEY-----\n-----END PRIVATE KEY-----"}`
	if _, err = f.WriteString(cred); err != nil {
		t.Fatalf("write cred: %v", err)
	}
	f.Close()
	return f.Name()
}

// newTestClient spins up an httptest server whose handler is supplied by caller,
// then returns (client, shutdownFn).
func newTestClient(t *testing.T, h http.HandlerFunc) (*GCSClient, func()) {
	ts := httptest.NewServer(h)

	cfg := &GCSConfig{
		Endpoint:        ts.URL, // override base
		ProjectID:       "proj",
		Bucket:          "mybucket",
		Name:            "object.txt",
		CredentialsPath: createCredFile(t), // dummy, never used
	}

	cli, err := NewGCSClient(cfg, nil)
	if err != nil {
		t.Fatalf("NewGCSClient: %v", err)
	}
	// Route all requests through the test server.
	cli.http = ts.Client()

	// Skip JWT + token exchange by planting a valid token.
	cli.accessToken = "TESTTOKEN"
	cli.expiry = time.Now().Add(time.Hour)

	return cli, ts.Close
}
