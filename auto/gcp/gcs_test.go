package gcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/internal/random"
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
		if r.URL.Query().Has("kmsKeyName") {
			t.Fatalf("expected no KMS key name, got %q", r.URL.Query().Get("kmsKeyName"))
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

func Test_Upload_KMSKeyName(t *testing.T) {
	const keyName = "projects/proj/locations/us/keyRings/ring/cryptoKeys/key"
	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method %s", r.Method)
		}
		q := r.URL.Query()
		if q.Get("uploadType") != "multipart" {
			t.Fatalf("uploadType = %q, want multipart", q.Get("uploadType"))
		}
		if q.Get("kmsKeyName") != keyName {
			t.Fatalf("kmsKeyName = %q, want %q", q.Get("kmsKeyName"), keyName)
		}
		mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || !strings.HasPrefix(mediaType, "multipart/related") {
			t.Fatalf("content-type: %s", r.Header.Get("Content-Type"))
		}
		mr := multipart.NewReader(r.Body, params["boundary"])
		// The key is a request parameter, not object metadata, so the
		// metadata part must be unchanged.
		p1, _ := mr.NextPart()
		var meta map[string]any
		if err := json.NewDecoder(p1).Decode(&meta); err != nil {
			t.Fatalf("metadata decode: %v", err)
		}
		if _, ok := meta["kmsKeyName"]; ok {
			t.Fatalf("metadata unexpectedly contains kmsKeyName: %v", meta)
		}
		p2, _ := mr.NextPart()
		body, _ := io.ReadAll(p2)
		if string(body) != "hello" {
			t.Fatalf("payload %q", body)
		}
		w.WriteHeader(http.StatusOK)
	}
	cli, shutdown := newTestClient(t, handler)
	defer shutdown()
	cli.cfg.KMSKeyName = keyName

	if err := cli.Upload(context.Background(),
		strings.NewReader("hello"), "v123"); err != nil {
		t.Fatalf("Upload: %v", err)
	}
}

func Test_NewGCSClient_KMSKeyName(t *testing.T) {
	credsPath := createCredFile(t)
	for _, tc := range []struct {
		name      string
		keyName   string
		expectErr bool
	}{
		{"Unset", "", false},
		{"Valid", "projects/proj/locations/us/keyRings/ring/cryptoKeys/key", false},
		{"ValidGlobal", "projects/my-proj/locations/global/keyRings/my-ring/cryptoKeys/my-key", false},
		{"BareKey", "my-key", true},
		{"MissingProject", "projects//locations/us/keyRings/ring/cryptoKeys/key", true},
		{"MissingKey", "projects/proj/locations/us/keyRings/ring/cryptoKeys/", true},
		{"WrongLabel", "projects/proj/regions/us/keyRings/ring/cryptoKeys/key", true},
		{"WrongCase", "projects/proj/locations/us/keyrings/ring/cryptokeys/key", true},
		{"KeyVersion", "projects/proj/locations/us/keyRings/ring/cryptoKeys/key/cryptoKeyVersions/1", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &GCSConfig{
				ProjectID:       "proj",
				Bucket:          "mybucket",
				Name:            "object.txt",
				CredentialsPath: credsPath,
				KMSKeyName:      tc.keyName,
			}
			cli, err := NewGCSClient(cfg, nil)
			if tc.expectErr {
				if !errors.Is(err, ErrInvalidKMSKeyName) {
					t.Fatalf("expected ErrInvalidKMSKeyName, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewGCSClient: %v", err)
			}
			if cli.cfg.KMSKeyName != tc.keyName {
				t.Fatalf("KMSKeyName = %q, want %q", cli.cfg.KMSKeyName, tc.keyName)
			}
		})
	}
}

// Test_KMSKeyNameApplied uploads to the real test bucket with a customer-managed
// key and checks that GCS reports the object as encrypted with it. It skips
// unless the GCS credentials and a KMS key name are set in the environment.
func Test_KMSKeyNameApplied(t *testing.T) {
	creds, ok := os.LookupEnv("RQLITE_GCS_CREDENTIALS")
	if !ok {
		t.Skip("RQLITE_GCS_CREDENTIALS is not set")
	}
	projectID, ok := os.LookupEnv("RQLITE_GCS_PROJECT_ID")
	if !ok {
		t.Skip("RQLITE_GCS_PROJECT_ID is not set")
	}
	keyName, ok := os.LookupEnv("RQLITE_GCS_KMS_KEY_NAME")
	if !ok {
		t.Skip("RQLITE_GCS_KMS_KEY_NAME is not set")
	}

	credsPath := filepath.Join(t.TempDir(), "creds.json")
	if err := os.WriteFile(credsPath, []byte(creds), 0600); err != nil {
		t.Fatalf("write credentials: %v", err)
	}
	cfg := &GCSConfig{
		ProjectID:       projectID,
		Bucket:          "rqlite-testing-circleci",
		Name:            random.String(),
		CredentialsPath: credsPath,
		KMSKeyName:      keyName,
	}
	cli, err := NewGCSClient(cfg, nil)
	if err != nil {
		t.Fatalf("NewGCSClient: %v", err)
	}
	if err := cli.Upload(context.Background(), strings.NewReader("test data"), "the-id"); err != nil {
		t.Fatalf("Upload: %v", err)
	}
	defer func() {
		cli.Delete(context.Background())
	}()

	// GCS reports the key it actually used, with the key version appended.
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, cli.objectURL, nil)
	if err := cli.addAuth(req); err != nil {
		t.Fatalf("addAuth: %v", err)
	}
	res, err := cli.http.Do(req)
	if err != nil {
		t.Fatalf("metadata fetch: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("metadata fetch failed: %s", b)
	}
	var obj struct {
		KMSKeyName string `json:"kmsKeyName"`
	}
	if err := json.NewDecoder(res.Body).Decode(&obj); err != nil {
		t.Fatalf("metadata decode: %v", err)
	}
	if !strings.HasPrefix(obj.KMSKeyName, keyName+"/cryptoKeyVersions/") {
		t.Fatalf("kmsKeyName = %q, want a version of %q", obj.KMSKeyName, keyName)
	}

	// The object must still be readable through the normal paths.
	id, err := cli.CurrentID(context.Background())
	if err != nil {
		t.Fatalf("CurrentID: %v", err)
	}
	if id != "the-id" {
		t.Fatalf("id = %q, want %q", id, "the-id")
	}
	var buf writerAtBuffer
	if err := cli.Download(context.Background(), &buf); err != nil {
		t.Fatalf("Download: %v", err)
	}
	if buf.String() != "test data" {
		t.Fatalf("downloaded %q, want %q", buf.String(), "test data")
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
