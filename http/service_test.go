package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cluster "github.com/rqlite/rqlite/v8/cluster/proto"
	command "github.com/rqlite/rqlite/v8/command/proto"
	"github.com/rqlite/rqlite/v8/store"
)

func Test_ResponseJSONMarshal(t *testing.T) {
	resp := NewResponse()
	b, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("error JSON marshaling empty Response: %s", err.Error())
	}
	if exp, got := `{"results":[]}`, string(b); exp != got {
		t.Fatalf("Incorrect marshal, exp: %s, got: %s", exp, got)
	}

	resp = NewResponse()
	resp.Results.ExecuteQueryResponse = []*command.ExecuteQueryResponse{
		{
			Result: &command.ExecuteQueryResponse_E{
				E: &command.ExecuteResult{
					LastInsertId: 39,
					RowsAffected: 45,
					Time:         1234,
				},
			},
		},
	}

	b, err = json.Marshal(resp)
	if err != nil {
		t.Fatalf("failed to JSON marshal empty Response: %s", err)
	}
	if exp, got := `{"results":[{"last_insert_id":39,"rows_affected":45,"time":1234}]}`, string(b); exp != got {
		t.Fatalf("Incorrect marshal, exp: %s, got: %s", exp, got)
	}

	resp = NewResponse()
	resp.Results.QueryRows = []*command.QueryRows{{
		Columns: []string{"id", "name"},
		Types:   []string{"int", "string"},
	}}
	b, err = json.Marshal(resp)
	if err != nil {
		t.Fatalf("failed to JSON marshal empty Response: %s", err)
	}
	if exp, got := `{"results":[{"columns":["id","name"],"types":["int","string"]}]}`, string(b); exp != got {
		t.Fatalf("Incorrect marshal, exp: %s, got: %s", exp, got)
	}

	resp = NewResponse()
	resp.Results.QueryRows = []*command.QueryRows{{
		Columns: []string{"id", "name"},
		Types:   []string{"int", "string"},
		Values: []*command.Values{
			{
				Parameters: []*command.Parameter{
					{
						Value: &command.Parameter_S{
							S: "fiona",
						},
					},
					{
						Value: &command.Parameter_I{
							I: 5,
						},
					},
				},
			},
		},
	}}
	b, err = json.Marshal(resp)
	if err != nil {
		t.Fatalf("failed to JSON marshal empty Response: %s", err)
	}
	if exp, got := `{"results":[{"columns":["id","name"],"types":["int","string"],"values":[["fiona",5]]}]}`, string(b); exp != got {
		t.Fatalf("Incorrect marshal, exp: %s, got: %s", exp, got)
	}
}

func Test_NewService(t *testing.T) {
	store := &MockStore{}
	cluster := &mockClusterService{}
	cred := &mockCredentialStore{HasPermOK: true}
	s := New("127.0.0.1:0", store, cluster, cred)
	if s == nil {
		t.Fatalf("failed to create new service")
	}
	if s.HTTPS() {
		t.Fatalf("expected service to report not HTTPS")
	}
}

func Test_HasVersionHeader(t *testing.T) {
	m := &MockStore{}
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()
	s.BuildInfo = map[string]interface{}{
		"version": "the version",
	}
	url := fmt.Sprintf("http://%s", s.Addr().String())

	client := &http.Client{}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("failed to make request")
	}

	if resp.Header.Get("X-RQLITE-VERSION") != "the version" {
		t.Fatalf("incorrect build version present in HTTP response header")
	}
}

func Test_HasAllowOriginHeader(t *testing.T) {
	m := &MockStore{}
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()
	url := fmt.Sprintf("http://%s", s.Addr().String())

	client := &http.Client{}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("failed to make request")
	}
	if resp.Header.Get("Access-Control-Allow-Origin") != "" {
		t.Fatalf("incorrect allow-origin present in HTTP response header")
	}

	s.SetAllowOrigin("https://www.philipotoole.com")
	resp, err = client.Get(url)
	if err != nil {
		t.Fatalf("failed to make request")
	}
	if resp.Header.Get("Access-Control-Allow-Origin") != "https://www.philipotoole.com" {
		t.Fatalf("incorrect allow-origin in HTTP response header")
	}
}

func Test_Options(t *testing.T) {
	m := &MockStore{}
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()
	url := fmt.Sprintf("http://%s", s.Addr().String())

	client := &http.Client{}
	req, err := http.NewRequest("OPTIONS", url, nil)
	if err != nil {
		t.Fatalf("failed to create request: %s", err.Error())
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("failed to make request: %s", err.Error())
	}
	if resp.StatusCode != 200 {
		t.Fatalf("failed to get expected 200 for OPTIONS, got %d", resp.StatusCode)
	}
}

func Test_HasContentTypeJSON(t *testing.T) {
	m := &MockStore{}
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	client := &http.Client{}
	resp, err := client.Get(fmt.Sprintf("http://%s/status", s.Addr().String()))
	if err != nil {
		t.Fatalf("failed to make request")
	}

	h := resp.Header.Get("Content-Type")
	if h != "application/json; charset=utf-8" {
		t.Fatalf("incorrect Content-type in HTTP response: %s", h)
	}
}

func Test_HasContentTypeOctetStream(t *testing.T) {
	m := &MockStore{}
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	client := &http.Client{}
	resp, err := client.Get(fmt.Sprintf("http://%s/db/backup", s.Addr().String()))
	if err != nil {
		t.Fatalf("failed to make request")
	}

	h := resp.Header.Get("Content-Type")
	if h != "application/octet-stream" {
		t.Fatalf("incorrect Content-type in HTTP response: %s", h)
	}
}

func Test_HasVersionHeaderUnknown(t *testing.T) {
	m := &MockStore{}
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()
	url := fmt.Sprintf("http://%s", s.Addr().String())

	client := &http.Client{}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("failed to make request")
	}

	if resp.Header.Get("X-RQLITE-VERSION") != "unknown" {
		t.Fatalf("incorrect build version present in HTTP response header")
	}
}

func Test_404Routes(t *testing.T) {
	m := &MockStore{}
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()
	host := fmt.Sprintf("http://%s", s.Addr().String())

	client := &http.Client{}

	resp, err := client.Get(host + "/db/xxx")
	if err != nil {
		t.Fatalf("failed to make request")
	}
	if resp.StatusCode != 404 {
		t.Fatalf("failed to get expected 404, got %d", resp.StatusCode)
	}

	resp, err = client.Post(host+"/xxx", "", nil)
	if err != nil {
		t.Fatalf("failed to make request")
	}
	if resp.StatusCode != 404 {
		t.Fatalf("failed to get expected 404, got %d", resp.StatusCode)
	}
}

func Test_405Routes(t *testing.T) {
	type testCase struct {
		method string
		path   string
	}

	tests := []testCase{
		{method: "GET", path: "/db/execute"},
		{method: "GET", path: "/boot"},
		{method: "GET", path: "/db/load"},
		{method: "GET", path: "/remove"},
		{method: "POST", path: "/remove"},
		{method: "GET", path: "/snapshot"},
		{method: "POST", path: "/db/backup"},
		{method: "POST", path: "/status"},
		{method: "POST", path: "/nodes"},
	}

	m := &MockStore{}
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()
	host := fmt.Sprintf("http://%s", s.Addr().String())

	client := &http.Client{}

	for _, tc := range tests {
		var resp *http.Response
		var err error

		switch tc.method {
		case "GET":
			resp, err = client.Get(host + tc.path)
		case "POST":
			resp, err = client.Post(host+tc.path, "", nil)
		default:
			t.Fatalf("unsupported method: %s", tc.method)
		}

		if err != nil {
			t.Fatalf("failed to make request for %s %s", tc.method, tc.path)
		}
		if resp.StatusCode != 405 {
			t.Fatalf("failed to get expected 405 for %s %s, got %d", tc.method, tc.path, resp.StatusCode)
		}
	}
}

func Test_400Routes(t *testing.T) {
	m := &MockStore{}
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()
	host := fmt.Sprintf("http://%s", s.Addr().String())

	client := &http.Client{}

	resp, err := client.Get(host + "/db/query?q=")
	if err != nil {
		t.Fatalf("failed to make request")
	}
	if resp.StatusCode != 400 {
		t.Fatalf("failed to get expected 400, got %d", resp.StatusCode)
	}
}

func Test_401Routes_NoBasicAuth(t *testing.T) {
	c := &mockCredentialStore{HasPermOK: false}

	m := &MockStore{}
	n := &mockClusterService{}
	s := New("127.0.0.1:0", m, n, c)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()
	host := fmt.Sprintf("http://%s", s.Addr().String())

	client := &http.Client{}
	for _, path := range []string{
		"/db/execute",
		"/db/query",
		"/db/request",
		"/db/backup",
		"/db/load",
		"/boot",
		"/remove",
		"/status",
		"/nodes",
		"/readyz",
		"/debug/vars",
		"/debug/pprof/cmdline",
		"/debug/pprof/profile",
		"/debug/pprof/symbol",
	} {
		resp, err := client.Get(host + path)
		if err != nil {
			t.Fatalf("failed to make request")
		}
		if resp.StatusCode != 401 {
			t.Fatalf("failed to get expected 401 for path %s, got %d", path, resp.StatusCode)
		}
	}
}

func Test_401Routes_BasicAuthBadPassword(t *testing.T) {
	c := &mockCredentialStore{HasPermOK: false}

	m := &MockStore{}
	n := &mockClusterService{}
	s := New("127.0.0.1:0", m, n, c)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()
	host := fmt.Sprintf("http://%s", s.Addr().String())

	client := &http.Client{}
	for _, path := range []string{
		"/db/execute",
		"/db/query",
		"/db/request",
		"/db/backup",
		"/db/load",
		"/boot",
		"/status",
		"/nodes",
		"/readyz",
		"/debug/vars",
		"/debug/pprof/cmdline",
		"/debug/pprof/profile",
		"/debug/pprof/symbol",
	} {
		req, err := http.NewRequest("GET", host+path, nil)
		if err != nil {
			t.Fatalf("failed to create request: %s", err.Error())
		}
		req.SetBasicAuth("username1", "password1")

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("failed to make request: %s", err.Error())
		}
		if resp.StatusCode != 401 {
			t.Fatalf("failed to get expected 401 for path %s, got %d", path, resp.StatusCode)
		}
	}
}

func Test_401Routes_BasicAuthBadPerm(t *testing.T) {
	c := &mockCredentialStore{HasPermOK: false}

	m := &MockStore{}
	n := &mockClusterService{}
	s := New("127.0.0.1:0", m, n, c)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()
	host := fmt.Sprintf("http://%s", s.Addr().String())

	client := &http.Client{}

	for _, path := range []string{
		"/db/execute",
		"/db/query",
		"/db/backup",
		"/db/request",
		"/db/load",
		"/boot",
		"/snapshot",
		"/status",
		"/nodes",
		"/readyz",
		"/debug/vars",
		"/debug/pprof/cmdline",
		"/debug/pprof/profile",
		"/debug/pprof/symbol",
	} {
		req, err := http.NewRequest("GET", host+path, nil)
		if err != nil {
			t.Fatalf("failed to create request: %s", err.Error())
		}
		req.SetBasicAuth("username1", "password1")

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("failed to make request: %s", err.Error())
		}
		if resp.StatusCode != 401 {
			t.Fatalf("failed to get expected 401 for path %s, got %d", path, resp.StatusCode)
		}
	}
}

func Test_BackupOK(t *testing.T) {
	m := &MockStore{}
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	m.backupFn = func(br *command.BackupRequest, dst io.Writer) error {
		return nil
	}

	client := &http.Client{}
	host := fmt.Sprintf("http://%s", s.Addr().String())
	resp, err := client.Get(host + "/db/backup")
	if err != nil {
		t.Fatalf("failed to make backup request")
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to get expected StatusOK for backup, got %d", resp.StatusCode)
	}
}

func Test_BackupVacuumOK(t *testing.T) {
	m := &MockStore{}
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	m.backupFn = func(br *command.BackupRequest, dst io.Writer) error {
		if !br.Vacuum {
			t.Fatal("expected vacuum to be true")
		}
		return nil
	}

	client := &http.Client{}
	host := fmt.Sprintf("http://%s", s.Addr().String())

	resp, err := client.Get(host + "/db/backup?vacuum")
	if err != nil {
		t.Fatalf("failed to make backup request")
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to get expected StatusOK for backup, got %d", resp.StatusCode)
	}
	resp, err = client.Get(host + "/db/backup?vacuum&fmt=binary")
	if err != nil {
		t.Fatalf("failed to make backup request")
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to get expected StatusOK for backup, got %d", resp.StatusCode)
	}
}

func Test_SnapshotOK(t *testing.T) {
	m := &MockStore{}
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	// Set the snapshot function to return OK
	var wg sync.WaitGroup
	wg.Add(1)
	m.snapshotFn = func(n uint64) error {
		defer wg.Done()
		if exp, got := uint64(100), n; exp != got {
			t.Fatalf("expected trailing logs to be %d, got %d", exp, got)
		}
		return nil
	}

	client := &http.Client{}
	host := fmt.Sprintf("http://%s", s.Addr().String())
	resp, err := client.Post(host+"/snapshot?trailing_logs=100", "", nil)
	if err != nil {
		t.Fatalf("failed to make Snapshot request")
	}

	wg.Wait()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to get expected StatusOK for Snapshot, got %d", resp.StatusCode)
	}
}

func Test_BackupFlagsNoLeaderRedirect(t *testing.T) {
	m := &MockStore{}
	c := &mockClusterService{
		apiAddr: "http://1.2.3.4:999",
	}

	s := New("127.0.0.1:0", m, c, nil)

	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	m.backupFn = func(br *command.BackupRequest, dst io.Writer) error {
		return store.ErrNotLeader
	}

	client := &http.Client{}
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	host := fmt.Sprintf("http://%s", s.Addr().String())
	resp, err := client.Get(host + "/db/backup?redirect")
	if err != nil {
		t.Fatalf("failed to make backup request: %s", err.Error())
	}
	if resp.StatusCode != http.StatusMovedPermanently {
		t.Fatalf("failed to get expected StatusServiceUnavailable for backup, got %d", resp.StatusCode)
	}
}

func Test_BackupFlagsNoLeaderRemoteFetch(t *testing.T) {
	m := &MockStore{
		leaderAddr: "foo:1234",
	}
	c := &mockClusterService{
		apiAddr: "http://1.2.3.4:999",
	}

	s := New("127.0.0.1:0", m, c, nil)

	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	m.backupFn = func(br *command.BackupRequest, dst io.Writer) error {
		return store.ErrNotLeader
	}

	backupData := "this is SQLite data"
	c.backupFn = func(br *command.BackupRequest, addr string, t time.Duration, w io.Writer) error {
		w.Write([]byte(backupData))
		return nil
	}

	client := &http.Client{}
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	host := fmt.Sprintf("http://%s", s.Addr().String())
	resp, err := client.Get(host + "/db/backup")
	if err != nil {
		t.Fatalf("failed to make backup request: %s", err.Error())
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to get expected StatusOK for remote backup fetch, got %d", resp.StatusCode)
	}
	defer resp.Body.Close()
	if exp, got := backupData, mustReadBody(t, resp); exp != got {
		t.Fatalf("received incorrect backup data, exp: %s, got: %s", exp, got)
	}
}

func Test_BackupFlagsNoLeaderOK(t *testing.T) {
	m := &MockStore{}
	c := &mockClusterService{
		apiAddr: "http://1.2.3.4:999",
	}

	s := New("127.0.0.1:0", m, c, nil)

	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	m.backupFn = func(br *command.BackupRequest, dst io.Writer) error {
		if !br.Leader {
			return nil
		}
		return store.ErrNotLeader
	}

	client := &http.Client{}
	host := fmt.Sprintf("http://%s", s.Addr().String())
	resp, err := client.Get(host + "/db/backup?noleader")
	if err != nil {
		t.Fatalf("failed to make backup request")
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to get expected StatusOK for backup, got %d", resp.StatusCode)
	}
}

func Test_BackupFlagsInvalid(t *testing.T) {
	m := &MockStore{}
	c := &mockClusterService{
		apiAddr: "http://1.2.3.4:999",
	}

	s := New("127.0.0.1:0", m, c, nil)

	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	m.backupFn = func(br *command.BackupRequest, dst io.Writer) error {
		if br.Vacuum && br.Format == command.BackupRequest_BACKUP_REQUEST_FORMAT_SQL {
			return store.ErrInvalidVacuum
		}
		return nil
	}

	client := &http.Client{}
	host := fmt.Sprintf("http://%s", s.Addr().String())
	resp, err := client.Get(host + "/db/backup?fmt=sql&vacuum")
	if err != nil {
		t.Fatalf("failed to make backup request")
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("failed to get expected StatusBadRequest for backup, got %d", resp.StatusCode)
	}
}

func Test_LoadOK(t *testing.T) {
	m := &MockStore{}
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	client := &http.Client{}
	host := fmt.Sprintf("http://%s", s.Addr().String())
	resp, err := client.Post(host+"/db/load", "application/octet-stream", strings.NewReader("SELECT"))
	if err != nil {
		t.Fatalf("failed to make load request")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to get expected StatusOK for load, got %d", resp.StatusCode)
	}
	if exp, got := `{"results":[]}`, mustReadBody(t, resp); exp != got {
		t.Fatalf("incorrect response body, exp: %s, got %s", exp, got)
	}
}

func Test_LoadFlagsNoLeader(t *testing.T) {
	m := &MockStore{
		leaderAddr: "foo:1234",
	}
	c := &mockClusterService{
		apiAddr: "http://1.2.3.4:999",
	}

	s := New("127.0.0.1:0", m, c, nil)

	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	testData, err := os.ReadFile("testdata/load.db")
	if err != nil {
		t.Fatalf("failed to load test SQLite data")
	}

	m.loadFn = func(lr *command.LoadRequest) error {
		return store.ErrNotLeader
	}

	clusterLoadCalled := false
	c.loadFn = func(lc *command.LoadRequest, nodeAddr string, timeout time.Duration) error {
		clusterLoadCalled = true
		if !bytes.Equal(lc.Data, testData) {
			t.Fatalf("wrong data passed to cluster load")
		}
		return nil
	}

	client := &http.Client{}
	host := fmt.Sprintf("http://%s", s.Addr().String())
	resp, err := client.Post(host+"/db/load", "application/octet-stream", bytes.NewReader(testData))
	if err != nil {
		t.Fatalf("failed to make load request")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to get expected StatusOK for load, got %d", resp.StatusCode)
	}

	if !clusterLoadCalled {
		t.Fatalf("cluster load was not called")
	}

	if exp, got := `{"results":[]}`, mustReadBody(t, resp); exp != got {
		t.Fatalf("incorrect response body, exp: %s, got %s", exp, got)
	}
}

func Test_LoadRemoteError(t *testing.T) {
	m := &MockStore{
		leaderAddr: "foo:1234",
	}
	c := &mockClusterService{
		apiAddr: "http://1.2.3.4:999",
	}

	s := New("127.0.0.1:0", m, c, nil)

	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	testData, err := os.ReadFile("testdata/load.db")
	if err != nil {
		t.Fatalf("failed to load test SQLite data")
	}

	m.loadFn = func(br *command.LoadRequest) error {
		return store.ErrNotLeader
	}
	clusterLoadCalled := false
	c.loadFn = func(lr *command.LoadRequest, addr string, t time.Duration) error {
		clusterLoadCalled = true
		return fmt.Errorf("the load failed")
	}

	client := &http.Client{}
	host := fmt.Sprintf("http://%s", s.Addr().String())
	resp, err := client.Post(host+"/db/load", "application/octet-stream", bytes.NewReader(testData))
	if err != nil {
		t.Fatalf("failed to make load request")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("failed to get expected StatusInternalServerError for load, got %d", resp.StatusCode)
	}

	if !clusterLoadCalled {
		t.Fatalf("cluster load was not called")
	}

	if exp, got := "the load failed\n", mustReadBody(t, resp); exp != got {
		t.Fatalf(`incorrect response body, exp: "%s", got: "%s"`, exp, got)
	}
}

func Test_Boot(t *testing.T) {
	m := &MockStore{
		leaderAddr: "foo:1234",
	}
	c := &mockClusterService{
		apiAddr: "http://1.2.3.4:999",
	}

	s := New("127.0.0.1:0", m, c, nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	testData, err := os.ReadFile("testdata/load.db")
	if err != nil {
		t.Fatalf("failed to load test SQLite data")
	}

	readFromCalled := false
	m.readFromFn = func(r io.Reader) (int64, error) {
		// read all data from r and compare to the data in testData
		b, err := io.ReadAll(r)
		if err != nil {
			return 0, err
		}
		if !bytes.Equal(b, testData) {
			t.Fatalf("wrong data passed to ReadFrom")
		}
		readFromCalled = true
		return int64(len(b)), nil
	}

	client := &http.Client{}
	host := fmt.Sprintf("http://%s", s.Addr().String())
	resp, err := client.Post(host+"/boot", "application/octet-stream", bytes.NewReader(testData))
	if err != nil {
		t.Fatalf("failed to make boot request")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to get expected StatusOK for boot, got %d", resp.StatusCode)
	}

	if !readFromCalled {
		t.Fatalf("ReadFrom was not called")
	}
}

func Test_RegisterStatus(t *testing.T) {
	var stats *mockStatusReporter
	m := &MockStore{}
	c := &mockClusterService{}

	s := New("127.0.0.1:0", m, c, nil)

	if err := s.RegisterStatus("foo", stats); err != nil {
		t.Fatalf("failed to register statusReporter: %s", err.Error())
	}

	if err := s.RegisterStatus("foo", stats); err == nil {
		t.Fatal("successfully re-registered statusReporter")
	}
}

func Test_FormRedirect(t *testing.T) {
	m := &MockStore{
		leaderAddr: "foo:4002",
	}
	c := &mockClusterService{
		apiAddr: "http://foo:4001",
	}

	s := New("127.0.0.1:0", m, c, nil)
	req := mustNewHTTPRequest("http://qux:4001")

	rd, err := s.FormRedirect(req)
	if err != nil {
		t.Fatalf("failed to form redirect: %s", err.Error())
	}
	if exp, got := "http://foo:4001", rd; exp != got {
		t.Fatalf("incorrect redirect, exp: %s, got: %s", exp, got)
	}
}

func Test_FormRedirectParam(t *testing.T) {
	m := &MockStore{
		leaderAddr: "foo:4002",
	}
	c := &mockClusterService{
		apiAddr: "http://foo:4001",
	}
	s := New("127.0.0.1:0", m, c, nil)
	req := mustNewHTTPRequest("http://qux:4001/db/query?x=y")

	rd, err := s.FormRedirect(req)
	if err != nil {
		t.Fatalf("failed to form redirect: %s", err.Error())
	}

	if exp, got := "http://foo:4001/db/query?x=y", rd; rd != got {
		t.Fatalf("incorrect redirect, exp: %s, got: %s", exp, got)
	}
}

func Test_FormRedirectHTTPS(t *testing.T) {
	m := &MockStore{
		leaderAddr: "foo:4002",
	}
	c := &mockClusterService{
		apiAddr: "https://foo:4001",
	}

	s := New("127.0.0.1:0", m, c, nil)
	req := mustNewHTTPRequest("http://qux:4001")

	rd, err := s.FormRedirect(req)
	if err != nil {
		t.Fatalf("failed to form redirect: %s", err.Error())
	}
	if exp, got := "https://foo:4001", rd; exp != got {
		t.Fatalf("incorrect redirect, exp: %s, got: %s", exp, got)
	}
}

func Test_DoRedirect(t *testing.T) {
	m := &MockStore{
		leaderAddr: "foo:4002",
	}
	c := &mockClusterService{
		apiAddr: "https://foo:4001",
	}
	s := New("127.0.0.1:0", m, c, nil)
	req := mustNewHTTPRequest("http://qux:4001")
	qp := mustGetQueryParams(req)

	if s.DoRedirect(nil, req, qp) {
		t.Fatalf("incorrectly redirected")
	}

	req = mustNewHTTPRequest("http://qux:4001/db/query?redirect")
	qp = mustGetQueryParams(req)
	w := httptest.NewRecorder()
	if !s.DoRedirect(w, req, qp) {
		t.Fatalf("incorrectly not redirected")
	}
	if exp, got := http.StatusMovedPermanently, w.Code; exp != got {
		t.Fatalf("incorrect redirect code, exp: %d, got: %d", exp, got)
	}
	// check location header
	if exp, got := "https://foo:4001/db/query?redirect", w.Header().Get("Location"); exp != got {
		t.Fatalf("incorrect redirect location, exp: %s, got: %s", exp, got)
	}
}

func Test_Nodes(t *testing.T) {
	m := &MockStore{
		leaderAddr: "foo:1234",
	}
	c := &mockClusterService{
		apiAddr: "https://bar:5678",
	}
	s := New("127.0.0.1:0", m, c, nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	client := &http.Client{}
	host := fmt.Sprintf("http://%s", s.Addr().String())
	resp, err := client.Get(host + "/nodes")
	if err != nil {
		t.Fatalf("failed to make nodes request")
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to get expected StatusOK for nodes, got %d", resp.StatusCode)
	}
}

func Test_RootRedirectToStatus(t *testing.T) {
	m := &MockStore{}
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.Get(fmt.Sprintf("http://%s/", s.Addr().String()))
	if err != nil {
		t.Fatalf("failed to make root request")
	}
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("failed to get expected StatusFound for root, got %d", resp.StatusCode)
	}
	if resp.Header["Location"][0] != "/status" {
		t.Fatalf("received incorrect redirect path")
	}

	resp, err = client.Get(fmt.Sprintf("http://%s", s.Addr().String()))
	if err != nil {
		t.Fatalf("failed to make root request")
	}
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("failed to get expected StatusFound for root, got %d", resp.StatusCode)
	}
	if resp.Header["Location"][0] != "/status" {
		t.Fatalf("received incorrect redirect path")
	}
}

func Test_Readyz(t *testing.T) {
	m := &MockStore{
		leaderAddr: "foo:1234",
	}
	c := &mockClusterService{
		apiAddr: "https://bar:5678",
	}
	s := New("127.0.0.1:0", m, c, nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	client := &http.Client{}
	host := fmt.Sprintf("http://%s", s.Addr().String())
	resp, err := client.Get(host + "/readyz")
	if err != nil {
		t.Fatalf("failed to make readyz request")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to get expected StatusOK for node, got %d", resp.StatusCode)
	}
	if exp, got := "[+]node ok\n[+]leader ok\n[+]store ok", mustReadBody(t, resp); exp != got {
		t.Fatalf("incorrect response body, exp: %s, got: %s", exp, got)
	}

	resp, err = client.Get(host + "/readyz?noleader")
	if err != nil {
		t.Fatalf("failed to make readyz request")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to get expected StatusOK, got %d", resp.StatusCode)
	}
	if exp, got := "[+]node ok", mustReadBody(t, resp); exp != got {
		t.Fatalf("incorrect response body, exp: %s, got: %s", exp, got)
	}

	m.notReady = true
	m.committedFn = func(timeout time.Duration) (uint64, error) {
		t.Fatal("committedFn should not have been called")
		return 0, nil
	}
	resp, err = client.Get(host + "/readyz")
	if err != nil {
		t.Fatalf("failed to make readyz request")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("failed to get expected StatusServiceUnavailable, got %d", resp.StatusCode)
	}
	if exp, got := "[+]node ok\n[+]leader ok\n[+]store not ready", mustReadBody(t, resp); exp != got {
		t.Fatalf("incorrect response body, exp: %s, got: %s", exp, got)
	}

	cnt := &atomic.Uint32{}
	m.notReady = false
	m.committedFn = func(timeout time.Duration) (uint64, error) {
		cnt.Store(1)
		return 0, fmt.Errorf("timeout")
	}
	resp, err = client.Get(host + "/readyz?sync")
	if err != nil {
		t.Fatalf("failed to make readyz request with sync set")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("failed to get expected StatusServiceUnavailable, got %d", resp.StatusCode)
	}
	if exp, got := "[+]node ok\n[+]leader ok\n[+]store ok\n[+]sync timeout", mustReadBody(t, resp); exp != got {
		t.Fatalf("incorrect response body, exp: %s, got: %s", exp, got)
	}
	if cnt.Load() != 1 {
		t.Fatalf("failed to call committedFn")
	}
	m.notReady = false
	m.committedFn = func(timeout time.Duration) (uint64, error) {
		cnt.Store(2)
		return 0, nil
	}
	resp, err = client.Get(host + "/readyz?sync")
	if err != nil {
		t.Fatalf("failed to make readyz request with sync set")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to get expected StatusOK, got %d", resp.StatusCode)
	}
	if exp, got := "[+]node ok\n[+]leader ok\n[+]store ok\n[+]sync ok", mustReadBody(t, resp); exp != got {
		t.Fatalf("incorrect response body, exp: %s, got: %s", exp, got)
	}
	if cnt.Load() != 2 {
		t.Fatalf("failed to call committedFn")
	}
}

func Test_ForwardingRedirectQuery(t *testing.T) {
	m := &MockStore{
		leaderAddr: "foo:1234",
	}
	m.queryFn = func(qr *command.QueryRequest) ([]*command.QueryRows, error) {
		return nil, store.ErrNotLeader
	}

	c := &mockClusterService{
		apiAddr: "https://bar:5678",
	}
	c.queryFn = func(qr *command.QueryRequest, addr string, timeout time.Duration) ([]*command.QueryRows, error) {
		rows := &command.QueryRows{
			Columns: []string{},
			Types:   []string{},
		}
		return []*command.QueryRows{rows}, nil
	}

	s := New("127.0.0.1:0", m, c, nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	// Check queries.
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	host := fmt.Sprintf("http://%s", s.Addr().String())

	resp, err := client.Get(host + "/db/query?pretty&timings&q=SELECT%20%2A%20FROM%20foo")
	if err != nil {
		t.Fatalf("failed to make query request")
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to get expected StatusOK for nodes, got %d", resp.StatusCode)
	}

	resp, err = client.Get(host + "/db/query?redirect&pretty&timings&q=SELECT%20%2A%20FROM%20foo")
	if err != nil {
		t.Fatalf("failed to make redirected query request: %s", err)
	}
	if resp.StatusCode != http.StatusMovedPermanently {
		t.Fatalf("failed to get expected StatusMovedPermanently for query, got %d", resp.StatusCode)
	}

	// Check leader failure case.
	m.leaderAddr = ""
	resp, err = client.Get(host + "/db/query?pretty&timings&q=SELECT%20%2A%20FROM%20foo")
	if err != nil {
		t.Fatalf("failed to make query forwarded request")
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("failed to get expected StatusServiceUnavailable for node with no leader, got %d", resp.StatusCode)
	}
}

func Test_ForwardingRedirectExecute(t *testing.T) {
	m := &MockStore{
		leaderAddr: "foo:1234",
	}
	m.executeFn = func(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, error) {
		return nil, store.ErrNotLeader
	}

	c := &mockClusterService{
		apiAddr: "https://bar:5678",
	}
	c.executeFn = func(er *command.ExecuteRequest, addr string, timeout time.Duration) ([]*command.ExecuteQueryResponse, error) {
		result := &command.ExecuteQueryResponse{
			Result: &command.ExecuteQueryResponse_E{
				E: &command.ExecuteResult{
					LastInsertId: 1234,
					RowsAffected: 5678,
				},
			},
		}
		return []*command.ExecuteQueryResponse{result}, nil
	}

	s := New("127.0.0.1:0", m, c, nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	// Check executes.
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	host := fmt.Sprintf("http://%s", s.Addr().String())

	resp, err := client.Post(host+"/db/execute", "application/json", strings.NewReader(`["Some SQL"]`))
	if err != nil {
		t.Fatalf("failed to make execute request")
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to get expected StatusOK for execute, got %d", resp.StatusCode)
	}

	resp, err = client.Post(host+"/db/execute?redirect", "application/json", strings.NewReader(`["Some SQL"]`))
	if err != nil {
		t.Fatalf("failed to make redirected execute request: %s", err)
	}
	if resp.StatusCode != http.StatusMovedPermanently {
		t.Fatalf("failed to get expected StatusMovedPermanently for execute, got %d", resp.StatusCode)
	}

	// Check leader failure case.
	m.leaderAddr = ""
	resp, err = client.Post(host+"/db/execute", "application/json", strings.NewReader(`["Some SQL"]`))
	if err != nil {
		t.Fatalf("failed to make execute request")
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("failed to get expected StatusServiceUnavailable for node with no leader, got %d", resp.StatusCode)
	}
}

func Test_ForwardingRedirectExecuteQuery(t *testing.T) {
	m := &MockStore{
		leaderAddr: "foo:1234",
	}
	m.requestFn = func(er *command.ExecuteQueryRequest) ([]*command.ExecuteQueryResponse, error) {
		return nil, store.ErrNotLeader
	}

	c := &mockClusterService{
		apiAddr: "https://bar:5678",
	}
	c.requestFn = func(er *command.ExecuteQueryRequest, addr string, timeout time.Duration) ([]*command.ExecuteQueryResponse, error) {
		resp := &command.ExecuteQueryResponse{
			Result: &command.ExecuteQueryResponse_E{
				E: &command.ExecuteResult{
					LastInsertId: 1234,
					RowsAffected: 5678,
				},
			},
		}
		return []*command.ExecuteQueryResponse{resp}, nil
	}

	s := New("127.0.0.1:0", m, c, nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	// Check ExecuteQuery.
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	host := fmt.Sprintf("http://%s", s.Addr().String())

	resp, err := client.Post(host+"/db/request", "application/json", strings.NewReader(`["Some SQL"]`))
	if err != nil {
		t.Fatalf("failed to make ExecuteQuery request")
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to get expected StatusOK for ExecuteQuery, got %d", resp.StatusCode)
	}

	resp, err = client.Post(host+"/db/request?redirect", "application/json", strings.NewReader(`["Some SQL"]`))
	if err != nil {
		t.Fatalf("failed to make redirected ExecuteQuery request: %s", err)
	}
	if resp.StatusCode != http.StatusMovedPermanently {
		t.Fatalf("failed to get expected StatusMovedPermanently for execute, got %d", resp.StatusCode)
	}

	// Check leader failure case.
	m.leaderAddr = ""
	resp, err = client.Post(host+"/db/request", "application/json", strings.NewReader(`["Some SQL"]`))
	if err != nil {
		t.Fatalf("failed to make ExecuteQuery request")
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("failed to get expected StatusServiceUnavailable for node with no leader, got %d", resp.StatusCode)
	}
}

func Test_timeoutVersionPrettyQueryParam(t *testing.T) {
	defStr := "10s"
	def := mustParseDuration(defStr)
	tests := []struct {
		u        string
		dur      string
		ver      string
		pretty   bool
		parseErr bool
	}{
		{
			u:      "http://localhost:4001/nodes?pretty&timeout=5s&ver=2",
			dur:    "5s",
			pretty: true,
			ver:    "2",
		},
		{
			u:   "http://localhost:4001/nodes?timeout=2m",
			dur: "2m",
		},
		{
			u:   "http://localhost:4001/nodes?x=777&timeout=5s",
			dur: "5s",
		},
		{
			u:   "http://localhost:4001/nodes",
			dur: defStr,
		},
		{
			u:   "http://localhost:4001/nodes?ver=666",
			dur: defStr,
			ver: "666",
		},
		{
			u:      "http://localhost:4001/nodes?pretty&ver=666",
			dur:    defStr,
			pretty: true,
			ver:    "666",
		},
		{
			u:        "http://localhost:4001/nodes?timeout=zdfjkh",
			parseErr: true,
		},
		{
			u:        "http://localhost:4001/db/query?q=",
			dur:      defStr,
			parseErr: true,
		},
	}

	for i, tt := range tests {
		mustURLParse(tt.u) // Make sure it's OK.
		req, err := http.NewRequest("GET", tt.u, nil)
		if err != nil {
			t.Fatalf("failed to create request: %s", err)
		}
		qp, err := NewQueryParams(req)
		if err != nil {
			if !tt.parseErr {
				t.Fatalf(" unexpectedly failed to parse query params on test %d: %s", i, err)
			}
			continue
		}

		if got, exp := qp.Timeout(def), mustParseDuration(tt.dur); got != exp {
			t.Fatalf("got wrong timeout on test %d, expected %s, got %s", i, exp, got)
		}
		if got, exp := qp.Version(), tt.ver; got != exp {
			t.Fatalf("got wrong version on test %d, expected %s, got %s", i, exp, got)
		}
		if got, exp := qp.Pretty(), tt.pretty; got != exp {
			t.Fatalf("got wrong pretty on test %d, expected %t, got %t", i, exp, got)
		}
	}
}

func Test_DBTimeoutQueryParam(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		want    time.Duration
		wantErr bool
	}{
		{
			name: "1s",
			url:  "http://localhost:4001/execute?db_timeout=1s",
			want: 1 * time.Second,
		},
		{
			name: "100ms",
			url:  "http://localhost:4001/execute?db_timeout=100ms",
			want: 100 * time.Millisecond,
		},
		{
			name: "default value",
			url:  "http://localhost:4001/execute",
			want: 0,
		},
		{
			name:    "parse error",
			url:     "http://localhost:4001/execute?db_timeout=xyz",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mustURLParse(tt.url)
			req, err := http.NewRequest("GET", tt.url, nil)
			if err != nil {
				t.Fatalf("failed to create request: %s", err)
			}
			qp, err := NewQueryParams(req)
			if err != nil {
				if !tt.wantErr {
					t.Fatalf("failed to create request: %s", err)
				}
			}

			got := qp.DBTimeout(0)
			if got != tt.want {
				t.Fatalf("want %d, got %d", tt.want, got)
			}
		})
	}
}

type MockStore struct {
	executeFn   func(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, error)
	queryFn     func(qr *command.QueryRequest) ([]*command.QueryRows, error)
	requestFn   func(eqr *command.ExecuteQueryRequest) ([]*command.ExecuteQueryResponse, error)
	backupFn    func(br *command.BackupRequest, dst io.Writer) error
	loadFn      func(lr *command.LoadRequest) error
	snapshotFn  func(n uint64) error
	readFromFn  func(r io.Reader) (int64, error)
	committedFn func(timeout time.Duration) (uint64, error)
	leaderAddr  string
	notReady    bool // Default value is true, easier to test.
}

func (m *MockStore) Execute(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, error) {
	if m.executeFn != nil {
		return m.executeFn(er)
	}
	return nil, nil
}

func (m *MockStore) Query(qr *command.QueryRequest) ([]*command.QueryRows, error) {
	if m.queryFn != nil {
		return m.queryFn(qr)
	}
	return nil, nil
}

func (m *MockStore) Request(eqr *command.ExecuteQueryRequest) ([]*command.ExecuteQueryResponse, error) {
	if m.requestFn != nil {
		return m.requestFn(eqr)
	}
	return nil, nil
}

func (m *MockStore) Join(jr *command.JoinRequest) error {
	return nil
}

func (m *MockStore) Notify(nr *command.NotifyRequest) error {
	return nil
}

func (m *MockStore) Remove(rn *command.RemoveNodeRequest) error {
	return nil
}

func (m *MockStore) LeaderAddr() (string, error) {
	return m.leaderAddr, nil
}

func (m *MockStore) Ready() bool {
	return !m.notReady
}

func (m *MockStore) Committed(timeout time.Duration) (uint64, error) {
	if m.committedFn != nil {
		return m.committedFn(timeout)
	}
	return 0, nil
}

func (m *MockStore) Stats() (map[string]interface{}, error) {
	return nil, nil
}

func (m *MockStore) Nodes() ([]*store.Server, error) {
	return nil, nil
}

func (m *MockStore) Backup(br *command.BackupRequest, w io.Writer) error {
	if m.backupFn == nil {
		return nil
	}
	return m.backupFn(br, w)
}

func (m *MockStore) Load(lr *command.LoadRequest) error {
	if m.loadFn != nil {
		return m.loadFn(lr)
	}
	return nil
}

func (m *MockStore) Snapshot(n uint64) error {
	if m.snapshotFn != nil {
		return m.snapshotFn(n)
	}
	return nil
}

func (m *MockStore) ReadFrom(r io.Reader) (int64, error) {
	if m.readFromFn != nil {
		return m.readFromFn(r)
	}
	return 0, nil
}

type mockClusterService struct {
	apiAddr      string
	executeFn    func(er *command.ExecuteRequest, addr string, t time.Duration) ([]*command.ExecuteQueryResponse, error)
	queryFn      func(qr *command.QueryRequest, addr string, t time.Duration) ([]*command.QueryRows, error)
	requestFn    func(eqr *command.ExecuteQueryRequest, nodeAddr string, timeout time.Duration) ([]*command.ExecuteQueryResponse, error)
	backupFn     func(br *command.BackupRequest, addr string, t time.Duration, w io.Writer) error
	loadFn       func(lr *command.LoadRequest, addr string, t time.Duration) error
	removeNodeFn func(rn *command.RemoveNodeRequest, nodeAddr string, t time.Duration) error
}

func (m *mockClusterService) GetNodeAPIAddr(a string, r int, t time.Duration) (string, error) {
	return m.apiAddr, nil
}

func (m *mockClusterService) Execute(er *command.ExecuteRequest, addr string, creds *cluster.Credentials, t time.Duration, r int) ([]*command.ExecuteQueryResponse, error) {
	if m.executeFn != nil {
		return m.executeFn(er, addr, t)
	}
	return nil, nil
}

func (m *mockClusterService) Query(qr *command.QueryRequest, addr string, creds *cluster.Credentials, t time.Duration) ([]*command.QueryRows, error) {
	if m.queryFn != nil {
		return m.queryFn(qr, addr, t)
	}
	return nil, nil
}

func (m *mockClusterService) Request(eqr *command.ExecuteQueryRequest, nodeAddr string, creds *cluster.Credentials, timeout time.Duration, r int) ([]*command.ExecuteQueryResponse, error) {
	if m.requestFn != nil {
		return m.requestFn(eqr, nodeAddr, timeout)
	}
	return nil, nil
}

func (m *mockClusterService) Backup(br *command.BackupRequest, addr string, creds *cluster.Credentials, t time.Duration, w io.Writer) error {
	if m.backupFn != nil {
		return m.backupFn(br, addr, t, w)
	}
	return nil
}

func (m *mockClusterService) Load(lr *command.LoadRequest, nodeAddr string, creds *cluster.Credentials, timeout time.Duration, r int) error {
	if m.loadFn != nil {
		return m.loadFn(lr, nodeAddr, timeout)
	}
	return nil
}

func (m *mockClusterService) RemoveNode(rn *command.RemoveNodeRequest, addr string, creds *cluster.Credentials, t time.Duration) error {
	if m.removeNodeFn != nil {
		return m.removeNodeFn(rn, addr, t)
	}
	return nil
}

type mockCredentialStore struct {
	HasPermOK bool
	aaFunc    func(username, password, perm string) bool
}

func (m *mockCredentialStore) AA(username, password, perm string) bool {
	if m == nil {
		return true
	}

	if m.aaFunc != nil {
		return m.aaFunc(username, password, perm)
	}
	return m.HasPermOK
}

func (m *mockClusterService) Stats() (map[string]interface{}, error) {
	return nil, nil
}

type mockStatusReporter struct {
}

func (m *mockStatusReporter) Stats() (map[string]interface{}, error) {
	return nil, nil
}

func mustNewHTTPRequest(url string) *http.Request {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic("failed to create HTTP request for testing")
	}
	return req
}

func mustURLParse(s string) *url.URL {
	u, err := url.Parse(s)
	if err != nil {
		panic("failed to URL parse string")
	}
	return u
}

func mustParseDuration(d string) time.Duration {
	if dur, err := time.ParseDuration(d); err != nil {
		panic(fmt.Sprintf("failed to parse duration %s: %s", d, err))
	} else {
		return dur
	}
}

func mustGetQueryParams(req *http.Request) QueryParams {
	qp, err := NewQueryParams(req)
	if err != nil {
		panic("failed to get query params")
	}
	return qp
}

func mustReadBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %s", err)
	}
	return string(b)
}
