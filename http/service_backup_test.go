package http

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	command "github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/proxy"
	"github.com/rqlite/rqlite/v10/store"
)

// Test_BackupOK_Local tests that a GET to /db/backup on a node that can
// serve the backup itself streams the data returned by the local Store
// directly to the client with a 200 OK, without involving the cluster service.
func Test_BackupOK_Local(t *testing.T) {
	m := &MockStore{}
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, proxy.New(m, c), nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	testData := "some random SQLite data"
	m.backupFn = func(br *command.BackupRequest, w io.Writer) error {
		_, err := w.Write([]byte(testData))
		return err
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
	defer resp.Body.Close()

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, resp.Body); err != nil {
		t.Fatalf("failed to read backup response body: %s", err)
	}
	if got, exp := buf.String(), testData; got != exp {
		t.Fatalf("unexpected backup data, got %q, exp %q", got, exp)
	}
}

// Test_BackupOK_Remote tests that when the local Store returns ErrNotLeader,
// the backup is fetched from the leader via the cluster service and the
// leader's data is streamed back to the client with a 200 OK.
func Test_BackupOK_Remote(t *testing.T) {
	m := &MockStore{
		leaderAddr: "foo:1234",
	}
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, proxy.New(m, c), nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	testData := "some random SQLite data"
	m.backupFn = func(br *command.BackupRequest, dst io.Writer) error {
		return store.ErrNotLeader
	}
	c.backupFn = func(br *command.BackupRequest, addr string, t time.Duration, w io.Writer) error {
		_, err := w.Write([]byte(testData))
		return err
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
	defer resp.Body.Close()

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, resp.Body); err != nil {
		t.Fatalf("failed to read backup response body: %s", err)
	}
	if got, exp := buf.String(), testData; got != exp {
		t.Fatalf("unexpected backup data, got %q, exp %q", got, exp)
	}
}

// Test_BackupStreamError_Local tests that when the local Store fails part way
// through writing a backup, the error is reported to the client via the
// StreamErrorHeader HTTP trailer. The 200 OK status has already been sent by
// the time the error occurs, so the trailer is the only way to signal it.
func Test_BackupStreamError_Local(t *testing.T) {
	m := &MockStore{}
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, proxy.New(m, c), nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	partialData := "partial SQLite data"
	m.backupFn = func(br *command.BackupRequest, w io.Writer) error {
		if _, err := w.Write([]byte(partialData)); err != nil {
			return err
		}
		return errors.New("local write failed")
	}

	client := &http.Client{}
	host := fmt.Sprintf("http://%s", s.Addr().String())
	resp, err := client.Get(host + "/db/backup")
	if err != nil {
		t.Fatalf("failed to make backup request: %s", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to get expected StatusOK for backup, got %d", resp.StatusCode)
	}
	if _, ok := resp.Trailer[http.CanonicalHeaderKey(StreamErrorHeader)]; !ok {
		t.Fatalf("expected %s to be declared as a trailer", StreamErrorHeader)
	}

	// Trailers are only populated once the body has been fully read.
	if got, exp := mustReadBody(t, resp), partialData; got != exp {
		t.Fatalf("unexpected backup data, got %q, exp %q", got, exp)
	}
	if got, exp := resp.Trailer.Get(StreamErrorHeader), "local write failed"; got != exp {
		t.Fatalf("unexpected %s trailer, got %q, exp %q", StreamErrorHeader, got, exp)
	}
}

// Test_BackupStreamError_Remote tests that when the local Store is not the
// leader and the cluster service fails part way through streaming the backup
// from the leader, the error is reported to the client via the
// StreamErrorHeader HTTP trailer.
func Test_BackupStreamError_Remote(t *testing.T) {
	m := &MockStore{
		leaderAddr: "foo:1234",
	}
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, proxy.New(m, c), nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	partialData := "partial SQLite data"
	m.backupFn = func(br *command.BackupRequest, dst io.Writer) error {
		return store.ErrNotLeader
	}
	c.backupFn = func(br *command.BackupRequest, addr string, t time.Duration, w io.Writer) error {
		if _, err := w.Write([]byte(partialData)); err != nil {
			return err
		}
		return errors.New("remote write failed")
	}

	client := &http.Client{}
	host := fmt.Sprintf("http://%s", s.Addr().String())
	resp, err := client.Get(host + "/db/backup")
	if err != nil {
		t.Fatalf("failed to make backup request: %s", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to get expected StatusOK for backup, got %d", resp.StatusCode)
	}
	if _, ok := resp.Trailer[http.CanonicalHeaderKey(StreamErrorHeader)]; !ok {
		t.Fatalf("expected %s to be declared as a trailer", StreamErrorHeader)
	}

	// Trailers are only populated once the body has been fully read.
	if got, exp := mustReadBody(t, resp), partialData; got != exp {
		t.Fatalf("unexpected backup data, got %q, exp %q", got, exp)
	}
	if got, exp := resp.Trailer.Get(StreamErrorHeader), "remote write failed"; got != exp {
		t.Fatalf("unexpected %s trailer, got %q, exp %q", StreamErrorHeader, got, exp)
	}
}

// Test_BackupVacuumSet tests that the vacuum query parameter results in
// Vacuum being set on the BackupRequest passed to the Store, both with the
// default format and when the binary format is explicitly requested.
func Test_BackupVacuumSet(t *testing.T) {
	m := &MockStore{}
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, proxy.New(m, c), nil)
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

// Test_BackupFlagsNoLeaderRedirect tests that when the local Store returns
// ErrNotLeader and the client sets the redirect query parameter, the service
// responds with a 301 Moved Permanently pointing at the leader rather than
// fetching the backup from the leader itself.
func Test_BackupFlagsNoLeaderRedirect(t *testing.T) {
	m := &MockStore{
		leaderAddr: "foo:1234",
	}
	c := &mockClusterService{
		apiAddr: "http://1.2.3.4:999",
	}

	s := New("127.0.0.1:0", m, c, proxy.New(m, c), nil)

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

// Test_BackupFlagsNoLeaderRemoteFetch tests that when the local Store returns
// ErrNotLeader and redirect is not requested, the service transparently
// fetches the backup from the leader via the cluster service and returns it
// with a 200 OK, rather than issuing a redirect.
func Test_BackupFlagsNoLeaderRemoteFetch(t *testing.T) {
	m := &MockStore{
		leaderAddr: "foo:1234",
	}
	c := &mockClusterService{
		apiAddr: "http://1.2.3.4:999",
	}

	s := New("127.0.0.1:0", m, c, proxy.New(m, c), nil)

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

// Test_BackupFlagsNoLeaderOK tests that the noleader query parameter results
// in Leader being false on the BackupRequest, allowing a non-leader node to
// serve the backup from its local copy of the database. The mock Store
// returns ErrNotLeader if Leader is set, so a 200 OK confirms the flag was
// honored.
func Test_BackupFlagsNoLeaderOK(t *testing.T) {
	m := &MockStore{}
	c := &mockClusterService{
		apiAddr: "http://1.2.3.4:999",
	}

	s := New("127.0.0.1:0", m, c, proxy.New(m, c), nil)

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

// Test_BackupFlagsInvalid tests that an invalid combination of backup
// options, in this case vacuum with the SQL format, causes the Store's
// ErrInvalidVacuum to be surfaced to the client as a 400 Bad Request.
func Test_BackupFlagsInvalid(t *testing.T) {
	m := &MockStore{}
	c := &mockClusterService{
		apiAddr: "http://1.2.3.4:999",
	}

	s := New("127.0.0.1:0", m, c, proxy.New(m, c), nil)

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

// Test_BackupDeleteOK tests that fmt=delete is translated into the DELETE
// backup format on the BackupRequest passed to the Store.
func Test_BackupDeleteOK(t *testing.T) {
	m := &MockStore{}
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, proxy.New(m, c), nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	// Track that the backup function is called with DELETE format
	var capturedRequest *command.BackupRequest
	m.backupFn = func(br *command.BackupRequest, dst io.Writer) error {
		capturedRequest = br
		return nil
	}

	client := &http.Client{}
	host := fmt.Sprintf("http://%s", s.Addr().String())
	resp, err := client.Get(host + "/db/backup?fmt=delete")
	if err != nil {
		t.Fatalf("failed to make backup request")
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to get expected StatusOK for backup, got %d", resp.StatusCode)
	}

	// Verify that the backup request has the DELETE format
	if capturedRequest == nil {
		t.Fatalf("backup function was not called")
	}
	if capturedRequest.Format != command.BackupRequest_BACKUP_REQUEST_FORMAT_DELETE {
		t.Fatalf("expected DELETE format, got %v", capturedRequest.Format)
	}
}

// Test_BackupTablesOK tests that the tables query parameter is parsed as a
// comma-separated list and passed, in order, as the Tables field of the
// BackupRequest, alongside the requested SQL format.
func Test_BackupTablesOK(t *testing.T) {
	m := &MockStore{}
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, proxy.New(m, c), nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	// Track that the backup function is called with specified tables
	var capturedRequest *command.BackupRequest
	m.backupFn = func(br *command.BackupRequest, dst io.Writer) error {
		capturedRequest = br
		return nil
	}

	client := &http.Client{}
	host := fmt.Sprintf("http://%s", s.Addr().String())
	resp, err := client.Get(host + "/db/backup?fmt=sql&tables=users,products")
	if err != nil {
		t.Fatalf("failed to make backup request")
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to get expected StatusOK for backup, got %d", resp.StatusCode)
	}

	// Verify that the backup request has the specified tables
	if capturedRequest == nil {
		t.Fatalf("backup function was not called")
	}
	if capturedRequest.Format != command.BackupRequest_BACKUP_REQUEST_FORMAT_SQL {
		t.Fatalf("expected SQL format, got %v", capturedRequest.Format)
	}
	expectedTables := []string{"users", "products"}
	if len(capturedRequest.Tables) != len(expectedTables) {
		t.Fatalf("expected %d tables, got %d", len(expectedTables), len(capturedRequest.Tables))
	}
	for i, table := range expectedTables {
		if capturedRequest.Tables[i] != table {
			t.Fatalf("expected table %s at index %d, got %s", table, i, capturedRequest.Tables[i])
		}
	}
}
