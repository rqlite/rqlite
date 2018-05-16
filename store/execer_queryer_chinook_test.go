package store

import (
	"os"
	"testing"
	"time"

	"github.com/rqlite/rqlite/testdata/chinook"
)

func TestStoreOnDiskChinook(t *testing.T) {
	t.Parallel()
	s := mustNewStore(false)
	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	defer os.RemoveAll(s.Path())
	s.WaitForLeader(10 * time.Second)
	testLoadChinook(t, s)
}

func TestStoreInMemChinook(t *testing.T) {
	t.Parallel()
	s := mustNewStore(true)
	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	defer os.RemoveAll(s.Path())
	s.WaitForLeader(10 * time.Second)
	testLoadChinook(t, s)
}

func TestStoreConnectionOnDiskChinook(t *testing.T) {
	t.Parallel()
	s := mustNewStore(false)
	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	defer os.RemoveAll(s.Path())
	s.WaitForLeader(10 * time.Second)
	c := mustNewConnection(s)
	defer c.Close()
	testLoadChinook(t, c)
}

func TestStoreConnectionInMemChinook(t *testing.T) {
	t.Parallel()
	s := mustNewStore(true)
	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	defer os.RemoveAll(s.Path())
	s.WaitForLeader(10 * time.Second)
	c := mustNewConnection(s)
	defer c.Close()
	testLoadChinook(t, c)
}

func testLoadChinook(t *testing.T, eq ExecerQueryer) {
	_, err := eq.Execute(&ExecuteRequest{[]string{chinook.DB}, false, false})
	if err != nil {
		t.Fatalf("%s: failed to load chinook dump: %s", err.Error(), t.Name())
	}

	// Check that data were loaded correctly.

	r, err := eq.Query(&QueryRequest{[]string{`SELECT count(*) FROM track`}, false, true, Strong})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["count(*)"]`, asJSON(r.Rows[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[3503]]`, asJSON(r.Rows[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = eq.Query(&QueryRequest{[]string{`SELECT count(*) FROM album`}, false, true, Strong})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["count(*)"]`, asJSON(r.Rows[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[347]]`, asJSON(r.Rows[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = eq.Query(&QueryRequest{[]string{`SELECT count(*) FROM artist`}, false, true, Strong})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["count(*)"]`, asJSON(r.Rows[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[275]]`, asJSON(r.Rows[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}
