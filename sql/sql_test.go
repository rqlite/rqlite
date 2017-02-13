package sql

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	"github.com/rqlite/rqlite/db"
	"github.com/rqlite/rqlite/store"
)

type testWrapper struct {
	which  int
	stores []*store.Store
}

func (tw *testWrapper) Store() (*store.Store, error) {
	return tw.stores[tw.which], nil
}

func (tw *testWrapper) ExecOn(addr, query string) (*db.Result, error) {
	for _, s := range tw.stores {
		if s.Addr().String() == addr {
			results, err := s.Execute([]string{query}, false, false)
			if err != nil {
				return nil, err
			}

			return results[0], nil
		}
	}

	return nil, fmt.Errorf("store %s not found", addr)
}

func (tw *testWrapper) QueryOn(addr, query string) (*db.Rows, error) {
	for _, s := range tw.stores {
		if s.Addr().String() == addr {
			rows, err := s.Query([]string{query}, false, false, store.Weak)
			if err != nil {
				return nil, err
			}

			return rows[0], err
		}
	}

	return nil, fmt.Errorf("store %s not found", addr)
}

func mustNewStore(inmem bool) *store.Store {
	path := mustTempDir()
	defer os.RemoveAll(path)

	dsn := ""
	if inmem {
		dsn = "mode=memory&cache=shared"
	}

	cfg := store.NewDBConfig(dsn, inmem)
	s := store.New(&store.StoreConfig{
		DBConf: cfg,
		Dir:    path,
		Tn:     mustMockTransport("localhost:0"),
	})
	if s == nil {
		panic("failed to create new store")
	}
	return s
}

type mockTransport struct {
	ln net.Listener
}

func mustMockTransport(addr string) store.Transport {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		panic("failed to create new transport")
	}
	return &mockTransport{ln}
}

func (m *mockTransport) Dial(addr string, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, timeout)
}

func (m *mockTransport) Accept() (net.Conn, error) { return m.ln.Accept() }

func (m *mockTransport) Close() error { return m.ln.Close() }

func (m *mockTransport) Addr() net.Addr { return m.ln.Addr() }

func mustTempDir() string {
	var err error
	path, err := ioutil.TempDir("", "rqlilte-test-")
	if err != nil {
		panic("failed to create temp dir")
	}
	return path
}

func Test_SQLWrapper(t *testing.T) {
	/*
		n := 2
		stores := []*store.Store{}

		for i := 0; i < n; i++ {
			s := mustNewStore(false)
			defer os.RemoveAll(s.Path())
			if err := s.Open(i == 0); err != nil {
				t.Fatalf("failed to open store: %s", err)
			}
			defer s.Close(true)

			stores = append(stores, s)
		}

		stores[0].WaitForLeader(10 * time.Second)

		for i := 1; i < len(stores); i++ {
			if err := stores[0].Join(stores[i].Addr().String()); err != nil {
				t.Fatalf("failed to join: %s", err)
			}
		}
	*/

	s0 := mustNewStore(false)
	defer os.RemoveAll(s0.Path())
	if err := s0.Open(true); err != nil {
		t.Fatalf("failed to open store: %s", err)
	}
	defer s0.Close(true)
	s0.WaitForLeader(10 * time.Second)

	s1 := mustNewStore(false)
	defer os.RemoveAll(s1.Path())
	if err := s1.Open(true); err != nil {
		t.Fatalf("failed to open store: %s", err)
	}
	defer s1.Close(true)

	if err := s0.Join(s1.Addr().String()); err != nil {
		t.Fatalf("failed to join node at %s: %s", s0.Addr().String(), err.Error())
	}

	s2 := mustNewStore(false)
	defer os.RemoveAll(s2.Path())
	if err := s2.Open(true); err != nil {
		t.Fatalf("failed to open store: %s", err)
	}
	defer s2.Close(true)

	if err := s0.Join(s2.Addr().String()); err != nil {
		t.Fatalf("failed to join node at %s: %s", s0.Addr().String(), err.Error())
	}

	stores := []*store.Store{s0, s1, s2}

	RegisterStore("tester0", &testWrapper{stores: stores, which: 0})
	RegisterStore("tester1", &testWrapper{stores: stores, which: 1})
	RegisterStore("tester2", &testWrapper{stores: stores, which: 2})

	db0, err := sql.Open("rqlite", "tester0")
	if err != nil {
		t.Fatalf("couldn't open sql driver: %s", err)
	}
	defer db0.Close()

	db1, err := sql.Open("rqlite", "tester1")
	if err != nil {
		t.Fatalf("couldn't open sql driver: %s", err)
	}
	defer db1.Close()

	db2, err := sql.Open("rqlite", "tester2")
	if err != nil {
		t.Fatalf("couldn't open sql driver: %s", err)
	}
	defer db2.Close()

	_, err = db0.Exec(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("couldn't create table: %s", err)
	}

	_, err = db1.Exec(`INSERT INTO foo(id, name) VALUES(1, "fiona")`)
	if err != nil {
		t.Fatalf("couldn't insert fiona into table: %s", err)
	}

	r, err := db2.Query(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("couldn't select from table: %s", err)
	}
	defer r.Close()

	for r.Next() {
		id := 0
		name := ""

		r.Scan(&id, &name)

		if id != 1 && name != "fiona" {
			t.Fatalf("got bad name (%s) or id (%d)", name, id)
		}

		return
	}

	t.Fatalf("no results return for select")
}
