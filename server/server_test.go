package server

import (
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestStart(t *testing.T) {
	if testing.Short() {
		t.Skip("server long test skip")
	}

	dir, err := ioutil.TempDir("", "rqlite-test-")
	t.Logf("tmp dir: %v", dir)
	if err != nil {
		noError(t, err)
	}
	defer os.RemoveAll(dir)

	s1 := New(&Config{},
		Defaults(dir+"/node1"),
		HTTPAddr("localhost:5001"),
	)
	s2 := New(&Config{},
		Defaults(dir+"/node2"),
		HTTPAddr("localhost:5003"),
		RaftAddr("localhost:5004"),
		JoinAddr("localhost:5001"),
	)
	s3 := New(&Config{},
		Defaults(dir+"+node3"),
		HTTPAddr("localhost:5005"),
		RaftAddr("localhost:5006"),
		JoinAddr("localhost:5001"),
	)

	err = s1.Start()
	noError(t, err)
	defer s1.Stop()

	err = s2.Start()
	noError(t, err)
	defer s2.Stop()

	err = s3.Start()
	noError(t, err)
	defer s3.Stop()

	tk := time.NewTicker(2 * time.Second)
	defer tk.Stop()
	tm := time.NewTimer(10 * time.Second)
	defer tm.Stop()
FOR:
	for {
		select {
		case <-tm.C:
			t.Fatalf("timeout waiting for servers to be ready: s1(%v) s2(%v) s3(%v)",
				s1.IsReady(),
				s2.IsReady(),
				s3.IsReady())
		case <-tk.C:
			ready := true
			ready = ready && s1.IsReady()
			t.Logf("server1 ready: %v\n", ready)
			ready = ready && s2.IsReady()
			t.Logf("server2 ready: %v\n", ready)
			ready = ready && s3.IsReady()
			t.Logf("server3 ready: %v\n", ready)
			if ready {
				break FOR
			}
		}
	}

}

func noError(t *testing.T, err error) {
	if err != nil {
		t.Fatalf("Received unexpected error:\n%+v", err)
	}
}
