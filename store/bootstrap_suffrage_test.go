package store

import (
	"testing"

	"github.com/hashicorp/raft"
)

func Test_BootstrapPreservesNonVoter(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close(true)
	if err := s.Bootstrap(
		NewServer(s.ID(), s.Addr(), true),
		NewServer("replica", "127.0.0.1:1", false),
	); err != nil {
		t.Fatal(err)
	}
	future := s.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		t.Fatal(err)
	}
	for _, server := range future.Configuration().Servers {
		if server.ID == "replica" {
			if server.Suffrage != raft.Nonvoter {
				t.Fatalf("replica suffrage = %v, want Nonvoter", server.Suffrage)
			}
			return
		}
	}
	t.Fatal("replica missing from bootstrap configuration")
}
