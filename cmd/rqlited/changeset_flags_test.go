package main

import (
	"strings"
	"testing"
)

func Test_Config_Changeset(t *testing.T) {
	base := func() *Config {
		return &Config{DataPath: t.TempDir(), HTTPAddr: "localhost:4001", RaftAddr: "localhost:4002", RaftAdv: "localhost:4002", HTTPAdv: "localhost:4001"}
	}
	c := base()
	c.ChangesetReplication = true
	c.CDCConfig = "http://example.invalid"
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "-changeset-replication") {
		t.Fatalf("changeset replication with CDC must be refused by name, got %v", err)
	}

	c = base()
	c.ChangesetMaxSize = -1
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "-changeset-max-size") {
		t.Fatalf("a negative maximum must be refused by name, got %v", err)
	}

	c = base()
	c.ChangesetMaxSize = 1 << 31
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "-changeset-max-size") {
		t.Fatalf("a maximum beyond 32 bits must be refused by name, got %v", err)
	}

	c = base()
	c.ChangesetReplication = true
	c.ChangesetMaxSize = 1 << 20
	if err := c.Validate(); err != nil {
		t.Fatalf("a plain changeset configuration was refused: %v", err)
	}
}
