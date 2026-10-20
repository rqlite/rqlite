package db

import (
	"os"
	"path/filepath"
	"testing"
)

func Test_OpenWithConfig_ConfigReuse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	cfg := NewConfig(ConnConfig{ForeignKeys: true}, true)
	db, err := OpenWithConfig(path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mustExecute(db, "CREATE TABLE foo (id INTEGER PRIMARY KEY)")
	mustExecute(db, "INSERT INTO foo VALUES (1)")

	// Reuse the caller's configuration for a database with different settings.
	cfg.WAL = false
	cfg.Writer.ForeignKeys = false
	cfg.Readers.ForeignKeys = false
	other, err := OpenWithConfig(filepath.Join(t.TempDir(), "other.db"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()

	// Serialization must still include the first database's uncheckpointed WAL.
	data, err := db.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	serializedPath := filepath.Join(t.TempDir(), "serialized.db")
	if err := os.WriteFile(serializedPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(serializedPath, false, false)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	rows := mustQuery(restored, "SELECT id FROM foo")
	if exp, got := `[{"columns":["id"],"types":["integer"],"values":[[1]]}]`, asJSON(rows); exp != got {
		t.Fatalf("serialized data: expected %s, got %s", exp, got)
	}
	if !db.WALEnabled() || db.WALPath() != WALPath(path) || !db.FKEnabled() {
		t.Fatal("reusing the config changed the first database's settings")
	}
}

func Test_OpenSwappable_ConfigReuse(t *testing.T) {
	cfg := NewConfig(ConnConfig{ForeignKeys: true}, true)
	cfg.MaxReaders = 2
	db, err := OpenSwappable(filepath.Join(t.TempDir(), "test.db"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	path := filepath.Join(t.TempDir(), "replacement.db")
	replacement, err := Open(path, false, false)
	if err != nil {
		t.Fatal(err)
	}
	mustExecute(replacement, "CREATE TABLE foo (id INTEGER PRIMARY KEY)")
	if err := replacement.Close(); err != nil {
		t.Fatal(err)
	}

	// Changing the caller's config must not change the policy used on a swap.
	cfg.WAL = false
	cfg.Writer.ForeignKeys = false
	cfg.Readers.ForeignKeys = false
	cfg.MaxReaders = 1
	if err := db.Swap(path); err != nil {
		t.Fatal(err)
	}
	if !db.WALEnabled() || !db.FKEnabled() {
		t.Fatal("swap used the caller's modified configuration")
	}
	if got := db.db.readers.poolStats().MaxOpenConnections; got != 2 {
		t.Fatalf("reader limit after swap: expected 2, got %d", got)
	}
	for role, pool := range map[string]*pool{"writer": &db.db.writer.pool, "readers": &db.db.readers.pool} {
		pragmas, err := pool.pragmas([]string{"journal_mode", "foreign_keys"})
		if err != nil {
			t.Fatal(err)
		}
		if pragmas["journal_mode"] != "wal" || pragmas["foreign_keys"] != "1" {
			t.Fatalf("%s settings after swap: %v", role, pragmas)
		}
	}
}
