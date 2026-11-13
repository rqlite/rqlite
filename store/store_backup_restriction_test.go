package store

import (
	"context"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/db"
	"github.com/rqlite/rqlite/v10/internal/fsutil"
	"github.com/rqlite/rqlite/v10/internal/rarchive"
)

// bkRestrictFixture is the test environment for the table-restricted backup
// tests: a single-node store loaded with three independent tables, each with
// its own index and trigger, plus a view over one of them. Three tables mean
// every test can name a strict subset, so a backup that ignored the subset
// would be caught. Everything the tests need hangs off this type, so the
// package gains no loose helper names.
type bkRestrictFixture struct {
	t *testing.T
	s *Store
}

const bkRestrictSchema = `PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE foo (id integer primary key autoincrement, name text);
INSERT INTO "foo" VALUES(1,'fiona');
INSERT INTO "foo" VALUES(2,'declan');
CREATE TABLE bar (id integer primary key autoincrement, age integer);
INSERT INTO "bar" VALUES(1,25);
CREATE TABLE qux (id integer primary key autoincrement, note text);
INSERT INTO "qux" VALUES(1,'kept out');
CREATE INDEX idx_foo_name ON foo(name);
CREATE INDEX idx_bar_age ON bar(age);
CREATE INDEX idx_qux_note ON qux(note);
CREATE TRIGGER trg_foo AFTER INSERT ON foo BEGIN SELECT 1; END;
CREATE TRIGGER trg_bar AFTER INSERT ON bar BEGIN SELECT 1; END;
CREATE TRIGGER trg_qux AFTER INSERT ON qux BEGIN SELECT 1; END;
CREATE VIEW vw_bar AS SELECT age FROM bar;
COMMIT;
`

// newBkRestrictFixture returns an open, bootstrapped single-node store loaded
// with the schema above, and a function that shuts it down.
func newBkRestrictFixture(t *testing.T) (*bkRestrictFixture, func()) {
	t.Helper()
	s, ln := mustNewStore(t)

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}
	if _, _, err := s.Execute(context.Background(), executeRequestFromString(bkRestrictSchema, false, false)); err != nil {
		t.Fatalf("failed to load schema: %s", err.Error())
	}

	return &bkRestrictFixture{t: t, s: s}, func() {
		s.Close(true)
		ln.Close()
	}
}

// backupToFile runs the backup and returns the path of the resulting SQLite
// database file, decompressing it first if the request asked for compression.
func (f *bkRestrictFixture) backupToFile(br *proto.BackupRequest) string {
	f.t.Helper()

	out, err := os.CreateTemp("", "rqlite-baktest-")
	if err != nil {
		f.t.Fatalf("failed to create temp file: %s", err)
	}
	defer out.Close()
	f.t.Cleanup(func() { fsutil.Remove(out.Name()) })

	if _, err := f.s.Backup(context.Background(), br, out); err != nil {
		f.t.Fatalf("backup failed: %s", err)
	}

	if !br.Compress {
		return out.Name()
	}

	path, err := rarchive.Gunzip(out.Name())
	if err != nil {
		f.t.Fatalf("failed to decompress backup: %s", err)
	}
	f.t.Cleanup(func() { fsutil.Remove(path) })
	return path
}

// objects returns the names of every schema object of the given type held by
// the SQLite database at path, ignoring SQLite's own internal objects.
func (f *bkRestrictFixture) objects(path, typ string) []string {
	f.t.Helper()

	d, err := db.Open(path, false, false)
	if err != nil {
		f.t.Fatalf("failed to open backup database: %s", err)
	}
	defer d.Close()

	rows, err := d.QueryStringStmt(
		"SELECT name FROM sqlite_master WHERE type='" + typ + "' AND name NOT LIKE 'sqlite_%' ORDER BY name")
	if err != nil {
		f.t.Fatalf("failed to query backup schema: %s", err)
	}

	var names []string
	for _, r := range rows {
		for _, v := range r.Values {
			names = append(names, v.Parameters[0].GetS())
		}
	}
	sort.Strings(names)
	return names
}

// query runs a query against the SQLite database at path and returns the
// results as JSON.
func (f *bkRestrictFixture) query(path, q string) string {
	f.t.Helper()

	d, err := db.Open(path, false, false)
	if err != nil {
		f.t.Fatalf("failed to open backup database: %s", err)
	}
	defer d.Close()

	rows, err := d.QueryStringStmt(q)
	if err != nil {
		f.t.Fatalf("failed to query backup database: %s", err)
	}
	return asJSON(rows)
}

// sameStrings reports whether a and b hold the same names in the same order.
func (f *bkRestrictFixture) sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// binaryReq and deleteReq build the two backup requests that return a database
// file, restricted to the given tables.
func (f *bkRestrictFixture) binaryReq(vacuum, compress bool, tables []string) *proto.BackupRequest {
	return &proto.BackupRequest{
		Format:   proto.BackupRequest_BACKUP_REQUEST_FORMAT_BINARY,
		Leader:   true,
		Vacuum:   vacuum,
		Compress: compress,
		Tables:   tables,
	}
}

func (f *bkRestrictFixture) deleteReq(vacuum, compress bool, tables []string) *proto.BackupRequest {
	return &proto.BackupRequest{
		Format:   proto.BackupRequest_BACKUP_REQUEST_FORMAT_DELETE,
		Leader:   true,
		Vacuum:   vacuum,
		Compress: compress,
		Tables:   tables,
	}
}

// Test_RestrictedBackupBinarySingleTable checks that a binary backup restricted to
// one table carries that table and its rows, and nothing else.
func Test_RestrictedBackupBinarySingleTable(t *testing.T) {
	f, cleanup := newBkRestrictFixture(t)
	defer cleanup()

	path := f.backupToFile(f.binaryReq(false, false, []string{"foo"}))

	if exp, got := []string{"foo"}, f.objects(path, "table"); !f.sameStrings(exp, got) {
		t.Fatalf("wrong tables in backup, exp %v, got %v", exp, got)
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"declan"]]}]`,
		f.query(path, "SELECT * FROM foo ORDER BY id"); exp != got {
		t.Fatalf("wrong rows in backup\nexp: %s\ngot: %s", exp, got)
	}
}

// Test_RestrictedBackupBinaryMultipleTables checks that naming several tables
// carries exactly those tables.
func Test_RestrictedBackupBinaryMultipleTables(t *testing.T) {
	f, cleanup := newBkRestrictFixture(t)
	defer cleanup()

	path := f.backupToFile(f.binaryReq(false, false, []string{"foo", "bar"}))

	if exp, got := []string{"bar", "foo"}, f.objects(path, "table"); !f.sameStrings(exp, got) {
		t.Fatalf("wrong tables in backup, exp %v, got %v", exp, got)
	}
	if exp, got := `[{"columns":["age"],"types":["integer"],"values":[[25]]}]`,
		f.query(path, "SELECT age FROM bar"); exp != got {
		t.Fatalf("wrong rows in backup\nexp: %s\ngot: %s", exp, got)
	}
}

// Test_RestrictedBackupBinaryVacuum checks the restriction holds for a vacuumed
// backup.
func Test_RestrictedBackupBinaryVacuum(t *testing.T) {
	f, cleanup := newBkRestrictFixture(t)
	defer cleanup()

	path := f.backupToFile(f.binaryReq(true, false, []string{"foo"}))

	if exp, got := []string{"foo"}, f.objects(path, "table"); !f.sameStrings(exp, got) {
		t.Fatalf("wrong tables in backup, exp %v, got %v", exp, got)
	}
	if exp, got := `[{"columns":["id"],"types":["integer"],"values":[[1],[2]]}]`,
		f.query(path, "SELECT id FROM foo ORDER BY id"); exp != got {
		t.Fatalf("wrong rows in backup\nexp: %s\ngot: %s", exp, got)
	}
}

// Test_RestrictedBackupBinaryCompressed checks the restriction holds for a
// compressed backup.
func Test_RestrictedBackupBinaryCompressed(t *testing.T) {
	f, cleanup := newBkRestrictFixture(t)
	defer cleanup()

	path := f.backupToFile(f.binaryReq(false, true, []string{"foo"}))

	if exp, got := []string{"foo"}, f.objects(path, "table"); !f.sameStrings(exp, got) {
		t.Fatalf("wrong tables in backup, exp %v, got %v", exp, got)
	}
}

// Test_RestrictedBackupBinaryVacuumCompressed checks the restriction holds when
// the backup is both vacuumed and compressed.
func Test_RestrictedBackupBinaryVacuumCompressed(t *testing.T) {
	f, cleanup := newBkRestrictFixture(t)
	defer cleanup()

	path := f.backupToFile(f.binaryReq(true, true, []string{"foo"}))

	if exp, got := []string{"foo"}, f.objects(path, "table"); !f.sameStrings(exp, got) {
		t.Fatalf("wrong tables in backup, exp %v, got %v", exp, got)
	}
	if exp, got := `[{"columns":["id"],"types":["integer"],"values":[[1],[2]]}]`,
		f.query(path, "SELECT id FROM foo ORDER BY id"); exp != got {
		t.Fatalf("wrong rows in backup\nexp: %s\ngot: %s", exp, got)
	}
}

// Test_RestrictedBackupDeleteFormat checks the restriction holds for the format
// that converts the backup to rollback-journal mode.
func Test_RestrictedBackupDeleteFormat(t *testing.T) {
	f, cleanup := newBkRestrictFixture(t)
	defer cleanup()

	path := f.backupToFile(f.deleteReq(false, false, []string{"foo"}))

	if exp, got := []string{"foo"}, f.objects(path, "table"); !f.sameStrings(exp, got) {
		t.Fatalf("wrong tables in backup, exp %v, got %v", exp, got)
	}
	if exp, got := `[{"columns":["id"],"types":["integer"],"values":[[1],[2]]}]`,
		f.query(path, "SELECT id FROM foo ORDER BY id"); exp != got {
		t.Fatalf("wrong rows in backup\nexp: %s\ngot: %s", exp, got)
	}
}

// Test_RestrictedBackupDeleteCompressed checks the restriction holds when the
// rollback-journal backup is compressed.
func Test_RestrictedBackupDeleteCompressed(t *testing.T) {
	f, cleanup := newBkRestrictFixture(t)
	defer cleanup()

	path := f.backupToFile(f.deleteReq(false, true, []string{"foo"}))

	if exp, got := []string{"foo"}, f.objects(path, "table"); !f.sameStrings(exp, got) {
		t.Fatalf("wrong tables in backup, exp %v, got %v", exp, got)
	}
	if exp, got := `[{"columns":["id"],"types":["integer"],"values":[[1],[2]]}]`,
		f.query(path, "SELECT id FROM foo ORDER BY id"); exp != got {
		t.Fatalf("wrong rows in backup\nexp: %s\ngot: %s", exp, got)
	}
}
