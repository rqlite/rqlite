package snapshot

import (
	"strings"
	"testing"

	"github.com/hashicorp/raft"
)

func Test_NewStore(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := NewStore(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	if s == nil {
		t.Fatal("expected non-nil store")
	}

	generations, err := s.GetGenerations()
	if err != nil {
		t.Fatalf("failed to get generations: %s", err.Error())
	}
	if len(generations) != 0 {
		t.Fatalf("expected 0 generation, got %d", len(generations))
	}

	_, ok, err := s.GetCurrentGenerationDir()
	if err != nil {
		t.Fatalf("failed to get current generation dir: %s", err.Error())
	}
	if ok {
		t.Fatalf("expected current generation dir not to exist")
	}

	nextGenDir, err := s.GetNextGenerationDir()
	if err != nil {
		t.Fatalf("failed to get next generation dir: %s", err.Error())
	}
	if !strings.HasSuffix(nextGenDir, firstGeneration) {
		t.Fatalf("expected next generation dir to be empty, got %s", nextGenDir)
	}
}

func Test_NewStore_ListEmpty(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(dir)
	if err != nil {
		t.Fatalf("failed to create snapshot store: %s", err)
	}
	if !s.FullNeeded() {
		t.Fatalf("expected full snapshots to be needed")
	}

	if snaps, err := s.List(); err != nil {
		t.Fatalf("failed to list snapshots: %s", err)
	} else if len(snaps) != 0 {
		t.Fatalf("expected 1 snapshots, got %d", len(snaps))
	}
}

// Test_WALSnapshotStore_CreateFull performs detailed testing of the
// snapshot creation process.
func Test_Store_CreateFullThenIncremental(t *testing.T) {

	checkSnapshotCount := func(s *Store, exp int) *raft.SnapshotMeta {
		snaps, err := s.List()
		if err != nil {
			t.Fatalf("failed to list snapshots: %s", err)
		}
		if exp, got := exp, len(snaps); exp != got {
			t.Fatalf("expected %d snapshots, got %d", exp, got)
		}
		if len(snaps) == 0 {
			return nil
		}
		return snaps[0]
	}

	dir := t.TempDir()
	str, err := NewStore(dir)
	if err != nil {
		t.Fatalf("failed to create snapshot store: %s", err)
	}
	if !str.FullNeeded() {
		t.Fatalf("expected full snapshots to be needed")
	}

	testConfig1 := makeTestConfiguration("1", "2")
	sink, err := str.Create(1, 22, 33, testConfig1, 4, nil)
	if err != nil {
		t.Fatalf("failed to create 1st snapshot sink: %s", err)
	}

	// Create a full snapshot and write it to the sink.
	fullSnap := NewFullSnapshot("testdata/db-and-wals/backup.db")
	if err := fullSnap.Persist(sink); err != nil {
		t.Fatalf("failed to persist full snapshot: %s", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("failed to close sink: %s", err)
	}
	if str.FullNeeded() {
		t.Fatalf("full snapshot still needed")
	}
	meta := checkSnapshotCount(str, 1)
	if meta.Index != 22 || meta.Term != 33 {
		t.Fatalf("unexpected snapshot metadata: %+v", meta)
	}

	// Open the latest snapshot and check that it's correct.
	_, _, err = str.Open(meta.ID)
	if err != nil {
		t.Fatalf("failed to open snapshot %s: %s", meta.ID, err)
	}

	// Incremental snapshot next
	sink, err = str.Create(2, 55, 66, testConfig1, 4, nil)
	if err != nil {
		t.Fatalf("failed to create 2nd snapshot sink: %s", err)
	}
	walData := mustReadFile("testdata/db-and-wals/wal-00")
	incSnap := NewWALSnapshot(walData)
	if err := incSnap.Persist(sink); err != nil {
		t.Fatalf("failed to persist incremental snapshot: %s", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("failed to close sink: %s", err)
	}
	meta = checkSnapshotCount(str, 1)
	if meta.Index != 55 || meta.Term != 66 {
		t.Fatalf("unexpected snapshot metadata: %+v", meta)
	}
}

func Test_Store_ReapGenerations(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(dir)
	if err != nil {
		t.Fatalf("failed to create snapshot store: %s", err)
	}

	testCurrGenDirIs := func(exp string) string {
		curGenDir, ok, err := s.GetCurrentGenerationDir()
		if err != nil {
			t.Fatalf("failed to get current generation dir: %s", err.Error())
		}
		if !ok {
			t.Fatalf("expected current generation dir to exist")
		}
		if curGenDir != exp {
			t.Fatalf("expected current generation dir to be %s, got %s", exp, curGenDir)
		}
		return curGenDir
	}

	testGenCountIs := func(exp int) {
		generations, err := s.GetGenerations()
		if err != nil {
			t.Fatalf("failed to get generations: %s", err.Error())
		}
		if exp, got := exp, len(generations); exp != got {
			t.Fatalf("expected %d generations, got %d", exp, got)
		}
	}

	testReapsOK := func(expN int) {
		n, err := s.ReapGenerations()
		if err != nil {
			t.Fatalf("reaping failed: %s", err.Error())
		}
		if n != expN {
			t.Fatalf("expected %d generations to be reaped, got %d", expN, n)
		}
	}

	var nextGenDir string

	nextGenDir, err = s.GetNextGenerationDir()
	if err != nil {
		t.Fatalf("failed to get next generation dir: %s", err.Error())
	}
	mustCreateDir(nextGenDir)
	testCurrGenDirIs(nextGenDir)
	testReapsOK(0)

	// Create another generation and then tell the Store to reap.
	nextGenDir, err = s.GetNextGenerationDir()
	if err != nil {
		t.Fatalf("failed to get next generation dir: %s", err.Error())
	}
	mustCreateDir(nextGenDir)
	testGenCountIs(2)
	testReapsOK(1)
	testCurrGenDirIs(nextGenDir)

	// Finally, test reaping lots of generations.
	for i := 0; i < 10; i++ {
		nextGenDir, err = s.GetNextGenerationDir()
		if err != nil {
			t.Fatalf("failed to get next generation dir: %s", err.Error())
		}
		mustCreateDir(nextGenDir)
	}
	testGenCountIs(11)
	testReapsOK(10)
	testGenCountIs(1)
	testCurrGenDirIs(nextGenDir)
}

// Test_Store_Reaping tests that the snapshot store correctly
// reaps snapshots that are no longer needed. Because it's critical that
// reaping is done correctly, this test checks internal implementation
// details.
// func Test_Store_Reaping(t *testing.T) {
// 	dir := t.TempDir()
// 	str, err := NewStore(dir)
// 	if err != nil {
// 		t.Fatalf("failed to create snapshot store: %s", err)
// 	}
// 	//str.noAutoReap = true

// 	testConfig := makeTestConfiguration("1", "2")

// 	createSnapshot := func(index, term uint64, file string) {
// 		b, err := os.ReadFile(file)
// 		if err != nil {
// 			t.Fatalf("failed to read file: %s", err)
// 		}
// 		sink, err := str.Create(1, index, term, testConfig, 4, nil)
// 		if err != nil {
// 			t.Fatalf("failed to create 2nd snapshot: %s", err)
// 		}
// 		if _, err = sink.Write(b); err != nil {
// 			t.Fatalf("failed to write to sink: %s", err)
// 		}
// 		sink.Close()
// 	}

// 	createSnapshot(1, 1, "testdata/reaping/backup.db")
// 	createSnapshot(3, 2, "testdata/reaping/wal-00")
// 	createSnapshot(5, 3, "testdata/reaping/wal-01")
// 	createSnapshot(7, 4, "testdata/reaping/wal-02")
// 	createSnapshot(9, 5, "testdata/reaping/wal-03")

// 	// There should be 5 snapshot directories, one of which should be
// 	// a full, and the rest incremental.
// 	snaps, err := str.getSnapshots()
// 	if err != nil {
// 		t.Fatalf("failed to list snapshots: %s", err)
// 	}
// 	if exp, got := 5, len(snaps); exp != got {
// 		t.Fatalf("expected %d snapshots, got %d", exp, got)
// 	}
// 	for _, snap := range snaps[0:4] {
// 		if snap.Full {
// 			t.Fatalf("snapshot %s is full", snap.ID)
// 		}
// 	}
// 	if !snaps[4].Full {
// 		t.Fatalf("snapshot %s is incremental", snaps[4].ID)
// 	}

// 	// Reap just the first snapshot, which is full.
// 	n, err := str.ReapSnapshots(4)
// 	if err != nil {
// 		t.Fatalf("failed to reap full snapshot: %s", err)
// 	}
// 	if exp, got := 1, n; exp != got {
// 		t.Fatalf("expected %d snapshots to be reaped, got %d", exp, got)
// 	}
// 	snaps, err = str.getSnapshots()
// 	if err != nil {
// 		t.Fatalf("failed to list snapshots: %s", err)
// 	}
// 	if exp, got := 4, len(snaps); exp != got {
// 		t.Fatalf("expected %d snapshots, got %d", exp, got)
// 	}

// 	// Reap all but the last two snapshots. The remaining snapshots
// 	// should all be incremental.
// 	n, err = str.ReapSnapshots(2)
// 	if err != nil {
// 		t.Fatalf("failed to reap snapshots: %s", err)
// 	}
// 	if exp, got := 2, n; exp != got {
// 		t.Fatalf("expected %d snapshots to be reaped, got %d", exp, got)
// 	}
// 	snaps, err = str.getSnapshots()
// 	if err != nil {
// 		t.Fatalf("failed to list snapshots: %s", err)
// 	}
// 	if exp, got := 2, len(snaps); exp != got {
// 		t.Fatalf("expected %d snapshots, got %d", exp, got)
// 	}
// 	for _, snap := range snaps {
// 		if snap.Full {
// 			t.Fatalf("snapshot %s is full", snap.ID)
// 		}
// 	}
// 	if snaps[0].Index != 9 && snaps[1].Term != 5 {
// 		t.Fatalf("snap 0 is wrong")
// 	}
// 	if snaps[1].Index != 7 && snaps[1].Term != 3 {
// 		t.Fatalf("snap 1 is wrong")
// 	}

// 	// Check the contents of the remaining snapshots by creating a new
// 	// SQLite from the Store
// 	dbPath, err := str.ReplayWALs()
// 	if err != nil {
// 		t.Fatalf("failed to replay WALs: %s", err)
// 	}
// 	db, err := db.Open(dbPath, false, true)
// 	if err != nil {
// 		t.Fatalf("failed to open database: %s", err)
// 	}
// 	defer db.Close()
// 	rows, err := db.QueryStringStmt("SELECT COUNT(*) FROM foo")
// 	if err != nil {
// 		t.Fatalf("failed to query database: %s", err)
// 	}
// 	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[4]]}]`, asJSON(rows); exp != got {
// 		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
// 	}
// }
