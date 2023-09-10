package snapshot

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/db"
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

func Test_NewStore_ListOpenEmpty(t *testing.T) {
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

	if _, _, err := s.Open("non-existent"); err != ErrSnapshotNotFound {
		t.Fatalf("expected ErrSnapshotNotFound, got %s", err)
	}
}

// Test_WALSnapshotStore_CreateFull performs detailed testing of the
// snapshot creation process. It is critical that snapshots are created
// correctly, so this test is thorough.
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

	//////////////////////////////////////////////////////////////////////////
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
	raftMeta, rc, err := str.Open(meta.ID)
	if err != nil {
		t.Fatalf("failed to open snapshot %s: %s", meta.ID, err)
	}
	crc := &countingReadCloser{rc: rc}
	streamHdr, _, err := NewStreamHeaderFromReader(crc)
	if err != nil {
		t.Fatalf("error reading stream header: %v", err)
	}
	streamSnap := streamHdr.GetFullSnapshot()
	if streamSnap == nil {
		t.Fatal("got nil FullSnapshot")
	}
	dbInfo := streamSnap.GetDb()
	if dbInfo == nil {
		t.Fatal("got nil DB info")
	}
	if !compareReaderToFile(crc, "testdata/db-and-wals/backup.db") {
		t.Fatalf("database file does not match what is in snapshot")
	}
	// should be no more data
	if _, err := crc.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
	if err := crc.Close(); err != nil {
		t.Fatalf("failed to close snapshot reader: %s", err)
	}
	if exp, got := raftMeta.Size, int64(crc.n); exp != got {
		t.Fatalf("expected snapshot size to be %d, got %d", exp, got)
	}
	crc.Close()

	//////////////////////////////////////////////////////////////////////////
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

	// Open the latest snapshot again, and recreate the database so we
	// can check its contents.
	raftMeta, rc, err = str.Open(meta.ID)
	if err != nil {
		t.Fatalf("failed to open snapshot %s: %s", meta.ID, err)
	}
	crc = &countingReadCloser{rc: rc}
	streamHdr, _, err = NewStreamHeaderFromReader(crc)
	if err != nil {
		t.Fatalf("error reading stream header: %v", err)
	}
	streamSnap = streamHdr.GetFullSnapshot()
	if streamSnap == nil {
		t.Fatal("got nil FullSnapshot")
	}
	tmpFile := t.TempDir() + "/db"
	if err := ReplayDB(streamSnap, crc, tmpFile); err != nil {
		t.Fatalf("failed to replay database: %s", err)
	}
	checkDB, err := db.Open(tmpFile, false, true)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	defer checkDB.Close()

	// Database should now have 1 one after replaying the WAL.
	rows, err := checkDB.QueryStringStmt("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query database: %s", err)
	}
	if exp, got := `[{"columns":["id","value"],"types":["integer","text"],"values":[[1,"Row 0"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results for query, exp %s, got %s", exp, got)
	}

	// should be no more data
	if _, err := crc.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
	if exp, got := raftMeta.Size, int64(crc.n); exp != got {
		t.Fatalf("expected snapshot size to be %d, got %d", exp, got)
	}
	crc.Close()

	//////////////////////////////////////////////////////////////////////////
	// Do it again!
	sink, err = str.Create(2, 77, 88, testConfig1, 4, nil)
	if err != nil {
		t.Fatalf("failed to create 2nd snapshot sink: %s", err)
	}
	walData = mustReadFile("testdata/db-and-wals/wal-01")
	incSnap = NewWALSnapshot(walData)
	if err := incSnap.Persist(sink); err != nil {
		t.Fatalf("failed to persist incremental snapshot: %s", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("failed to close sink: %s", err)
	}
	meta = checkSnapshotCount(str, 1)
	if meta.Index != 77 || meta.Term != 88 {
		t.Fatalf("unexpected snapshot metadata: %+v", meta)
	}

	// Open the latest snapshot again, and recreate the database so we
	// can check its contents.
	raftMeta, rc, err = str.Open(meta.ID)
	if err != nil {
		t.Fatalf("failed to open snapshot %s: %s", meta.ID, err)
	}
	crc = &countingReadCloser{rc: rc}
	streamHdr, _, err = NewStreamHeaderFromReader(crc)
	if err != nil {
		t.Fatalf("error reading stream header: %v", err)
	}
	streamSnap = streamHdr.GetFullSnapshot()
	if streamSnap == nil {
		t.Fatal("got nil FullSnapshot")
	}
	tmpFile = t.TempDir() + "/db"
	if err := ReplayDB(streamSnap, crc, tmpFile); err != nil {
		t.Fatalf("failed to replay database: %s", err)
	}
	checkDB, err = db.Open(tmpFile, false, true)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	defer checkDB.Close()
	rows, err = checkDB.QueryStringStmt("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query database: %s", err)
	}
	if exp, got := `[{"columns":["id","value"],"types":["integer","text"],"values":[[1,"Row 0"],[2,"Row 1"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results for query, exp %s, got %s", exp, got)
	}

	// should be no more data
	if _, err := crc.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
	if exp, got := raftMeta.Size, int64(crc.n); exp != got {
		t.Fatalf("expected snapshot size to be %d, got %d", exp, got)
	}
	crc.Close()

	//////////////////////////////////////////////////////////////////////////
	// One last time, after a reaping took place in the middle.
	sink, err = str.Create(2, 100, 200, testConfig1, 4, nil)
	if err != nil {
		t.Fatalf("failed to create 2nd snapshot sink: %s", err)
	}
	walData = mustReadFile("testdata/db-and-wals/wal-02")
	incSnap = NewWALSnapshot(walData)
	if err := incSnap.Persist(sink); err != nil {
		t.Fatalf("failed to persist incremental snapshot: %s", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("failed to close sink: %s", err)
	}
	meta = checkSnapshotCount(str, 1)
	if meta.Index != 100 || meta.Term != 200 {
		t.Fatalf("unexpected snapshot metadata: %+v", meta)
	}

	// Open the latest snapshot again, and recreate the database so we
	// can check its contents.
	raftMeta, rc, err = str.Open(meta.ID)
	if err != nil {
		t.Fatalf("failed to open snapshot %s: %s", meta.ID, err)
	}
	crc = &countingReadCloser{rc: rc}
	streamHdr, _, err = NewStreamHeaderFromReader(crc)
	if err != nil {
		t.Fatalf("error reading stream header: %v", err)
	}
	streamSnap = streamHdr.GetFullSnapshot()
	if streamSnap == nil {
		t.Fatal("got nil FullSnapshot")
	}
	tmpFile = t.TempDir() + "/db"
	if err := ReplayDB(streamSnap, crc, tmpFile); err != nil {
		t.Fatalf("failed to replay database: %s", err)
	}
	checkDB, err = db.Open(tmpFile, false, true)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	defer checkDB.Close()
	rows, err = checkDB.QueryStringStmt("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query database: %s", err)
	}
	if exp, got := `[{"columns":["id","value"],"types":["integer","text"],"values":[[1,"Row 0"],[2,"Row 1"],[3,"Row 2"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results for query, exp %s, got %s", exp, got)
	}

	// should be no more data
	if _, err := crc.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
	if exp, got := raftMeta.Size, int64(crc.n); exp != got {
		t.Fatalf("expected snapshot size to be %d, got %d", exp, got)
	}
	crc.Close()

	// Finally, test via Restore() function.
	tmpDir := t.TempDir()
	rPath, err := str.Restore(meta.ID, tmpDir)
	if err != nil {
		t.Fatalf("failed to restore snapshot: %s", err)
	}
	restoredDB, err := db.Open(rPath, false, true)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	defer restoredDB.Close()
	rows, err = restoredDB.QueryStringStmt("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query database: %s", err)
	}
	if exp, got := `[{"columns":["id","value"],"types":["integer","text"],"values":[[1,"Row 0"],[2,"Row 1"],[3,"Row 2"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results for query, exp %s, got %s", exp, got)
	}

}

// Test_WALSnapshotStore_CreateFullThenFull ensures two full snapshots
// can be created and persisted back-to-back.
func Test_Store_CreateFullThenFull(t *testing.T) {
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

	//////////////////////////////////////////////////////////////////////////
	// Create a full snapshot and write it to the sink.
	sink, err := str.Create(1, 22, 33, testConfig1, 4, nil)
	if err != nil {
		t.Fatalf("failed to create 1st snapshot sink: %s", err)
	}

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

	//////////////////////////////////////////////////////////////////////////
	// Create a second full snapshot and write it to the sink.
	sink, err = str.Create(1, 44, 55, testConfig1, 4, nil)
	if err != nil {
		t.Fatalf("failed to create 1st snapshot sink: %s", err)
	}

	fullSnap = NewFullSnapshot("testdata/db-and-wals/backup.db")
	if err := fullSnap.Persist(sink); err != nil {
		t.Fatalf("failed to persist full snapshot: %s", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("failed to close sink: %s", err)
	}
	if str.FullNeeded() {
		t.Fatalf("full snapshot still needed")
	}
	meta = checkSnapshotCount(str, 1)
	if meta.Index != 44 || meta.Term != 55 {
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

func compareReaderToFile(r io.Reader, path string) bool {
	b := mustReadFile(path)
	rb := mustReadAll(r)
	return bytes.Equal(b, rb)
}

func mustReadAll(r io.Reader) []byte {
	b, err := io.ReadAll(r)
	if err != nil {
		panic(err)
	}
	return b
}

type countingReadCloser struct {
	rc io.ReadCloser
	n  int
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.rc.Read(p)
	c.n += n
	return n, err
}

func (c *countingReadCloser) Close() error {
	return c.rc.Close()
}

func Test_StoreReaping(t *testing.T) {
	dir := t.TempDir()
	str, err := NewStore(dir)
	if err != nil {
		t.Fatalf("failed to create snapshot store: %s", err)
	}
	str.noAutoreap = true
	testConfig := makeTestConfiguration("1", "2")

	// Create a full snapshot.
	snapshot := NewFullSnapshot("testdata/db-and-wals/backup.db")
	sink, err := str.Create(1, 1, 1, testConfig, 4, nil)
	if err != nil {
		t.Fatalf("failed to create snapshot sink: %s", err)
	}
	stream, err := snapshot.OpenStream()
	if err != nil {
		t.Fatalf("failed to open snapshot stream: %s", err)
	}
	_, err = io.Copy(sink, stream)
	if err != nil {
		t.Fatalf("failed to write snapshot: %s", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("failed to close snapshot sink: %s", err)
	}

	createIncSnapshot := func(index, term uint64, file string) {
		snapshot := NewWALSnapshot(mustReadFile(file))
		sink, err := str.Create(1, index, term, testConfig, 4, nil)
		if err != nil {
			t.Fatalf("failed to create snapshot sink: %s", err)
		}
		stream, err := snapshot.OpenStream()
		if err != nil {
			t.Fatalf("failed to open snapshot stream: %s", err)
		}
		_, err = io.Copy(sink, stream)
		if err != nil {
			t.Fatalf("failed to write snapshot: %s", err)
		}
		if err := sink.Close(); err != nil {
			t.Fatalf("failed to close snapshot sink: %s", err)
		}
	}

	createIncSnapshot(3, 2, "testdata/db-and-wals/wal-00")
	createIncSnapshot(5, 3, "testdata/db-and-wals/wal-01")
	createIncSnapshot(7, 4, "testdata/db-and-wals/wal-02")
	createIncSnapshot(9, 5, "testdata/db-and-wals/wal-03")

	// There should be 5 snapshot directories in the current generation.
	generationsDir, ok, err := str.GetCurrentGenerationDir()
	if err != nil {
		t.Fatalf("failed to get generations dir: %s", err)
	}
	if !ok {
		t.Fatalf("expected generations dir to exist")
	}
	snaps, err := str.getSnapshots(generationsDir)
	if err != nil {
		t.Fatalf("failed to list snapshots: %s", err)
	}
	if exp, got := 5, len(snaps); exp != got {
		t.Fatalf("expected %d snapshots, got %d", exp, got)
	}
	for _, snap := range snaps[0:4] {
		if snap.Full {
			t.Fatalf("snapshot %s is full", snap.ID)
		}
	}
	if !snaps[4].Full {
		t.Fatalf("snapshot %s is incremental", snaps[4].ID)
	}

	// Reap just the first snapshot, which is full.
	n, err := str.ReapSnapshots(generationsDir, 4)
	if err != nil {
		t.Fatalf("failed to reap full snapshot: %s", err)
	}
	if exp, got := 1, n; exp != got {
		t.Fatalf("expected %d snapshots to be reaped, got %d", exp, got)
	}
	snaps, err = str.getSnapshots(generationsDir)
	if err != nil {
		t.Fatalf("failed to list snapshots: %s", err)
	}
	if exp, got := 4, len(snaps); exp != got {
		t.Fatalf("expected %d snapshots, got %d", exp, got)
	}

	// Reap all but the last two snapshots. The remaining snapshots
	// should all be incremental.
	n, err = str.ReapSnapshots(generationsDir, 2)
	if err != nil {
		t.Fatalf("failed to reap snapshots: %s", err)
	}
	if exp, got := 2, n; exp != got {
		t.Fatalf("expected %d snapshots to be reaped, got %d", exp, got)
	}
	snaps, err = str.getSnapshots(generationsDir)
	if err != nil {
		t.Fatalf("failed to list snapshots: %s", err)
	}
	if exp, got := 2, len(snaps); exp != got {
		t.Fatalf("expected %d snapshots, got %d", exp, got)
	}
	for _, snap := range snaps {
		if snap.Full {
			t.Fatalf("snapshot %s is full", snap.ID)
		}
	}
	if snaps[0].Index != 9 && snaps[1].Term != 5 {
		t.Fatal("snap 0 is wrong, exp: ", snaps[0].Index, snaps[1].Term)
	}
	if snaps[1].Index != 7 && snaps[1].Term != 3 {
		t.Fatal("snap 1 is wrong, exp:", snaps[1].Index, snaps[1].Term)
	}

	// Open the latest snapshot, write it to disk, and check its contents.
	_, rc, err := str.Open(snaps[0].ID)
	if err != nil {
		t.Fatalf("failed to open snapshot %s: %s", snaps[0].ID, err)
	}
	defer rc.Close()
	strHdr, _, err := NewStreamHeaderFromReader(rc)
	if err != nil {
		t.Fatalf("error reading stream header: %v", err)
	}
	streamSnap := strHdr.GetFullSnapshot()
	if streamSnap == nil {
		t.Fatal("got nil FullSnapshot")
	}

	tmpFile := t.TempDir() + "/db"
	if err := ReplayDB(streamSnap, rc, tmpFile); err != nil {
		t.Fatalf("failed to replay database: %s", err)
	}

	// Check the database.
	db, err := db.Open(tmpFile, false, true)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	defer db.Close()
	rows, err := db.QueryStringStmt("SELECT COUNT(*) FROM foo")
	if err != nil {
		t.Fatalf("failed to query database: %s", err)
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[4]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results for query exp: %s got: %s", exp, got)
	}
}
