package snapshot

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func Test_NewStreamHeader(t *testing.T) {
	strHdr := NewStreamHeader()
	if strHdr == nil {
		t.Fatal("StreamHeader is nil")
	}
	if strHdr.Version != streamVersion {
		t.Errorf("StreamHeader version is incorrect, got: %d, want: %d", strHdr.Version, streamVersion)
	}
	if strHdr.Payload != nil {
		t.Error("StreamHeader payload should be nil")
	}
	if strHdr.FileSize() != 0 {
		t.Errorf("Expected file size to be 0, got: %d", strHdr.FileSize())
	}
}

func Test_StreamHeaderFileSize(t *testing.T) {
	strHdr := NewStreamHeader()
	if strHdr == nil {
		t.Fatal("StreamHeader is nil")
	}

	// Test with no full snapshot
	if size := strHdr.FileSize(); size != 0 {
		t.Errorf("Expected file size to be 0 for no full snapshot, got: %d", size)
	}

	// Test with a full snapshot
	dbSize := int64(100)
	walSizes := []int64{200, 300}
	strHdr.Payload = &StreamHeader_FullSnapshot{
		FullSnapshot: &FullSnapshot{
			Db: &FullSnapshot_DataInfo{
				Size: dbSize,
			},
			Wals: []*FullSnapshot_DataInfo{
				{Size: walSizes[0]},
				{Size: walSizes[1]},
			},
		},
	}

	expectedSize := dbSize + walSizes[0] + walSizes[1]
	if size := strHdr.FileSize(); size != expectedSize {
		t.Errorf("Expected file size to be %d, got: %d", expectedSize, size)
	}
}

func Test_NewIncrementalStream(t *testing.T) {
	data := []byte("test data")

	stream, err := NewIncrementalStream(data)
	if err != nil {
		t.Fatalf("Failed to create new incremental stream: %v", err)
	}
	if stream == nil {
		t.Fatal("Expected non-nil stream, got nil")
	}

	// Get the header
	strHdr, n, err := NewStreamHeaderFromReader(stream)
	if err != nil {
		t.Fatalf("Failed to read from stream: %v", err)
	}
	if n != stream.Size() {
		t.Errorf("Expected to read %d bytes, got: %d", stream.Size(), n)
	}
	if strHdr.FileSize() != 0 {
		t.Errorf("Expected file size to be 0, got: %d", strHdr.FileSize())
	}

	// Check the data
	if strHdr.GetIncrementalSnapshot() == nil {
		t.Error("StreamHeader payload should not be nil")
	}
	if !bytes.Equal(strHdr.GetIncrementalSnapshot().Data, data) {
		t.Errorf("Expected data to be %s, got: %s", data, strHdr.GetIncrementalSnapshot().Data)
	}

	// Should be no more data
	buf := make([]byte, 1)
	if _, err := stream.Read(buf); err != io.EOF {
		t.Fatalf("Expected EOF, got: %v", err)
	}

	if err := stream.Close(); err != nil {
		t.Fatalf("unexpected error closing IncrementalStream: %v", err)
	}
}

func Test_NewFullStream(t *testing.T) {
	contents := [][]byte{
		[]byte("test1.db contents"),
		[]byte("test1.db-wal0 contents"),
		[]byte("test1.db-wal1 contents"),
	}
	contentsSz := int64(0)
	files := make([]string, len(contents))
	for i, c := range contents {
		files[i] = mustWriteToTemp(c)
		contentsSz += int64(len(c))
	}
	defer func() {
		for _, f := range files {
			os.Remove(f)
		}
	}()

	str, err := NewFullStream(files...)
	if err != nil {
		t.Fatalf("unexpected error creating FullStream: %v", err)
	}
	totalSizeRead := int64(0)

	// Get the header
	strHdr, sz, err := NewStreamHeaderFromReader(str)
	if err != nil {
		t.Fatalf("Failed to read from stream: %v", err)
	}
	if strHdr.FileSize() != contentsSz {
		t.Errorf("Expected file size to be %d, got: %d", contentsSz, strHdr.FileSize())
	}

	totalSizeRead += sz

	// Read the database contents and compare to the first file.
	fullSnapshot := strHdr.GetFullSnapshot()
	if fullSnapshot == nil {
		t.Fatalf("got nil FullSnapshot")
	}
	dbData := fullSnapshot.GetDb()
	if dbData == nil {
		t.Fatalf("got nil Db")
	}
	if dbData.Size != int64(len(contents[0])) {
		t.Errorf("unexpected Db size, got: %d, want: %d", dbData.Size, len(contents[0]))
	}
	buf := make([]byte, dbData.Size)
	n, err := io.ReadFull(str, buf)
	if err != nil {
		t.Fatalf("unexpected error reading from FullEncoder: %v", err)
	}
	totalSizeRead += int64(n)
	if string(buf) != string(contents[0]) {
		t.Errorf("unexpected database contents, got: %s, want: %s", buf, contents[0])
	}

	// Check the "WALs"
	if len(fullSnapshot.GetWals()) != 2 {
		t.Fatalf("unexpected number of WALs, got: %d, want: %d", len(fullSnapshot.GetWals()), 2)
	}
	for i := 0; i < len(fullSnapshot.GetWals()); i++ {
		walData := fullSnapshot.GetWals()[i]
		if walData == nil {
			t.Fatalf("got nil WAL")
		}
		if walData.Size != int64(len(contents[i+1])) {
			t.Errorf("unexpected WAL size, got: %d, want: %d", walData.Size, len(contents[i+1]))
		}
		buf = make([]byte, walData.Size)
		n, err = io.ReadFull(str, buf)
		if err != nil {
			t.Fatalf("unexpected error reading from FullEncoder: %v", err)
		}
		totalSizeRead += int64(n)
		if string(buf) != string(contents[i+1]) {
			t.Errorf("unexpected WAL contents, got: %s, want: %s", buf, contents[i+1])
		}
	}

	// Should be no more data to read
	buf = make([]byte, 1)
	n, err = str.Read(buf)
	if err != io.EOF {
		t.Fatalf("expected EOF, got: %v", err)
	}
	totalSizeRead += int64(n)

	// Verify that the total number of bytes read from the FullEncoder
	// matches the expected size
	if totalSizeRead != str.Size() {
		t.Errorf("unexpected total number of bytes read from FullEncoder, got: %d, want: %d", totalSizeRead, str.Size())
	}

	if err := str.Close(); err != nil {
		t.Fatalf("unexpected error closing FullStream: %v", err)
	}
}

func mustWriteToTemp(b []byte) string {
	f, err := os.CreateTemp("", "snapshot-enc-dec-test-*")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if _, err := f.Write(b); err != nil {
		panic(err)
	}
	return f.Name()
}
