package snapshot

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
)

func Test_EncodeValidHeader(t *testing.T) {
	h := NewHeader(42)
	encoded := h.Encode()
	if exp, got := magicString, string(encoded[:len(magicString)]); exp != got {
		t.Errorf(`unexpected magic string in encoded Header, exp "%s", got "%s"`, exp, got)
	}
	if exp, got := headerVersion, binary.LittleEndian.Uint16(encoded[16:18]); exp != got {
		t.Errorf("unexpected version in encoded Header, exp %d, got %d", exp, got)
	}
	if exp, got := h.SnapshotHeaderSize, binary.LittleEndian.Uint16(encoded[20:22]); exp != got {
		t.Errorf("unexpected SnapshotHeaderSize in encoded Header, exp %d, got %d", exp, got)
	}
}

func Test_DecodeValidEncodedHeader(t *testing.T) {
	h := NewHeader(42)
	encoded := h.Encode()
	decoded, err := DecodeHeader(encoded)
	if err != nil || !bytes.Equal(decoded.Magic[:], h.Magic[:]) ||
		decoded.Version != h.Version ||
		decoded.SnapshotHeaderSize != h.SnapshotHeaderSize {
		t.Errorf("unexpected decoding result: %v, error: %v", decoded, err)
	}
}

func Test_DecodeInsufficientBufferLength(t *testing.T) {
	buf := make([]byte, sizeofHeader-1)
	_, err := DecodeHeader(buf)
	if err == nil || !strings.Contains(err.Error(), "buffer too small") {
		t.Errorf("expected buffer too small error, got: %v", err)
	}
}

func Test_DecodeInvalidMagicString(t *testing.T) {
	buf := make([]byte, sizeofHeader)
	copy(buf[:15], "invalid magic")
	_, err := DecodeHeader(buf)
	if err == nil || !strings.Contains(err.Error(), "invalid magic string") {
		t.Errorf("expected invalid magic string error, got: %v", err)
	}
}

func Test_EncodeDecodeRoundTrip(t *testing.T) {
	h := NewHeader(42)
	encoded := h.Encode()
	decoded, err := DecodeHeader(encoded)
	if err != nil || !bytes.Equal(decoded.Magic[:], h.Magic[:]) ||
		decoded.Version != h.Version ||
		decoded.SnapshotHeaderSize != h.SnapshotHeaderSize {
		t.Errorf("round trip failed, original: %v, decoded: %v, error: %v", h, decoded, err)
	}
}

func Test_IncrementalEncoder(t *testing.T) {
	enc := NewIncrementalEncoder()
	data := []byte("test data")

	err := enc.Open(data)
	if err != nil {
		t.Fatalf("unexpected error opening IncrementalEncoder: %v", err)
	}
	totalSizeRead := int64(0)

	// Verify header
	buf := make([]byte, sizeofHeader)
	n, err := io.ReadFull(enc, buf)
	if err != nil {
		t.Fatalf("unexpected error reading from IncrementalEncoder: %v", err)
	}
	totalSizeRead += int64(n)
	if n != sizeofHeader {
		t.Fatalf("unexpected number of bytes read from IncrementalEncoder, got: %d, want: %d", n, sizeofHeader)
	}
	h, err := DecodeHeader(buf)
	if err != nil {
		t.Fatalf("unexpected error decoding header: %v", err)
	}

	// Get the size of the FSM snapshot header
	buf = make([]byte, h.SnapshotHeaderSize)
	n, err = io.ReadFull(enc, buf)
	if err != nil {
		t.Fatalf("unexpected error reading from IncrementalEncoder: %v", err)
	}
	totalSizeRead += int64(n)

	if n != int(h.SnapshotHeaderSize) {
		t.Fatalf("unexpected number of bytes read from IncrementalEncoder, got: %d, want: %d", n, h.SnapshotHeaderSize)
	}
	// Unmarshal the FSM snapshot from the buffer
	fsmSnap := &FSMSnapshot{}
	err = proto.Unmarshal(buf, fsmSnap)
	if err != nil {
		t.Fatalf("unexpected error unmarshaling FSM snapshot: %v", err)
	}

	// Verify that the FSM snapshot data matches the original input
	if fsmData := fsmSnap.GetIncrementalSnapshot().Data; string(fsmData) != string(data) {
		t.Errorf("unexpected FSM snapshot data, got: %s, want: %s", fsmData, data)
	}

	// Should be no more data to read
	buf = make([]byte, 1)
	_, err = enc.Read(buf)
	if err != io.EOF {
		t.Fatalf("expected EOF, got: %v", err)
	}

	// Verify that the total number of bytes read from the IncrementalEncoder
	// matches the expected size
	if totalSizeRead != enc.Size() {
		t.Errorf("unexpected total number of bytes read from IncrementalEncoder, got: %d, want: %d", totalSizeRead, enc.Size())
	}

	if enc.Close() != nil {
		t.Fatalf("unexpected error closing IncrementalEncoder: %v", err)
	}
}

func Test_FullEncoder(t *testing.T) {
	contents := [][]byte{[]byte("test1.db contents"), []byte("test1.db-wal0 contents"), []byte("test1.db-wal1 contents")}
	files := make([]string, len(contents))
	for i, c := range contents {
		files[i] = mustWriteToTemp(c)
	}
	defer func() {
		for _, f := range files {
			os.Remove(f)
		}
	}()

	enc := NewFullEncoder()
	err := enc.Open(files...)
	if err != nil {
		t.Fatalf("unexpected error opening FullEncoder: %v", err)
	}
	totalSizeRead := int64(0)

	// Verify header
	buf := make([]byte, sizeofHeader)
	n, err := io.ReadFull(enc, buf)
	if err != nil {
		t.Fatalf("unexpected error reading from FullEncoder: %v", err)
	}
	totalSizeRead += int64(n)
	if n != sizeofHeader {
		t.Fatalf("unexpected number of bytes read from FullEncoder, got: %d, want: %d", n, sizeofHeader)
	}
	h, err := DecodeHeader(buf)
	if err != nil {
		t.Fatalf("unexpected error decoding header: %v", err)
	}

	// Get the size of the FSM snapshot header
	buf = make([]byte, h.SnapshotHeaderSize)
	n, err = io.ReadFull(enc, buf)
	if err != nil {
		t.Fatalf("unexpected error reading from FullEncoder: %v", err)
	}
	totalSizeRead += int64(n)

	// Unmarshal the FSM snapshot from the buffer
	fsmSnap := &FSMSnapshot{}
	err = proto.Unmarshal(buf, fsmSnap)
	if err != nil {
		t.Fatalf("unexpected error unmarshaling FSM snapshot: %v", err)
	}

	fullSnapshot := fsmSnap.GetFullSnapshot()
	if fullSnapshot == nil {
		t.Fatalf("got nil FullSnapshot")
	}

	// Read the database contents and compare to the first file.
	dbData := fullSnapshot.GetDb()
	if dbData == nil {
		t.Fatalf("got nil Db")
	}
	if dbData.Size != int64(len(contents[0])) {
		t.Errorf("unexpected Db size, got: %d, want: %d", dbData.Size, len(contents[0]))
	}
	buf = make([]byte, dbData.Size)
	n, err = io.ReadFull(enc, buf)
	if err != nil {
		t.Fatalf("unexpected error reading from FullEncoder: %v", err)
	}
	totalSizeRead += int64(n)
	if string(buf) != string(contents[0]) {
		t.Errorf("unexpected database contents, got: %s, want: %s", buf, contents[0])
	}

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
		n, err = io.ReadFull(enc, buf)
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
	n, err = enc.Read(buf)
	if err != io.EOF {
		t.Fatalf("expected EOF, got: %v", err)
	}
	totalSizeRead += int64(n)

	// Verify that the total number of bytes read from the FullEncoder
	// matches the expected size
	if totalSizeRead != enc.Size() {
		t.Errorf("unexpected total number of bytes read from FullEncoder, got: %d, want: %d", totalSizeRead, enc.Size())
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
