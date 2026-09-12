package raftdb

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
	raftbench "github.com/hashicorp/raft/bench"
)

// upstreamEncodeMsgPack is the unmodified upstream raft-boltdb encoding,
// used to prove wire-format compatibility of this package's codec.
func upstreamEncodeMsgPack(in interface{}) (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(nil)
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(buf, &hd)
	err := enc.Encode(in)
	return buf, err
}

// upstreamDecodeMsgPack is the unmodified upstream raft-boltdb decoding.
func upstreamDecodeMsgPack(buf []byte, out interface{}) error {
	r := bytes.NewBuffer(buf)
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(r, &hd)
	return dec.Decode(out)
}

func testRaftLog(i uint64) *raft.Log {
	return &raft.Log{
		Index:      i,
		Term:       i + 1,
		Type:       raft.LogCommand,
		Data:       []byte("the quick brown fox jumps over the lazy dog"),
		Extensions: []byte("ext"),
		AppendedAt: time.Now().UTC().Truncate(time.Microsecond),
	}
}

// TestEncodeDecodeCompatibility verifies that this package's encoding is
// byte-identical to upstream's, and that each can decode the other's output.
func TestEncodeDecodeCompatibility(t *testing.T) {
	log := testRaftLog(42)

	// Encode with this package; decode with upstream.
	ours, err := encodeMsgPack(log)
	if err != nil {
		t.Fatalf("encodeMsgPack failed: %v", err)
	}
	var viaUpstream raft.Log
	if err := upstreamDecodeMsgPack(ours, &viaUpstream); err != nil {
		t.Fatalf("upstream failed to decode our encoding: %v", err)
	}
	if !logsEqual(&viaUpstream, log) {
		t.Fatalf("roundtrip mismatch via upstream decoder: got %+v want %+v", viaUpstream, *log)
	}

	// Encode with upstream; decode with this package.
	theirs, err := upstreamEncodeMsgPack(log)
	if err != nil {
		t.Fatalf("upstream encode failed: %v", err)
	}
	if !bytes.Equal(ours, theirs.Bytes()) {
		t.Fatalf("wire format differs from upstream:\n ours: %x\ntheirs: %x", ours, theirs.Bytes())
	}
	var viaOurs raft.Log
	if err := decodeMsgPack(theirs.Bytes(), &viaOurs); err != nil {
		t.Fatalf("decodeMsgPack failed on upstream encoding: %v", err)
	}
	if !logsEqual(&viaOurs, log) {
		t.Fatalf("roundtrip mismatch via our decoder: got %+v want %+v", viaOurs, *log)
	}
}

// TestEncodeDecodeMinimalLog checks a log with nil Data/Extensions, as
// produced for noop entries.
func TestEncodeDecodeMinimalLog(t *testing.T) {
	log := &raft.Log{
		Index:      7,
		Term:       2,
		Type:       raft.LogNoop,
		AppendedAt: time.Unix(1600000000, 0).UTC(),
	}

	ours, err := encodeMsgPack(log)
	if err != nil {
		t.Fatalf("encodeMsgPack failed: %v", err)
	}
	var out raft.Log
	if err := decodeMsgPack(ours, &out); err != nil {
		t.Fatalf("decodeMsgPack failed: %v", err)
	}
	if !logsEqual(&out, log) {
		t.Fatalf("roundtrip mismatch: got %+v want %+v", out, *log)
	}

	// Upstream must also read it.
	var viaUpstream raft.Log
	if err := upstreamDecodeMsgPack(ours, &viaUpstream); err != nil {
		t.Fatalf("upstream failed to decode our minimal encoding: %v", err)
	}
	if !logsEqual(&viaUpstream, log) {
		t.Fatalf("upstream roundtrip mismatch: got %+v want %+v", viaUpstream, *log)
	}
}

// TestDecodeLegacyV055TimeFormat is a regression test for reading raft log
// entries written by the original upstream raft-boltdb using go-msgpack
// v0.5.5. That version encoded time.Time via encoding.BinaryMarshaler and
// wrote it as a msgpack string; v0.5.5-encoded data appears in raft.db files
// created by older rqlite releases. Only go-msgpack v2 decoders understand
// this format - which is why this package must use go-msgpack/v2.
//
// The fixture below was generated with go-msgpack v0.5.5 encoding:
//
//	raft.Log{Index: 42, Term: 7, Type: LogCommand,
//	         Data: "hello world", Extensions: "ext",
//	         AppendedAt: time.Unix(1700000000, 123456789).UTC()}
func TestDecodeLegacyV055TimeFormat(t *testing.T) {
	fixture := []byte{
		0x86, 0xaa, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x65, 0x64, 0x41, 0x74,
		0xaf, 0x01, 0x00, 0x00, 0x00, 0x0e, 0xdc, 0xe5, 0xe8, 0x00, 0x07, 0x5b,
		0xcd, 0x15, 0xff, 0xff, 0xa4, 0x44, 0x61, 0x74, 0x61, 0xab, 0x68, 0x65,
		0x6c, 0x6c, 0x6f, 0x20, 0x77, 0x6f, 0x72, 0x6c, 0x64, 0xaa, 0x45, 0x78,
		0x74, 0x65, 0x6e, 0x73, 0x69, 0x6f, 0x6e, 0x73, 0xa3, 0x65, 0x78, 0x74,
		0xa5, 0x49, 0x6e, 0x64, 0x65, 0x78, 0x2a, 0xa4, 0x54, 0x65, 0x72, 0x6d,
		0x07, 0xa4, 0x54, 0x79, 0x70, 0x65, 0x00,
	}

	var out raft.Log
	if err := decodeMsgPack(fixture, &out); err != nil {
		t.Fatalf("failed to decode legacy v0.5.5 encoding: %v", err)
	}
	want := time.Unix(1700000000, 123456789).UTC()
	if !out.AppendedAt.Equal(want) {
		t.Fatalf("AppendedAt mismatch: got %v want %v", out.AppendedAt, want)
	}
	if out.Index != 42 || out.Term != 7 || out.Type != raft.LogCommand {
		t.Fatalf("field mismatch: got %+v", out)
	}
	if !bytes.Equal(out.Data, []byte("hello world")) || !bytes.Equal(out.Extensions, []byte("ext")) {
		t.Fatalf("data mismatch: got %+v", out)
	}

	// The new-format encoding of the same log must round-trip identically.
	newFormat, err := encodeMsgPack(&out)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	var back raft.Log
	if err := decodeMsgPack(newFormat, &back); err != nil {
		t.Fatalf("re-decode failed: %v", err)
	}
	if !logsEqual(&back, &out) {
		t.Fatalf("roundtrip mismatch: got %+v want %+v", back, out)
	}
}

// TestSharedHandleConcurrentAccess exercises the shared package-level
// MsgpackHandle from many goroutines to surface any data race (run with -race).
func TestSharedHandleConcurrentAccess(t *testing.T) {
	log := testRaftLog(99)

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				buf, err := encodeMsgPack(log)
				if err != nil {
					t.Errorf("encode failed: %v", err)
					return
				}
				var out raft.Log
				if err := decodeMsgPack(buf, &out); err != nil {
					t.Errorf("decode failed: %v", err)
					return
				}
				if !logsEqual(&out, log) {
					t.Errorf("mismatch: got %+v want %+v", out, *log)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// TestSharedHandleConcurrentColdCache hammers the shared handle from many
// goroutines with raft.Log types NOT previously seen by this process, so the
// codec's handle-level function cache (rtidFns) is still cold. This drives
// the copy-on-write cache-fill path concurrently - the trickiest part of
// sharing one handle - and must complete cleanly under -race.
func TestSharedHandleConcurrentColdCache(t *testing.T) {
	type coldLog struct {
		Index      uint64
		Term       uint64
		UniqueTag  string // unique per goroutine so each fills a distinct cache entry
		Data       []byte
		AppendedAt time.Time
	}

	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			l := &coldLog{
				Index:      uint64(g),
				UniqueTag:  fmt.Sprintf("goroutine-%d", g),
				Data:       []byte("cold cache fill"),
				AppendedAt: time.Unix(1700000000, int64(g)).UTC(),
			}
			for i := 0; i < 50; i++ {
				var buf []byte
				if err := codec.NewEncoderBytes(&buf, &msgpackHandle).Encode(l); err != nil {
					t.Errorf("encode failed: %v", err)
					return
				}
				var out coldLog
				if err := codec.NewDecoderBytes(buf, &msgpackHandle).Decode(&out); err != nil {
					t.Errorf("decode failed: %v", err)
					return
				}
				if out.Index != l.Index || out.UniqueTag != l.UniqueTag || !out.AppendedAt.Equal(l.AppendedAt) {
					t.Errorf("mismatch: got %+v want %+v", out, *l)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}

// TestStoreEndToEnd verifies a basic store-retrieve cycle through BoltDB.
func TestStoreEndToEnd(t *testing.T) {
	path := mustTempFile(t)
	store, err := NewBoltStore(path)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()
	defer os.Remove(path)

	log := testRaftLog(1)
	if err := store.StoreLog(log); err != nil {
		t.Fatalf("failed to store log: %v", err)
	}

	var out raft.Log
	if err := store.GetLog(1, &out); err != nil {
		t.Fatalf("failed to get log: %v", err)
	}
	if !logsEqual(&out, log) {
		t.Fatalf("mismatch: got %+v want %+v", out, *log)
	}

	fi, err := store.FirstIndex()
	if err != nil {
		t.Fatalf("failed to get first index: %v", err)
	}
	li, err := store.LastIndex()
	if err != nil {
		t.Fatalf("failed to get last index: %v", err)
	}
	if fi != 1 || li != 1 {
		t.Fatalf("got indexes %d, %d; want 1, 1", fi, li)
	}

	if err := store.DeleteRange(1, 1); err != nil {
		t.Fatalf("failed to delete range: %v", err)
	}
	fi, _ = store.FirstIndex()
	li, _ = store.LastIndex()
	if fi != 0 || li != 0 {
		t.Fatalf("got indexes %d, %d after delete; want 0, 0", fi, li)
	}
}

func logsEqual(a, b *raft.Log) bool {
	return a.Index == b.Index &&
		a.Term == b.Term &&
		a.Type == b.Type &&
		bytes.Equal(a.Data, b.Data) &&
		bytes.Equal(a.Extensions, b.Extensions) &&
		a.AppendedAt.Equal(b.AppendedAt)
}

func mustTempFile(t testing.TB) string {
	t.Helper()
	f, err := os.CreateTemp("", "boltdb-test")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	name := f.Name()
	if err := f.Close(); err != nil {
		t.Fatalf("failed to close temp file: %v", err)
	}
	os.Remove(name)
	return name
}

// Benchmark comparisons against the standard hashicorp raft-bench harness.
func BenchmarkBoltStore_FirstIndex(b *testing.B) {
	store := testBoltStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.FirstIndex(b, store)
}

func BenchmarkBoltStore_LastIndex(b *testing.B) {
	store := testBoltStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.LastIndex(b, store)
}

func BenchmarkBoltStore_GetLog(b *testing.B) {
	store := testBoltStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.GetLog(b, store)
}

func BenchmarkBoltStore_StoreLog(b *testing.B) {
	store := testBoltStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.StoreLog(b, store)
}

func BenchmarkBoltStore_StoreLogs(b *testing.B) {
	store := testBoltStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.StoreLogs(b, store)
}

func BenchmarkBoltStore_DeleteRange(b *testing.B) {
	store := testBoltStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.DeleteRange(b, store)
}

func BenchmarkBoltStore_Set(b *testing.B) {
	store := testBoltStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.Set(b, store)
}

func BenchmarkBoltStore_Get(b *testing.B) {
	store := testBoltStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.Get(b, store)
}

func BenchmarkBoltStore_SetUint64(b *testing.B) {
	store := testBoltStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.SetUint64(b, store)
}

func BenchmarkBoltStore_GetUint64(b *testing.B) {
	store := testBoltStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.GetUint64(b, store)
}

func testBoltStore(b *testing.B) *BoltStore {
	b.Helper()
	path := mustTempFile(b)
	store, err := NewBoltStore(path)
	if err != nil {
		b.Fatalf("failed to create store: %v", err)
	}
	store.path = path
	return store
}
