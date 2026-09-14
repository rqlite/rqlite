package raftdb

import (
	"encoding/binary"

	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
)

// msgpackHandle is shared by every Encoder and Decoder created in this
// package.
//
// codec.MsgpackHandle's lazily initialized state (format flags, and the
// reflect-derived codec function cache in rtidFns) is guarded by a mutex /
// atomic.Value, so one shared handle is safe for concurrent use. Sharing it
// means the expensive per-type reflection work happens exactly once per
// process, instead of being rebuilt for every raft log stored or retrieved.
// Upstream raft-boltdb constructs a fresh handle per call and pays that cost
// every time.
//
// The zero value of MsgpackHandle matches the default configuration used by
// upstream raft-boltdb.
//
// The v2 module is required (not v1): its decoder understands the legacy
// time.Time encoding emitted by go-msgpack v0.5.5 — the version originally
// pinned by upstream raft-boltdb, and therefore the format found in existing
// rqlite raft.db files. The v1 decoder rejects that format. Encoding is
// byte-identical between v1.1.5 and v2.1.5, so the write path is unchanged.
var msgpackHandle codec.MsgpackHandle

// Decode reverses the encode operation on a byte slice input.
//
// Unlike upstream, which wraps the input in a bytes.Buffer and creates both a
// new Handle and a new Decoder per call, this uses codec.NewDecoderBytes for
// zero-copy decoding directly from the input slice and shares a
// package-level handle.
//
// The wire format is unchanged, so existing databases remain fully readable.
func decodeMsgPack(buf []byte, out *raft.Log) error {
	return codec.NewDecoderBytes(buf, &msgpackHandle).Decode(out)
}

// Encode writes an encoded raft.Log to a new byte slice.
//
// Unlike upstream, which creates a new Handle and an io.Writer-based encoder
// over a bytes.Buffer per call, this encodes directly into the destination
// slice via codec.NewEncoderBytes (no bytes.Buffer allocation, no io
// indirection) and shares a package-level handle.
//
// The wire format is unchanged, so databases remain fully interoperable with
// upstream raft-boltdb.
func encodeMsgPack(in *raft.Log) ([]byte, error) {
	var buf []byte
	err := codec.NewEncoderBytes(&buf, &msgpackHandle).Encode(in)
	return buf, err
}

// Converts bytes to an integer
func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// Converts a uint to a byte slice
func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}
