package raftdb

import (
	"bytes"
	"testing"
	"time"

	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
)

// The benchmarks below isolate the MsgPack encode/decode layer, comparing
// this package's optimized implementation against the upstream raft-boltdb
// implementation (new Handle + bytes.Buffer + io.Writer per call).

func benchLog() *raft.Log {
	return &raft.Log{
		Index:      12345,
		Term:       67,
		Type:       raft.LogCommand,
		Data:       bytes.Repeat([]byte("x"), 256),
		Extensions: nil,
		AppendedAt: time.Unix(1700000000, 123456789).UTC(),
	}
}

// BenchmarkEncodeOptimized measures this package's encodeMsgPack.
func BenchmarkEncodeOptimized(b *testing.B) {
	log := benchLog()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := encodeMsgPack(log); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEncodeUpstream measures the upstream-style encoding.
func BenchmarkEncodeUpstream(b *testing.B) {
	log := benchLog()
	b.ReportAllocs()
	for b.Loop() {
		buf := bytes.NewBuffer(nil)
		hd := codec.MsgpackHandle{}
		enc := codec.NewEncoder(buf, &hd)
		if err := enc.Encode(log); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecodeOptimized measures this package's decodeMsgPack.
func BenchmarkDecodeOptimized(b *testing.B) {
	log := benchLog()
	buf, err := encodeMsgPack(log)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		var out raft.Log
		if err := decodeMsgPack(buf, &out); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecodeUpstream measures the upstream-style decoding.
func BenchmarkDecodeUpstream(b *testing.B) {
	log := benchLog()
	var buf bytes.Buffer
	hd := codec.MsgpackHandle{}
	if err := codec.NewEncoder(&buf, &hd).Encode(log); err != nil {
		b.Fatal(err)
	}
	enc := buf.Bytes()
	b.ReportAllocs()
	for b.Loop() {
		r := bytes.NewBuffer(enc)
		hd := codec.MsgpackHandle{}
		var out raft.Log
		if err := codec.NewDecoder(r, &hd).Decode(&out); err != nil {
			b.Fatal(err)
		}
	}
}
