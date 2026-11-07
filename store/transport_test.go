package store

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func Test_NewTransport(t *testing.T) {
	if NewTransport(nil) == nil {
		t.Fatal("failed to create new Transport")
	}
}

func Test_NewNodeTransport(t *testing.T) {
	nt := NewNodeTransport(nil, false, t.TempDir())
	if nt == nil {
		t.Fatal("failed to create new NodeTransport")
	}
	if err := nt.Close(); err != nil {
		t.Fatalf("failed to close NodeTransport: %s", err.Error())
	}
	if err := nt.Close(); err != nil {
		t.Fatalf("failed to double-close NodeTransport: %s", err.Error())
	}
}

// Test_NodeTransport_InstallSnapshot_Compressed sends a snapshot between two
// NodeTransports with compression enabled, and checks that the receiving side
// gets back exactly the bytes, and the size, that Raft expects. Incompressible
// data is the important case: zstd cannot shrink it, so the compressed stream
// is larger than the snapshot itself.
func Test_NodeTransport_InstallSnapshot_Compressed(t *testing.T) {
	incompressible := make([]byte, 1<<20)
	if _, err := rand.Read(incompressible); err != nil {
		t.Fatalf("failed to generate random data: %s", err.Error())
	}
	for _, tc := range []struct {
		name    string
		payload []byte
	}{
		{"compressible", bytes.Repeat([]byte("abcd"), 1<<18)},
		{"incompressible", incompressible},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testInstallSnapshotCompressed(t, tc.payload)
		})
	}
}

func testInstallSnapshotCompressed(t *testing.T, payload []byte) {
	t.Helper()
	rxLy := mustMockLayer("localhost:0")
	rx := NewNodeTransport(raft.NewNetworkTransport(NewTransport(rxLy), 1, 5*time.Second, nil), true, t.TempDir())
	defer rx.Close()
	txLy := mustMockLayer("localhost:0")
	tx := NewNodeTransport(raft.NewNetworkTransport(NewTransport(txLy), 1, 5*time.Second, nil), true, t.TempDir())
	defer tx.Close()

	// Stand in for Raft on the receiving side. Raft copies the stream into a
	// snapshot sink and rejects the snapshot unless the number of bytes copied
	// equals the size in the request.
	type received struct {
		data []byte
		size int64
		err  error
	}
	rxCh := make(chan received, 1)
	rpcCh := rx.Consumer()
	go func() {
		rpc := <-rpcCh
		req := rpc.Command.(*raft.InstallSnapshotRequest)
		data, err := io.ReadAll(rpc.Reader)
		rpc.Respond(&raft.InstallSnapshotResponse{Success: err == nil}, err)
		rxCh <- received{data: data, size: req.Size, err: err}
	}()

	args := &raft.InstallSnapshotRequest{
		RPCHeader:       raft.RPCHeader{ProtocolVersion: raft.ProtocolVersionMax},
		SnapshotVersion: raft.SnapshotVersionMax,
		Size:            int64(len(payload)),
	}
	var resp raft.InstallSnapshotResponse
	if err := tx.InstallSnapshot("rx", raft.ServerAddress(rxLy.Addr().String()), args, &resp,
		bytes.NewReader(payload)); err != nil {
		t.Fatalf("failed to install snapshot: %s", err.Error())
	}

	var got received
	select {
	case got = <-rxCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for receiver to read snapshot")
	}
	if got.err != nil {
		t.Fatalf("receiver failed to read snapshot stream: %s", got.err.Error())
	}
	if int64(len(got.data)) != got.size {
		t.Fatalf("receiver read %d bytes but request size is %d", len(got.data), got.size)
	}
	if !bytes.Equal(got.data, payload) {
		t.Fatalf("receiver got different data from that sent")
	}
}
