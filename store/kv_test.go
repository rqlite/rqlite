package store

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/command/proto"
)

func mustNewLeaderStore(t *testing.T) (*Store, func()) {
	t.Helper()
	s, ln := mustNewStore(t)
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open store: %s", err)
	}
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap store: %s", err)
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("error waiting for leader: %s", err)
	}
	cleanup := func() {
		s.Close(true)
		ln.Close()
	}
	return s, cleanup
}

func kvGetNone(t *testing.T, s *Store, key string) ([]byte, error) {
	t.Helper()
	v, _, err := s.GetKey(context.Background(), key, proto.ConsistencyLevel_NONE, 0, false, 0)
	return v, err
}

func kvTableExists(t *testing.T, s *Store) bool {
	t.Helper()
	qr := &proto.QueryRequest{
		Request: &proto.Request{
			Statements: []*proto.Statement{{
				Sql: `SELECT name FROM sqlite_master WHERE type='table' AND name='__rqlite_kv'`,
			}},
		},
		Level: proto.ConsistencyLevel_NONE,
	}
	rows, _, _, err := s.Query(context.Background(), qr)
	if err != nil {
		t.Fatalf("sqlite_master query failed: %s", err)
	}
	return len(rows) > 0 && len(rows[0].Values) > 0
}

func Test_KV_SetGet_RoundTrip(t *testing.T) {
	s, cleanup := mustNewLeaderStore(t)
	defer cleanup()

	idx, err := s.SetKey(context.Background(), "alpha", []byte("hello"))
	if err != nil {
		t.Fatalf("SetKey failed: %s", err)
	}
	if idx == 0 {
		t.Fatalf("SetKey returned zero raft index")
	}

	v, err := kvGetNone(t, s, "alpha")
	if err != nil {
		t.Fatalf("GetKey failed: %s", err)
	}
	if !bytes.Equal(v, []byte("hello")) {
		t.Fatalf("GetKey returned %q, want %q", v, "hello")
	}
}

func Test_KV_Get_MissingKey(t *testing.T) {
	s, cleanup := mustNewLeaderStore(t)
	defer cleanup()

	// Create the table first by setting some other key.
	if _, err := s.SetKey(context.Background(), "present", []byte("v")); err != nil {
		t.Fatalf("SetKey failed: %s", err)
	}

	_, err := kvGetNone(t, s, "absent")
	if !errors.Is(err, ErrKVKeyNotFound) {
		t.Fatalf("GetKey on missing key returned %v, want ErrKVKeyNotFound", err)
	}
}

func Test_KV_Get_MissingTable(t *testing.T) {
	s, cleanup := mustNewLeaderStore(t)
	defer cleanup()

	_, err := kvGetNone(t, s, "anything")
	if !errors.Is(err, ErrKVKeyNotFound) {
		t.Fatalf("GetKey on fresh store returned %v, want ErrKVKeyNotFound", err)
	}
	if kvTableExists(t, s) {
		t.Fatalf("__rqlite_kv was created by a GetKey on missing table")
	}
}

func Test_KV_Set_Overwrite(t *testing.T) {
	s, cleanup := mustNewLeaderStore(t)
	defer cleanup()

	ctx := context.Background()
	if _, err := s.SetKey(ctx, "a", []byte("v1")); err != nil {
		t.Fatalf("first SetKey failed: %s", err)
	}
	if _, err := s.SetKey(ctx, "a", []byte("v2")); err != nil {
		t.Fatalf("second SetKey failed: %s", err)
	}
	v, err := kvGetNone(t, s, "a")
	if err != nil {
		t.Fatalf("GetKey failed: %s", err)
	}
	if !bytes.Equal(v, []byte("v2")) {
		t.Fatalf("GetKey returned %q, want %q", v, "v2")
	}
}

func Test_KV_Delete_MissingTable(t *testing.T) {
	s, cleanup := mustNewLeaderStore(t)
	defer cleanup()

	_, err := s.DeleteKey(context.Background(), "x")
	if !errors.Is(err, ErrKVKeyNotFound) {
		t.Fatalf("DeleteKey on fresh store returned %v, want ErrKVKeyNotFound", err)
	}
	if kvTableExists(t, s) {
		t.Fatalf("__rqlite_kv was created by DeleteKey on missing table")
	}
}

func Test_KV_Delete_MissingKey(t *testing.T) {
	s, cleanup := mustNewLeaderStore(t)
	defer cleanup()

	if _, err := s.SetKey(context.Background(), "present", []byte("v")); err != nil {
		t.Fatalf("SetKey failed: %s", err)
	}
	_, err := s.DeleteKey(context.Background(), "absent")
	if !errors.Is(err, ErrKVKeyNotFound) {
		t.Fatalf("DeleteKey on missing key returned %v, want ErrKVKeyNotFound", err)
	}
}

func Test_KV_Delete_DropsTableOnLastKey(t *testing.T) {
	s, cleanup := mustNewLeaderStore(t)
	defer cleanup()

	ctx := context.Background()
	if _, err := s.SetKey(ctx, "only", []byte("v")); err != nil {
		t.Fatalf("SetKey failed: %s", err)
	}
	if !kvTableExists(t, s) {
		t.Fatalf("__rqlite_kv not created by SetKey")
	}

	if _, err := s.DeleteKey(ctx, "only"); err != nil {
		t.Fatalf("DeleteKey failed: %s", err)
	}
	if kvTableExists(t, s) {
		t.Fatalf("__rqlite_kv was not dropped after last DeleteKey")
	}

	_, err := kvGetNone(t, s, "only")
	if !errors.Is(err, ErrKVKeyNotFound) {
		t.Fatalf("GetKey after drop returned %v, want ErrKVKeyNotFound", err)
	}
}

func Test_KV_Delete_KeepsTableWhenOthersRemain(t *testing.T) {
	s, cleanup := mustNewLeaderStore(t)
	defer cleanup()

	ctx := context.Background()
	if _, err := s.SetKey(ctx, "a", []byte("av")); err != nil {
		t.Fatalf("SetKey a failed: %s", err)
	}
	if _, err := s.SetKey(ctx, "b", []byte("bv")); err != nil {
		t.Fatalf("SetKey b failed: %s", err)
	}
	if _, err := s.DeleteKey(ctx, "a"); err != nil {
		t.Fatalf("DeleteKey a failed: %s", err)
	}
	if !kvTableExists(t, s) {
		t.Fatalf("__rqlite_kv was incorrectly dropped while another key remained")
	}
	v, err := kvGetNone(t, s, "b")
	if err != nil {
		t.Fatalf("GetKey b after partial delete failed: %s", err)
	}
	if !bytes.Equal(v, []byte("bv")) {
		t.Fatalf("GetKey b returned %q, want %q", v, "bv")
	}
}

func Test_KV_RecreateAfterDrop(t *testing.T) {
	s, cleanup := mustNewLeaderStore(t)
	defer cleanup()

	ctx := context.Background()
	if _, err := s.SetKey(ctx, "k", []byte("v1")); err != nil {
		t.Fatalf("SetKey failed: %s", err)
	}
	if _, err := s.DeleteKey(ctx, "k"); err != nil {
		t.Fatalf("DeleteKey failed: %s", err)
	}
	if kvTableExists(t, s) {
		t.Fatalf("table not dropped after last delete")
	}
	if _, err := s.SetKey(ctx, "k", []byte("v2")); err != nil {
		t.Fatalf("SetKey after drop failed: %s", err)
	}
	v, err := kvGetNone(t, s, "k")
	if err != nil {
		t.Fatalf("GetKey after recreate failed: %s", err)
	}
	if !bytes.Equal(v, []byte("v2")) {
		t.Fatalf("GetKey returned %q after recreate, want %q", v, "v2")
	}
}

func Test_KV_EmptyKey(t *testing.T) {
	s, cleanup := mustNewLeaderStore(t)
	defer cleanup()

	ctx := context.Background()
	if _, err := s.SetKey(ctx, "", []byte("v")); !errors.Is(err, ErrKVEmptyKey) {
		t.Fatalf("SetKey('') returned %v, want ErrKVEmptyKey", err)
	}
	if _, _, err := s.GetKey(ctx, "", proto.ConsistencyLevel_NONE, 0, false, 0); !errors.Is(err, ErrKVEmptyKey) {
		t.Fatalf("GetKey('') returned %v, want ErrKVEmptyKey", err)
	}
	if _, err := s.DeleteKey(ctx, ""); !errors.Is(err, ErrKVEmptyKey) {
		t.Fatalf("DeleteKey('') returned %v, want ErrKVEmptyKey", err)
	}
}

func Test_KV_BinaryValue(t *testing.T) {
	s, cleanup := mustNewLeaderStore(t)
	defer cleanup()

	val := make([]byte, 256)
	for i := range val {
		val[i] = byte(i)
	}
	if _, err := s.SetKey(context.Background(), "bin", val); err != nil {
		t.Fatalf("SetKey failed: %s", err)
	}
	got, err := kvGetNone(t, s, "bin")
	if err != nil {
		t.Fatalf("GetKey failed: %s", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("binary round-trip mismatch")
	}
}

func Test_KV_LargeValue(t *testing.T) {
	s, cleanup := mustNewLeaderStore(t)
	defer cleanup()

	val := bytes.Repeat([]byte{0xab}, 1<<20) // 1 MiB
	if _, err := s.SetKey(context.Background(), "big", val); err != nil {
		t.Fatalf("SetKey failed: %s", err)
	}
	got, err := kvGetNone(t, s, "big")
	if err != nil {
		t.Fatalf("GetKey failed: %s", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("large value round-trip mismatch (lens: got %d want %d)", len(got), len(val))
	}
}

func Test_KV_EmptyValue(t *testing.T) {
	s, cleanup := mustNewLeaderStore(t)
	defer cleanup()

	if _, err := s.SetKey(context.Background(), "empty", []byte{}); err != nil {
		t.Fatalf("SetKey with empty value failed: %s", err)
	}
	got, err := kvGetNone(t, s, "empty")
	if err != nil {
		t.Fatalf("GetKey for empty value failed: %s", err)
	}
	if len(got) != 0 {
		t.Fatalf("empty-value round-trip returned %d bytes, want 0", len(got))
	}
}

func Test_KV_RaftIndexMonotonic(t *testing.T) {
	s, cleanup := mustNewLeaderStore(t)
	defer cleanup()

	ctx := context.Background()
	idx1, err := s.SetKey(ctx, "a", []byte("v1"))
	if err != nil {
		t.Fatalf("SetKey a failed: %s", err)
	}
	idx2, err := s.SetKey(ctx, "b", []byte("v2"))
	if err != nil {
		t.Fatalf("SetKey b failed: %s", err)
	}
	idx3, err := s.DeleteKey(ctx, "a")
	if err != nil {
		t.Fatalf("DeleteKey a failed: %s", err)
	}
	if !(idx1 < idx2 && idx2 < idx3) {
		t.Fatalf("raft indexes not monotonic: %d, %d, %d", idx1, idx2, idx3)
	}
}

func Test_KV_DirectSQLRead(t *testing.T) {
	s, cleanup := mustNewLeaderStore(t)
	defer cleanup()

	val := []byte{0x00, 0x01, 0x02, 0xff}
	if _, err := s.SetKey(context.Background(), "k", val); err != nil {
		t.Fatalf("SetKey failed: %s", err)
	}

	qr := &proto.QueryRequest{
		Request: &proto.Request{
			Statements: []*proto.Statement{{
				Sql: `SELECT v FROM __rqlite_kv WHERE k = 'k'`,
			}},
		},
		Level: proto.ConsistencyLevel_NONE,
	}
	rows, _, _, err := s.Query(context.Background(), qr)
	if err != nil {
		t.Fatalf("direct SQL query failed: %s", err)
	}
	if len(rows) == 0 || len(rows[0].Values) == 0 || len(rows[0].Values[0].Parameters) == 0 {
		t.Fatalf("direct SQL query returned no rows")
	}
	got := paramAsBytes(rows[0].Values[0].Parameters[0])
	if !bytes.Equal(got, val) {
		t.Fatalf("direct SQL read returned %v, want %v", got, val)
	}
}
