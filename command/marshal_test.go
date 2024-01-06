package command

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"os"
	"sync"
	"testing"

	"github.com/rqlite/rqlite/v8/command/proto"
	pb "google.golang.org/protobuf/proto"
)

func Test_NewRequestMarshaler(t *testing.T) {
	r := NewRequestMarshaler()
	if r == nil {
		t.Fatal("failed to create Request marshaler")
	}
}

func Test_MarshalUncompressed(t *testing.T) {
	rm := NewRequestMarshaler()
	r := &proto.QueryRequest{
		Request: &proto.Request{
			Statements: []*proto.Statement{
				{
					Sql: `INSERT INTO "names" VALUES(1,'bob','123-45-678')`,
				},
			},
		},
		Timings:   true,
		Freshness: 100,
	}

	b, comp, err := rm.Marshal(r)
	if err != nil {
		t.Fatalf("failed to marshal QueryRequest: %s", err)
	}
	if comp {
		t.Fatal("Marshaled QueryRequest incorrectly compressed")
	}

	c := &proto.Command{
		Type:       proto.Command_COMMAND_TYPE_QUERY,
		SubCommand: b,
		Compressed: comp,
	}

	b, err = Marshal(c)
	if err != nil {
		t.Fatalf("failed to marshal Command: %s", err)
	}

	var nc proto.Command
	if err := Unmarshal(b, &nc); err != nil {
		t.Fatalf("failed to unmarshal Command: %s", err)
	}
	if nc.Type != proto.Command_COMMAND_TYPE_QUERY {
		t.Fatalf("unmarshaled command has wrong type: %s", nc.Type)
	}
	if nc.Compressed {
		t.Fatal("Unmarshaled QueryRequest incorrectly marked as compressed")
	}

	var nr proto.QueryRequest
	if err := UnmarshalSubCommand(&nc, &nr); err != nil {
		t.Fatalf("failed to unmarshal sub command: %s", err)
	}
	if nr.Timings != r.Timings {
		t.Fatalf("unmarshaled timings incorrect")
	}
	if nr.Freshness != r.Freshness {
		t.Fatalf("unmarshaled Freshness incorrect")
	}
	if len(nr.Request.Statements) != 1 {
		t.Fatalf("unmarshaled number of statements incorrect")
	}
	if nr.Request.Statements[0].Sql != `INSERT INTO "names" VALUES(1,'bob','123-45-678')` {
		t.Fatalf("unmarshaled SQL incorrect")
	}
}

func Test_MarshalCompressedBatch(t *testing.T) {
	rm := NewRequestMarshaler()
	rm.BatchThreshold = 1
	rm.ForceCompression = true

	r := &proto.QueryRequest{
		Request: &proto.Request{
			Statements: []*proto.Statement{
				{
					Sql: `INSERT INTO "names" VALUES(1,'bob','123-45-678')`,
				},
			},
		},
		Timings:   true,
		Freshness: 100,
	}

	b, comp, err := rm.Marshal(r)
	if err != nil {
		t.Fatalf("failed to marshal QueryRequest: %s", err)
	}
	if !comp {
		t.Fatal("Marshaled QueryRequest wasn't compressed")
	}

	c := &proto.Command{
		Type:       proto.Command_COMMAND_TYPE_QUERY,
		SubCommand: b,
		Compressed: comp,
	}

	b, err = Marshal(c)
	if err != nil {
		t.Fatalf("failed to marshal Command: %s", err)
	}

	var nc proto.Command
	if err := Unmarshal(b, &nc); err != nil {
		t.Fatalf("failed to unmarshal Command: %s", err)
	}
	if nc.Type != proto.Command_COMMAND_TYPE_QUERY {
		t.Fatalf("unmarshaled command has wrong type: %s", nc.Type)
	}
	if !nc.Compressed {
		t.Fatal("Unmarshaled QueryRequest incorrectly marked as uncompressed")
	}

	var nr proto.QueryRequest
	if err := UnmarshalSubCommand(&nc, &nr); err != nil {
		t.Fatalf("failed to unmarshal sub command: %s", err)
	}
	if !pb.Equal(&nr, r) {
		t.Fatal("Original and unmarshaled Query Request are not equal")
	}
}

func Test_MarshalCompressedSize(t *testing.T) {
	rm := NewRequestMarshaler()
	rm.SizeThreshold = 1
	rm.ForceCompression = true

	r := &proto.QueryRequest{
		Request: &proto.Request{
			Statements: []*proto.Statement{
				{
					Sql: `INSERT INTO "names" VALUES(1,'bob','123-45-678')`,
				},
			},
		},
		Timings:   true,
		Freshness: 100,
	}

	b, comp, err := rm.Marshal(r)
	if err != nil {
		t.Fatalf("failed to marshal QueryRequest: %s", err)
	}
	if !comp {
		t.Fatal("Marshaled QueryRequest wasn't compressed")
	}

	c := &proto.Command{
		Type:       proto.Command_COMMAND_TYPE_QUERY,
		SubCommand: b,
		Compressed: comp,
	}

	b, err = Marshal(c)
	if err != nil {
		t.Fatalf("failed to marshal Command: %s", err)
	}

	var nc proto.Command
	if err := Unmarshal(b, &nc); err != nil {
		t.Fatalf("failed to unmarshal Command: %s", err)
	}
	if nc.Type != proto.Command_COMMAND_TYPE_QUERY {
		t.Fatalf("unmarshaled command has wrong type: %s", nc.Type)
	}
	if !nc.Compressed {
		t.Fatal("Unmarshaled QueryRequest incorrectly marked as uncompressed")
	}

	var nr proto.QueryRequest
	if err := UnmarshalSubCommand(&nc, &nr); err != nil {
		t.Fatalf("failed to unmarshal sub command: %s", err)
	}
	if !pb.Equal(&nr, r) {
		t.Fatal("Original and unmarshaled Query Request are not equal")
	}
}

func Test_MarshalWontCompressBatch(t *testing.T) {
	rm := NewRequestMarshaler()
	rm.BatchThreshold = 1

	r := &proto.QueryRequest{
		Request: &proto.Request{
			Statements: []*proto.Statement{
				{
					Sql: `INSERT INTO "names" VALUES(1,'bob','123-45-678')`,
				},
			},
		},
		Timings:   true,
		Freshness: 100,
	}

	_, comp, err := rm.Marshal(r)
	if err != nil {
		t.Fatalf("failed to marshal QueryRequest: %s", err)
	}
	if comp {
		t.Fatal("Marshaled QueryRequest was compressed")
	}
}

func Test_MarshalCompressedConcurrent(t *testing.T) {
	rm := NewRequestMarshaler()
	rm.SizeThreshold = 1
	rm.ForceCompression = true

	r := &proto.QueryRequest{
		Request: &proto.Request{
			Statements: []*proto.Statement{
				{
					Sql: `INSERT INTO "names" VALUES(1,'bob','123-45-678')`,
				},
			},
		},
		Timings:   true,
		Freshness: 100,
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, comp, err := rm.Marshal(r)
			if err != nil {
				t.Logf("failed to marshal QueryRequest: %s", err)
			}
			if !comp {
				t.Logf("Marshaled QueryRequest wasn't compressed")
			}
		}()
	}
	wg.Wait()
}

func Test_MarshalWontCompressSize(t *testing.T) {
	rm := NewRequestMarshaler()
	rm.SizeThreshold = 1

	r := &proto.QueryRequest{
		Request: &proto.Request{
			Statements: []*proto.Statement{
				{
					Sql: `INSERT INTO "names" VALUES(1,'bob','123-45-678')`,
				},
			},
		},
		Timings:   true,
		Freshness: 100,
	}

	_, comp, err := rm.Marshal(r)
	if err != nil {
		t.Fatalf("failed to marshal QueryRequest: %s", err)
	}
	if comp {
		t.Fatal("Marshaled QueryRequest was compressed")
	}
}

// Test_NewBackupHeader_FileNotFound tests NewBackupHeader for a non-existent file.
func Test_NewBackupHeader_FileNotFound(t *testing.T) {
	_, err := NewBackupHeader("nonexistent.file")
	if err == nil {
		t.Errorf("Expected an error for a non-existent file, but got nil")
	}
}

// Test_NewBackupHeader_OK tests NewBackupHeader for a valid file.
func Test_NewBackupHeader_OK(t *testing.T) {
	content := []byte("hello world")
	tmpfile := mustCreateTempFile()
	defer os.Remove(tmpfile)
	mustWriteFile(tmpfile, content)

	// Call NewBackupHeader
	header, err := NewBackupHeader(tmpfile)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Check the size
	if header.Size != int64(len(content)) {
		t.Errorf("Expected size %d, got %d", len(content), header.Size)
	}

	// Check the MD5 sum
	hasher := md5.New()
	hasher.Write(content)
	expectedMd5 := hasher.Sum(nil)
	if !bytes.Equal(header.Md5Sum, expectedMd5) {
		t.Errorf("Expected MD5 sum %s, got %s", hex.EncodeToString(expectedMd5), hex.EncodeToString(header.Md5Sum))
	}
}

// Test_WriteBackupHeaderTo_OK tests WriteBackupHeaderTo for successful writing.
func Test_WriteBackupHeaderTo_OK(t *testing.T) {
	// Create a sample BackupHeader
	header := &proto.BackupHeader{
		Version: 1,
		Size:    11,
		Md5Sum:  []byte("1234567890ab"),
	}

	var buf bytes.Buffer
	if err := WriteBackupHeaderTo(header, &buf); err != nil {
		t.Fatalf("WriteBackupHeaderTo failed: %v", err)
	}

	var length uint64
	if err := binary.Read(bytes.NewReader(buf.Bytes()[:8]), binary.LittleEndian, &length); err != nil {
		t.Fatalf("Failed to read length: %v", err)
	}

	// Now the specified length of bytes should be the header
	var newHeader proto.BackupHeader
	if err := UnmarshalBackupHeader(buf.Bytes()[8:8+length], &newHeader); err != nil {
		t.Fatalf("Failed to unmarshal header: %v", err)
	}

	if newHeader.Version != header.Version {
		t.Errorf("Expected version %d, got %d", header.Version, newHeader.Version)
	}

	if newHeader.Size != header.Size {
		t.Errorf("Expected size %d, got %d", header.Size, newHeader.Size)
	}

	if !bytes.Equal(newHeader.Md5Sum, header.Md5Sum) {
		t.Errorf("Expected MD5 sum %s, got %s", hex.EncodeToString(header.Md5Sum), hex.EncodeToString(newHeader.Md5Sum))
	}
}

func mustCreateTempFile() string {
	fd, err := os.CreateTemp("", "command-marshall-test")
	if err != nil {
		panic(err)
	}
	fd.Close()
	return fd.Name()
}

func mustWriteFile(path string, contents []byte) {
	err := os.WriteFile(path, contents, 0644)
	if err != nil {
		panic("failed to write to file")
	}
}
