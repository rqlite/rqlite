package command

import (
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
