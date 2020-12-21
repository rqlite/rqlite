package command

import (
	"testing"
)

func Test_NewRequestMarshaler(t *testing.T) {
	rm := NewRequestMarshaler()
	if rm == nil {
		t.Fatal("failed to create Request marshaler")
	}
}

func Test_MarshalUncompressed(t *testing.T) {
	rm := NewRequestMarshaler()
	r := &QueryRequest{
		Request: &Request{
			Statements: []*Statement{
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

	c := &Command{
		Type:       Command_COMMAND_TYPE_QUERY,
		SubCommand: b,
		Compressed: comp,
	}

	b, err = Marshal(c)
	if err != nil {
		t.Fatalf("failed to marshal Command: %s", err)
	}

	var nc Command
	if err := Unmarshal(b, &nc); err != nil {
		t.Fatalf("failed to unmarshal Command: %s", err)
	}
	if nc.Type != Command_COMMAND_TYPE_QUERY {
		t.Fatalf("unmarshaled command has wrong type: %s", nc.Type)
	}
	if nc.Compressed {
		t.Fatal("Unmarshaled QueryRequest incorrectly marked as compressed")
	}

	var nr QueryRequest
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
