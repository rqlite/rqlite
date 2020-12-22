package legacy

import (
	"reflect"
	"testing"

	"github.com/rqlite/rqlite/command"
)

func Test_SimpleExecute(t *testing.T) {
	// "CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT, age INTEGER)" timings=true
	b := []byte{123, 34, 115, 117, 98, 34, 58, 123, 34, 113, 117, 101, 114, 105, 101, 115, 34, 58, 91, 34, 67, 82, 69, 65, 84, 69, 32, 84, 65, 66, 76, 69, 32, 102, 111, 111, 32, 40, 105, 100, 32, 73, 78, 84, 69, 71, 69, 82, 32, 78, 79, 84, 32, 78, 85, 76, 76, 32, 80, 82, 73, 77, 65, 82, 89, 32, 75, 69, 89, 44, 32, 110, 97, 109, 101, 32, 84, 69, 88, 84, 44, 32, 97, 103, 101, 32, 73, 78, 84, 69, 71, 69, 82, 41, 34, 93, 44, 34, 80, 97, 114, 97, 109, 101, 116, 101, 114, 115, 34, 58, 91, 110, 117, 108, 108, 93, 44, 34, 116, 105, 109, 105, 110, 103, 115, 34, 58, 116, 114, 117, 101, 125, 125}

	var c command.Command
	var er command.ExecuteRequest
	if err := Unmarshal(b, &c); err != nil {
		t.Fatalf("failed to Unmarshal: %s", err)
	}

	if c.Type != command.Command_COMMAND_TYPE_EXECUTE {
		t.Fatalf("incorrect command type: %s", c.Type)
	}
	if err := command.UnmarshalSubCommand(&c, &er); err != nil {
		t.Fatalf("failed to Unmarshal subcommand: %s", err)
	}
	if !er.Timings {
		t.Fatalf("timings not set")
	}
	if er.Request.Transaction {
		t.Fatalf("transaction set")
	}
	if n := len(er.Request.Statements); n != 1 {
		t.Fatalf("incorrect number of statments: %d", n)
	}
	if s := er.Request.Statements[0].Sql; s != "CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT, age INTEGER)" {
		t.Fatalf("incorrect SQL: %s", s)
	}
}

func Test_SimpleQuery(t *testing.T) {
	// "SELECT * FROM foo", timings=true
	b := []byte{123, 34, 116, 121, 112, 34, 58, 49, 44, 34, 115, 117, 98, 34, 58, 123, 34, 113, 117, 101, 114, 105, 101, 115, 34, 58, 91, 34, 83, 69, 76, 69, 67, 84, 32, 42, 32, 70, 82, 79, 77, 32, 102, 111, 111, 34, 93, 44, 34, 80, 97, 114, 97, 109, 101, 116, 101, 114, 115, 34, 58, 91, 110, 117, 108, 108, 93, 44, 34, 116, 105, 109, 105, 110, 103, 115, 34, 58, 116, 114, 117, 101, 125, 125}

	var c command.Command
	var qr command.QueryRequest
	if err := Unmarshal(b, &c); err != nil {
		t.Fatalf("failed to Unmarshal: %s", err)
	}

	if c.Type != command.Command_COMMAND_TYPE_QUERY {
		t.Fatalf("incorrect command type: %s", c.Type)
	}
	if err := command.UnmarshalSubCommand(&c, &qr); err != nil {
		t.Fatalf("failed to Unmarshal subcommand: %s", err)
	}
	if !qr.Timings {
		t.Fatalf("timings not set")
	}
	if qr.Request.Transaction {
		t.Fatalf("transaction set")
	}
	if n := len(qr.Request.Statements); n != 1 {
		t.Fatalf("incorrect number of statments: %d", n)
	}
	if s := qr.Request.Statements[0].Sql; s != "SELECT * FROM foo" {
		t.Fatalf("incorrect SQL: %s", s)
	}
}

func Test_SingleParameterized(t *testing.T) {
	// ["INSERT INTO foo(name, age) VALUES(?, ?)", "fiona", 20], timings=true
	b := []byte{123, 34, 115, 117, 98, 34, 58, 123, 34, 113, 117, 101, 114, 105, 101, 115, 34, 58, 91, 34, 73, 78, 83, 69, 82, 84, 32, 73, 78, 84, 79, 32, 102, 111, 111, 40, 110, 97, 109, 101, 44, 32, 97, 103, 101, 41, 32, 86, 65, 76, 85, 69, 83, 40, 63, 44, 32, 63, 41, 34, 93, 44, 34, 80, 97, 114, 97, 109, 101, 116, 101, 114, 115, 34, 58, 91, 91, 34, 102, 105, 111, 110, 97, 34, 44, 50, 48, 93, 93, 44, 34, 116, 105, 109, 105, 110, 103, 115, 34, 58, 116, 114, 117, 101, 125, 125}

	var c command.Command
	var er command.ExecuteRequest
	if err := Unmarshal(b, &c); err != nil {
		t.Fatalf("failed to Unmarshal: %s", err)
	}

	if c.Type != command.Command_COMMAND_TYPE_EXECUTE {
		t.Fatalf("incorrect command type: %s", c.Type)
	}
	if err := command.UnmarshalSubCommand(&c, &er); err != nil {
		t.Fatalf("failed to Unmarshal subcommand: %s", err)
	}
	if !er.Timings {
		t.Fatalf("timings not set")
	}
	if er.Request.Transaction {
		t.Fatalf("transaction set")
	}
	if n := len(er.Request.Statements); n != 1 {
		t.Fatalf("incorrect number of statments: %d", n)
	}
	if s := er.Request.Statements[0].Sql; s != "INSERT INTO foo(name, age) VALUES(?, ?)" {
		t.Fatalf("incorrect SQL: %s", s)
	}
	if l := len(er.Request.Statements[0].Parameters); l != 2 {
		t.Fatalf("incorrect number of parameters: %d", l)
	}
	if v := er.Request.Statements[0].Parameters[0].GetS(); v != "fiona" {
		t.Fatalf("incorrect value for 1st parameter: %s", v)
	}
	if v := er.Request.Statements[0].Parameters[1].GetD(); v != 20 {
		t.Fatalf("incorrect value for 2nd parameter: %f", v)
	}
}

func Test_MultipleParameterized(t *testing.T) {
	// ["INSERT INTO foo(name, age) VALUES(?, ?)", "fiona", 20], ["INSERT INTO bar(name, age, address) VALUES(?, ?, ?)", "declan", 5, "galway"], timings=true
	b := []byte{123, 34, 115, 117, 98, 34, 58, 123, 34, 113, 117, 101, 114, 105, 101, 115, 34, 58, 91, 34, 73, 78, 83, 69, 82, 84, 32, 73, 78, 84, 79, 32, 102, 111, 111, 40, 110, 97, 109, 101, 44, 32, 97, 103, 101, 41, 32, 86, 65, 76, 85, 69, 83, 40, 63, 44, 32, 63, 41, 34, 44, 34, 73, 78, 83, 69, 82, 84, 32, 73, 78, 84, 79, 32, 98, 97, 114, 40, 110, 97, 109, 101, 44, 32, 97, 103, 101, 44, 32, 97, 100, 100, 114, 101, 115, 115, 41, 32, 86, 65, 76, 85, 69, 83, 40, 63, 44, 32, 63, 44, 32, 63, 41, 34, 93, 44, 34, 80, 97, 114, 97, 109, 101, 116, 101, 114, 115, 34, 58, 91, 91, 34, 102, 105, 111, 110, 97, 34, 44, 50, 48, 93, 44, 91, 34, 100, 101, 99, 108, 97, 110, 34, 44, 53, 44, 34, 103, 97, 108, 119, 97, 121, 34, 93, 93, 44, 34, 116, 105, 109, 105, 110, 103, 115, 34, 58, 116, 114, 117, 101, 125, 125}

	var c command.Command
	var er command.ExecuteRequest
	if err := Unmarshal(b, &c); err != nil {
		t.Fatalf("failed to Unmarshal: %s", err)
	}

	if c.Type != command.Command_COMMAND_TYPE_EXECUTE {
		t.Fatalf("incorrect command type: %s", c.Type)
	}
	if err := command.UnmarshalSubCommand(&c, &er); err != nil {
		t.Fatalf("failed to Unmarshal subcommand: %s", err)
	}
	if !er.Timings {
		t.Fatalf("timings not set")
	}
	if er.Request.Transaction {
		t.Fatalf("transaction set")
	}
	if n := len(er.Request.Statements); n != 2 {
		t.Fatalf("incorrect number of statments: %d", n)
	}

	if s := er.Request.Statements[0].Sql; s != "INSERT INTO foo(name, age) VALUES(?, ?)" {
		t.Fatalf("incorrect SQL: %s", s)
	}
	if l := len(er.Request.Statements[0].Parameters); l != 2 {
		t.Fatalf("incorrect number of parameters: %d", l)
	}
	if v := er.Request.Statements[0].Parameters[0].GetS(); v != "fiona" {
		t.Fatalf("incorrect value for 1st parameter: %s", v)
	}
	if v := er.Request.Statements[0].Parameters[1].GetD(); v != 20 {
		t.Fatalf("incorrect value for 2nd parameter: %f", v)
	}

	if s := er.Request.Statements[1].Sql; s != "INSERT INTO bar(name, age, address) VALUES(?, ?, ?)" {
		t.Fatalf("incorrect SQL: %s", s)
	}
	if l := len(er.Request.Statements[1].Parameters); l != 3 {
		t.Fatalf("incorrect number of parameters: %d", l)
	}
	if v := er.Request.Statements[1].Parameters[0].GetS(); v != "declan" {
		t.Fatalf("incorrect value for 1st parameter: %s", v)
	}
	if v := er.Request.Statements[1].Parameters[1].GetD(); v != 5 {
		t.Fatalf("incorrect value for 2nd parameter: %f", v)
	}
	if v := er.Request.Statements[1].Parameters[2].GetS(); v != "galway" {
		t.Fatalf("incorrect value for 1st parameter: %s", v)
	}
}

func Test_MetadataSet(t *testing.T) {
	b := []byte{123, 34, 116, 121, 112, 34, 58, 50, 44, 34, 115, 117, 98, 34, 58, 123, 34, 114, 97, 102, 116, 95, 105, 100, 34, 58, 34, 108, 111, 99, 97, 108, 104, 111, 115, 116, 58, 52, 48, 48, 50, 34, 44, 34, 100, 97, 116, 97, 34, 58, 123, 34, 97, 112, 105, 95, 97, 100, 100, 114, 34, 58, 34, 108, 111, 99, 97, 108, 104, 111, 115, 116, 58, 52, 48, 48, 49, 34, 44, 34, 97, 112, 105, 95, 112, 114, 111, 116, 111, 34, 58, 34, 104, 116, 116, 112, 34, 125, 125, 125}

	var c command.Command
	var ms command.MetadataSet

	if err := Unmarshal(b, &c); err != nil {
		t.Fatalf("failed to Unmarshal: %s", err)
	}

	if c.Type != command.Command_COMMAND_TYPE_METADATA_SET {
		t.Fatalf("incorrect command type: %s", c.Type)
	}
	if err := command.UnmarshalSubCommand(&c, &ms); err != nil {
		t.Fatalf("failed to Unmarshal subcommand: %s", err)
	}
	if id := ms.RaftId; id != "localhost:4002" {
		t.Fatalf("incorrect Raft ID: %s", id)
	}
	if !reflect.DeepEqual(ms.Data, map[string]string{"api_addr": "localhost:4001", "api_proto": "http"}) {
		t.Fatalf("map is incorrect: %s", ms.Data)
	}
}

func Test_MetadataDelete(t *testing.T) {
	b := []byte{123, 34, 116, 121, 112, 34, 58, 51, 44, 34, 115, 117, 98, 34, 58, 34, 108, 111, 99, 97, 108, 104, 111, 115, 116, 58, 52, 48, 48, 52, 34, 125}

	var c command.Command
	var md command.MetadataDelete

	if err := Unmarshal(b, &c); err != nil {
		t.Fatalf("failed to Unmarshal: %s", err)
	}

	if c.Type != command.Command_COMMAND_TYPE_METADATA_DELETE {
		t.Fatalf("incorrect command type: %s", c.Type)
	}
	if err := command.UnmarshalSubCommand(&c, &md); err != nil {
		t.Fatalf("failed to Unmarshal subcommand: %s", err)
	}
	if id := md.RaftId; id != "localhost:4004" {
		t.Fatalf("incorrect Raft ID: %s", id)
	}
}
