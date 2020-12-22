package legacy

import (
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
