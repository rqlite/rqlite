package sql_test

import (
	"testing"

	"github.com/rqlite/rqlite/rsql"
)

func TestPos_String(t *testing.T) {
	if got, want := (sql.Pos{}).String(), `-`; got != want {
		t.Fatalf("String()=%q, want %q", got, want)
	}
}
