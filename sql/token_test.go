package sql_test

import (
	"testing"

	"github.com/benbjohnson/lux/sql"
)

func TestPos_String(t *testing.T) {
	if got, want := (sql.Pos{}).String(), `-`; got != want {
		t.Fatalf("String()=%q, want %q", got, want)
	}
}
