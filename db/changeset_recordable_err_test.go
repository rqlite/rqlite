package db

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// Test_Changeset_RecordableCheckErrorFailsCapture: when the pre-check of the
// tables a request writes cannot run, the capture must fail with that error and
// must not carry on, nor fall back as if the request were merely not capturable.
func Test_Changeset_RecordableCheckErrorFailsCapture(t *testing.T) {
	a, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)")
	boom := errors.New("boom")
	real := queryAll
	defer func() { queryAll = real }()
	hit := false
	queryAll = func(ctx context.Context, cn *Conn, q string, args []any, d func() []any, e func() error) error {
		if strings.Contains(q, "pragma_foreign_key_list") {
			hit = true
			return boom
		}
		return real(ctx, cn, q, args, d, e)
	}
	_, cs, err := a.ExecuteCapture(context.Background(), csRequest(false, "INSERT INTO foo(v) VALUES('x')"), false)
	if !hit {
		t.Fatal("the foreign key query was never run")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("got err=%v, want the query error", err)
	}
	if len(cs) != 0 {
		t.Fatalf("a changeset was returned despite the error")
	}
}
