package db

import (
	"context"
	"errors"
	"testing"
)

// The session extension reports bytes it cannot parse as SQLITE_CORRUPT, which
// is also the code of a damaged database file. A sound file means the bytes are
// at fault (malformed, not fatal); a damaged one is a failure of this node.
func Test_Changeset_CorruptCodeIsToldApartByTheFile(t *testing.T) {
	// The input the fuzzer found: it walks cleanly and the apply reports CORRUPT.
	bad := []byte("T\x02\x01\x00p\x00\t00\x00")
	a, _ := newPair(t, "CREATE TABLE p (a INTEGER PRIMARY KEY, b TEXT)")

	c, err := a.AcquireWriter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	err = c.ApplyChangeset(bad)
	if !errors.Is(err, ErrChangesetMalformed) {
		t.Fatalf("sound file: expected a malformed changeset, got %v", err)
	}
	var fatal *FatalError
	if errors.As(err, &fatal) {
		t.Fatal("sound file: a malformed changeset was fatal")
	}

	real := quickCheck
	defer func() { quickCheck = real }()
	quickCheck = func(*Conn) (string, error) { return "row 1 missing from index x", nil }
	err = c.ApplyChangeset(bad)
	if !errors.As(err, &fatal) || errors.Is(err, ErrChangesetMalformed) {
		t.Fatalf("damaged file: expected a fatal error, got %v", err)
	}
	quickCheck = func(*Conn) (string, error) { return "", errors.New("boom") }
	if err = c.ApplyChangeset(bad); !errors.As(err, &fatal) {
		t.Fatalf("unreadable file: expected a fatal error, got %v", err)
	}
}
