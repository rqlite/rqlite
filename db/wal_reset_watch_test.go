package db

import (
	"testing"

	"github.com/rqlite/rqlite/v10/db/wal"
)

// Test_WALResetWatch tests the watch through its states. Unarmed, it reports
// frame 0 and no reset. Armed, it reports the resume frame for as long as the
// salt is unchanged. Once the salt changes it reports a reset and frame 0, and
// disarms itself, so the next check reports no reset. Disarm returns an armed
// watch to the unarmed state.
func Test_WALResetWatch(t *testing.T) {
	saltA, saltB := wal.Salt{1, 2}, wal.Salt{1, 3}
	check := func(w *WALResetWatch, salt wal.Salt, expIdx int64, expReset bool) {
		t.Helper()
		idx, reset := w.Check(salt)
		if idx != expIdx || reset != expReset {
			t.Fatalf("expected frame index %d and reset %t, got %d and %t", expIdx, expReset, idx, reset)
		}
	}

	// The zero value is unarmed.
	var w WALResetWatch
	check(&w, saltA, 0, false)

	// Armed, the watch keeps reporting the resume frame while the salt holds.
	w.Arm(saltA, 7)
	check(&w, saltA, 7, false)
	check(&w, saltA, 7, false)

	// A changed salt is a reset, and the watch disarms itself.
	check(&w, saltB, 0, true)
	check(&w, saltB, 0, false)
	check(&w, saltA, 0, false)

	// Arming again replaces the salt and the resume frame.
	w.Arm(saltB, 3)
	check(&w, saltB, 3, false)

	// Disarm clears the watch, even for the salt it was armed with.
	w.Disarm()
	check(&w, saltB, 0, false)
}
