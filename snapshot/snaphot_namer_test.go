package snapshot

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Test_NewSnapshotNamer_NilNowFn checks that a nil nowFn falls back to
// time.Now, by bracketing a call to MakeName with real clock readings.
func Test_NewSnapshotNamer_NilNowFn(t *testing.T) {
	sn := NewSnapshotNamer(nil)
	if sn == nil {
		t.Fatal("NewSnapshotNamer(nil) returned nil")
	}
	if sn.nowFn == nil {
		t.Fatal("nowFn is nil, want time.Now")
	}

	before := time.Now().UnixNano() / int64(time.Millisecond)
	name := sn.MakeName(7, 8)
	after := time.Now().UnixNano() / int64(time.Millisecond)

	term, index, msec := parseName(t, name)
	if term != 7 || index != 8 {
		t.Fatalf("got term=%d index=%d, want 7 and 8", term, index)
	}
	if msec < before || msec > after {
		t.Fatalf("timestamp %d outside range [%d, %d]", msec, before, after)
	}
}

// Test_NewSnapshotNamer_CustomNowFn checks that a supplied clock is used.
func Test_NewSnapshotNamer_CustomNowFn(t *testing.T) {
	tm := time.Unix(1500000000, 0).UTC() // 1500000000000 msec
	sn := NewSnapshotNamer(fixedClock(tm))

	if got, want := sn.MakeName(1, 1), "1-1-1500000000000"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// Test_MakeName_Format covers the term and index fields across their range.
func Test_MakeName_Format(t *testing.T) {
	tm := time.Unix(1500000000, 0).UTC()
	sn := NewSnapshotNamer(fixedClock(tm))

	tests := []struct {
		name  string
		term  uint64
		index uint64
		want  string
	}{
		{"zero values", 0, 0, "0-0-1500000000000"},
		{"small values", 1, 2, "1-2-1500000000000"},
		{"large values", 1 << 40, 1 << 41, "1099511627776-2199023255552-1500000000000"},
		{
			"max uint64",
			math.MaxUint64,
			math.MaxUint64,
			"18446744073709551615-18446744073709551615-1500000000000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sn.MakeName(tt.term, tt.index); got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// Test_MakeName_TimestampTruncation checks that sub-millisecond precision is
// discarded, and that the division truncates towards zero rather than
// flooring.
func Test_MakeName_TimestampTruncation(t *testing.T) {
	tests := []struct {
		name string
		now  time.Time
		want int64
	}{
		{"epoch", time.Unix(0, 0), 0},
		{"just under 1 msec", time.Unix(0, 999999), 0},
		{"exactly 1 msec", time.Unix(0, 1000000), 1},
		{"just under 2 msec", time.Unix(0, 1999999), 1},
		{"1.5 seconds", time.Unix(1, 500000000), 1500},
		{"1 nsec before epoch truncates to zero", time.Unix(0, -1), 0},
		{"1 second before epoch", time.Unix(-1, 0), -1000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sn := NewSnapshotNamer(fixedClock(tt.now))
			want := fmt.Sprintf("3-4-%d", tt.want)
			if got := sn.MakeName(3, 4); got != want {
				t.Fatalf("got %q, want %q", got, want)
			}
		})
	}
}

// Test_MakeName_PreEpochAddsAField records that a pre-epoch clock yields a
// negative timestamp, so the name splits into four fields, not three.
func Test_MakeName_PreEpochAddsAField(t *testing.T) {
	sn := NewSnapshotNamer(fixedClock(time.Unix(-1, 0)))

	name := sn.MakeName(1, 2)
	if got, want := name, "1-2--1000"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if got, want := len(strings.Split(name, "-")), 4; got != want {
		t.Fatalf("got %d fields, want %d", got, want)
	}
}

// Test_MakeName_CallsNowFnOncePerCall checks the clock is read exactly once
// per name.
func Test_MakeName_CallsNowFnOncePerCall(t *testing.T) {
	var calls int
	sn := NewSnapshotNamer(func() time.Time {
		calls++
		return time.Unix(0, 0)
	})

	for i := 0; i < 5; i++ {
		sn.MakeName(uint64(i), uint64(i))
	}
	if calls != 5 {
		t.Fatalf("nowFn called %d times, want 5", calls)
	}
}

// Test_MakeName_CollidesWithinSameMillisecond records that names are a pure
// function of their inputs: identical term, index and millisecond produce
// identical names.
func Test_MakeName_CollidesWithinSameMillisecond(t *testing.T) {
	sn := NewSnapshotNamer(fixedClock(time.Unix(1500000000, 0)))

	first := sn.MakeName(1, 2)
	second := sn.MakeName(1, 2)
	if first != second {
		t.Fatalf("got %q and %q, want identical names", first, second)
	}
}

// Test_MakeName_DistinctInputsDistinctNames covers the axes that do vary.
func Test_MakeName_DistinctInputsDistinctNames(t *testing.T) {
	now := time.Unix(1500000000, 0)
	sn := NewSnapshotNamer(func() time.Time {
		n := now
		now = now.Add(time.Millisecond)
		return n
	})

	seen := make(map[string]bool)
	for _, tc := range []struct{ term, index uint64 }{
		{1, 1}, {1, 2}, {2, 1}, {1, 1},
	} {
		name := sn.MakeName(tc.term, tc.index)
		if seen[name] {
			t.Fatalf("duplicate name %q", name)
		}
		seen[name] = true
	}
}

// Test_MakeName_Concurrent exercises MakeName under the race detector.
func Test_MakeName_Concurrent(t *testing.T) {
	const goroutines, iterations = 8, 200
	sn := NewSnapshotNamer(fixedClock(time.Unix(1500000000, 0)))
	want := "1-2-1500000000000"

	var wg sync.WaitGroup
	errs := make(chan string, goroutines*iterations)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				if got := sn.MakeName(1, 2); got != want {
					errs <- got
				}
			}
		}()
	}
	wg.Wait()
	close(errs)

	for got := range errs {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// fixedClock returns a nowFn that always reports t.
func fixedClock(t time.Time) func() time.Time {
	return func() time.Time { return t }
}

// parseName splits a name into its three fields. It requires a
// non-negative timestamp, since a negative one introduces a fourth field.
func parseName(t *testing.T, name string) (term, index uint64, msec int64) {
	t.Helper()
	parts := strings.Split(name, "-")
	if len(parts) != 3 {
		t.Fatalf("name %q: got %d fields, want 3", name, len(parts))
	}
	term, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		t.Fatalf("name %q: bad term field: %s", name, err)
	}
	index, err = strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		t.Fatalf("name %q: bad index field: %s", name, err)
	}
	msec, err = strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		t.Fatalf("name %q: bad timestamp field: %s", name, err)
	}
	return term, index, msec
}

func Test_ParseSnapshotName(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantTerm    uint64
		wantIndex   uint64
		wantMsec    int64
		errContains string // empty means no error expected
	}{
		// Valid input.
		{
			name:      "simple",
			input:     "1-2-3",
			wantTerm:  1,
			wantIndex: 2,
			wantMsec:  3,
		},
		{
			name:  "all zero",
			input: "0-0-0",
		},
		{
			name:      "realistic name",
			input:     "4-1234567-1500000000000",
			wantTerm:  4,
			wantIndex: 1234567,
			wantMsec:  1500000000000,
		},
		{
			name:      "maximum values",
			input:     "18446744073709551615-18446744073709551615-9223372036854775807",
			wantTerm:  math.MaxUint64,
			wantIndex: math.MaxUint64,
			wantMsec:  math.MaxInt64,
		},
		{
			name:      "leading zeroes accepted",
			input:     "01-002-0003",
			wantTerm:  1,
			wantIndex: 2,
			wantMsec:  3,
		},
		{
			name:      "explicit plus accepted in timestamp only",
			input:     "1-2-+3",
			wantTerm:  1,
			wantIndex: 2,
			wantMsec:  3,
		},

		// Wrong number of fields.
		{
			name:        "empty string",
			input:       "",
			errContains: "3 parts",
		},
		{
			name:        "no separators",
			input:       "123",
			errContains: "3 parts",
		},
		{
			name:        "two fields",
			input:       "1-2",
			errContains: "3 parts",
		},
		{
			name:        "four fields",
			input:       "1-2-3-4",
			errContains: "3 parts",
		},
		{
			name:        "negative term adds a field",
			input:       "-1-2-3",
			errContains: "3 parts",
		},
		{
			name:        "negative index adds a field",
			input:       "1--2-3",
			errContains: "3 parts",
		},
		{
			name:        "negative timestamp adds a field",
			input:       "1-2--3",
			errContains: "3 parts",
		},
		{
			name:        "trailing separator",
			input:       "1-2-3-",
			errContains: "3 parts",
		},

		// Bad term.
		{
			name:        "empty term",
			input:       "-2-3",
			errContains: "3 parts", // leading separator, so counted first
		},
		{
			name:        "all fields empty",
			input:       "--",
			errContains: "bad term field",
		},
		{
			name:        "non-numeric term",
			input:       "a-2-3",
			errContains: "bad term field",
		},
		{
			name:        "term overflows uint64",
			input:       "18446744073709551616-2-3",
			errContains: "bad term field",
		},
		{
			name:        "signed term rejected",
			input:       "+1-2-3",
			errContains: "bad term field",
		},
		{
			name:        "hex term rejected",
			input:       "0x10-2-3",
			errContains: "bad term field",
		},
		{
			name:        "underscore separator rejected",
			input:       "1_000-2-3",
			errContains: "bad term field",
		},
		{
			name:        "leading space in term",
			input:       " 1-2-3",
			errContains: "bad term field",
		},

		// Bad index.
		{
			name:        "empty index",
			input:       "1--3",
			errContains: "bad index field",
		},
		{
			name:        "non-numeric index",
			input:       "1-b-3",
			errContains: "bad index field",
		},
		{
			name:        "index overflows uint64",
			input:       "1-18446744073709551616-3",
			errContains: "bad index field",
		},
		{
			name:        "float index",
			input:       "1-2.0-3",
			errContains: "bad index field",
		},

		// Bad timestamp.
		{
			name:        "empty timestamp",
			input:       "1-2-",
			errContains: "bad timestamp field",
		},
		{
			name:        "non-numeric timestamp",
			input:       "1-2-c",
			errContains: "bad timestamp field",
		},
		{
			name:        "timestamp overflows int64",
			input:       "1-2-9223372036854775808",
			errContains: "bad timestamp field",
		},
		{
			name:        "trailing newline",
			input:       "1-2-3\n",
			errContains: "bad timestamp field",
		},
		{
			name:        "trailing extension",
			input:       "1-2-3.snap",
			errContains: "bad timestamp field",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			term, index, msec, err := ParseSnapshotName(tt.input)

			if tt.errContains != "" {
				if err == nil {
					t.Fatalf("ParseSnapshotName(%q) returned no error, want one containing %q",
						tt.input, tt.errContains)
				}
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Fatalf("ParseSnapshotName(%q) error %q does not contain %q",
						tt.input, err, tt.errContains)
				}
				// Every failure path must zero all three values.
				if term != 0 || index != 0 || msec != 0 {
					t.Fatalf("ParseSnapshotName(%q) returned (%d, %d, %d) alongside an error, want zeroes",
						tt.input, term, index, msec)
				}
				return
			}

			if err != nil {
				t.Fatalf("ParseSnapshotName(%q) returned unexpected error: %s", tt.input, err)
			}
			if term != tt.wantTerm || index != tt.wantIndex || msec != tt.wantMsec {
				t.Fatalf("ParseSnapshotName(%q) = (%d, %d, %d), want (%d, %d, %d)",
					tt.input, term, index, msec, tt.wantTerm, tt.wantIndex, tt.wantMsec)
			}
		})
	}
}

// Test_ParseSnapshotName_RoundTrip checks that every name MakeName can
// produce, given a post-epoch clock, parses back to its inputs.
func Test_ParseSnapshotName_RoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		term  uint64
		index uint64
		now   time.Time
	}{
		{"zero values at epoch", 0, 0, time.Unix(0, 0)},
		{"small values", 1, 2, time.Unix(1500000000, 0)},
		{"sub-millisecond clock", 3, 4, time.Unix(1500000000, 999999)},
		{"large values", 1 << 40, 1 << 41, time.Unix(1500000000, 0)},
		{"max term and index", math.MaxUint64, math.MaxUint64, time.Unix(1500000000, 0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := tt.now
			sn := NewSnapshotNamer(func() time.Time { return now })

			name := sn.MakeName(tt.term, tt.index)
			term, index, msec, err := ParseSnapshotName(name)
			if err != nil {
				t.Fatalf("ParseSnapshotName(%q) returned error: %s", name, err)
			}
			if term != tt.term || index != tt.index {
				t.Fatalf("round trip of %q gave (%d, %d), want (%d, %d)",
					name, term, index, tt.term, tt.index)
			}
			if want := now.UnixNano() / int64(time.Millisecond); msec != want {
				t.Fatalf("round trip of %q gave timestamp %d, want %d", name, msec, want)
			}
		})
	}
}

// Test_ParseSnapshotName_PreEpochNameFails records that names produced by a
// pre-epoch clock cannot be parsed, since the negative timestamp splits into
// a fourth field.
func Test_ParseSnapshotName_PreEpochNameFails(t *testing.T) {
	now := time.Unix(-1, 0)
	sn := NewSnapshotNamer(func() time.Time { return now })

	name := sn.MakeName(1, 2)
	if _, _, _, err := ParseSnapshotName(name); err == nil {
		t.Fatalf("ParseSnapshotName(%q) returned no error, want one", name)
	}
}

// Test_ParseSnapshotName_NotInjective records that distinct names can parse
// to the same triple, so a parsed triple does not identify a single name.
func Test_ParseSnapshotName_NotInjective(t *testing.T) {
	first, second := "1-2-3", "01-02-03"

	fTerm, fIndex, fMsec, err := ParseSnapshotName(first)
	if err != nil {
		t.Fatalf("ParseSnapshotName(%q) returned error: %s", first, err)
	}
	sTerm, sIndex, sMsec, err := ParseSnapshotName(second)
	if err != nil {
		t.Fatalf("ParseSnapshotName(%q) returned error: %s", second, err)
	}

	if fTerm != sTerm || fIndex != sIndex || fMsec != sMsec {
		t.Fatalf("%q and %q parsed differently, want the same triple", first, second)
	}
}

func Fuzz_ParseSnapshotName(f *testing.F) {
	for _, seed := range []string{
		"", "1-2-3", "0-0-0", "--", "-1-2-3", "1-2-3-4",
		"18446744073709551616-2-3", "1-2-9223372036854775808",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, name string) {
		term, index, msec, err := ParseSnapshotName(name)
		if err != nil {
			if term != 0 || index != 0 || msec != 0 {
				t.Fatalf("ParseSnapshotName(%q) returned (%d, %d, %d) alongside an error",
					name, term, index, msec)
			}
			return
		}
		if got := strings.Count(name, "-"); got != 2 {
			t.Fatalf("ParseSnapshotName(%q) succeeded with %d separators, want 2", name, got)
		}
	})
}
