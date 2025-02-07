package random

import (
	"strings"
	"testing"
	"time"
)

func Test_StringNLength(t *testing.T) {
	str := StringN(10)
	if exp, got := 10, len(str); exp != got {
		t.Errorf("String() returned a string of length %d; want %d", got, exp)
	}
}

func Test_StringNUniqueness(t *testing.T) {
	const numStrings = 100
	strs := make(map[string]bool, numStrings)

	for i := 0; i < numStrings; i++ {
		str := StringN(50)
		if strs[str] {
			t.Errorf("StringN() returned a non-unique string: %s", str)
		}
		strs[str] = true
	}
}

func Test_StringLength(t *testing.T) {
	str := String()
	if exp, got := 20, len(str); exp != got {
		t.Errorf("String() returned a string of length %d; want %d", got, exp)
	}
}

func Test_StringUniqueness(t *testing.T) {
	const numStrings = 100
	strs := make(map[string]bool, numStrings)

	for i := 0; i < numStrings; i++ {
		str := String()
		if strs[str] {
			t.Errorf("String() returned a non-unique string: %s", str)
		}
		strs[str] = true
	}
}

func Test_StringPatternReplacement(t *testing.T) {
	str := StringPattern("xxx-⌘-XXX-⌘")
	if strings.Contains(str, "xxx-") {
		t.Errorf("StringPattern() did not replace the lowercased 'x' with a random character: %q", str)
	}
	if strings.Contains(str, "-XXX-") {
		t.Errorf("StringPattern() did not replace the uppercased 'XXX' with a random character: %q", str)
	}
	if strings.Count(str, "-⌘") != 2 {
		t.Errorf("StringPattern() didn't work with runes: %q", str)
	}
}

func Test_StringPatternUniqueness(t *testing.T) {
	const numStrings = 100
	strs := make(map[string]bool, numStrings)

	for i := 0; i < numStrings; i++ {
		str := StringPattern("tmp-XXXX-XXXX-XXXX")
		if !strings.HasPrefix(str, "tmp-") {
			t.Errorf("StringPattern() returned a string that does not start with 'tmp-': %s", str)
		}
		if strs[str] {
			t.Errorf("StringPattern() returned a non-unique string: %s", str)
		}
		strs[str] = true
	}
}

func Test_BytesLength(t *testing.T) {
	bytes := Bytes(0)
	if exp := len(bytes); exp != 0 {
		t.Errorf("Bytes() returned a byte slice of non-zero length %q", bytes)
	}

	bytes = Bytes(10)
	if exp, got := 10, len(bytes); exp != got {
		t.Errorf("Bytes() returned a byte slice of length %q; want %d", bytes, exp)
	}
}

func Test_BytesUniqueness(t *testing.T) {
	const numByteSlices = 100
	bytes := make(map[string]bool, numByteSlices)

	for i := 0; i < numByteSlices; i++ {
		b := Bytes(20)
		if bytes[string(b)] {
			t.Errorf("Bytes() returned a non-unique byte slice: %v", b)
		}
		bytes[string(b)] = true
	}
}

func Test_Jitter(t *testing.T) {
	for n := 0; n < 100; n++ {
		dur := 100 * time.Millisecond
		lower := dur
		upper := 2 * dur
		if got := Jitter(dur); got < lower || got >= upper {
			t.Errorf("Jitter(%s) returned a duration of %s; want between %s and %s",
				dur, got, lower, upper)
		}
	}
}
