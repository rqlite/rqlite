package random

import (
	"testing"
)

func Test_RandomStringLength(t *testing.T) {
	str := RandomString()
	if len(str) != 20 {
		t.Errorf("RandomString() returned a string of length %d; want 20", len(str))
	}
}

func Test_RandomStringUniqueness(t *testing.T) {
	const numStrings = 100
	strs := make(map[string]bool, numStrings)

	for i := 0; i < numStrings; i++ {
		str := RandomString()
		if strs[str] {
			t.Errorf("RandomString() returned a non-unique string: %s", str)
		}
		strs[str] = true
	}
}
