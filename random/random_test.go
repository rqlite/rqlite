package random

import (
	"testing"
	"time"
)

func Test_StringLength(t *testing.T) {
	str := String()
	if len(str) != 20 {
		t.Errorf("String() returned a string of length %d; want 20", len(str))
	}
}

func Test_StringUniqueness(t *testing.T) {
	const numStrings = 10
	strs := make(map[string]bool, numStrings)

	for i := 0; i < numStrings; i++ {
		str := String()
		if strs[str] {
			t.Errorf("String() returned a non-unique string: %s", str)
		}
		strs[str] = true
	}
}

func Test_Float64Uniqueness(t *testing.T) {
	const numFloat64s = 10
	floats := make(map[float64]bool, numFloat64s)

	for i := 0; i < numFloat64s; i++ {
		f := Float64()
		if floats[f] {
			t.Errorf("Float64() returned a non-unique float64: %f", f)
		}
		floats[f] = true
	}
}

func Test_IntnUniqueness(t *testing.T) {
	const numIntns = 10
	intns := make(map[int]bool, numIntns)

	for i := 0; i < numIntns; i++ {
		n := Intn(1000000000)
		if intns[n] {
			t.Errorf("Intn() returned a non-unique int: %d", n)
		}
		intns[n] = true
	}
}

func Test_Jitter(t *testing.T) {
	for n := 0; n < 100; n++ {
		d := Jitter(100 * time.Millisecond)
		if d < 100*time.Millisecond || d >= 200*time.Millisecond {
			t.Errorf("Jitter(100ms) returned a duration of %s; want between 0 and 200ms", d)
		}
	}
}
