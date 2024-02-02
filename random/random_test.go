package random

import (
	"testing"
	"time"
)

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

func Test_Float64Uniqueness(t *testing.T) {
	const numFloat64s = 100
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
	const numIntns = 100
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
		dur := 100 * time.Millisecond
		lower := dur
		upper := 2 * dur
		if got := Jitter(dur); got < lower || got >= upper {
			t.Errorf("Jitter(%s) returned a duration of %s; want between %s and %s",
				dur, got, lower, upper)
		}
	}
}
