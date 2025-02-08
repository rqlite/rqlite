package random

import (
	"math/rand/v2"
	"strings"
	"time"
)

const srcChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// String returns a random string of n characters long.
func StringN(n int) string {
	var output strings.Builder
	output.Grow(n)
	for range n {
		random := rand.N(len(srcChars))
		output.WriteByte(srcChars[random])
	}
	return output.String()
}

// String returns a random string, 20 characters long.
func String() string {
	return StringN(20)
}

// StringPattern returns a random string, with all occurrences of 'X' or 'x'
// replaced with a random character.
func StringPattern(s string) string {
	var output strings.Builder
	output.Grow(len(s))
	for _, c := range s {
		if c == 'X' || c == 'x' {
			random := rand.N(len(srcChars))
			output.WriteByte(srcChars[random])
		} else {
			output.WriteRune(c)
		}
	}
	return output.String()
}

// Bytes returns a random slice of bytes, n bytes long.
func Bytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rand.N(256))
	}
	return b
}

// Jitter returns a randomly-chosen duration between d and 2d.
func Jitter(d time.Duration) time.Duration {
	return d + rand.N(d)
}
