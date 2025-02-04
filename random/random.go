package random

import (
	"math/rand"
	"strings"
	"sync"
	"time"
)

var r *rand.Rand
var mu sync.Mutex

const (
	srcChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

func init() {
	r = rand.New(rand.NewSource(time.Now().UnixNano()))
}

// String returns a random string, 20 characters long.
func String() string {
	mu.Lock()
	defer mu.Unlock()
	var output strings.Builder
	for i := 0; i < 20; i++ {
		random := r.Intn(len(srcChars))
		output.WriteString(string(srcChars[random]))
	}
	return output.String()
}

// StringPattern returns a random string, with all occurrences of 'X' or 'x'
// replaced with a random character.
func StringPattern(s string) string {
	mu.Lock()
	defer mu.Unlock()
	var output strings.Builder
	for _, c := range s {
		if c == 'X' || c == 'x' {
			random := r.Intn(len(srcChars))
			output.WriteString(string(srcChars[random]))
		} else {
			output.WriteString(string(c))
		}
	}
	return output.String()
}

// Float64 returns a random float64 between 0 and 1.
func Float64() float64 {
	mu.Lock()
	defer mu.Unlock()
	return r.Float64()
}

// Intn returns a random int >=0 and < n.
func Intn(n int) int {
	mu.Lock()
	defer mu.Unlock()
	return r.Intn(n)
}

// Bytes returns a random slice of bytes, n bytes long.
func Bytes(n int) []byte {
	mu.Lock()
	defer mu.Unlock()
	b := make([]byte, n)
	r.Read(b)
	return b
}

// Jitter returns a randomly-chosen duration between d and 2d.
func Jitter(d time.Duration) time.Duration {
	mu.Lock()
	defer mu.Unlock()
	return d + time.Duration(r.Float64()*float64(d))
}
