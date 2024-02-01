package random

import (
	"math/rand"
	"strings"
	"sync"
	"time"
)

var r *rand.Rand
var mu sync.Mutex

func init() {
	r = rand.New(rand.NewSource(time.Now().UnixNano()))
}

// String returns a random string of 20 characters
func String() string {
	mu.Lock()
	defer mu.Unlock()
	var output strings.Builder
	chars := "abcdedfghijklmnopqrstABCDEFGHIJKLMNOP"
	for i := 0; i < 20; i++ {
		random := r.Intn(len(chars))
		randomChar := chars[random]
		output.WriteString(string(randomChar))
	}
	return output.String()
}

// Float64 returns a random float64 between 0 and 1.
func Float64() float64 {
	mu.Lock()
	defer mu.Unlock()
	return r.Float64()
}

// Intn returns a random int
func Intn(n int) int {
	mu.Lock()
	defer mu.Unlock()
	return r.Intn(n)
}

// Jitter adds a little bit of randomness to a given duration. This is
// useful to prevent nodes across the cluster performing certain operations
// all at the same time.
func Jitter(d time.Duration) time.Duration {
	return d + time.Duration(Float64()*float64(d))
}
