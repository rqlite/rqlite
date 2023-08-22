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

// RandomString returns a random string of 20 characters
func RandomString() string {
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
