// go test -bench=.
// This benchmark compares the performance of using strings.Contains and regular
// expressions to search for keywords in a string. It is used to demonstrate that
// Contains is faster than regular expressions for simple searches.
package sql

import (
	"regexp"
	"strings"
	"testing"

	"github.com/rqlite/rqlite/v8/random"
)

// Benchmark using strings.Contains
func BenchmarkContains(b *testing.B) {
	// The set of strings to search for
	keywords := []string{"date", "time", "julianday", "unixepoch", "random", "returning"}
	// Generate a random string
	text := random.StringN(100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lowerText := strings.ToLower(text)
		for _, keyword := range keywords {
			_ = strings.Contains(lowerText, keyword)
		}
	}
}

// Benchmark using regular expressions
func BenchmarkRegex(b *testing.B) {
	// Compile the regex once
	pattern := `(?i)date|time|julianday|unixepoch|random|returning`
	regex := regexp.MustCompile(pattern)
	// Generate a random string
	text := random.StringN(100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = regex.MatchString(text)
	}
}
