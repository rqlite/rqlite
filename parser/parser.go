package parser

import (
	"math/rand"
	"strconv"
	"strings"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func ReplaceRandom(s string, f func() uint64) string {
	var n uint64
	if f != nil {
		n = f()
	} else {
		n = rand.Uint64()
	}

	state := 0

	for {

	}
	builder := strings.Builder
	for i := range s {
		c := s[i]
		if c == '\'' {
			if singleQuoted {
				singleQuoted = false
			}
		}
		if c == '"' {
			if doubleQuoted {
				doubleQuoted = false
			}
		}

	}
	return strings.ReplaceAll(s, "random()", strconv.Itoa(int(n)))
}
