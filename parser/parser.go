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
	return strings.ReplaceAll(s, "random()", strconv.Itoa(int(n)))
}
