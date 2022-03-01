package rsql

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/rqlite/sql"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type RandomFunc func() uint64

type Rewriter struct {
	rewriteNow    bool
	rewriteRandom bool

	randomFn func() uint64
}

func NewRewriter(now, random bool, randomFn RandomFunc) *Rewriter {
	rw := &Rewriter{
		rewriteNow:    now,
		rewriteRandom: random,
		randomFn:      randomFn,
	}
	if rw.randomFn == nil {
		rw.randomFn = rand.Uint64
	}
	return rw
}

func (rw *Rewriter) Do(s string) (string, error) {
	if !rw.rewriteNow && !rw.rewriteRandom {
		return s, nil
	}

	stmt, err := sql.NewParser(strings.NewReader(s)).ParseStatement()
	if err != nil {
		return "", fmt.Errorf("failed to parse statement: %s", err.Error())
	}
	fmt.Println(">>>>", stmt.String())

	v := visitor{rw}
	if err := sql.Walk(v, stmt); err != nil {
		return "", fmt.Errorf("failed to walk statement: %s", err.Error())
	}
	return stmt.String(), nil
}

type visitor struct {
	rw *Rewriter
}

func (v visitor) Visit(node sql.Node) (w sql.Visitor, err error) {
	if v.rw.rewriteRandom {
		c, ok := node.(*sql.Call)
		if ok && strings.ToUpper(c.Name.Name) == "RANDOM" {
			node = &sql.NumberLit{
				Value: strconv.Itoa(int(v.rw.randomFn())),
			}
		}
	}
	return v, nil
}

func (v visitor) VisitEnd(node sql.Node) error {
	return nil
}
