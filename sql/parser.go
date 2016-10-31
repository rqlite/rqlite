package sql

import (
	"bufio"
	"bytes"
	"io"
)

type stack struct {
	c []rune
}

func newStack() *stack {
	return &stack{
		c: make([]rune, 0),
	}
}

func (s *stack) push(r rune) {
	s.c = append(s.c, r)
}

func (s *stack) pop() rune {
	if len(s.c) == 0 {
		return rune(0)
	}
	c := s.c[len(s.c)-1]
	s.c = s.c[:len(s.c)-1]
	return c
}

func (s *stack) peek() rune {
	if len(s.c) == 0 {
		return rune(0)
	}
	c := s.c[len(s.c)-1]
	return c
}

func (s *stack) empty() bool {
	return len(s.c) == 0
}

// Scanner represents a SQL statement scanner.
type Scanner struct {
	r *bufio.Reader
	c *stack
}

// NewScanner returns a new instance of Scanner.
func NewScanner(r io.Reader) *Scanner {
	return &Scanner{
		r: bufio.NewReader(r),
		c: newStack(),
	}
}

// read reads the next rune from the bufferred reader.
// Returns the rune(0) if an error occurs (or io.EOF is returned).
func (s *Scanner) read() rune {
	ch, _, err := s.r.ReadRune()
	if err != nil {
		return eof
	}
	return ch
}

// Scan returns the next SQL statement.
func (s *Scanner) Scan() string {
	var buf bytes.Buffer
	seekSemi := true

	for {
		ch := s.read()

		if ch == eof {
			break
		}

		// Store the character.
		_, _ = buf.WriteRune(ch)

		if ch == '\'' || ch == '"' {
			if s.c.empty() {
				// add to stack
				seekSemi = false
			} else if s.c.peek() != ch {
				s.c.push(ch)
				seekSemi = false
			} else {
				s.c.pop()
				if s.c.empty() {
					seekSemi = true
				}
			}
		} else if ch == ';' && seekSemi {
			break
		}
	}

	return buf.String()
}

// eof represents a marker rune for the end of the reader.
var eof = rune(0)
