package sql

import (
	"bufio"
	"bytes"
	"io"
	"strings"
)

// stack represents a stack.
type stack struct {
	c []rune
}

// newStack returns an instance of a stack.
func newStack() *stack {
	return &stack{
		c: make([]rune, 0),
	}
}

// push pushes a rune onto the stack.
func (s *stack) push(r rune) {
	s.c = append(s.c, r)
}

// pop pops a rune off the stack.
func (s *stack) pop() rune {
	if len(s.c) == 0 {
		return rune(0)
	}
	c := s.c[len(s.c)-1]
	s.c = s.c[:len(s.c)-1]
	return c
}

// peek returns what is on the stack, without changing the stack.
func (s *stack) peek() rune {
	if len(s.c) == 0 {
		return rune(0)
	}
	c := s.c[len(s.c)-1]
	return c
}

// empty returns whether the stack is empty.
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
func (s *Scanner) Scan() (string, error) {
	var buf bytes.Buffer
	seekSemi := true

	for {
		ch := s.read()

		if ch == eof {
			return "", io.EOF
		}

		// Store the character.
		_, _ = buf.WriteRune(ch)

		if ch == '\'' || ch == '"' {
			if s.c.empty() {
				s.c.push(ch)
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

	return strings.Trim(strings.TrimRight(buf.String(), ";"), "\n"), nil
}

// eof represents a marker rune for the end of the reader.
var eof = rune(0)
