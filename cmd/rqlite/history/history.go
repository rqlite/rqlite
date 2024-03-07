package history

import (
	"bufio"
	"io"
	"os"
	"strconv"
	"strings"
)

const MaxHistSize = 100

// Size returns the max number of commands that should be stored in history.
func Size() int {
	maxSize := MaxHistSize
	maxSizeStr := os.Getenv("RQLITE_HISTFILESIZE")
	if maxSizeStr != "" {
		sz, err := strconv.Atoi(maxSizeStr)
		if err == nil && maxSize > 0 {
			maxSize = sz
		}
	}
	return maxSize
}

// Dedupe returns a copy of the slice with contiguous dupes removed.
func Dedupe(s []string) []string {
	if s == nil {
		return nil
	}

	o := make([]string, 0, len(s))
	for si := 0; si < len(s); si++ {
		if si == 0 || s[si] != o[len(o)-1] {
			o = append(o, s[si])
		}
	}
	return o
}

// Filter returns a copy of the slice with empty and whitespace-only elements
// removed.
func Filter(s []string) []string {
	if s == nil {
		return nil
	}
	o := make([]string, 0, len(s))
	for si := 0; si < len(s); si++ {
		if s[si] == "" || len(strings.Fields(s[si])) == 0 {
			continue
		}
		o = append(o, s[si])
	}
	return o
}

// Read reads the command history from the reader.
func Read(r io.Reader) ([]string, error) {
	if r == nil {
		return nil, nil
	}

	cmds := make([]string, 0)
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		cmds = append(cmds, scanner.Text())
	}

	return Filter(cmds), scanner.Err()
}

// Write writes the command history to the writer.
func Write(j []string, maxSz int, w io.Writer) error {
	if len(j) == 0 {
		return nil
	}

	if w == nil {
		return nil
	}

	if maxSz <= 0 {
		return nil
	}

	k := Dedupe(Filter(j))
	if len(k) == 0 {
		return nil
	}

	if len(k) > maxSz {
		k = k[len(k)-maxSz:]
	}

	for i := 0; i < len(k)-1; i++ {
		if _, err := w.Write([]byte(k[i] + "\n")); err != nil {
			return err
		}
	}
	_, err := w.Write([]byte(k[len(k)-1]))
	return err
}
