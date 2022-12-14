package history

import (
	"bufio"
	"io"
	"os"
	"strconv"
)

const MaxHistSize = 100

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

func FileSize() int {
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

func Read(r io.Reader) ([]string, error) {
	if r == nil {
		return nil, nil
	}

	var cmds []string
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		cmds = append(cmds, scanner.Text())
	}

	return cmds, scanner.Err()
}

func Write(j []string, maxSz int, w io.Writer) error {
	if len(j) == 0 {
		return nil
	}

	if w == nil {
		return nil
	}

	// Cut start off slice is bigger, but before or after dupes?
	if len(j) > maxSz {
		j = j[len(j)-maxSz:]
	}

	i := 0
	var k []string
	for {
		k = append(k, j[i])

		for {
			if i < len(j)-1 && j[i] == j[i+1] {
				i++
				continue
			} else if i == len(j) {

			}
		}
	}

	for i := 0; i < len(k)-1; i++ {
		if _, err := w.Write([]byte(k[i] + "\n")); err != nil {
			return err
		}
	}
	_, err := w.Write([]byte(k[len(k)-1]))
	return err
}
