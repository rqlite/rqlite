package log

import (
	"bytes"
	"io"
	"os"
	"testing"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) {
	TestingT(t)
}

type LogSuite struct{}

var _ = Suite(&LogSuite{})

func (s *LogSuite) TestSetLevel(c *C) {
	SetLevel("DEBUG")
	c.Assert(Level, Equals, DEBUG)

	SetLevel("INFO")
	c.Assert(Level, Equals, INFO)

	SetLevel("WARN")
	c.Assert(Level, Equals, WARN)

	SetLevel("ERROR")
	c.Assert(Level, Equals, ERROR)

	SetLevel("TRACE")
	c.Assert(Level, Equals, TRACE)
}

func (s *LogSuite) TestLog(c *C) {
	logFile, err := os.OpenFile("tmptestfile.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	c.Check(err, IsNil)
	defer logFile.Close()
	defer os.Remove("tmptestfile.txt")

	SetOutput(logFile)

	num, err := lineCounter(logFile)
	c.Check(num, Equals, 0)
	c.Check(err, IsNil)

	tmpFile, _ := os.Open("tmptestfile.txt")
	Level = ERROR
	Trace("a")
	Debug("a")
	Info("a")
	Warn("a")
	Error("a")
	num, err = lineCounter(tmpFile)
	c.Check(num, Equals, 1)
	c.Check(err, IsNil)
	tmpFile.Close()

	tmpFile, _ = os.Open("tmptestfile.txt")
	Level = WARN
	Trace("a")
	Debug("a")
	Info("a")
	Warn("a")
	Error("a")
	num, err = lineCounter(tmpFile)
	c.Check(num, Equals, 3)
	c.Check(err, IsNil)
	tmpFile.Close()

	tmpFile, _ = os.Open("tmptestfile.txt")
	Level = INFO
	Trace("a")
	Debug("a")
	Info("a")
	Warn("a")
	Error("a")
	num, err = lineCounter(tmpFile)
	c.Check(num, Equals, 6)
	c.Check(err, IsNil)
	tmpFile.Close()

	tmpFile, _ = os.Open("tmptestfile.txt")
	Level = DEBUG
	Trace("a")
	Debug("a")
	Info("a")
	Warn("a")
	Error("a")
	num, err = lineCounter(tmpFile)
	c.Check(num, Equals, 10)
	c.Check(err, IsNil)
	tmpFile.Close()

	tmpFile, _ = os.Open("tmptestfile.txt")
	Level = TRACE
	Trace("a")
	Debug("a")
	Info("a")
	Warn("a")
	Error("a")
	num, err = lineCounter(tmpFile)
	c.Check(num, Equals, 15)
	c.Check(err, IsNil)
	tmpFile.Close()
}

// Taken from http://stackoverflow.com/a/24563853/1187471
func lineCounter(r io.Reader) (int, error) {
	buf := make([]byte, 8196)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		if err != nil && err != io.EOF {
			return count, err
		}

		count += bytes.Count(buf[:c], lineSep)

		if err == io.EOF {
			break
		}
	}

	return count, nil
}
