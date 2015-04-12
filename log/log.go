package log

import (
	"io"
	"log"
)

// Log levels
const (
	TRACE = iota // 0
	DEBUG = iota // 1
	INFO  = iota // 2
	WARN  = iota // 3
	ERROR = iota // 4
)

// Level set for logs
var Level = TRACE

// SetLevel sets the log level
// given a string
func SetLevel(level string) {
	switch level {
	case "TRACE":
		Level = TRACE
	case "DEBUG":
		Level = DEBUG
	case "INFO":
		Level = INFO
	case "WARN":
		Level = WARN
	case "ERROR":
		Level = ERROR
	default:
		Level = TRACE
	}
}

// SetOutput set the output destination
// of logs
func SetOutput(w io.Writer) {
	log.SetOutput(w)
}

// Tracef writes a formatted log on TRACE level
func Tracef(format string, v ...interface{}) {
	if Level <= TRACE {
		log.Printf("[TRACE] "+format, v...)
	}
}

// Trace writes a log on TRACE level
func Trace(s string) {
	Tracef(s)
}

// Debugf writes a formatted log on DEBUG level
func Debugf(format string, v ...interface{}) {
	if Level <= DEBUG {
		log.Printf("[DEBUG] "+format, v...)
	}
}

// Debug writes a log on DEBUG level
func Debug(s string) {
	Debugf(s)
}

// Infof writes a formatted log on INFO level
func Infof(format string, v ...interface{}) {
	if Level <= INFO {
		log.Printf("[INFO ] "+format, v...)
	}
}

// Info write a log on INFO level
func Info(s string) {
	Infof(s)
}

// Warnf writes a formatted log on WARN level
func Warnf(format string, v ...interface{}) {
	if Level <= WARN {
		log.Printf("[WARN ] "+format, v...)
	}
}

// Warn write a log on WARN level
func Warn(s string) {
	Warnf(s)
}

// Errorf writes a formatted log on ERROR level
func Errorf(format string, v ...interface{}) {
	if Level <= ERROR {
		log.Printf("[ERROR] "+format, v...)
	}
}

// Error write a log on ERROR level
func Error(s string) {
	Errorf(s)
}
