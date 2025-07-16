package rqlog

import (
	"io"
	"log"
)

type RqLogger struct {
	stdLog *log.Logger
}

func NewRqLogger(out io.Writer, prefix string, flag int) *RqLogger {
	return &RqLogger{
		stdLog: log.New(out, prefix, flag),
	}
}

func (l *RqLogger) Print(v ...any) {
	l.stdLog.Print(v...)
}

func (l *RqLogger) Printf(format string, v ...any) {
	l.stdLog.Printf(format, v...)
}

func (l *RqLogger) Println(v ...any) {
	l.stdLog.Println(v...)
}

func (l *RqLogger) Fatal(v ...any) {
	l.stdLog.Fatal(v...)
}

func (l *RqLogger) Fatalf(format string, v ...any) {
	l.stdLog.Fatalf(format, v...)
}

func (l *RqLogger) Fatalln(v ...any) {
	l.stdLog.Fatalln(v...)
}

func (l *RqLogger) Prefix() string {
	return l.stdLog.Prefix()
}

func (l *RqLogger) SetPrefix(prefix string) {
	l.stdLog.SetPrefix(prefix)
}

func (l *RqLogger) StandardLogger() *log.Logger {
	return l.stdLog
}

func (l *RqLogger) WithName(name string) Logger {
	return &RqLogger{
		stdLog: log.New(l.stdLog.Writer(), name, l.stdLog.Flags()),
	}
}

func (l *RqLogger) WithOutput(output io.Writer) Logger {
	return &RqLogger{
		stdLog: log.New(output, l.stdLog.Prefix(), l.stdLog.Flags()),
	}
}
