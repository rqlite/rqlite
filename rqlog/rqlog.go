package rqlog

import (
	"io"
	"log"
	"sync"
)

type Logger interface {
	Print(v ...any)
	Printf(format string, v ...any)
	Println(v ...any)
	Fatal(v ...any)
	Fatalf(format string, v ...any)
	Fatalln(v ...any)
	Prefix() string
	SetPrefix(prefix string)

	StandardLogger() *log.Logger
	WithName(name string) Logger
	WithOutput(output io.Writer) Logger
}

var (
	syncOnce sync.Once
	def      Logger
)

func SetDefault(logger Logger) {
	def = logger
}

func Default() Logger {
	syncOnce.Do(func() {
		if def == nil {
			def = NewRqLogger(log.Default().Writer(), log.Default().Prefix(), log.Default().Flags())
		}
	})
	return def
}
