package main

import (
	"log"
	"os"
	"os/signal"
)

const (
	sigChSize = 32
)

// HandleSignals returns a channel on which to receive the specified signals.
func HandleSignals(sigs ...os.Signal) <-chan os.Signal {
	ch := make(chan os.Signal, sigChSize)
	go func() {
		sigCh := make(chan os.Signal, sigChSize)
		signal.Notify(sigCh, sigs...)
		for {
			sig := <-sigCh
			log.Printf(`received signal "%s"`, sig.String())
			ch <- sig
		}
	}()
	return ch
}
