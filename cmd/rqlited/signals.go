package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/hashicorp/go-hclog"
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
			hclog.Default().Info(fmt.Sprintf(`received signal "%s"`, sig))
			ch <- sig
		}
	}()
	return ch
}

// CreateContext creates a context which is canceled if signals are received
// on the given channel.
func CreateContext(ch <-chan os.Signal) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-ch
		cancel()
	}()
	return ctx, cancel
}
