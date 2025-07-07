package rsync

import (
	"sync"
	"sync/atomic"
)

// ReadyChannels is a collection of channels that can be used to signal readiness.
type ReadyChannels struct {
	mu    sync.Mutex
	chans []<-chan struct{}

	numClosedReadyChannels *atomic.Int64
}

// NewReadyChannels returns a new ReadyChannels.
func NewReadyChannels() *ReadyChannels {
	return &ReadyChannels{
		numClosedReadyChannels: &atomic.Int64{},
	}
}

// Register registers a channel with the ReadyChannels. Until the given
// channel is closed, the ReadyChannels is not considered ready.
func (r *ReadyChannels) Register(ch <-chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.chans = append(r.chans, ch)
	go func() {
		<-ch
		r.numClosedReadyChannels.Add(1)
	}()
}

// Ready returns true if all registered channels have been closed.
func (r *ReadyChannels) Ready() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return int(r.numClosedReadyChannels.Load()) == len(r.chans)
}
