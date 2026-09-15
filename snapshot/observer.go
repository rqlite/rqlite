package snapshot

import (
	"sync"
	"sync/atomic"
	"time"
)

// ReapObservation is sent to registered observers after a successful reap.
type ReapObservation struct {
	SnapshotsReaped int
	WALsReaped      int
	Duration        time.Duration
}

// FilterFn is a function that can be used to filter observations. If it
// returns true the observation is included, otherwise it is filtered out.
type FilterFn func(o *ReapObservation) bool

// Observer is used to observe reap events. It holds a channel to receive
// ReapObservation events and an optional filter function. Observations
// are always sent in a non-blocking manner; if the channel is full the
// observation is dropped.
type Observer struct {
	// channel receives observations.
	channel chan ReapObservation

	// filter is an optional function to filter observations.
	filter FilterFn

	// numObserved and numDropped are counters for the number of
	// observations received and dropped, respectively.
	numObserved uint64
	numDropped  uint64
}

// NewObserver creates a new observer. The filter function, if non-nil,
// is called for each observation and only observations for which the
// function returns true are sent to the channel. If the channel is full,
// observations are dropped.
func NewObserver(channel chan ReapObservation, filter FilterFn) *Observer {
	return &Observer{
		channel: channel,
		filter:  filter,
	}
}

// GetNumObserved returns the number of observations sent to this observer.
func (o *Observer) GetNumObserved() uint64 {
	return atomic.LoadUint64(&o.numObserved)
}

// GetNumDropped returns the number of observations dropped because the
// channel was full.
func (o *Observer) GetNumDropped() uint64 {
	return atomic.LoadUint64(&o.numDropped)
}

// observe sends an observation to this observer in a non-blocking manner,
// respecting the filter setting.
func (o *Observer) observe(obs ReapObservation) {
	if o.filter != nil && !o.filter(&obs) {
		return
	}
	select {
	case o.channel <- obs:
		atomic.AddUint64(&o.numObserved, 1)
	default:
		atomic.AddUint64(&o.numDropped, 1)
	}
}

// observerSet manages a set of registered observers.
type observerSet struct {
	mu        sync.RWMutex
	observers map[*Observer]struct{}
}

// newObserverSet creates a new observerSet.
func newObserverSet() *observerSet {
	return &observerSet{
		observers: make(map[*Observer]struct{}),
	}
}

// register adds an observer to the set. If o is nil, the call is a no-op.
func (s *observerSet) register(o *Observer) {
	if o == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.observers[o] = struct{}{}
}

// deregister removes an observer from the set.
func (s *observerSet) deregister(o *Observer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.observers, o)
}

// notify sends an observation to all registered observers.
func (s *observerSet) notify(obs ReapObservation) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for o := range s.observers {
		o.observe(obs)
	}
}
