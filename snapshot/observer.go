package snapshot

import (
	"sync"
	"sync/atomic"
)

// ReapObservation is sent to registered observers after a successful reap.
type ReapObservation struct {
	SnapshotsReaped int
	WALsReaped      int
	Duration        int64 // milliseconds
}

// FilterFn is a function that can be used to filter observations. If it
// returns true the observation is included, otherwise it is filtered out.
type FilterFn func(o *ReapObservation) bool

// Observer is used to observe reap events. It holds a channel to receive
// ReapObservation events, a blocking flag, and an optional filter function.
type Observer struct {
	// channel receives observations.
	channel chan ReapObservation

	// blocking, if true, will cause the observer to block when the
	// channel is full. Otherwise, observations are dropped.
	blocking bool

	// filter is an optional function to filter observations.
	filter FilterFn

	// numObserved and numDropped are counters for the number of
	// observations received and dropped, respectively.
	numObserved uint64
	numDropped  uint64
}

// NewObserver creates a new observer. If blocking is true, the observer
// will block when the channel is full. Otherwise, observations are
// dropped. The filter function, if non-nil, is called for each
// observation and only observations for which the function returns true
// are sent to the channel.
func NewObserver(channel chan ReapObservation, blocking bool, filter FilterFn) *Observer {
	return &Observer{
		channel:  channel,
		blocking: blocking,
		filter:   filter,
	}
}

// GetNumObserved returns the number of observations sent to this observer.
func (o *Observer) GetNumObserved() uint64 {
	return atomic.LoadUint64(&o.numObserved)
}

// GetNumDropped returns the number of observations dropped because the
// channel was full and blocking was false.
func (o *Observer) GetNumDropped() uint64 {
	return atomic.LoadUint64(&o.numDropped)
}

// observe sends an observation to this observer, respecting filter and
// blocking settings. Returns true if the observation was sent.
func (o *Observer) observe(obs ReapObservation) {
	if o.filter != nil && !o.filter(&obs) {
		return
	}
	if o.blocking {
		o.channel <- obs
		atomic.AddUint64(&o.numObserved, 1)
	} else {
		select {
		case o.channel <- obs:
			atomic.AddUint64(&o.numObserved, 1)
		default:
			atomic.AddUint64(&o.numDropped, 1)
		}
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

// register adds an observer to the set.
func (s *observerSet) register(o *Observer) {
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
