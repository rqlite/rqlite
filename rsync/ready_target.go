package rsync

import "sync"

// ReadyTarget is a mechanism where by a target can be signalled as reached.
type ReadyTarget struct {
	mu            sync.RWMutex
	currentTarget uint64
	subscribers   []*Subscriber
}

// NewReadyTarget returns a new ReadyTarget.
func NewReadyTarget() *ReadyTarget {
	return &ReadyTarget{
		subscribers: make([]*Subscriber, 0),
	}
}

// Subscriber is a subscription to a target.
type Subscriber struct {
	target uint64
	ch     chan struct{}
}

// Subscribe returns a channel that will be closed when the target is reached.
// If the target has already been a reached the channel will be returned in a
// closed state.
func (r *ReadyTarget) Subscribe(target uint64) <-chan struct{} {
	ch := make(chan struct{})
	r.mu.Lock()
	defer r.mu.Unlock()
	if target <= r.currentTarget {
		close(ch)
	} else {
		subscriber := &Subscriber{
			target: target,
			ch:     ch,
		}
		r.subscribers = append(r.subscribers, subscriber)
	}
	return ch
}

// Unsubscribe removes the subscription for the given target.
func (r *ReadyTarget) Unsubscribe(ch <-chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, subscriber := range r.subscribers {
		if subscriber.ch == ch {
			r.subscribers = append(r.subscribers[:i], r.subscribers[i+1:]...)
			break
		}
	}
}

// Signal informs the ReadyTarget that the given target has been reached.
// If the target is less than or equal to the current target, then the
// index is ignored.
func (r *ReadyTarget) Signal(index uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if index <= r.currentTarget {
		return
	}
	r.currentTarget = index
	var remainingSubscribers []*Subscriber
	for _, subscriber := range r.subscribers {
		if index >= subscriber.target {
			close(subscriber.ch)
		} else {
			remainingSubscribers = append(remainingSubscribers, subscriber)
		}
	}
	r.subscribers = remainingSubscribers
}

// Len returns the number of subscribers.
func (r *ReadyTarget) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.subscribers)
}
