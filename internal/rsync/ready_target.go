package rsync

import "sync"

// Ordered is a type that can be compared.
type Ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64 | ~string
}

// ReadyTarget is a mechanism whereby a target can be signalled as reached.
type ReadyTarget[T Ordered] struct {
	mu            sync.RWMutex
	currentTarget T
	subscribers   []*Subscriber[T]
}

// NewReadyTarget returns a new ReadyTarget.
func NewReadyTarget[T Ordered]() *ReadyTarget[T] {
	return &ReadyTarget[T]{
		subscribers: make([]*Subscriber[T], 0),
	}
}

// Subscriber is a subscription to notifications for a target being reached.
type Subscriber[T Ordered] struct {
	target T
	ch     chan struct{}
}

// Subscribe returns a channel that will be closed when the target is reached.
// If the target has already been a reached the channel will be returned in a
// closed state.
func (r *ReadyTarget[T]) Subscribe(target T) <-chan struct{} {
	ch := make(chan struct{})
	r.mu.Lock()
	defer r.mu.Unlock()
	if target <= r.currentTarget {
		close(ch)
	} else {
		subscriber := &Subscriber[T]{
			target: target,
			ch:     ch,
		}
		r.subscribers = append(r.subscribers, subscriber)
	}
	return ch
}

// Unsubscribe removes the subscription for the given target.
func (r *ReadyTarget[T]) Unsubscribe(ch <-chan struct{}) {
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
func (r *ReadyTarget[T]) Signal(index T) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if index <= r.currentTarget {
		return
	}
	r.currentTarget = index
	var remainingSubscribers []*Subscriber[T]
	for _, subscriber := range r.subscribers {
		if index >= subscriber.target {
			close(subscriber.ch)
		} else {
			remainingSubscribers = append(remainingSubscribers, subscriber)
		}
	}
	r.subscribers = remainingSubscribers
}

// Reset resets the ReadyTarget back to the initial state, removing
// any subscribers.
func (r *ReadyTarget[T]) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	var t T
	r.currentTarget = t
	r.subscribers = make([]*Subscriber[T], 0)
}

// Len returns the number of subscribers.
func (r *ReadyTarget[T]) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.subscribers)
}
