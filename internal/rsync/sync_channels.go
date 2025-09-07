package rsync

import (
	"sync"
	"time"
)

// SyncChannels supports blocking synchronization. Clients can register channels
// to receive a sync signal. When Sync is called, it sends a channel to each
// registered channel and waits for that channel to close.
type SyncChannels struct {
	chsMu sync.Mutex
	chs   []chan<- chan struct{}
}

// NewSyncChannels creates a new SyncChannels instance.
func NewSyncChannels() *SyncChannels {
	return &SyncChannels{
		chs: make([]chan<- chan struct{}, 0),
	}
}

// Register adds a channel to the list of channels to be synchronized.
func (sc *SyncChannels) Register(ch chan<- chan struct{}) {
	sc.chsMu.Lock()
	defer sc.chsMu.Unlock()
	sc.chs = append(sc.chs, ch)
}

// Sync sends channel to all registered channels and waits for those channels
// to close.
func (sc *SyncChannels) Sync(timeout time.Duration) error {
	sc.chsMu.Lock()
	defer sc.chsMu.Unlock()

	var wg sync.WaitGroup
	wg.Add(len(sc.chs))
	for _, ch := range sc.chs {
		go func(ch chan<- chan struct{}) {
			defer wg.Done()
			syncCh := make(chan struct{})
			ch <- syncCh
			<-syncCh
		}(ch)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return ErrTimeout
	}
}
