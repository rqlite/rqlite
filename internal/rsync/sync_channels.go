package rsync

import (
	"sync"
	"sync/atomic"
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
// to close. Returns the total number of channels and the number that
// successfully synchronized before any error or timeout.
func (sc *SyncChannels) Sync(timeout time.Duration) (int, int, error) {
	sc.chsMu.Lock()
	defer sc.chsMu.Unlock()
	cancel := make(chan struct{})
	defer close(cancel)

	var nOK atomic.Int32
	var wg sync.WaitGroup
	wg.Add(len(sc.chs))
	for _, ch := range sc.chs {
		go func(ch chan<- chan struct{}) {
			defer wg.Done()
			syncCh := make(chan struct{})
			ch <- syncCh
			select {
			case <-syncCh:
				nOK.Add(1)
			case <-cancel:
			}
		}(ch)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	var err error
	select {
	case <-done:
	case <-time.After(timeout):
		err = ErrTimeout
	}
	return len(sc.chs), int(nOK.Load()), err
}
