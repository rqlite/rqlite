package rsync

import (
	"errors"
	"testing"
	"time"
)

// helper duration constants to keep tests fast but reliable.
const (
	shortDelay   = 20 * time.Millisecond
	longDelay    = 60 * time.Millisecond
	testTimeout  = 500 * time.Millisecond
	smallTimeout = 50 * time.Millisecond
)

func Test_Sync_NoRegistrations(t *testing.T) {
	sc := NewSyncChannels()
	total, ok, err := sc.Sync(testTimeout)
	if err != nil {
		t.Fatalf("Sync() with no registrations returned error: %v", err)
	}
	if total != 0 || ok != 0 {
		t.Fatalf("Sync() with no registrations returned total=%d, ok=%d; want 0,0", total, ok)
	}
}

func Test_Sync_SingleChannel_Succeeds(t *testing.T) {
	sc := NewSyncChannels()
	ch := make(chan chan struct{})
	sc.Register(ch)

	// Receiver: upon getting the sync channel, close it to signal completion.
	go func() {
		syncCh := <-ch
		close(syncCh)
	}()

	total, ok, err := sc.Sync(testTimeout)
	if err != nil {
		t.Fatalf("Sync() returned unexpected error: %v", err)
	}
	if total != 1 || ok != 1 {
		t.Fatalf("Sync() returned total=%d, ok=%d; want 1,0", total, ok)
	}
}

func Test_Sync_MultipleChannels_WaitsForAll(t *testing.T) {
	sc := NewSyncChannels()

	ch1 := make(chan chan struct{})
	ch2 := make(chan chan struct{})
	sc.Register(ch1)
	sc.Register(ch2)

	// ch1 closes quickly; ch2 closes after a delay. Sync should not return
	// until ch2 has closed, but should finish well before the timeout.
	go func() {
		syncCh := <-ch1
		time.Sleep(shortDelay)
		close(syncCh)
	}()
	go func() {
		syncCh := <-ch2
		time.Sleep(longDelay)
		close(syncCh)
	}()

	total, ok, err := sc.Sync(testTimeout)
	if err != nil {
		t.Fatalf("Sync() returned unexpected error: %v", err)
	}
	if total != 2 || ok != 2 {
		t.Fatalf("Sync() returned total=%d, ok=%d; want 2,2", total, ok)
	}
}

func Test_Sync_Timeout_WhenParticipantNeverCloses(t *testing.T) {
	sc := NewSyncChannels()

	chGood := make(chan chan struct{})
	chStuck := make(chan chan struct{})
	sc.Register(chGood)
	sc.Register(chStuck)

	// Good participant closes promptly.
	go func() {
		syncCh := <-chGood
		close(syncCh)
	}()

	// Stuck participant: read the sync channel but never close it.
	// This avoids goroutine leaks while ensuring Sync times out.
	go func() {
		syncCh := <-chStuck
		_ = syncCh // intentionally never close
	}()

	total, ok, err := sc.Sync(smallTimeout)
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("Sync() error = %v, want ErrTimeout", err)
	}
	if total != 2 || ok != 1 {
		t.Fatalf("Sync() returned total=%d, ok=%d; want 2,1", total, ok)
	}
}

func Test_Sync_ReusedAcrossCalls(t *testing.T) {
	sc := NewSyncChannels()

	ch1 := make(chan chan struct{})
	ch2 := make(chan chan struct{})
	sc.Register(ch1)
	sc.Register(ch2)

	// First sync: close both quickly.
	done1 := make(chan struct{})
	go func() {
		s1 := <-ch1
		close(s1)
		s2 := <-ch2
		close(s2)
		close(done1)
	}()
	total, ok, err := sc.Sync(testTimeout)
	if err != nil {
		t.Fatalf("first Sync() error: %v", err)
	}
	<-done1

	if total != 2 || ok != 2 {
		t.Fatalf("first Sync() returned total=%d, ok=%d; want 2,2", total, ok)
	}

	// Second sync: staggered closes, still should succeed before timeout.
	go func() {
		s1 := <-ch1
		time.Sleep(shortDelay)
		close(s1)
		s2 := <-ch2
		time.Sleep(shortDelay)
		close(s2)
	}()
	total, ok, err = sc.Sync(testTimeout)
	if err != nil {
		t.Fatalf("second Sync() error: %v", err)
	}
	if total != 2 || ok != 2 {
		t.Fatalf("second Sync() returned total=%d, ok=%d; want 2,2", total, ok)
	}
}
