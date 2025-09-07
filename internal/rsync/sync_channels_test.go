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

func TestSync_NoRegistrations(t *testing.T) {
	sc := NewSyncChannels()
	start := time.Now()
	if err := sc.Sync(testTimeout); err != nil {
		t.Fatalf("Sync() with no registrations returned error: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 25*time.Millisecond {
		t.Fatalf("Sync() with no registrations should return near-immediately, took %v", elapsed)
	}
}

func TestSync_SingleChannel_Succeeds(t *testing.T) {
	sc := NewSyncChannels()
	ch := make(chan chan struct{})
	sc.Register(ch)

	// Receiver: upon getting the sync channel, close it to signal completion.
	go func() {
		syncCh := <-ch
		close(syncCh)
	}()

	if err := sc.Sync(testTimeout); err != nil {
		t.Fatalf("Sync() returned unexpected error: %v", err)
	}
}

func TestSync_MultipleChannels_WaitsForAll(t *testing.T) {
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

	start := time.Now()
	if err := sc.Sync(testTimeout); err != nil {
		t.Fatalf("Sync() returned unexpected error: %v", err)
	}
	elapsed := time.Since(start)
	// It should take at least the longDelay, but not approach the timeout.
	if elapsed < longDelay {
		t.Fatalf("Sync() returned too early; elapsed=%v, want >= %v", elapsed, longDelay)
	}
	if elapsed > testTimeout/2 {
		t.Fatalf("Sync() took suspiciously long; elapsed=%v, timeout=%v", elapsed, testTimeout)
	}
}

func TestSync_Timeout_WhenParticipantNeverCloses(t *testing.T) {
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

	err := sc.Sync(smallTimeout)
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("Sync() error = %v, want ErrTimeout", err)
	}
}

func TestSync_ReusedAcrossCalls(t *testing.T) {
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
	if err := sc.Sync(testTimeout); err != nil {
		t.Fatalf("first Sync() error: %v", err)
	}
	<-done1

	// Second sync: staggered closes, still should succeed before timeout.
	go func() {
		s1 := <-ch1
		time.Sleep(shortDelay)
		close(s1)
		s2 := <-ch2
		time.Sleep(shortDelay)
		close(s2)
	}()
	if err := sc.Sync(testTimeout); err != nil {
		t.Fatalf("second Sync() error: %v", err)
	}
}
