package rsync

import (
	"sync"
	"testing"
	"time"
)

// TestNewReadyChannels ensures that a new ReadyChannels instance is ready when no channels are registered.
func Test_NewReadyChannels(t *testing.T) {
	rc := NewReadyChannels()
	if !rc.Ready() {
		t.Errorf("Expected Ready() to be true for new ReadyChannels with no registered channels")
	}
}

// Test_Ready_AllClosedChannels checks that Ready() returns true when all registered channels are already closed.
func Test_Ready_AllClosedChannels(t *testing.T) {
	rc := NewReadyChannels()

	// Create and close multiple channels
	numChannels := 5
	for i := 0; i < numChannels; i++ {
		ch := make(chan struct{})
		close(ch)
		rc.Register(ch)
	}

	if !trueOrTimeout(t, rc.Ready, 100*time.Millisecond) {
		t.Errorf("Expected Ready() to be true when all registered channels are already closed")
	}
}

// Test_Ready_SomeClosedChannels ensures that Ready() returns false when some channels are still open.
func Test_Ready_SomeClosedChannels(t *testing.T) {
	rc := NewReadyChannels()

	// Create multiple channels but do not close them
	numChannels := 5
	for i := 0; i < numChannels; i++ {
		ch := make(chan struct{})
		rc.Register(ch)
	}

	if rc.Ready() {
		t.Errorf("Expected Ready() to be false when some registered channels are still open")
	}
}

// Test_Ready_ChannelsFullCycle verifies that Ready() becomes true after all registered channels are closed.
func Test_Ready_ChannelsFullCycle(t *testing.T) {
	rc := NewReadyChannels()

	numChannels := 5
	channels := make([]chan struct{}, numChannels)

	// Register channels
	for i := 0; i < numChannels; i++ {
		ch := make(chan struct{})
		channels[i] = ch
		rc.Register(ch)
	}

	// Initially, Ready() should be false
	if rc.Ready() {
		t.Errorf("Expected Ready() to be false before closing any channels")
	}

	// Close channels one by one and check Ready()
	for i, ch := range channels {
		close(ch)
		if i < numChannels-1 {
			// Wait up to 50 milliseconds to confirm it doesn't become ready
			if trueOrTimeout(t, rc.Ready, 50*time.Millisecond) {
				t.Errorf("Expected Ready() to be false after closing %d/%d channels", i+1, numChannels)
			}
		}
	}

	if !trueOrTimeout(t, rc.Ready, 100*time.Millisecond) {
		t.Errorf("Expected Ready() to be true when all registered channels are closed")
	}
}

// TestReadyIsThreadSafe ensures that Ready() can be called concurrently without race conditions.
func TestReadyIsThreadSafe(t *testing.T) {
	rc := NewReadyChannels()
	var wg sync.WaitGroup

	numChannels := 50
	channels := make([]chan struct{}, numChannels)

	// Register channels
	for i := 0; i < numChannels; i++ {
		ch := make(chan struct{})
		channels[i] = ch
		rc.Register(ch)
	}

	// Start goroutines that call Ready() concurrently
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = rc.Ready()
		}()
	}

	// Close all channels
	for _, ch := range channels {
		close(ch)
	}

	wg.Wait()

	if !trueOrTimeout(t, rc.Ready, 100*time.Millisecond) {
		t.Errorf("Expected Ready() to be true when all registered channels are closed")
	}
}

func trueOrTimeout(t *testing.T, fn func() bool, dur time.Duration) bool {
	t.Helper()
	timer := time.NewTimer(dur)
	defer timer.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timer.C:
			return false
		case <-ticker.C:
			if fn() {
				return true
			}
		}
	}
}
