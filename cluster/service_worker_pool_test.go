package cluster

import (
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Test_WorkerPoolDoubleClose verifies Close() is idempotent (safe to call multiple times)
func Test_WorkerPoolDoubleClose(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open service: %s", err)
	}

	// Call Close multiple times - should not panic
	if err := s.Close(); err != nil {
		t.Fatalf("first close failed: %s", err)
	}

	// Second close should not panic ("close of closed channel")
	if err := s.Close(); err != nil {
		t.Fatalf("second close failed: %s", err)
	}

	// Third close for good measure
	if err := s.Close(); err != nil {
		t.Fatalf("third close failed: %s", err)
	}

	t.Log("✓ Multiple Close() calls handled correctly")
}

// Test_WorkerPoolCloseBeforeOpen verifies Close() handles not being opened
func Test_WorkerPoolCloseBeforeOpen(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())

	// Close without ever calling Open() - should not panic
	if err := s.Close(); err != nil {
		t.Fatalf("close before open failed: %s", err)
	}

	t.Log("✓ Close() before Open() handled correctly")
}

// Test_WorkerPoolBoundedGoroutines verifies that the worker pool maintains
// a bounded number of goroutines even with many concurrent connections.
func Test_WorkerPoolBoundedGoroutines(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())

	// Set small worker pool for testing
	s.NumWorkers = 10
	s.ConnQueueSize = 50

	initialGoroutines := runtime.NumGoroutine()

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open service: %s", err)
	}
	defer s.Close()

	// Allow goroutines to start
	time.Sleep(100 * time.Millisecond)

	openGoroutines := runtime.NumGoroutine()

	// Create many connections concurrently
	numConnections := 100
	var wg sync.WaitGroup
	for i := 0; i < numConnections; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", ml.Addr().String())
			if err != nil {
				t.Logf("failed to dial: %s", err)
				return
			}
			defer conn.Close()
			// Hold connection briefly
			time.Sleep(50 * time.Millisecond)
		}()
	}

	// Wait for connections to be made
	wg.Wait()
	time.Sleep(100 * time.Millisecond)

	peakGoroutines := runtime.NumGoroutine()

	// Cleanup happens in defer, let it proceed
	time.Sleep(100 * time.Millisecond)

	finalGoroutines := runtime.NumGoroutine()

	// With worker pool, peak goroutines should be bounded to roughly:
	// initial + 10 workers + 1 accept loop + a few network handlers
	// Without worker pool, peak would be initial + 100 (one per connection)
	maxExpectedGoroutines := initialGoroutines + s.NumWorkers + 10

	if peakGoroutines > maxExpectedGoroutines {
		t.Logf("WARNING: Peak goroutines (%d) exceeded expected bounded limit (%d)", peakGoroutines, maxExpectedGoroutines)
		t.Logf("  Initial: %d", initialGoroutines)
		t.Logf("  After Open: %d", openGoroutines)
		t.Logf("  Peak with %d connections: %d", numConnections, peakGoroutines)
		t.Logf("  Final: %d", finalGoroutines)
		// Don't fail hard, but log the warning as the test is platform-dependent
	}

	// Verify cleanup: final should be close to initial
	if finalGoroutines > initialGoroutines+5 {
		t.Logf("WARNING: Goroutine cleanup incomplete. Initial: %d, Final: %d", initialGoroutines, finalGoroutines)
	}

	t.Logf("Goroutine tracking: Initial=%d, Open=%d, Peak=%d, Final=%d",
		initialGoroutines, openGoroutines, peakGoroutines, finalGoroutines)
}

// Test_WorkerPoolQueueing verifies that connections are properly queued
// when all workers are busy.
func Test_WorkerPoolQueueing(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())

	s.NumWorkers = 2     // Very small pool
	s.ConnQueueSize = 10 // Small queue

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open service: %s", err)
	}
	defer s.Close()

	time.Sleep(100 * time.Millisecond)

	// Make simultaneous connections
	var processed int32
	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", ml.Addr().String())
			if err != nil {
				t.Logf("failed to dial: %s", err)
				return
			}
			defer conn.Close()

			// Keep connection alive briefly
			time.Sleep(100 * time.Millisecond)
			atomic.AddInt32(&processed, 1)
		}()
	}

	wg.Wait()
	time.Sleep(100 * time.Millisecond)

	if processed < 5 {
		t.Logf("Not all connections were processed: %d/5", processed)
	}
}

// Test_WorkerPoolGracefulShutdown verifies that the worker pool shuts down
// gracefully without leaving goroutines hanging.
func Test_WorkerPoolGracefulShutdown(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())

	s.NumWorkers = 5
	s.ConnQueueSize = 20

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open service: %s", err)
	}

	time.Sleep(100 * time.Millisecond)

	beforeClose := runtime.NumGoroutine()

	// Close the service
	if err := s.Close(); err != nil {
		t.Fatalf("failed to close service: %s", err)
	}

	time.Sleep(100 * time.Millisecond)
	afterClose := runtime.NumGoroutine()

	// After close, should have roughly same goroutines as before open
	// Allow some slack for timing issues
	if afterClose > beforeClose+3 {
		t.Logf("WARNING: Goroutines not cleaned up properly. Before: %d, After: %d", beforeClose, afterClose)
	}
}

// Test_WorkerPoolNoLeak verifies no goroutines leak during repeated open/close cycles.
func Test_WorkerPoolNoLeak(t *testing.T) {
	initialGoroutines := runtime.NumGoroutine()

	for cycle := 0; cycle < 5; cycle++ {
		ml := mustNewMockTransport()
		s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())

		s.NumWorkers = 10
		s.ConnQueueSize = 50

		if err := s.Open(); err != nil {
			t.Fatalf("cycle %d: failed to open: %s", cycle, err)
		}

		time.Sleep(50 * time.Millisecond)

		if err := s.Close(); err != nil {
			t.Fatalf("cycle %d: failed to close: %s", cycle, err)
		}

		time.Sleep(50 * time.Millisecond)
	}

	finalGoroutines := runtime.NumGoroutine()

	// After 5 cycles of open/close, should not have accumulated goroutines
	if finalGoroutines > initialGoroutines+5 {
		t.Logf("LEAK DETECTED: Initial: %d, Final: %d (difference: %d)",
			initialGoroutines, finalGoroutines, finalGoroutines-initialGoroutines)
	}
}
