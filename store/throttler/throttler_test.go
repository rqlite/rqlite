package throttler

import (
	"context"
	"testing"
	"time"
)

func Test_ThrottlerNewDefault(t *testing.T) {
	th := DefaultThrottler()
	if th.Level() != 0 {
		t.Fatalf("expected level 0, got %d", th.Level())
	}
	if th.GetDelay() != 0 {
		t.Fatalf("expected zero delay, got %s", th.GetDelay())
	}
}

func Test_ThrottlerSignal(t *testing.T) {
	th := New([]time.Duration{0, 10 * time.Millisecond, 50 * time.Millisecond}, 1, 0)

	th.Signal()
	if th.Level() != 1 {
		t.Fatalf("expected level 1, got %d", th.Level())
	}
	if th.GetDelay() != 10*time.Millisecond {
		t.Fatalf("expected 10ms, got %s", th.GetDelay())
	}

	th.Signal()
	if th.Level() != 2 {
		t.Fatalf("expected level 2, got %d", th.Level())
	}
	if th.GetDelay() != 50*time.Millisecond {
		t.Fatalf("expected 50ms, got %s", th.GetDelay())
	}
}

func Test_ThrottlerSignalCeiling(t *testing.T) {
	th := New([]time.Duration{0, 10 * time.Millisecond}, 1, 0)

	th.Signal()
	th.Signal()
	th.Signal()
	if th.Level() != 1 {
		t.Fatalf("expected level 1 (ceiling), got %d", th.Level())
	}
}

func Test_ThrottlerReleaseByOne(t *testing.T) {
	th := New([]time.Duration{0, 10 * time.Millisecond, 50 * time.Millisecond}, 1, 0)

	th.Signal()
	th.Signal()
	if th.Level() != 2 {
		t.Fatalf("expected level 2, got %d", th.Level())
	}

	th.Release()
	if th.Level() != 1 {
		t.Fatalf("expected level 1, got %d", th.Level())
	}

	th.Release()
	if th.Level() != 0 {
		t.Fatalf("expected level 0, got %d", th.Level())
	}
}

func Test_ThrottlerReleaseByTwo(t *testing.T) {
	th := New([]time.Duration{
		0, 10 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond,
	}, 2, 0)

	th.Signal()
	th.Signal()
	th.Signal()
	th.Signal()
	if th.Level() != 4 {
		t.Fatalf("expected level 4, got %d", th.Level())
	}

	th.Release()
	if th.Level() != 2 {
		t.Fatalf("expected level 2, got %d", th.Level())
	}

	th.Release()
	if th.Level() != 0 {
		t.Fatalf("expected level 0, got %d", th.Level())
	}
}

func Test_ThrottlerReleaseByTwoFloor(t *testing.T) {
	th := New([]time.Duration{0, 10 * time.Millisecond, 50 * time.Millisecond}, 2, 0)

	th.Signal()
	if th.Level() != 1 {
		t.Fatalf("expected level 1, got %d", th.Level())
	}

	// Release by 2 from level 1 should clamp to 0, not go negative.
	th.Release()
	if th.Level() != 0 {
		t.Fatalf("expected level 0 (floor), got %d", th.Level())
	}
}

func Test_ThrottlerReleaseFloor(t *testing.T) {
	th := DefaultThrottler()

	th.Release()
	th.Release()
	if th.Level() != 0 {
		t.Fatalf("expected level 0 (floor), got %d", th.Level())
	}
}

func Test_ThrottlerReset(t *testing.T) {
	th := DefaultThrottler()

	th.Signal()
	th.Signal()
	th.Signal()
	th.Reset()
	if th.Level() != 0 {
		t.Fatalf("expected level 0 after reset, got %d", th.Level())
	}
	if th.GetDelay() != 0 {
		t.Fatalf("expected zero delay after reset, got %s", th.GetDelay())
	}
}

func Test_ThrottlerDelayZero(t *testing.T) {
	th := DefaultThrottler()

	start := time.Now()
	err := th.Delay(context.Background())
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if elapsed > time.Millisecond {
		t.Fatalf("expected near-zero delay, got %s", elapsed)
	}
}

func Test_ThrottlerDelayNonZero(t *testing.T) {
	th := New([]time.Duration{0, 50 * time.Millisecond}, 1, 0)
	th.Signal()

	start := time.Now()
	err := th.Delay(context.Background())
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if elapsed < 40*time.Millisecond {
		t.Fatalf("expected ~50ms delay, got %s", elapsed)
	}
}

func Test_ThrottlerDelayContextCancellation(t *testing.T) {
	th := New([]time.Duration{0, 5 * time.Second}, 1, 0)
	th.Signal()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := th.Delay(ctx)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected context error, got nil")
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("expected early return on cancellation, took %s", elapsed)
	}
}

func Test_ThrottlerNewEmpty(t *testing.T) {
	th := New(nil, 1, 0)
	if th.Level() != 0 {
		t.Fatalf("expected level 0, got %d", th.Level())
	}
	if th.GetDelay() != 0 {
		t.Fatalf("expected zero delay, got %s", th.GetDelay())
	}
}

func Test_ThrottlerIdleTimeout(t *testing.T) {
	th := New([]time.Duration{0, 10 * time.Millisecond, 50 * time.Millisecond}, 1, 100*time.Millisecond)

	th.Signal()
	th.Signal()
	if th.Level() != 2 {
		t.Fatalf("expected level 2, got %d", th.Level())
	}

	// Wait for idle timeout to fire.
	time.Sleep(200 * time.Millisecond)
	if th.Level() != 0 {
		t.Fatalf("expected level 0 after idle timeout, got %d", th.Level())
	}
}

func Test_ThrottlerIdleTimeoutResetBySignal(t *testing.T) {
	th := New([]time.Duration{0, 10 * time.Millisecond, 50 * time.Millisecond}, 1, 150*time.Millisecond)

	th.Signal()
	th.Signal()

	// Signal again before timeout expires, resetting the timer.
	time.Sleep(100 * time.Millisecond)
	th.Signal() // still at ceiling (2), but restarts idle timer
	if th.Level() != 2 {
		t.Fatalf("expected level 2, got %d", th.Level())
	}

	// 100ms later: 100ms since last signal, timeout is 150ms, should not have fired.
	time.Sleep(100 * time.Millisecond)
	if th.Level() != 2 {
		t.Fatalf("expected level 2 (timer not yet fired), got %d", th.Level())
	}

	// 100ms more: 200ms since last signal, timeout should have fired.
	time.Sleep(100 * time.Millisecond)
	if th.Level() != 0 {
		t.Fatalf("expected level 0 after idle timeout, got %d", th.Level())
	}
}

func Test_ThrottlerNoIdleTimeout(t *testing.T) {
	th := New([]time.Duration{0, 10 * time.Millisecond}, 1, 0)

	th.Signal()
	time.Sleep(50 * time.Millisecond)
	if th.Level() != 1 {
		t.Fatalf("expected level 1 (no idle timeout configured), got %d", th.Level())
	}
}
