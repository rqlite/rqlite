package rsync

import (
	"sync"
	"testing"
	"time"
)

func Test_NewReadyTarget(t *testing.T) {
	t.Parallel()
	rt := NewReadyTarget[uint64]()
	if rt == nil {
		t.Fatal("NewReadyTarget returned nil")
	}
	if rt.Len() != 0 {
		t.Fatalf("ReadyTarget has non-zero length: %d", rt.Len())
	}

	// Ensure reset doesn't panic.
	rt.Reset()
	rt.Reset()
}

// Test_ReadyTargetSignal_NoSubscribers tests the ReadyTarget.Signal method
// when there are no subscribers. Basically, it should do nothing.
func Test_ReadyTargetSignal_NoSubscribers(t *testing.T) {
	t.Parallel()
	rt := NewReadyTarget[uint64]()
	rt.Signal(1)
	rt.Signal(1)
	rt.Signal(0)
	rt.Signal(2)
}

func Test_ReadyTargetSignal_SubscribeReset(t *testing.T) {
	t.Parallel()
	rt := NewReadyTarget[uint64]()
	rt.Subscribe(1)
	if rt.Len() != 1 {
		t.Fatalf("ReadyTarget has non-zero length: %d", rt.Len())
	}
	rt.Reset()
	if rt.Len() != 0 {
		t.Fatalf("ReadyTarget has non-zero length: %d", rt.Len())
	}
}

func Test_ReadyTargetSignal_SubscribeSignalled(t *testing.T) {
	t.Parallel()
	rt := NewReadyTarget[uint64]()

	ch1 := rt.Subscribe(1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ch1
	}()

	rt.Signal(2)
	wg.Wait()
}

func Test_ReadyTargetSignal_SubscribeSignalled_Double(t *testing.T) {
	t.Parallel()
	rt := NewReadyTarget[uint64]()

	ch1 := rt.Subscribe(1)
	ch2 := rt.Subscribe(2)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ch1
		<-ch2
	}()

	rt.Signal(2)
	wg.Wait()
}

func Test_ReadyTargetSignal_SubscribeNotSignalled(t *testing.T) {
	t.Parallel()
	rt := NewReadyTarget[uint64]()

	ch1 := rt.Subscribe(2)
	called := false
	go func() {
		<-ch1
		called = true
	}()

	rt.Signal(1)
	time.Sleep(200 * time.Millisecond)
	if called {
		t.Fatal("Subscriber was signalled when it should not have been")
	}
}

func Test_ReadyTargetSignal_SubscribeNotSignalled_Unsubscribed(t *testing.T) {
	t.Parallel()
	rt := NewReadyTarget[uint64]()

	ch1 := rt.Subscribe(1)
	rt.Unsubscribe(ch1)
	if rt.Len() != 0 {
		t.Fatalf("ReadyTarget has non-zero length: %d", rt.Len())
	}
	called := false
	go func() {
		<-ch1
		called = true
	}()

	rt.Signal(2)
	time.Sleep(200 * time.Millisecond)
	if called {
		t.Fatal("Subscriber was signalled when it should not have been")
	}
}

func Test_ReadyTargetSignal_Subscribe_DoubleNotSignalled(t *testing.T) {
	t.Parallel()
	rt := NewReadyTarget[uint64]()

	ch1 := rt.Subscribe(1)
	var wg1 sync.WaitGroup
	wg1.Add(1)
	go func() {
		defer wg1.Done()
		<-ch1
	}()

	ch2 := rt.Subscribe(3)
	var wg2 sync.WaitGroup
	wg2.Add(1)
	called2 := false
	go func() {
		defer wg2.Done()
		<-ch2
		called2 = true
	}()

	rt.Signal(2)
	wg1.Wait()
	if called2 {
		t.Fatal("Subscriber 2 was signalled when it should not have been")
	}

	rt.Signal(3)
	wg2.Wait()
}

func Test_ReadyTargetSignal_SubscribeSignalled_Earlier(t *testing.T) {
	t.Parallel()
	rt := NewReadyTarget[uint64]()
	rt.Signal(2)

	ch1 := rt.Subscribe(1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ch1
	}()

	rt.Signal(2)
	wg.Wait()
}
