package rsync

import (
	"errors"
	"testing"
	"time"
)

func Test_CloseOrTimeout_Closed(t *testing.T) {
	ch := make(chan struct{})
	close(ch)
	if err := CloseOrTimeout(ch, time.Second); err != nil {
		t.Fatal(err)
	}
}

func Test_CloseOrTimeout_Close(t *testing.T) {
	called := AtomicBool{}
	ch := make(chan struct{})
	go func() {
		defer called.Set()
		time.Sleep(100 * time.Millisecond)
		close(ch)
	}()
	if err := CloseOrTimeout(ch, time.Second); err != nil {
		t.Fatal(err)
	}
	if !called.Is() {
		t.Fatal("expected called")
	}
}

func Test_CloseOrTimeout_Timeout(t *testing.T) {
	ch := make(chan struct{})
	if err := CloseOrTimeout(ch, 100*time.Millisecond); !errors.Is(err, ErrTimeout) {
		t.Fatalf("expected %v, got %v", ErrTimeout, err)
	}
}
