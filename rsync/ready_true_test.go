package rsync

import (
	"testing"
	"time"
)

func Test_NewPollTrue(t *testing.T) {
	pt := NewPollTrue(func() bool { return true }, 100*time.Millisecond, 100*time.Millisecond)
	if pt == nil {
		t.Error("NewPollTrue returned nil")
	}
}

func Test_PollTrue_Run_Success(t *testing.T) {
	pt := NewPollTrue(func() bool { return true }, 100*time.Millisecond, 100*time.Millisecond)
	err := pt.Run("test")
	if err != nil {
		t.Errorf("Expected Run to succeed, got error: %v", err)
	}
}

func Test_PollTrue_Run_Failure(t *testing.T) {
	pt := NewPollTrue(func() bool { return false }, 100*time.Millisecond, 100*time.Millisecond)
	err := pt.Run("test")
	if err == nil {
		t.Error("Expected Run to fail, got nil error")
	}
}
