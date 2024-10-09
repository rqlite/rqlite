package rsync

import (
	"errors"
	"fmt"
	"time"
)

// ErrTimeout is returned when a timeout occurs.
var ErrTimeout = errors.New("timeout")

// CloseOrTimeout waits for a channel to be closed or a timeout expires.
func CloseOrTimeout(ch <-chan struct{}, timeout time.Duration) error {
	select {
	case <-ch:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("timeout after %v, %w", timeout, ErrTimeout)
	}
}
