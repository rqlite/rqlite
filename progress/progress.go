package progress

import (
	"context"
	"io"
	"sync"
	"time"
)

const (
	countingMonitorInterval = 10 * time.Second
)

// CountingReader is an io.Reader that counts the number of bytes read.
type CountingReader struct {
	reader io.Reader

	mu    sync.RWMutex
	count int64
}

// NewCountingReader returns a new CountingReader.
func NewCountingReader(reader io.Reader) *CountingReader {
	return &CountingReader{reader: reader}
}

// Read reads from the underlying reader, and counts the number of bytes read.
func (c *CountingReader) Read(p []byte) (int, error) {
	n, err := c.reader.Read(p)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.count += int64(n)
	return n, err
}

// Count returns the number of bytes read.
func (c *CountingReader) Count() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.count
}

// CountingWriter is an io.Writer that counts the number of bytes written.
type CountingWriter struct {
	writer io.Writer

	mu    sync.RWMutex
	count int64
}

// NewCountingWriter returns a new CountingWriter.
func NewCountingWriter(writer io.Writer) *CountingWriter {
	return &CountingWriter{writer: writer}
}

// Write writes to the underlying writer, and counts the number of bytes written.
func (c *CountingWriter) Write(p []byte) (int, error) {
	n, err := c.writer.Write(p)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.count += int64(n)
	return n, err
}

// Count returns the number of bytes written.
func (c *CountingWriter) Count() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.count
}

// LoggerFunc is a function that can be used to log the current count.
type LoggerFunc func(n int64)

// Counter is an interface that can be used to get the current count.
type Counter interface {
	Count() int64
}

// CountingMonitor is a monitor that periodically logs the current count.
type CountingMonitor struct {
	loggerFn LoggerFunc
	ctr      Counter

	once   sync.Once
	cancel func()
	doneCh chan struct{}
}

// StartCountingMonitor starts a CountingMonitor.
func StartCountingMonitor(loggerFn LoggerFunc, ctr Counter) *CountingMonitor {
	ctx, cancel := context.WithCancel(context.Background())

	m := &CountingMonitor{
		loggerFn: loggerFn,
		ctr:      ctr,
		cancel:   cancel,
		doneCh:   make(chan struct{}),
	}
	go m.run(ctx)
	return m
}

func (cm *CountingMonitor) run(ctx context.Context) {
	defer close(cm.doneCh)

	ticker := time.NewTicker(countingMonitorInterval)
	defer ticker.Stop()

	ranOnce := false
	for {
		select {
		case <-ctx.Done():
			if !ranOnce {
				cm.runOnce()
			}
			return
		case <-ticker.C:
			cm.runOnce()
			ranOnce = true
		}
	}
}

func (cm *CountingMonitor) runOnce() {
	cm.loggerFn(cm.ctr.Count())
}

func (m *CountingMonitor) StopAndWait() {
	m.once.Do(func() {
		m.cancel()
		<-m.doneCh
	})
}
