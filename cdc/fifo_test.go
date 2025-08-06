package cdc

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Test_NewQueue tests the creation of a new queue and ensures the DB file exists.
func Test_NewQueue(t *testing.T) {
	q, _, cleanup := newTestQueue(t)
	defer cleanup()

	if q == nil {
		t.Fatal("NewQueue returned a nil queue")
	}
	if q.db == nil {
		t.Fatal("Queue has a nil db instance")
	}

	// Verify the database file was created by checking its path.
	dbPath := q.db.Path()
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatalf("Database file was not created at %s", dbPath)
	}

	// Ensure the queue is empty
	e, err := q.Empty()
	if err != nil {
		t.Fatalf("Queue Empty check failed: %v", err)
	}
	if !e {
		t.Fatal("Newly created queue should be empty")
	}
}

func Test_EnqueueEvents_Simple(t *testing.T) {
	q, _, cleanup := newTestQueue(t)
	defer cleanup()

	// Get the events channel
	eventsCh := q.Events()

	// Enqueue a single item then receive it from events channel.
	item1 := []byte("hello world")
	idx1 := uint64(10)
	if err := q.Enqueue(idx1, item1); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Ensure the queue is not empty
	e, err := q.Empty()
	if err != nil {
		t.Fatalf("Queue Empty check failed: %v", err)
	}
	if e {
		t.Fatal("Queue should not be empty")
	}

	// Should be items to receive from events channel.
	if !q.HasNext() {
		t.Fatalf("HasNext should be true after enqueuing an item")
	}

	// Receive the event from the channel
	select {
	case event := <-eventsCh:
		if event.Index != idx1 {
			t.Errorf("Expected event index to be %d, got %d", idx1, event.Index)
		}
		if !bytes.Equal(event.Data, item1) {
			t.Errorf("Expected event data to be '%s', got '%s'", item1, event.Data)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out waiting for event from channel")
	}
}

// Test_EnqueueEvents_Multi tests multiple Enqueue operations and receiving events.
func Test_EnqueueEvents_Multi(t *testing.T) {
	q, _, cleanup := newTestQueue(t)
	defer cleanup()

	// Get the events channel
	eventsCh := q.Events()

	// Enqueue multiple items
	items := []struct {
		idx  uint64
		data []byte
	}{
		{10, []byte("first item")},
		{20, []byte("second item")},
		{30, []byte("third item")},
	}

	for _, item := range items {
		if err := q.Enqueue(item.idx, item.data); err != nil {
			t.Fatalf("Enqueue failed for index %d: %v", item.idx, err)
		}
	}

	// Receive all events in order
	for i, expectedItem := range items {
		select {
		case event := <-eventsCh:
			if event.Index != expectedItem.idx {
				t.Errorf("Event %d: expected index %d, got %d", i, expectedItem.idx, event.Index)
			}
			if !bytes.Equal(event.Data, expectedItem.data) {
				t.Errorf("Event %d: expected data '%s', got '%s'", i, expectedItem.data, event.Data)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("Timed out waiting for event %d from channel", i)
		}
	}

	// Check highest key
	hi, err := q.HighestKey()
	if err != nil {
		t.Fatalf("HighestKey failed after enqueue: %v", err)
	}
	if hi != 30 {
		t.Errorf("Expected highest key to be 30, got %d", hi)
	}
}

func Test_EnqueueHighest(t *testing.T) {
	q, _, cleanup := newTestQueue(t)
	defer cleanup()

	// Enqueue a single item
	item1 := []byte("hello world")
	idx1 := uint64(10)
	if err := q.Enqueue(idx1, item1); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Now insert an "older" item, it shouldn't actually be inserted.
	item2 := []byte("older item")
	idx2 := uint64(5)
	if err := q.Enqueue(idx2, item2); err != nil {
		t.Fatalf("Enqueue of older item failed: %v", err)
	}

	// Check that the highest key is still idx1.
	hi, err := q.HighestKey()
	if err != nil {
		t.Fatalf("HighestKey failed after enqueue: %v", err)
	}
	if hi != idx1 {
		t.Errorf("Expected highest key to be %d, got %d", idx1, hi)
	}
}

func Test_DeleteRange(t *testing.T) {
	q, _, cleanup := newTestQueue(t)
	defer cleanup()

	// Test that deleting on a empty queue is a no-op.
	if err := q.DeleteRange(1); err != nil {
		t.Fatalf("DeleteRange on empty queue should not error: %v", err)
	}

	// Get the events channel before enqueuing
	eventsCh := q.Events()

	// Enqueue a few items.
	items := []struct {
		idx  uint64
		data []byte
	}{
		{1, []byte("one")},
		{2, []byte("two")},
		{3, []byte("three")},
	}
	for _, item := range items {
		if err := q.Enqueue(item.idx, item.data); err != nil {
			t.Fatalf("Enqueue failed for index %d: %v", item.idx, err)
		}
	}

	// Consume all events that were sent during enqueue
	for i := 0; i < len(items); i++ {
		select {
		case event := <-eventsCh:
			// Verify the events are in order
			expectedItem := items[i]
			if event.Index != expectedItem.idx {
				t.Fatalf("Event %d: expected index %d, got %d", i, expectedItem.idx, event.Index)
			}
			if !bytes.Equal(event.Data, expectedItem.data) {
				t.Fatalf("Event %d: expected data '%s', got '%s'", i, expectedItem.data, event.Data)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("Timed out waiting for event %d", i)
		}
	}

	// Delete a range of items.
	if err := q.DeleteRange(2); err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}

	// Should still have item 3 in the database
	if !q.HasNext() {
		t.Fatalf("HasNext should be true, item 3 should still exist after DeleteRange")
	}

	// No more events should be available since all were consumed
	select {
	case event := <-eventsCh:
		t.Fatalf("Unexpected event received after DeleteRange: index=%d", event.Index)
	case <-time.After(100 * time.Millisecond):
		// Expected - no more events
	}
}

func Test_QueueHighestKey(t *testing.T) {
	q, path, _ := newTestQueue(t)

	// Enqueue a few items.
	items := []struct {
		idx  uint64
		data []byte
	}{
		{1, []byte("one")},
		{2, []byte("two")},
		{3, []byte("three")},
	}
	for _, item := range items {
		if err := q.Enqueue(item.idx, item.data); err != nil {
			t.Fatalf("Enqueue failed for index %d: %v", item.idx, err)
		}
	}

	// Check the highest key.
	hi, err := q.HighestKey()
	if err != nil {
		t.Fatalf("HighestKey failed: %v", err)
	}
	if hi != 3 {
		t.Fatalf("Expected highest key to be 3, got %d", hi)
	}

	// Delete all keys, highest key should still be 3.
	if err := q.DeleteRange(3); err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}
	hi, err = q.HighestKey()
	if err != nil {
		t.Fatalf("HighestKey failed after DeleteRange: %v", err)
	}
	if hi != 3 {
		t.Fatalf("Expected highest key to still be 3 after DeleteRange, got %d", hi)
	}

	// Close and reopen queue, highest key should still be 3.
	q.Close()

	q, err = NewQueue(path)
	if err != nil {
		t.Fatalf("Failed to reopen queue: %v", err)
	}
	hi, err = q.HighestKey()
	if err != nil {
		t.Fatalf("HighestKey failed after reopening queue: %v", err)
	}
	if hi != 3 {
		t.Fatalf("Expected highest key to still be 3 after reopening, got %d", hi)
	}

	// Free up database file so it can be removed.
	q.Close()
}

// Test_Events_ChannelCloseOnQueueClose tests that the events channel is closed when the queue is closed.
func Test_Events_ChannelCloseOnQueueClose(t *testing.T) {
	q, _, _ := newTestQueue(t) // Don't call cleanup automatically

	// Get the events channel
	eventsCh := q.Events()

	// Close the queue
	q.Close()

	// The events channel should be closed
	select {
	case _, ok := <-eventsCh:
		if ok {
			t.Error("Events channel should be closed when queue is closed")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out waiting for events channel to close")
	}
}

// Test_QueuePersistence ensures that items remain in the queue after closing and reopening it.
func Test_QueuePersistence(t *testing.T) {
	// This test cannot run in parallel because it relies on a specific file path.
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "persist_test.db")

	item := []byte("survivor")
	idx := uint64(42)

	// reate a queue, add an item, and close it.
	q1, err := NewQueue(dbPath)
	if err != nil {
		t.Fatalf("Failed to create initial queue: %v", err)
	}
	if err := q1.Enqueue(idx, item); err != nil {
		t.Fatalf("Failed to enqueue item: %v", err)
	}
	q1.Close()

	// Reopen the queue from the same file.
	q2, err := NewQueue(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen queue: %v", err)
	}

	// Get the events channel and receive the item to verify it's the one we saved.
	eventsCh := q2.Events()
	select {
	case event := <-eventsCh:
		if !bytes.Equal(event.Data, item) {
			t.Errorf("Expected item '%s' after reopening, got '%s'", item, event.Data)
		}
		if event.Index != idx {
			t.Errorf("Expected index %d after reopening, got %d", idx, event.Index)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out waiting for event from reopened queue")
	}

	// Close the queue again.
	q2.Close()

	// Reopen the queue from the same file.
	q3, err := NewQueue(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen queue: %v", err)
	}

	// Receive the item again from events channel, ensuring it's still available after reopening. This
	// tests that the queue's state is persistent across closures because no deletion
	// has occurred.
	eventsCh = q3.Events()
	select {
	case event := <-eventsCh:
		if !bytes.Equal(event.Data, item) {
			t.Errorf("Expected item '%s' after reopening, got '%s'", item, event.Data)
		}
		if event.Index != idx {
			t.Errorf("Expected index %d after reopening, got %d", idx, event.Index)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out waiting for event from reopened queue")
	}

	// Now, let's actually delete the item and ensure it is gone, even after reopening.
	if err := q3.DeleteRange(idx); err != nil {
		t.Fatalf("Failed to delete item: %v", err)
	}

	// Close the queue and reopen it again.
	q3.Close()

	q4, err := NewQueue(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen queue after deletion: %v", err)
	}

	// Queue should actually be empty this time.
	e, err := q4.Empty()
	if err != nil {
		t.Fatalf("Queue Empty check failed after deletion: %v", err)
	}
	if !e {
		t.Fatal("Queue should be empty after deleting last item")
	}

	// Ensure HasNext returns false after deletion.
	if q4.HasNext() {
		t.Fatal("HasNext should return false after deleting last item")
	}

	// Close the queue.
	q4.Close()
}

// Test_Events_SameChannelReturned tests that calling Events() multiple times returns the same channel.
func Test_Events_SameChannelReturned(t *testing.T) {
	q, _, cleanup := newTestQueue(t)
	defer cleanup()

	ch1 := q.Events()
	ch2 := q.Events()

	if ch1 != ch2 {
		t.Error("Events() should return the same channel when called multiple times")
	}
}

// newTestQueue is a helper function that creates a new Queue for testing.
// It creates a temporary file for the bbolt database and returns the Queue,
// the path to the database file, and a cleanup function to be called with defer.
func newTestQueue(t *testing.T) (*Queue, string, func()) {
	t.Helper()

	// Create a temporary file for the test database.
	// Using a subdirectory helps keep the project root clean.
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	q, err := NewQueue(dbPath)
	if err != nil {
		t.Fatalf("Failed to create new queue: %v", err)
	}

	// The cleanup function closes the queue and removes the database file.
	cleanup := func() {
		q.Close()
	}

	return q, dbPath, cleanup
}
