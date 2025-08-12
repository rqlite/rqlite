package cdc

import (
	"bytes"
	"fmt"
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

// Test_EnqueueEvents_Simple tests a single Enqueue operation and receiving the event.
func Test_EnqueueEvents_Simple(t *testing.T) {
	q, _, cleanup := newTestQueue(t)
	defer cleanup()

	// Get the events channel
	eventsCh := q.C

	// Enqueue a single item then receive it from events channel.
	item1 := []byte("hello world")
	idx1 := uint64(10)
	if err := q.Enqueue(&Event{Index: idx1, Data: item1}); err != nil {
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
	eventsCh := q.C

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
		if err := q.Enqueue(&Event{Index: item.idx, Data: item.data}); err != nil {
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

// Test_EnqueueHighest tests that enqueuing an older item does not change the highest key.
func Test_EnqueueHighest(t *testing.T) {
	q, _, cleanup := newTestQueue(t)
	defer cleanup()

	// Enqueue a single item
	item1 := []byte("hello world")
	idx1 := uint64(10)
	if err := q.Enqueue(&Event{Index: idx1, Data: item1}); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Now insert an "older" item, it shouldn't actually be inserted.
	item2 := []byte("older item")
	idx2 := uint64(5)
	if err := q.Enqueue(&Event{Index: idx2, Data: item2}); err != nil {
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

// Test_DeleteRange tests deleting a range of items from the queue.
func Test_DeleteRange(t *testing.T) {
	q, _, cleanup := newTestQueue(t)
	defer cleanup()

	// Test that deleting on a empty queue is a no-op.
	if err := q.DeleteRange(1); err != nil {
		t.Fatalf("DeleteRange on empty queue should not error: %v", err)
	}

	// Get the events channel before enqueuing
	eventsCh := q.C

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
		if err := q.Enqueue(&Event{Index: item.idx, Data: item.data}); err != nil {
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

// Test_QueueHighestKey tests that HighestKey returns the correct value after various operations.
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
		if err := q.Enqueue(&Event{Index: item.idx, Data: item.data}); err != nil {
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
	eventsCh := q.C

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
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "persist_test.db")

	item := []byte("survivor")
	idx := uint64(42)

	// create a queue, add an item, and close it.
	q1, err := NewQueue(dbPath)
	if err != nil {
		t.Fatalf("Failed to create initial queue: %v", err)
	}
	if err := q1.Enqueue(&Event{Index: idx, Data: item}); err != nil {
		t.Fatalf("Failed to enqueue item: %v", err)
	}
	q1.Close()

	// Reopen the queue from the same file.
	q2, err := NewQueue(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen queue: %v", err)
	}

	// Get the events channel and receive the item to verify it's the one we saved.
	eventsCh := q2.C
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
	eventsCh = q3.C
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

// Test_Events_NoReader tests that events can be enqueued when no one is reading from the Events channel.
func Test_Events_NoReader(t *testing.T) {
	q, _, cleanup := newTestQueue(t)
	defer cleanup()

	// Enqueue several items without reading from Events channel
	items := []struct {
		idx  uint64
		data []byte
	}{
		{1, []byte("first")},
		{2, []byte("second")},
		{3, []byte("third")},
		{4, []byte("fourth")},
		{5, []byte("fifth")},
	}

	for _, item := range items {
		if err := q.Enqueue(&Event{Index: item.idx, Data: item.data}); err != nil {
			t.Fatalf("Enqueue failed for index %d: %v", item.idx, err)
		}
	}

	// Verify queue is not empty
	if empty, err := q.Empty(); err != nil {
		t.Fatalf("Empty check failed: %v", err)
	} else if empty {
		t.Fatal("Queue should not be empty after enqueuing items")
	}

	// Verify highest key
	if hi, err := q.HighestKey(); err != nil {
		t.Fatalf("HighestKey failed: %v", err)
	} else if hi != 5 {
		t.Errorf("Expected highest key to be 5, got %d", hi)
	}

	// Now get the events channel and verify all events are available
	eventsCh := q.C
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
			t.Fatalf("Timed out waiting for event %d", i)
		}
	}
}

// Test_Events_ReaderAppearsLater tests when a reader appears after events have already been queued.
func Test_Events_ReaderAppearsLater(t *testing.T) {
	q, _, cleanup := newTestQueue(t)
	defer cleanup()

	// Enqueue items before getting Events channel
	items := []struct {
		idx  uint64
		data []byte
	}{
		{10, []byte("pre-reader-1")},
		{20, []byte("pre-reader-2")},
		{30, []byte("pre-reader-3")},
	}

	for _, item := range items {
		if err := q.Enqueue(&Event{Index: item.idx, Data: item.data}); err != nil {
			t.Fatalf("Enqueue failed for index %d: %v", item.idx, err)
		}
	}

	// Wait a bit to ensure enqueue operations complete
	time.Sleep(50 * time.Millisecond)

	// Now get the events channel - this should make all queued events available
	eventsCh := q.C

	// Receive all pre-queued events
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
			t.Fatalf("Timed out waiting for pre-queued event %d", i)
		}
	}

	// Enqueue more items after reader is active
	postItems := []struct {
		idx  uint64
		data []byte
	}{
		{40, []byte("post-reader-1")},
		{50, []byte("post-reader-2")},
	}

	for _, item := range postItems {
		if err := q.Enqueue(&Event{Index: item.idx, Data: item.data}); err != nil {
			t.Fatalf("Enqueue failed for post-reader index %d: %v", item.idx, err)
		}
	}

	// Receive post-reader events
	for i, expectedItem := range postItems {
		select {
		case event := <-eventsCh:
			if event.Index != expectedItem.idx {
				t.Errorf("Post-reader event %d: expected index %d, got %d", i, expectedItem.idx, event.Index)
			}
			if !bytes.Equal(event.Data, expectedItem.data) {
				t.Errorf("Post-reader event %d: expected data '%s', got '%s'", i, expectedItem.data, event.Data)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("Timed out waiting for post-reader event %d", i)
		}
	}
}

// Test_Events_BufferedChannelBehavior tests the behavior when the events channel buffer fills up.
func Test_Events_BufferedChannelBehavior(t *testing.T) {
	q, _, cleanup := newTestQueue(t)
	defer cleanup()

	// Get events channel but don't read from it initially
	eventsCh := q.C

	// Enqueue items up to and beyond the buffer size (which is 10)
	const numItems = 15
	items := make([]struct {
		idx  uint64
		data []byte
	}, numItems)

	for i := 0; i < numItems; i++ {
		items[i] = struct {
			idx  uint64
			data []byte
		}{
			idx:  uint64(i + 1),
			data: []byte(fmt.Sprintf("item-%d", i+1)),
		}
	}

	// Enqueue all items
	for _, item := range items {
		if err := q.Enqueue(&Event{Index: item.idx, Data: item.data}); err != nil {
			t.Fatalf("Enqueue failed for index %d: %v", item.idx, err)
		}
	}

	// Wait a bit for processing
	time.Sleep(100 * time.Millisecond)

	// Start reading events - should get at least the buffered events
	// Note: Due to the non-blocking send in the implementation, we may not get all events
	// if the buffer was full, but we should get the buffered ones in order
	receivedEvents := make([]*Event, 0, numItems)

	// Try to receive events with timeout
	for i := 0; i < numItems; i++ {
		select {
		case event := <-eventsCh:
			receivedEvents = append(receivedEvents, event)
		case <-time.After(100 * time.Millisecond):
			// No more events available immediately
		}
	}

	// Verify we received events in order and they match expectations
	if len(receivedEvents) == 0 {
		t.Fatal("Should have received at least some events")
	}

	for i, event := range receivedEvents {
		expectedIdx := uint64(i + 1)
		expectedData := []byte(fmt.Sprintf("item-%d", i+1))

		if event.Index != expectedIdx {
			t.Errorf("Event %d: expected index %d, got %d", i, expectedIdx, event.Index)
		}
		if !bytes.Equal(event.Data, expectedData) {
			t.Errorf("Event %d: expected data '%s', got '%s'", i, expectedData, event.Data)
		}
	}
}

// Test_Events_InterruptedReader tests when a reader stops and resumes reading.
func Test_Events_InterruptedReader(t *testing.T) {
	q, _, cleanup := newTestQueue(t)
	defer cleanup()

	eventsCh := q.C

	// Enqueue first batch of items
	firstBatch := []struct {
		idx  uint64
		data []byte
	}{
		{1, []byte("batch1-item1")},
		{2, []byte("batch1-item2")},
	}

	for _, item := range firstBatch {
		if err := q.Enqueue(&Event{Index: item.idx, Data: item.data}); err != nil {
			t.Fatalf("Enqueue failed for index %d: %v", item.idx, err)
		}
	}

	// Read first batch
	for i, expectedItem := range firstBatch {
		select {
		case event := <-eventsCh:
			if event.Index != expectedItem.idx {
				t.Errorf("First batch event %d: expected index %d, got %d", i, expectedItem.idx, event.Index)
			}
			if !bytes.Equal(event.Data, expectedItem.data) {
				t.Errorf("First batch event %d: expected data '%s', got '%s'", i, expectedItem.data, event.Data)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("Timed out waiting for first batch event %d", i)
		}
	}

	// Stop reading (simulate interrupted reader) and enqueue more items
	secondBatch := []struct {
		idx  uint64
		data []byte
	}{
		{3, []byte("batch2-item1")},
		{4, []byte("batch2-item2")},
		{5, []byte("batch2-item3")},
	}

	for _, item := range secondBatch {
		if err := q.Enqueue(&Event{Index: item.idx, Data: item.data}); err != nil {
			t.Fatalf("Enqueue failed for index %d: %v", item.idx, err)
		}
	}

	// Wait a bit to simulate gap in reading
	time.Sleep(200 * time.Millisecond)

	// Resume reading - should get all items from second batch
	for i, expectedItem := range secondBatch {
		select {
		case event := <-eventsCh:
			if event.Index != expectedItem.idx {
				t.Errorf("Second batch event %d: expected index %d, got %d", i, expectedItem.idx, event.Index)
			}
			if !bytes.Equal(event.Data, expectedItem.data) {
				t.Errorf("Second batch event %d: expected data '%s', got '%s'", i, expectedItem.data, event.Data)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("Timed out waiting for second batch event %d", i)
		}
	}
}

// Test_Events_ConcurrentReaders tests multiple goroutines reading from the same Events channel.
func Test_Events_ConcurrentReaders(t *testing.T) {
	q, _, cleanup := newTestQueue(t)
	defer cleanup()

	eventsCh := q.C
	const numReaders = 3
	const numItemsPerReader = 5

	// Channel to collect events from all readers
	allEvents := make(chan *Event, numReaders*numItemsPerReader)

	// Start multiple readers
	for i := 0; i < numReaders; i++ {
		go func(readerID int) {
			for {
				select {
				case event, ok := <-eventsCh:
					if !ok {
						return // Channel closed
					}
					allEvents <- event
				case <-time.After(2 * time.Second):
					return // Timeout
				}
			}
		}(i)
	}

	// Enqueue items
	const totalItems = numReaders * numItemsPerReader
	for i := 1; i <= totalItems; i++ {
		item := []byte(fmt.Sprintf("concurrent-item-%d", i))
		if err := q.Enqueue(&Event{Index: uint64(i), Data: item}); err != nil {
			t.Fatalf("Enqueue failed for index %d: %v", i, err)
		}
	}

	// Collect all events
	receivedEvents := make([]*Event, 0, totalItems)
	for i := 0; i < totalItems; i++ {
		select {
		case event := <-allEvents:
			receivedEvents = append(receivedEvents, event)
		case <-time.After(2 * time.Second):
			t.Fatalf("Timed out waiting for event %d", i+1)
		}
	}

	// Verify we received all events
	if len(receivedEvents) != totalItems {
		t.Errorf("Expected %d events, got %d", totalItems, len(receivedEvents))
	}

	// Create a map to track which events we received
	receivedIndices := make(map[uint64]bool)
	for _, event := range receivedEvents {
		receivedIndices[event.Index] = true
	}

	// Verify all indices from 1 to totalItems were received
	for i := uint64(1); i <= totalItems; i++ {
		if !receivedIndices[i] {
			t.Errorf("Missing event with index %d", i)
		}
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
