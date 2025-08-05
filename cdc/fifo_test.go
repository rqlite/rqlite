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

func Test_EnqueueDequeue_Simple(t *testing.T) {
	q, _, cleanup := newTestQueue(t)
	defer cleanup()

	// Enqueue a single item then dequeue it.
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

	// Should be items to dequeue.
	if !q.HasNext() {
		t.Fatalf("HasNext should be false after dequeuing last item")
	}

	gotIdx, gotItem, err := q.Dequeue()
	if err != nil {
		t.Fatalf("First() failed after enqueue: %v", err)
	}
	if gotIdx != idx1 {
		t.Errorf("Expected first index to be %d, got %d", idx1, gotIdx)
	}
	if !bytes.Equal(gotItem, item1) {
		t.Errorf("Expected first item to be '%s', got '%s'", item1, gotItem)
	}
}

// Test_EnqueueDequeue_Multi tests multiple Enqueue and Dequeue operations.
func Test_EnqueueDequeue_Multi(t *testing.T) {
	q, _, cleanup := newTestQueue(t)
	defer cleanup()

	// Enqueue a single item then dequeue it.
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

	// Should be items to dequeue.
	if !q.HasNext() {
		t.Fatalf("HasNext should be false after dequeuing last item")
	}

	// Dequeue the first item.
	gotIdx, gotItem, err := q.Dequeue()
	if err != nil {
		t.Fatalf("First() failed after enqueue: %v", err)
	}
	if gotIdx != idx1 {
		t.Errorf("Expected first index to be %d, got %d", idx1, gotIdx)
	}
	if !bytes.Equal(gotItem, item1) {
		t.Errorf("Expected first item to be '%s', got '%s'", item1, gotItem)
	}

	// Should be no more items to dequeue.
	if q.HasNext() {
		t.Fatalf("HasNext should be false after enqueueing an item")
	}

	// check highest key
	hi, err := q.HighestKey()
	if err != nil {
		t.Fatalf("HighestKey failed after enqueue: %v", err)
	}
	if hi != idx1 {
		t.Errorf("Expected highest key to be %d, got %d", idx1, hi)
	}

	// Next enqueue two items, and dequeue each, ensuring we get them in the
	// expected order.
	item2 := []byte("second item")
	idx2 := uint64(20)
	if err := q.Enqueue(idx2, item2); err != nil {
		t.Fatalf("Enqueue failed for second item: %v", err)
	}

	// Should be more items to dequeue.
	if !q.HasNext() {
		t.Fatalf("HasNext should be true after enqueueing an item")
	}

	item3 := []byte("third item")
	idx3 := uint64(30)
	if err := q.Enqueue(idx3, item3); err != nil {
		t.Fatalf("Enqueue failed for third item: %v", err)
	}

	// Should be more items to dequeue.
	if !q.HasNext() {
		t.Fatalf("HasNext should be true after enqueueing an item")
	}

	// Dequeue the second item.
	gotIdx, gotItem, err = q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue failed for second item: %v", err)
	}
	if gotIdx != idx2 {
		t.Fatalf("Expected second index to be %d, got %d", idx2, gotIdx)
	}
	if !bytes.Equal(gotItem, item2) {
		t.Fatalf("Expected second item to be '%s', got '%s'", item2, gotItem)
	}

	// Should be more items to dequeue since we only removed one.
	if !q.HasNext() {
		t.Fatalf("HasNext should be true after enqueueing an item")
	}

	// Dequeue the third item.
	gotIdx, gotItem, err = q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue failed for third item: %v", err)
	}
	if gotIdx != idx3 {
		t.Fatalf("Expected third index to be %d, got %d", idx3, gotIdx)
	}
	if !bytes.Equal(gotItem, item3) {
		t.Fatalf("Expected third item to be '%s', got '%s'", item3, gotItem)
	}

	// check highest key
	hi, err = q.HighestKey()
	if err != nil {
		t.Fatalf("HighestKey failed after enqueue: %v", err)
	}
	if hi != idx3 {
		t.Errorf("Expected highest key to be %d, got %d", idx1, hi)
	}

	// Should be no items to dequeue.
	if q.HasNext() {
		t.Fatalf("HasNext should be false after dequeuing last item")
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

	// Delete a range of items.
	if err := q.DeleteRange(2); err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}

	// Should still have another item to dequeue.
	if !q.HasNext() {
		t.Fatalf("HasNext failed after DeleteRange")
	}

	// Dequeue next item, should be 3.
	gotIdx, gotItem, err := q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue failed after DeleteRange: %v", err)
	}
	if gotIdx != 3 {
		t.Fatalf("Expected index 3 after DeleteRange, got %d", gotIdx)
	}
	if !bytes.Equal(gotItem, []byte("three")) {
		t.Fatalf("Expected item 'three' after DeleteRange, got '%s'", gotItem)
	}

	// Should be no more items to dequeue.
	if q.HasNext() {
		t.Fatalf("HasNext should be false after dequeuing last item")
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

// Test_DequeueBlocking tests that Dequeue blocks when the queue is empty
// and unblocks when an item is added.
func Test_DequeueBlocking(t *testing.T) {
	q, _, cleanup := newTestQueue(t)
	defer cleanup()

	item := []byte("unblock me")
	itemChan := make(chan []byte)

	// This goroutine will call Dequeue and block until an item is available.
	go func() {
		_, dequeuedItem, err := q.Dequeue()
		if err != nil {
			t.Errorf("Dequeue in goroutine failed: %v", err)
			close(itemChan)
			return
		}
		itemChan <- dequeuedItem
	}()

	// Give the goroutine a moment to start and call Dequeue, which should block.
	time.Sleep(100 * time.Millisecond)

	// Now, enqueue an item. This should unblock the waiting goroutine.
	if err := q.Enqueue(1, item); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Wait for the dequeued item from the channel, with a timeout.
	select {
	case receivedItem := <-itemChan:
		if !bytes.Equal(receivedItem, item) {
			t.Errorf("Expected dequeued item to be '%s', got '%s'", item, receivedItem)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out waiting for Dequeue to unblock")
	}
}

// Test_Events_Basic tests the basic functionality of the Events channel.
func Test_Events_Basic(t *testing.T) {
	q, _, cleanup := newTestQueue(t)
	defer cleanup()

	// Get the events channel
	eventsCh := q.Events()

	// Enqueue an item
	item := []byte("test event")
	idx := uint64(1)
	if err := q.Enqueue(idx, item); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Receive the event from the channel
	select {
	case event := <-eventsCh:
		if event.Index != idx {
			t.Errorf("Expected event index %d, got %d", idx, event.Index)
		}
		if !bytes.Equal(event.Data, item) {
			t.Errorf("Expected event data '%s', got '%s'", item, event.Data)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out waiting for event from channel")
	}
}

// Test_Events_Multiple tests receiving multiple events from the channel.
func Test_Events_Multiple(t *testing.T) {
	q, _, cleanup := newTestQueue(t)
	defer cleanup()

	// Get the events channel
	eventsCh := q.Events()

	// Enqueue multiple items
	items := []struct {
		idx  uint64
		data []byte
	}{
		{1, []byte("first")},
		{2, []byte("second")},
		{3, []byte("third")},
	}

	for _, item := range items {
		if err := q.Enqueue(item.idx, item.data); err != nil {
			t.Fatalf("Enqueue failed for index %d: %v", item.idx, err)
		}
	}

	// Receive all events
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

// Test_Events_ChannelClosedOnQueueClose tests that the events channel is closed when the queue is closed.
func Test_Events_ChannelClosedOnQueueClose(t *testing.T) {
	q, _, _ := newTestQueue(t) // Don't call cleanup automatically

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

// Test_Events_WithDequeue tests that both Events channel and Dequeue can work,
// but not necessarily on the same items (since Events() is meant as an alternative to Dequeue()).
func Test_Events_WithDequeue(t *testing.T) {
	q, _, cleanup := newTestQueue(t)
	defer cleanup()

	// Scenario: Use Events channel first, then switch to Dequeue for remaining items

	// Enqueue some items
	if err := q.Enqueue(1, []byte("item1")); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}
	if err := q.Enqueue(2, []byte("item2")); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}
	if err := q.Enqueue(3, []byte("item3")); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Use Events() to consume some items
	eventsCh := q.Events()

	// Read a couple of items from events channel
	for i := 0; i < 2; i++ {
		select {
		case event := <-eventsCh:
			expectedIndex := uint64(i + 1)
			expectedData := []byte(fmt.Sprintf("item%d", i+1))
			if event.Index != expectedIndex || !bytes.Equal(event.Data, expectedData) {
				t.Errorf("Event %d: expected index=%d data=%s, got index=%d data=%s",
					i, expectedIndex, expectedData, event.Index, event.Data)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("Timed out waiting for event %d", i)
		}
	}

	// Now try to use Dequeue() for any remaining items
	// This might not work if all items were consumed by events channel,
	// but that's expected behavior - Events() is an alternative to Dequeue()

	// The events channel might have consumed the third item too, let's check
	select {
	case event := <-eventsCh:
		expectedIndex := uint64(3)
		expectedData := []byte("item3")
		if event.Index != expectedIndex || !bytes.Equal(event.Data, expectedData) {
			t.Errorf("Third event: expected index=%d data=%s, got index=%d data=%s",
				expectedIndex, expectedData, event.Index, event.Data)
		}
		t.Logf("Third item was also consumed by events channel")
	case <-time.After(100 * time.Millisecond):
		t.Logf("Third item not consumed by events channel yet")
	}

	// Add a new item and see where it goes
	if err := q.Enqueue(4, []byte("item4")); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// This could go to either events channel or be available for dequeue
	// depending on events channel buffer state
	eventsGotItem := false
	select {
	case event := <-eventsCh:
		if event.Index == 4 && bytes.Equal(event.Data, []byte("item4")) {
			eventsGotItem = true
			t.Logf("Fourth item was consumed by events channel")
		} else {
			t.Errorf("Unexpected event: index=%d data=%s", event.Index, event.Data)
		}
	case <-time.After(100 * time.Millisecond):
		t.Logf("Fourth item not consumed by events channel immediately")
	}

	if !eventsGotItem {
		// Try dequeue for the fourth item
		done := make(chan bool)
		go func() {
			defer close(done)
			idx, data, err := q.Dequeue()
			if err != nil {
				t.Errorf("Dequeue failed: %v", err)
				return
			}
			// Could be item 3 or 4 depending on what events channel consumed
			if (idx == 3 && bytes.Equal(data, []byte("item3"))) ||
				(idx == 4 && bytes.Equal(data, []byte("item4"))) {
				t.Logf("Dequeue got item %d successfully", idx)
			} else {
				t.Errorf("Unexpected dequeue result: index=%d data=%s", idx, data)
			}
		}()

		select {
		case <-done:
			// Dequeue completed successfully
		case <-time.After(1 * time.Second):
			t.Fatal("Dequeue failed to get an available item")
		}
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

	// Dequeue the item and verify it's the one we saved.
	_, dequeuedItem, err := q2.Dequeue()
	if err != nil {
		t.Fatalf("Failed to dequeue from reopened queue: %v", err)
	}
	if !bytes.Equal(dequeuedItem, item) {
		t.Errorf("Expected item '%s' after reopening, got '%s'", item, dequeuedItem)
	}

	// Queue should not have any next items.
	if q2.HasNext() {
		t.Fatal("Queue should not have next items after dequeuing last item")
	}

	// Close the queue again.
	q2.Close()

	// Reopen the queue from the same file.
	q3, err := NewQueue(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen queue: %v", err)
	}

	// Dequeue the item again, ensuring it's still available after reopening. This
	// tests that the queue's state is persistent across closures because no deletion
	// has occurred.
	_, dequeuedItem, err = q3.Dequeue()
	if err != nil {
		t.Fatalf("Failed to dequeue from reopened queue: %v", err)
	}
	if !bytes.Equal(dequeuedItem, item) {
		t.Errorf("Expected item '%s' after reopening, got '%s'", item, dequeuedItem)
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
