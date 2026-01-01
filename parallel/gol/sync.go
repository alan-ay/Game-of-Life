package gol

import "sync"

// EventQueue is a threadsafe FIFO buffer for game events.
type EventQueue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	closed bool
	events []Event
}

// NewEventQueue constructs an empty EventQueue.
func NewEventQueue() *EventQueue {
	q := &EventQueue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Push appends a new event to the queue.
func (q *EventQueue) Push(event Event) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.events = append(q.events, event)
	q.cond.Signal()
}

// Wait removes and returns the next event, blocking while the queue is empty.
// The second return value is false when the queue has been closed and drained.
func (q *EventQueue) Wait() (Event, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.events) == 0 && !q.closed {
		q.cond.Wait()
	}
	if len(q.events) == 0 && q.closed {
		return nil, false
	}
	event := q.events[0]
	q.events = q.events[1:]
	return event, true
}

// Drain removes and returns all queued events without blocking.
// The boolean indicates whether the queue is closed and now empty.
func (q *EventQueue) Drain() ([]Event, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.events) == 0 {
		return nil, q.closed
	}
	events := append([]Event(nil), q.events...)
	q.events = q.events[:0]
	return events, q.closed && len(q.events) == 0
}

// Close marks the queue as closed and wakes any waiters.
func (q *EventQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	q.cond.Broadcast()
}

// KeyQueue is a threadsafe FIFO buffer for keyboard input.
type KeyQueue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	closed bool
	keys   []rune
}

// NewKeyQueue constructs an empty KeyQueue.
func NewKeyQueue() *KeyQueue {
	k := &KeyQueue{}
	k.cond = sync.NewCond(&k.mu)
	return k
}

// Push enqueues a key press. It returns false if the queue has been closed.
func (k *KeyQueue) Push(r rune) bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.closed {
		return false
	}
	k.keys = append(k.keys, r)
	k.cond.Signal()
	return true
}

// Pop removes and returns the next key press, blocking while the queue is empty.
// The boolean result is false when the queue has been closed and drained.
func (k *KeyQueue) Pop() (rune, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	for len(k.keys) == 0 && !k.closed {
		k.cond.Wait()
	}
	if len(k.keys) == 0 && k.closed {
		return 0, false
	}
	r := k.keys[0]
	k.keys = k.keys[1:]
	return r, true
}

// TryPop removes and returns the next key press without blocking.
func (k *KeyQueue) TryPop() (rune, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if len(k.keys) == 0 {
		return 0, false
	}
	r := k.keys[0]
	k.keys = k.keys[1:]
	return r, true
}

// Close marks the queue as closed and wakes any waiters.
func (k *KeyQueue) Close() {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.closed {
		return
	}
	k.closed = true
	k.cond.Broadcast()
}
