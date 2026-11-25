package nsq

import (
	"errors"
	"sync"
	"time"
)

// ErrDeadLetterQueueFull is returned by a DeadLetterSink when it cannot
// accept another message because it has reached its configured capacity.
var ErrDeadLetterQueueFull = errors.New("dead letter queue is full")

// DeadLetter is a message that could not be processed after the configured
// number of attempts and was therefore removed from its normal channel.
//
// It preserves everything an operator needs to inspect and replay the
// message: the original body, how many deliveries were attempted, the last
// processing error and where the message came from.
type DeadLetter struct {
	// ID is the ID of the original NSQ message.
	ID MessageID
	// Body is a copy of the original message body.
	Body []byte
	// Topic the original message belonged to.
	Topic string
	// Channel the original message was consumed from.
	Channel string
	// NSQDAddress is the nsqd instance that delivered the message.
	NSQDAddress string
	// Attempts is the number of delivery attempts made before giving up.
	Attempts uint16
	// LastError is the error returned by the handler on the final attempt.
	LastError string
	// OriginalTimestamp is the timestamp attached to the original message.
	OriginalTimestamp int64
	// DeadLetteredAt records when the message was moved into the queue.
	DeadLetteredAt time.Time
}

// DeadLetterSink is the storage used for messages that exhausted their
// processing attempts.
//
// Implementations must be safe for concurrent use.
type DeadLetterSink interface {
	// Enqueue stores a dead letter. It returns ErrDeadLetterQueueFull (or
	// another error) when the sink cannot accept the message.
	Enqueue(*DeadLetter) error
	// Len returns the number of dead letters currently held.
	Len() int
	// DeadLetters returns a snapshot of the dead letters currently held,
	// oldest first. It must not return internal slices that callers could mutate.
	DeadLetters() []*DeadLetter
	// Remove deletes the dead letter with the given ID. It returns false when
	// no such message is held.
	Remove(id MessageID) bool
}

// DeadLetterPublisher re-publishes a dead letter back to an NSQ topic so that
// it can be processed again. *Producer satisfies this interface.
type DeadLetterPublisher interface {
	Publish(topic string, body []byte) error
}

// deadLetterQueue is a bounded, in-memory DeadLetterSink.
//
// When the capacity is reached the oldest entry is evicted to make room for
// the new one, so a poison message stream can never turn the dead letter
// queue into an unbounded buffer. The number of evictions is exposed via
// DroppedCount so evictions remain observable rather than silent.
type deadLetterQueue struct {
	mtx       sync.RWMutex
	capacity  int
	letters   []*DeadLetter
	index     map[MessageID]int
	dropped   uint64
	onDropped func(*DeadLetter)
}

// NewDeadLetterQueue creates a bounded in-memory dead letter queue.
//
// capacity must be greater than zero. onDropped, when non-nil, is called for
// every entry evicted because the queue was full.
func NewDeadLetterQueue(capacity int, onDropped func(*DeadLetter)) DeadLetterSink {
	if capacity <= 0 {
		panic("dead letter queue capacity must be greater than zero")
	}
	return &deadLetterQueue{
		capacity:  capacity,
		letters:   make([]*DeadLetter, 0, capacity),
		index:     make(map[MessageID]int),
		onDropped: onDropped,
	}
}

func (q *deadLetterQueue) Enqueue(letter *DeadLetter) error {
	if letter == nil {
		return errors.New("cannot enqueue a nil dead letter")
	}

	q.mtx.Lock()
	defer q.mtx.Unlock()

	if _, ok := q.index[letter.ID]; ok {
		return errors.New("dead letter for message ID already exists")
	}

	// store a private copy so later caller mutation of the message body cannot
	// corrupt a dead letter that operators may later replay
	body := make([]byte, len(letter.Body))
	copy(body, letter.Body)
	letter.Body = body

	if len(q.letters) >= q.capacity {
		evicted := q.letters[0]
		q.letters = q.letters[1:]
		delete(q.index, evicted.ID)
		q.reindex()
		q.dropped++
		if q.onDropped != nil {
			q.onDropped(evicted)
		}
	}

	q.index[letter.ID] = len(q.letters)
	q.letters = append(q.letters, letter)
	return nil
}

func (q *deadLetterQueue) Len() int {
	q.mtx.RLock()
	defer q.mtx.RUnlock()
	return len(q.letters)
}

func (q *deadLetterQueue) DeadLetters() []*DeadLetter {
	q.mtx.RLock()
	defer q.mtx.RUnlock()

	snapshot := make([]*DeadLetter, len(q.letters))
	copy(snapshot, q.letters)
	return snapshot
}

func (q *deadLetterQueue) Remove(id MessageID) bool {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	pos, ok := q.index[id]
	if !ok {
		return false
	}

	q.letters = append(q.letters[:pos], q.letters[pos+1:]...)
	delete(q.index, id)
	q.reindex()
	return true
}

// DroppedCount reports how many dead letters were evicted because the queue
// had reached its capacity.
func (q *deadLetterQueue) DroppedCount() uint64 {
	q.mtx.RLock()
	defer q.mtx.RUnlock()
	return q.dropped
}

// reindex rebuilds the ID-to-position map. Callers must hold q.mtx (write).
func (q *deadLetterQueue) reindex() {
	for i, letter := range q.letters {
		q.index[letter.ID] = i
	}
}
