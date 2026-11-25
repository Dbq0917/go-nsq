package nsq

import (
	"errors"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"
)

// failingHandler fails every message, recording the attempts it observed.
type failingHandler struct {
	mu       sync.Mutex
	attempts []uint16
}

func (h *failingHandler) HandleMessage(message *Message) error {
	h.mu.Lock()
	h.attempts = append(h.attempts, message.Attempts)
	h.mu.Unlock()
	return errors.New("always failing")
}

func (h *failingHandler) observed() []uint16 {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]uint16, len(h.attempts))
	copy(out, h.attempts)
	return out
}

// mockPublisher records replays instead of touching the network.
type mockPublisher struct {
	mu        sync.Mutex
	published []publishedMessage
	err       error
}

type publishedMessage struct {
	topic string
	body  []byte
}

func (p *mockPublisher) Publish(topic string, body []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	copied := make([]byte, len(body))
	copy(copied, body)
	p.published = append(p.published, publishedMessage{topic, copied})
	return nil
}

func (p *mockPublisher) calls() []publishedMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]publishedMessage, len(p.published))
	copy(out, p.published)
	return out
}

func frameMessageWithAttempts(id MessageID, body string, attempts uint16) []byte {
	msg := NewMessage(id, []byte(body))
	msg.Attempts = attempts
	return frameMessage(msg)
}

// TestConsumerDeadLetter verifies that a poison message is requeued until it
// exhausts MaxAttempts, then moved to the dead letter queue and finished on the
// channel so subsequent messages keep flowing.
func TestConsumerDeadLetter(t *testing.T) {
	msgIDBad := MessageID{'b', 'a', 'd', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '1'}
	msgIDGood := MessageID{'g', 'o', 'o', 'd', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '1'}

	script := []instruction{
		{0, FrameTypeResponse, []byte("OK")}, // IDENTIFY
		{0, FrameTypeResponse, []byte("OK")}, // SUB
		{20 * time.Millisecond, FrameTypeMessage, frameMessageWithAttempts(msgIDBad, "bad", 1)},
		{20 * time.Millisecond, FrameTypeMessage, frameMessageWithAttempts(msgIDBad, "bad", 2)},
		{20 * time.Millisecond, FrameTypeMessage, frameMessageWithAttempts(msgIDBad, "bad", 3)},
		{20 * time.Millisecond, FrameTypeMessage, frameMessageWithAttempts(msgIDBad, "bad", 4)},
		{20 * time.Millisecond, FrameTypeMessage, frameMessage(NewMessage(msgIDGood, []byte("good")))},
		{200 * time.Millisecond, -1, []byte("exit")},
	}

	addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	n := newMockNSQD(t, script, addr.String())

	topicName := "test_dlq" + strconv.Itoa(int(time.Now().UnixNano()))
	config := NewConfig()
	config.MaxInFlight = 1
	config.MaxAttempts = 3
	config.MaxDeadLetters = 10
	config.DefaultRequeueDelay = 0
	config.BackoffMultiplier = 5 * time.Millisecond
	config.MaxBackoffDuration = 25 * time.Millisecond

	q, err := NewConsumer(topicName, "ch", config)
	if err != nil {
		t.Fatal(err)
	}
	q.SetLogger(nullLogger, LogLevelDebug)

	handler := &failingHandler{}
	goodHandler := &succeedAfterBadHandler{failing: handler}
	q.AddHandler(goodHandler)

	if err := q.ConnectToNSQD(n.tcpAddr.String()); err != nil {
		t.Fatal(err)
	}

	select {
	case <-n.exitChan:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for mock nsqd script to finish")
	}

	// gracefully stop the consumer so connection cleanup goroutines don't outlive the test
	q.Stop()
	select {
	case <-q.StopChan:
	case <-time.After(2 * time.Second):
		t.Fatal("consumer did not stop cleanly")
	}

	// the handler processed attempts 1..3; the 4th delivery exceeds MaxAttempts
	// and is dead lettered straight away without invoking the handler again
	if got := handler.observed(); len(got) != 3 {
		t.Fatalf("handler should process 3 deliveries before dead lettering, got %v", got)
	}

	deadLetters := q.DeadLetters()
	if len(deadLetters) != 1 {
		t.Fatalf("expected 1 dead letter, got %d", len(deadLetters))
	}
	dl := deadLetters[0]
	if dl.ID != msgIDBad {
		t.Fatalf("dead letter ID mismatch: %s", dl.ID)
	}
	if string(dl.Body) != "bad" {
		t.Fatalf("dead letter body not preserved: %q", dl.Body)
	}
	if dl.Attempts != 4 {
		t.Fatalf("dead letter should record 4 attempts, got %d", dl.Attempts)
	}
	if dl.LastError != "always failing" {
		t.Fatalf("dead letter should record last error, got %q", dl.LastError)
	}
	if dl.Topic != topicName || dl.Channel != "ch" {
		t.Fatalf("dead letter origin not preserved: topic=%q channel=%q", dl.Topic, dl.Channel)
	}
	if dl.DeadLetteredAt.IsZero() {
		t.Fatal("DeadLetteredAt should be set")
	}

	if q.Stats().MessagesDeadLettered != 1 {
		t.Fatalf("expected 1 MessagesDeadLettered, got %d", q.Stats().MessagesDeadLettered)
	}

	// the dead lettered message is FINished and the following good message still flows
	var sawFIN, sawGoodFIN bool
	for _, raw := range n.got {
		cmd := string(raw)
		if cmd == "FIN "+string(msgIDBad[:]) {
			sawFIN = true
		}
		if cmd == "FIN "+string(msgIDGood[:]) {
			sawGoodFIN = true
		}
	}
	if !sawFIN {
		t.Fatalf("poison message was not FINished out of the channel: %v", n.got)
	}
	if !sawGoodFIN {
		t.Fatalf("channel stayed blocked, good message was not FINished: %v", n.got)
	}
	if !goodHandler.goodProcessed() {
		t.Fatal("good message behind the poison message was never processed")
	}

	// replay re-publishes the original body and removes the entry from the queue
	publisher := &mockPublisher{}
	if err := q.ReplayDeadLetter(msgIDBad, publisher); err != nil {
		t.Fatalf("replay failed: %s", err)
	}
	calls := publisher.calls()
	if len(calls) != 1 || calls[0].topic != topicName || string(calls[0].body) != "bad" {
		t.Fatalf("replay did not republish original body: %+v", calls)
	}
	if q.DeadLetter(msgIDBad) != nil {
		t.Fatal("dead letter should be removed after successful replay")
	}

	// a failed replay keeps the dead letter in place
	if err := q.ReplayDeadLetter(msgIDGood, &mockPublisher{}); err == nil {
		t.Fatal("replaying an unknown ID should return an error")
	}
}

// succeedAfterBadHandler fails "bad" messages and finishes anything else.
type succeedAfterBadHandler struct {
	failing *failingHandler
	mu      sync.Mutex
	good    bool
}

func (h *succeedAfterBadHandler) HandleMessage(message *Message) error {
	if string(message.Body) == "bad" {
		return h.failing.HandleMessage(message)
	}
	h.mu.Lock()
	h.good = true
	h.mu.Unlock()
	return nil
}

func (h *succeedAfterBadHandler) goodProcessed() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.good
}

// TestDeadLetterQueueBounded verifies the queue never exceeds its capacity and
// that eviction of the oldest entry is counted rather than silent.
func TestDeadLetterQueueBounded(t *testing.T) {
	var evicted []MessageID
	q := NewDeadLetterQueue(2, func(letter *DeadLetter) {
		evicted = append(evicted, letter.ID)
	}).(*deadLetterQueue)

	id1 := MessageID{'1'}
	id2 := MessageID{'2'}
	id3 := MessageID{'3'}

	originalBodies := make(map[MessageID][]byte)
	for _, id := range []MessageID{id1, id2, id3} {
		body := []byte("x")
		originalBodies[id] = body
		if err := q.Enqueue(&DeadLetter{ID: id, Body: body}); err != nil {
			t.Fatal(err)
		}
	}

	if q.Len() != 2 {
		t.Fatalf("queue should be capped at 2, got %d", q.Len())
	}
	letters := q.DeadLetters()
	if letters[0].ID != id2 || letters[1].ID != id3 {
		t.Fatalf("oldest entry should have been evicted, got %s %s", letters[0].ID, letters[1].ID)
	}
	if len(evicted) != 1 || evicted[0] != id1 {
		t.Fatalf("eviction callback should report id1, got %v", evicted)
	}
	if q.DroppedCount() != 1 {
		t.Fatalf("DroppedCount should be 1, got %d", q.DroppedCount())
	}

	// the stored body is independent of the caller's slice
	originalBodies[id2][0] = 'z'
	if q.DeadLetters()[0].Body[0] != 'x' {
		t.Fatal("Enqueue must copy the body so caller mutation cannot corrupt the queue")
	}

	if !q.Remove(id2) || q.Len() != 1 {
		t.Fatal("Remove should delete the entry and shrink the queue")
	}
	if q.Remove(id2) {
		t.Fatal("removing an unknown ID should report false")
	}

	if err := q.Enqueue(nil); err == nil {
		t.Fatal("enqueuing nil should error")
	}
}

// TestConsumerDeadLetterDisabled keeps the historical behavior: with capacity 0
// no dead letter queue exists and exhausted messages are just finished.
func TestConsumerDeadLetterDisabled(t *testing.T) {
	msgIDBad := MessageID{'d', 'i', 's', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '1'}

	script := []instruction{
		{0, FrameTypeResponse, []byte("OK")},
		{0, FrameTypeResponse, []byte("OK")},
		{20 * time.Millisecond, FrameTypeMessage, frameMessageWithAttempts(msgIDBad, "bad", 4)},
		{200 * time.Millisecond, -1, []byte("exit")},
	}

	addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	n := newMockNSQD(t, script, addr.String())

	topicName := "test_dlq_off" + strconv.Itoa(int(time.Now().UnixNano()))
	config := NewConfig()
	config.MaxInFlight = 1
	config.MaxAttempts = 3
	config.MaxDeadLetters = 0

	q, err := NewConsumer(topicName, "ch", config)
	if err != nil {
		t.Fatal(err)
	}
	q.SetLogger(nullLogger, LogLevelError)

	failing := &failingHandler{}
	logging := &failedLoggingHandler{failing: failing}
	q.AddHandler(logging)

	if err := q.ConnectToNSQD(n.tcpAddr.String()); err != nil {
		t.Fatal(err)
	}

	select {
	case <-n.exitChan:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for mock nsqd script to finish")
	}

	q.Stop()
	select {
	case <-q.StopChan:
	case <-time.After(2 * time.Second):
		t.Fatal("consumer did not stop cleanly")
	}

	if q.DeadLetters() != nil {
		t.Fatal("no dead letters should be retained when the queue is disabled")
	}
	if q.Stats().MessagesDeadLettered != 0 {
		t.Fatal("MessagesDeadLettered should stay 0 when disabled")
	}
	if !logging.failedLogged() {
		t.Fatal("FailedMessageLogger hook should still fire when DLQ is disabled")
	}
}

type failedLoggingHandler struct {
	failing *failingHandler
	mu      sync.Mutex
	logged  bool
}

func (h *failedLoggingHandler) HandleMessage(message *Message) error {
	return h.failing.HandleMessage(message)
}

func (h *failedLoggingHandler) LogFailedMessage(message *Message) {
	h.mu.Lock()
	h.logged = true
	h.mu.Unlock()
}

func (h *failedLoggingHandler) failedLogged() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.logged
}
