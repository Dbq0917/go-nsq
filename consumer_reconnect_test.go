package nsq

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type reconnectBlockingHandler struct {
	started chan struct{}
	release chan struct{}
}

func (h *reconnectBlockingHandler) HandleMessage(message *Message) error {
	h.started <- struct{}{}
	<-h.release
	return nil
}

func TestConsumerReconnectDoesNotWaitForBlockedHandlers(t *testing.T) {
	msgID := MessageID{'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 's', 'd', 'f', 'g', 'h'}

	script := []instruction{
		{0, FrameTypeResponse, []byte("OK")},
		{0, FrameTypeResponse, []byte("OK")},
		{10 * time.Millisecond, FrameTypeMessage, frameMessage(NewMessage(msgID, []byte("block")))},
		{10 * time.Millisecond, FrameTypeMessage, frameMessage(NewMessage(msgID, []byte("block")))},
		{10 * time.Millisecond, FrameTypeMessage, frameMessage(NewMessage(msgID, []byte("block")))},
		{50 * time.Millisecond, -1, []byte("exit")},
	}

	addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	first := newMockNSQD(t, script, addr.String())

	config := NewConfig()
	config.MaxInFlight = 5
	config.LookupdPollInterval = 10 * time.Millisecond
	config.RDYRedistributeInterval = 10 * time.Millisecond
	q, _ := NewConsumer("test_reconnect_blocked"+strconv.Itoa(int(time.Now().UnixNano())), "ch", config)
	q.SetLogger(nullLogger, LogLevelError)

	handler := &reconnectBlockingHandler{
		started: make(chan struct{}, 3),
		release: make(chan struct{}),
	}
	q.AddHandler(handler)

	if err := q.ConnectToNSQD(first.tcpAddr.String()); err != nil {
		t.Fatal(err)
	}

	select {
	case <-handler.started:
	case <-time.After(time.Second):
		t.Fatal("handler did not receive a message")
	}

	<-first.exitChan

	secondScript := []instruction{
		{0, FrameTypeResponse, []byte("OK")},
		{0, FrameTypeResponse, []byte("OK")},
		{200 * time.Millisecond, -1, []byte("exit")},
	}
	second := newMockNSQD(t, secondScript, first.tcpAddr.String())

	select {
	case <-second.exitChan:
	case <-time.After(time.Second):
		t.Fatal("consumer did not reconnect while handlers were blocked")
	}

	close(handler.release)
	q.Stop()
	<-q.StopChan

	expectedPrefix := []string{
		"IDENTIFY",
		"SUB " + q.topic + " ch",
		"RDY 1",
	}
	if len(second.got) < len(expectedPrefix) {
		t.Fatalf("reconnect produced %d commands, expected at least %d: %v",
			len(second.got), len(expectedPrefix), second.got)
	}
	for i, want := range expectedPrefix {
		if got := string(second.got[i]); got != want {
			t.Fatalf("reconnect command %d = %q, want %q", i, got, want)
		}
	}
}

func TestConsumerReconnectSlowStartsRDY(t *testing.T) {
	script := []instruction{
		{0, FrameTypeResponse, []byte("OK")},
		{0, FrameTypeResponse, []byte("OK")},
		{20 * time.Millisecond, -1, []byte("exit")},
	}

	addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	first := newMockNSQD(t, script, addr.String())

	config := NewConfig()
	config.MaxInFlight = 50
	config.LookupdPollInterval = 10 * time.Millisecond
	config.RDYRedistributeInterval = 10 * time.Millisecond
	q, _ := NewConsumer("test_reconnect_rdy"+strconv.Itoa(int(time.Now().UnixNano())), "ch", config)
	q.SetLogger(nullLogger, LogLevelError)
	q.AddHandler(&testHandler{})

	if err := q.ConnectToNSQD(first.tcpAddr.String()); err != nil {
		t.Fatal(err)
	}
	<-first.exitChan

	second := newMockNSQD(t, script, first.tcpAddr.String())
	select {
	case <-second.exitChan:
	case <-time.After(time.Second):
		t.Fatal("consumer did not reconnect")
	}

	q.Stop()
	<-q.StopChan

	expectedPrefix := []string{
		"IDENTIFY",
		"SUB " + q.topic + " ch",
		"RDY 1",
	}
	if len(second.got) < len(expectedPrefix) {
		t.Fatalf("reconnect produced %d commands, expected at least %d: %v",
			len(second.got), len(expectedPrefix), second.got)
	}
	for i, want := range expectedPrefix {
		if got := string(second.got[i]); got != want {
			t.Fatalf("reconnect command %d = %q, want %q", i, got, want)
		}
	}
}

type reconnectFlakyMessage struct {
	id   string
	body []byte
}

type reconnectFlakyNSQD struct {
	t              *testing.T
	ln             net.Listener
	closeAfter     int
	maxDisconnects int

	mu          sync.Mutex
	queue       []*reconnectFlakyMessage
	delivered   map[string]int
	disconnects int
}

func newFlakyNSQD(t *testing.T, addr string, count, closeAfter, maxDisconnects int) *reconnectFlakyNSQD {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	s := &reconnectFlakyNSQD{
		t:              t,
		ln:             ln,
		closeAfter:     closeAfter,
		maxDisconnects: maxDisconnects,
		delivered:      make(map[string]int),
	}
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("%016x", i)
		s.queue = append(s.queue, &reconnectFlakyMessage{id: id, body: []byte("order-" + strconv.Itoa(i))})
	}
	go s.acceptLoop()
	return s
}

func (s *reconnectFlakyNSQD) acceptLoop() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		go s.handle(conn)
	}
}

func (s *reconnectFlakyNSQD) handle(conn net.Conn) {
	defer conn.Close()

	rdy := 0
	delivered := 0
	disconnected := false
	inFlight := make(map[string]*reconnectFlakyMessage)

	defer func() {
		s.mu.Lock()
		for _, msg := range inFlight {
			s.queue = append(s.queue, msg)
		}
		if disconnected {
			s.disconnects++
		}
		s.mu.Unlock()
	}()

	var magic [4]byte
	if _, err := io.ReadFull(conn, magic[:]); err != nil {
		return
	}
	reader := bufio.NewReader(conn)

	send := func(frameType int32, body []byte) error {
		size := make([]byte, 4)
		binary.BigEndian.PutUint32(size, uint32(len(body)+4))
		if _, err := conn.Write(size); err != nil {
			return err
		}
		ft := make([]byte, 4)
		binary.BigEndian.PutUint32(ft, uint32(frameType))
		if _, err := conn.Write(ft); err != nil {
			return err
		}
		_, err := conn.Write(body)
		return err
	}

	pump := func() error {
		for {
			s.mu.Lock()
			if rdy <= 0 || len(inFlight) >= rdy || len(s.queue) == 0 {
				s.mu.Unlock()
				return nil
			}
			msg := s.queue[0]
			s.queue = s.queue[1:]
			inFlight[msg.id] = msg
			s.delivered[msg.id]++
			s.mu.Unlock()

			delivered++
			var body []byte
			ts := make([]byte, 8)
			binary.BigEndian.PutUint64(ts, uint64(time.Now().UnixNano()))
			attempts := make([]byte, 2)
			binary.BigEndian.PutUint16(attempts, 1)
			body = append(body, ts...)
			body = append(body, attempts...)
			body = append(body, []byte(msg.id)...)
			body = append(body, msg.body...)
			if err := send(FrameTypeMessage, body); err != nil {
				return err
			}

			if delivered >= s.closeAfter {
				s.mu.Lock()
				disconnect := s.disconnects < s.maxDisconnects
				s.mu.Unlock()
				if disconnect {
					disconnected = true
					return fmt.Errorf("simulated disconnect")
				}
			}
		}
	}

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimSuffix(line, "\n")
		parts := strings.Split(line, " ")
		switch parts[0] {
		case "IDENTIFY":
			var size int32
			if err := binary.Read(reader, binary.BigEndian, &size); err != nil {
				return
			}
			identify := make([]byte, size)
			if _, err := io.ReadFull(reader, identify); err != nil {
				return
			}
			var payload map[string]interface{}
			if err := json.Unmarshal(identify, &payload); err != nil {
				return
			}
			if err := send(FrameTypeResponse, []byte(`{"max_rdy_count":2500}`)); err != nil {
				return
			}
		case "SUB":
			if err := send(FrameTypeResponse, []byte("OK")); err != nil {
				return
			}
		case "RDY":
			rdy, _ = strconv.Atoi(parts[1])
			if err := pump(); err != nil {
				return
			}
		case "FIN":
			s.mu.Lock()
			delete(inFlight, parts[1])
			s.mu.Unlock()
			if err := pump(); err != nil {
				return
			}
		case "REQ":
			s.mu.Lock()
			msg := inFlight[parts[1]]
			delete(inFlight, parts[1])
			if msg != nil {
				s.queue = append(s.queue, msg)
			}
			s.mu.Unlock()
			if err := pump(); err != nil {
				return
			}
		case "CLS":
			_ = send(FrameTypeResponse, []byte("CLOSE_WAIT"))
			return
		}
	}
}

func (s *reconnectFlakyNSQD) stats() (int, int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	unique := 0
	duplicates := 0
	for _, count := range s.delivered {
		if count > 0 {
			unique++
		}
		if count > 1 {
			duplicates += count - 1
		}
	}
	return unique, duplicates, s.disconnects
}

type reconnectCountingHandler struct {
	mu     sync.Mutex
	seen   map[string]struct{}
	total  int
	target int
	done   chan struct{}
}

func (h *reconnectCountingHandler) HandleMessage(message *Message) error {
	h.mu.Lock()
	h.seen[string(message.Body)] = struct{}{}
	h.total++
	unique := len(h.seen)
	h.mu.Unlock()
	time.Sleep(time.Millisecond)
	if unique >= h.target {
		select {
		case <-h.done:
		default:
			close(h.done)
		}
	}
	return nil
}

func TestConsumerTenReconnectsAtLeastOnce(t *testing.T) {
	const total = 400
	s := newFlakyNSQD(t, "127.0.0.1:0", total, 7, 10)
	defer s.ln.Close()

	config := NewConfig()
	config.MaxInFlight = 20
	config.LookupdPollInterval = 10 * time.Millisecond
	config.RDYRedistributeInterval = 10 * time.Millisecond
	q, _ := NewConsumer("test_reconnect_ten"+strconv.Itoa(int(time.Now().UnixNano())), "ch", config)
	q.SetLogger(nullLogger, LogLevelError)

	handler := &reconnectCountingHandler{
		seen:   make(map[string]struct{}),
		target: total,
		done:   make(chan struct{}),
	}
	q.AddHandler(handler)

	if err := q.ConnectToNSQD(s.ln.Addr().String()); err != nil {
		t.Fatal(err)
	}

	select {
	case <-handler.done:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for all messages")
	}

	q.Stop()
	<-q.StopChan

	handler.mu.Lock()
	unique := len(handler.seen)
	handled := handler.total
	handler.mu.Unlock()
	serverUnique, duplicates, disconnects := s.stats()

	if disconnects < 10 {
		t.Fatalf("simulated %d disconnects, expected at least 10", disconnects)
	}
	if unique != total {
		t.Fatalf("lost %d messages: handled %d unique of %d", total-unique, unique, total)
	}
	if serverUnique != total {
		t.Fatalf("server lost messages: delivered %d unique of %d", serverUnique, total)
	}
	t.Logf("disconnects=%d lost=0 duplicates=%d (handler saw %d deliveries)",
		disconnects, duplicates, handled)
}
