package nsq

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// reconnectingMockNSQD is a minimal nsqd stand-in that accepts several
// successive client connections (each reconnect is a new Accept) and records
// the PUB command bodies it receives, in arrival order.
type reconnectingMockNSQD struct {
	t        *testing.T
	listener net.Listener

	mu     sync.Mutex
	got    [][]byte
	conns  int
	closed bool
	active map[net.Conn]struct{}
	exitCh chan struct{}
}

func newReconnectingMockNSQD(t *testing.T) *reconnectingMockNSQD {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed - %s", err)
	}
	n := &reconnectingMockNSQD{
		t:        t,
		listener: listener,
		exitCh:   make(chan struct{}),
		active:   make(map[net.Conn]struct{}),
	}
	go n.serve()
	return n
}

func (n *reconnectingMockNSQD) addr() string {
	return n.listener.Addr().String()
}

func (n *reconnectingMockNSQD) serve() {
	defer close(n.exitCh)
	for {
		conn, err := n.listener.Accept()
		if err != nil {
			return
		}
		n.mu.Lock()
		n.conns++
		connIdx := n.conns
		n.mu.Unlock()
		go n.handle(conn, connIdx)
	}
}

func (n *reconnectingMockNSQD) handle(conn net.Conn, idx int) {
	defer conn.Close()

	n.mu.Lock()
	n.active[conn] = struct{}{}
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		delete(n.active, conn)
		n.mu.Unlock()
	}()

	// protocol magic
	magic := make([]byte, 4)
	if _, err := io.ReadFull(conn, magic); err != nil {
		return
	}

	rdr := bufio.NewReader(conn)
	for {
		line, err := rdr.ReadBytes('\n')
		if err != nil {
			return
		}
		params := bytes.Split(line[:len(line)-1], []byte(" "))

		switch string(params[0]) {
		case "IDENTIFY":
			sizeBuf := make([]byte, 4)
			if _, err := io.ReadFull(rdr, sizeBuf); err != nil {
				return
			}
			size := int(binary.BigEndian.Uint32(sizeBuf))
			if _, err := io.CopyN(io.Discard, rdr, int64(size)); err != nil {
				return
			}
			if _, err := conn.Write(framedResponse(FrameTypeResponse, []byte("OK"))); err != nil {
				return
			}
		case "PUB":
			sizeBuf := make([]byte, 4)
			if _, err := io.ReadFull(rdr, sizeBuf); err != nil {
				return
			}
			size := int(binary.BigEndian.Uint32(sizeBuf))
			body := make([]byte, size)
			if _, err := io.ReadFull(rdr, body); err != nil {
				return
			}
			n.mu.Lock()
			n.got = append(n.got, body)
			n.mu.Unlock()
			if _, err := conn.Write(framedResponse(FrameTypeResponse, []byte("OK"))); err != nil {
				return
			}
		default:
			n.t.Logf("mock conn %d ignoring command %q", idx, params[0])
		}
	}
}

func (n *reconnectingMockNSQD) stop() {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return
	}
	n.closed = true
	n.mu.Unlock()
	n.listener.Close()
	<-n.exitCh
}

// dropActiveConnections closes every accepted socket from the server side,
// producing the immediate EOF a weak-network failure gives the client.
func (n *reconnectingMockNSQD) dropActiveConnections() {
	n.mu.Lock()
	conns := make([]net.Conn, 0, len(n.active))
	for conn := range n.active {
		conns = append(conns, conn)
	}
	n.mu.Unlock()
	for _, conn := range conns {
		conn.Close()
	}
}

func (n *reconnectingMockNSQD) connectionCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.conns
}

func (n *reconnectingMockNSQD) received() [][]byte {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([][]byte, len(n.got))
	copy(out, n.got)
	return out
}

// TestProducerReconnectPreservesOrder verifies that transactions buffered
// when the connection drops are delivered in their original enqueue order,
// ahead of transactions published after the reconnect.
func TestProducerReconnectPreservesOrder(t *testing.T) {
	nsqd := newReconnectingMockNSQD(t)
	defer nsqd.stop()

	config := NewConfig()
	config.DialTimeout = time.Second
	producer, err := NewProducer(nsqd.addr(), config)
	if err != nil {
		t.Fatalf("NewProducer - %s", err)
	}
	producer.SetLogger(nullLogger, LogLevelInfo)
	defer producer.Stop()

	topic := "reconnect_order"

	// 1. establish the first connection and publish one message so the
	// producer router is fully wired up
	if err := producer.Publish(topic, []byte("m0")); err != nil {
		t.Fatalf("initial publish - %s", err)
	}

	// 2. simulate a weak-network drop: the nsqd side vanishes. The router
	// survives and the following publishes are buffered while disconnected.
	nsqd.dropActiveConnections()
	waitUntilDisconnected(t, producer)

	// these have a causal order the downstream relies on: create must
	// precede update
	backlog := []string{"m1-create", "m2-update"}
	for _, body := range backlog {
		if err := producer.PublishAsync(topic, []byte(body), nil); err != nil {
			t.Fatalf("buffered publish %q - %s", body, err)
		}
	}

	// 3. wait for the router to reconnect and then publish a fresh message
	waitForState(t, producer, StateConnected)
	if err := producer.Publish(topic, []byte("m3-after-reconnect")); err != nil {
		t.Fatalf("post-reconnect publish - %s", err)
	}

	// 4. compare the order observed downstream
	deadline := time.Now().Add(5 * time.Second)
	var got [][]byte
	for time.Now().Before(deadline) {
		got = nsqd.received()
		if len(got) >= 4 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	expected := []string{"m0", "m1-create", "m2-update", "m3-after-reconnect"}
	if len(got) != len(expected) {
		t.Fatalf("expected %d messages, got %d: %v", len(expected), len(got), got)
	}
	for i, want := range expected {
		if string(got[i]) != want {
			t.Fatalf("order violation at index %d: got %q want %q (full: %v)",
				i, got[i], want, got)
		}
	}

	if nsqd.connectionCount() < 2 {
		t.Fatalf("expected a reconnect, saw %d connections", nsqd.connectionCount())
	}
}

// TestProducerStopWhileDisconnectedFailsBuffer makes sure a permanent Stop
// still surfaces ErrNotConnected for buffered transactions instead of
// hanging on the reconnect backoff.
func TestProducerStopWhileDisconnectedFailsBuffer(t *testing.T) {
	nsqd := newReconnectingMockNSQD(t)

	config := NewConfig()
	config.DialTimeout = 100 * time.Millisecond
	producer, err := NewProducer(nsqd.addr(), config)
	if err != nil {
		t.Fatalf("NewProducer - %s", err)
	}
	producer.SetLogger(nullLogger, LogLevelInfo)

	if err := producer.Publish("t", []byte("m0")); err != nil {
		t.Fatalf("initial publish - %s", err)
	}

	nsqd.dropActiveConnections()
	waitUntilDisconnected(t, producer)

	done := make(chan *ProducerTransaction, 1)
	if err := producer.PublishAsync("t", []byte("m1"), done); err != nil {
		t.Fatalf("buffered publish - %s", err)
	}

	// take the server away permanently so reconnect attempts keep failing
	nsqd.stop()
	producer.Stop()

	select {
	case txn := <-done:
		if txn.Error != ErrNotConnected {
			t.Fatalf("expected ErrNotConnected, got %v", txn.Error)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() did not unblock the buffered transaction")
	}
}

func waitForState(t *testing.T, producer *Producer, want int32) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&producer.state) == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("producer never reached state %d (current=%d)", want, atomic.LoadInt32(&producer.state))
}

func waitUntilDisconnected(t *testing.T, producer *Producer) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&producer.state) != StateConnected {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("producer never disconnected (state=%d)", atomic.LoadInt32(&producer.state))
}

// TestProducerRepeatedReconnectsKeepOrder exercises the hourly weak-network
// scenario: many disconnect/reconnect cycles with buffered publishes must
// preserve per-cycle ordering every single time.
func TestProducerRepeatedReconnectsKeepOrder(t *testing.T) {
	nsqd := newReconnectingMockNSQD(t)
	defer nsqd.stop()

	config := NewConfig()
	producer, err := NewProducer(nsqd.addr(), config)
	if err != nil {
		t.Fatalf("NewProducer - %s", err)
	}
	producer.SetLogger(nullLogger, LogLevelInfo)
	defer producer.Stop()

	if err := producer.Publish("t", []byte("start")); err != nil {
		t.Fatalf("initial publish - %s", err)
	}

	const cycles = 12
	for cycle := 0; cycle < cycles; cycle++ {
		nsqd.dropActiveConnections()
		waitUntilDisconnected(t, producer)

		// an ordered create/update pair buffered every outage
		create := []byte(fmt.Sprintf("c%d-create", cycle))
		update := []byte(fmt.Sprintf("c%d-update", cycle))
		if err := producer.PublishAsync("t", create, nil); err != nil {
			t.Fatalf("cycle %d create - %s", cycle, err)
		}
		if err := producer.PublishAsync("t", update, nil); err != nil {
			t.Fatalf("cycle %d update - %s", cycle, err)
		}

		waitForState(t, producer, StateConnected)
	}
	if err := producer.Publish("t", []byte("end")); err != nil {
		t.Fatalf("final publish - %s", err)
	}

	// verify each create strictly precedes its update in the downstream stream
	deadline := time.Now().Add(5 * time.Second)
	var got [][]byte
	for time.Now().Before(deadline) {
		got = nsqd.received()
		if len(got) >= 2+2*cycles {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	indexOf := func(want string) int {
		for i, body := range got {
			if string(body) == want {
				return i
			}
		}
		return -1
	}
	for cycle := 0; cycle < cycles; cycle++ {
		createIdx := indexOf(fmt.Sprintf("c%d-create", cycle))
		updateIdx := indexOf(fmt.Sprintf("c%d-update", cycle))
		if createIdx < 0 || updateIdx < 0 {
			t.Fatalf("cycle %d message missing from downstream: %v", cycle, got)
		}
		if createIdx > updateIdx {
			t.Fatalf("cycle %d reordered: update(index=%d) before create(index=%d)",
				cycle, updateIdx, createIdx)
		}
	}
}
