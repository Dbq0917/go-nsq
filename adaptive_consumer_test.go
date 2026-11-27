package nsq

import (
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// recordingDelegate captures FIN/REQ calls without touching a real connection.
type recordingDelegate struct {
	finished int32
	requeued int32
}

func (d *recordingDelegate) OnFinish(*Message)                       { atomic.AddInt32(&d.finished, 1) }
func (d *recordingDelegate) OnRequeue(*Message, time.Duration, bool) { atomic.AddInt32(&d.requeued, 1) }
func (d *recordingDelegate) OnTouch(*Message)                        {}

func TestNewConsumerAdaptiveConfigValidation(t *testing.T) {
	config := NewConfig()
	config.EnableAdaptiveInFlight = true
	config.MaxInFlight = 3
	config.AdaptiveMinInFlight = 5

	if _, err := NewConsumer("topic", "ch", config); err == nil {
		t.Fatal("expected error when max_in_flight < adaptive_min_in_flight")
	}

	// A MaxInFlight of 0 (paused) remains valid.
	config.MaxInFlight = 0
	if _, err := NewConsumer("topic", "ch", config); err != nil {
		t.Fatalf("paused consumer should be valid, got %v", err)
	}
}

func TestAdaptiveConfigOptionsSettable(t *testing.T) {
	config := NewConfig()
	if err := config.Set("enable_adaptive_in_flight", true); err != nil {
		t.Fatal(err)
	}
	if err := config.Set("adaptive_adjust_interval", "250ms"); err != nil {
		t.Fatal(err)
	}
	if err := config.Set("adaptive_min_in_flight", 2); err != nil {
		t.Fatal(err)
	}
	if err := config.Set("adaptive_failure_rate_threshold", 0.2); err != nil {
		t.Fatal(err)
	}
	if err := config.Set("adaptive_growth_step", 0.25); err != nil {
		t.Fatal(err)
	}
	if err := config.Set("adaptive_msg_timeout_ratio", 0.8); err != nil {
		t.Fatal(err)
	}
	if !config.EnableAdaptiveInFlight ||
		config.AdaptiveAdjustInterval != 250*time.Millisecond ||
		config.AdaptiveMinInFlight != 2 ||
		config.AdaptiveFailureRateThreshold != 0.2 ||
		config.AdaptiveGrowthStep != 0.25 ||
		config.AdaptiveMsgTimeoutRatio != 0.8 {
		t.Fatalf("adaptive config options not applied: %+v", config)
	}
}

func TestConsumerAdaptiveWiring(t *testing.T) {
	config := NewConfig()
	config.MaxInFlight = 10
	config.AdaptiveMinInFlight = 1
	config.AdaptiveFailureRateThreshold = 0.1
	config.EnableAdaptiveInFlight = true

	topicName := "test_adaptive" + strconv.Itoa(int(time.Now().UnixNano()))
	q, err := NewConsumer(topicName, "ch", config)
	if err != nil {
		t.Fatal(err)
	}
	q.SetLogger(newTestLogger(t), LogLevelError)

	if q.adaptive == nil {
		t.Fatal("expected adaptive limiter to be initialized")
	}
	if q.Stats().MaxInFlight != 10 {
		t.Fatalf("expected initial effective limit 10, got %d", q.Stats().MaxInFlight)
	}

	// Record a full window of failed handling results (the same call the
	// handler loop makes when HandleMessage returns an error).
	for i := 0; i < adaptiveMinWindowSamples; i++ {
		q.adaptive.observe(time.Millisecond, false)
	}

	// Drive the limiter deterministically instead of waiting for the ticker.
	limit := q.adaptive.evaluate()
	q.applyAdaptiveLimit()

	expected := int(float64(10) * adaptiveFailureDecreaseFactor)
	if limit != expected {
		t.Fatalf("expected failure-driven limit %d, got %d", expected, limit)
	}
	if q.Stats().MaxInFlight != int32(expected) {
		t.Fatalf("expected Stats().MaxInFlight %d, got %d",
			expected, q.Stats().MaxInFlight)
	}

	// Healthy windows must grow the limit back to the configured ceiling.
	for w := 0; w < 40; w++ {
		for i := 0; i < adaptiveMinWindowSamples; i++ {
			q.adaptive.observe(time.Millisecond, true)
		}
		q.adaptive.evaluate()
	}
	q.applyAdaptiveLimit()
	if q.Stats().MaxInFlight != 10 {
		t.Fatalf("expected recovery to ceiling 10, got %d", q.Stats().MaxInFlight)
	}

	// ChangeMaxInFlight only moves the ceiling in adaptive mode; the tuned
	// limit is re-clamped underneath it.
	q.ChangeMaxInFlight(5)
	if q.Stats().MaxInFlight != 5 {
		t.Fatalf("expected ceiling change to clamp limit to 5, got %d",
			q.Stats().MaxInFlight)
	}
	q.ChangeMaxInFlight(0)
	if q.Stats().MaxInFlight != 0 {
		t.Fatalf("expected ChangeMaxInFlight(0) to pause flow, got %d",
			q.Stats().MaxInFlight)
	}

	q.Stop()
}

func TestConsumerAdaptiveHandlerObservation(t *testing.T) {
	config := NewConfig()
	config.MaxInFlight = 50
	config.EnableAdaptiveInFlight = true

	topicName := "test_adaptive_obs" + strconv.Itoa(int(time.Now().UnixNano()))
	q, err := NewConsumer(topicName, "ch", config)
	if err != nil {
		t.Fatal(err)
	}
	q.SetLogger(newTestLogger(t), LogLevelError)

	delegate := &recordingDelegate{}
	handler := HandlerFunc(func(*Message) error {
		if atomic.LoadInt32(&delegate.requeued) == 0 &&
			atomic.LoadInt32(&delegate.finished) == 0 {
			return errors.New("first message fails")
		}
		return nil
	})
	q.AddHandler(handler)

	failedMsg := NewMessage(MessageID{'f'}, []byte("x"))
	failedMsg.Delegate = delegate
	goodMsg := NewMessage(MessageID{'g'}, []byte("x"))
	goodMsg.Delegate = delegate

	q.incomingMessages <- failedMsg
	q.incomingMessages <- goodMsg

	// Wait for the handler goroutine to respond to both messages.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) &&
		(atomic.LoadInt32(&delegate.requeued) != 1 ||
			atomic.LoadInt32(&delegate.finished) != 1) {
		time.Sleep(time.Millisecond)
	}
	if atomic.LoadInt32(&delegate.requeued) != 1 ||
		atomic.LoadInt32(&delegate.finished) != 1 {
		t.Fatalf("expected 1 requeue and 1 finish, got %d/%d",
			atomic.LoadInt32(&delegate.requeued),
			atomic.LoadInt32(&delegate.finished))
	}

	q.adaptive.mu.Lock()
	observed := q.adaptive.windowCount
	failures := q.adaptive.windowFailures
	q.adaptive.mu.Unlock()
	if observed != 2 || failures != 1 {
		t.Fatalf("expected 2 observations with 1 failure, got %d (%d failures)",
			observed, failures)
	}

	q.Stop()
}
