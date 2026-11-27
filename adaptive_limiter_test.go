package nsq

import (
	"testing"
	"time"
)

func newTestAdaptiveLimiter() *adaptiveLimiter {
	c := NewConfig()
	c.MaxInFlight = 100
	c.AdaptiveMinInFlight = 1
	c.AdaptiveFailureRateThreshold = 0.1
	c.AdaptiveGrowthStep = 0.1
	c.MsgTimeout = 100 * time.Millisecond
	c.AdaptiveMsgTimeoutRatio = 0.7
	return newAdaptiveLimiter(c)
}

func feedWindow(l *adaptiveLimiter, n int, duration time.Duration, success bool) {
	for i := 0; i < n; i++ {
		l.observe(duration, success)
	}
}

func TestAdaptiveLimiterNoopBelowMinSamples(t *testing.T) {
	l := newTestAdaptiveLimiter()
	feedWindow(l, adaptiveMinWindowSamples-1, time.Millisecond, true)
	if got := l.evaluate(); got != 100 {
		t.Fatalf("expected limit unchanged at 100 with too few samples, got %d", got)
	}
}

func TestAdaptiveLimiterGrowsOnHealthyWindow(t *testing.T) {
	l := newTestAdaptiveLimiter()
	// Start below the ceiling so additive growth is observable.
	l.setCeiling(100)
	l.limit = 20
	// Establish the no-load baseline.
	feedWindow(l, adaptiveMinWindowSamples, time.Millisecond, true)
	l.evaluate()

	limit := l.currentLimit()
	for i := 0; i < 5; i++ {
		feedWindow(l, adaptiveMinWindowSamples, time.Millisecond, true)
		newLimit := l.evaluate()
		if newLimit <= limit {
			t.Fatalf("healthy window should grow the limit: %d -> %d", limit, newLimit)
		}
		limit = newLimit
	}
	if limit > 100 {
		t.Fatalf("limit must not exceed the ceiling, got %d", limit)
	}
}

func TestAdaptiveLimiterGrowthIsCappedByCeiling(t *testing.T) {
	l := newTestAdaptiveLimiter()
	feedWindow(l, adaptiveMinWindowSamples, time.Millisecond, true)
	l.evaluate()
	for i := 0; i < 50; i++ {
		feedWindow(l, adaptiveMinWindowSamples, time.Millisecond, true)
		l.evaluate()
	}
	if got := l.currentLimit(); got != 100 {
		t.Fatalf("expected limit to settle at ceiling 100, got %d", got)
	}
}

func TestAdaptiveLimiterShrinksOnElevatedLatency(t *testing.T) {
	l := newTestAdaptiveLimiter()
	// Fast window establishes a low no-load baseline.
	feedWindow(l, adaptiveMinWindowSamples, time.Millisecond, true)
	l.evaluate()

	// A window running roughly 2x the no-load latency must shrink.
	feedWindow(l, adaptiveMinWindowSamples, 2*time.Millisecond, true)
	after := l.evaluate()
	if after >= 100 {
		t.Fatalf("expected elevated latency to shrink the limit, got %d", after)
	}

	// Healthy windows afterwards must grow it back toward the ceiling.
	for i := 0; i < 40; i++ {
		feedWindow(l, adaptiveMinWindowSamples, time.Millisecond, true)
		l.evaluate()
	}
	if got := l.currentLimit(); got != 100 {
		t.Fatalf("expected limit to recover to ceiling 100, got %d", got)
	}
}

func TestAdaptiveLimiterShrinksOnFailureRate(t *testing.T) {
	l := newTestAdaptiveLimiter()
	feedWindow(l, adaptiveMinWindowSamples, time.Millisecond, true)
	l.evaluate()

	// 25% failure rate exceeds the 10% threshold even with flat latency.
	for i := 0; i < adaptiveMinWindowSamples; i++ {
		l.observe(time.Millisecond, i%4 == 0)
	}
	after := l.evaluate()
	expected := int(float64(100) * adaptiveFailureDecreaseFactor)
	if after != expected {
		t.Fatalf("expected failure-driven decrease to %d, got %d", expected, after)
	}
}

func TestAdaptiveLimiterBelowFailureThresholdKeepsLimit(t *testing.T) {
	l := newTestAdaptiveLimiter()
	l.limit = 20
	feedWindow(l, adaptiveMinWindowSamples, time.Millisecond, true)
	l.evaluate()

	// One failure out of 20 == 5%, under the 10% threshold; healthy latency
	// means the limit should grow rather than shrink.
	for i := 0; i < adaptiveMinWindowSamples; i++ {
		l.observe(time.Millisecond, i != 0)
	}
	after := l.evaluate()
	if after <= 20 {
		t.Fatalf("expected sub-threshold failures not to shrink, got %d", after)
	}
}

func TestAdaptiveLimiterTimeoutPressureShedsHardest(t *testing.T) {
	l := newTestAdaptiveLimiter()
	feedWindow(l, adaptiveMinWindowSamples, time.Millisecond, true)
	l.evaluate()

	// One message at 80ms of a 100ms timeout (ratio threshold 0.7 == 70ms).
	for i := 0; i < adaptiveMinWindowSamples; i++ {
		duration := time.Millisecond
		if i == 0 {
			duration = 80 * time.Millisecond
		}
		l.observe(duration, true)
	}
	after := l.evaluate()
	expected := int(float64(100) * adaptiveTimeoutDecreaseFactor)
	if after != expected {
		t.Fatalf("expected timeout-driven decrease to %d, got %d", expected, after)
	}
}

func TestAdaptiveLimiterRespectsFloor(t *testing.T) {
	c := NewConfig()
	c.MaxInFlight = 10
	c.AdaptiveMinInFlight = 4
	c.MsgTimeout = 100 * time.Millisecond
	c.AdaptiveMsgTimeoutRatio = 0.7
	l := newAdaptiveLimiter(c)

	// Repeatedly trigger the harshest (timeout) decrease.
	for w := 0; w < 10; w++ {
		for i := 0; i < adaptiveMinWindowSamples; i++ {
			l.observe(80*time.Millisecond, true)
		}
		l.evaluate()
	}
	if got := l.currentLimit(); got < 4 {
		t.Fatalf("limit must not drop below floor 4, got %d", got)
	}

	// And recover back up to the ceiling once healthy.
	for w := 0; w < 30; w++ {
		feedWindow(l, adaptiveMinWindowSamples, time.Millisecond, true)
		l.evaluate()
	}
	if got := l.currentLimit(); got != 10 {
		t.Fatalf("expected recovery to ceiling 10, got %d", got)
	}
}

func TestAdaptiveLimiterCeilingZeroPauses(t *testing.T) {
	l := newTestAdaptiveLimiter()
	feedWindow(l, adaptiveMinWindowSamples, time.Millisecond, true)
	l.evaluate()

	l.setCeiling(0)
	feedWindow(l, adaptiveMinWindowSamples, time.Millisecond, true)
	if got := l.evaluate(); got != 0 {
		t.Fatalf("expected paused limit 0, got %d", got)
	}

	// Restoring the ceiling allows growth to resume.
	l.setCeiling(100)
	for w := 0; w < 40; w++ {
		feedWindow(l, adaptiveMinWindowSamples, time.Millisecond, true)
		l.evaluate()
	}
	if got := l.currentLimit(); got != 100 {
		t.Fatalf("expected recovery to ceiling 100, got %d", got)
	}
}

func TestAdaptiveLimiterCeilingBelowFloorUsesCeiling(t *testing.T) {
	c := NewConfig()
	c.MaxInFlight = 3
	c.AdaptiveMinInFlight = 5
	l := newAdaptiveLimiter(c)
	l.setCeiling(2)
	feedWindow(l, adaptiveMinWindowSamples, time.Millisecond, true)
	if got := l.evaluate(); got != 2 {
		t.Fatalf("expected limit clamped to ceiling 2, got %d", got)
	}
}

func TestAdaptiveLimiterBaselineRecoversFromTransientSlowdown(t *testing.T) {
	l := newTestAdaptiveLimiter()
	// Establish a 1ms no-load baseline.
	feedWindow(l, adaptiveMinWindowSamples, time.Millisecond, true)
	l.evaluate()
	baseline := l.snapshot().RTTNoLoad

	// A slow window must not permanently anchor the baseline high.
	feedWindow(l, adaptiveMinWindowSamples, 10*time.Millisecond, true)
	l.evaluate()

	// After healthy windows the slow-elevated baseline drifts back down and
	// must remain close to the original no-load value.
	for w := 0; w < 100; w++ {
		feedWindow(l, adaptiveMinWindowSamples, time.Millisecond, true)
		l.evaluate()
	}
	recovered := l.snapshot().RTTNoLoad
	if recovered >= baseline*2 {
		t.Fatalf("baseline failed to recover: initial %s, recovered %s", baseline, recovered)
	}
}

func TestAdaptiveLimiterConcurrentObserve(t *testing.T) {
	l := newTestAdaptiveLimiter()
	done := make(chan struct{})
	for g := 0; g < 8; g++ {
		go func() {
			for i := 0; i < 1000; i++ {
				l.observe(time.Millisecond, i%5 != 0)
			}
			done <- struct{}{}
		}()
	}
	for g := 0; g < 8; g++ {
		<-done
	}
	if l.windowCount != 8000 {
		t.Fatalf("expected 8000 observations, got %d", l.windowCount)
	}
}
