package nsq

import (
	"math"
	"sync"
	"time"
)

const (
	// adaptiveMinWindowSamples is the minimum number of observed message
	// handling results required within an adjustment window before the limit
	// is allowed to move. With too few samples a single slow or failed
	// message would otherwise cause noisy, oscillating adjustments.
	adaptiveMinWindowSamples = 20

	// adaptiveGradientMin bounds how aggressively a latency regression is
	// allowed to shrink the limit in a single window (Netflix Gradient2 style).
	adaptiveGradientMin = 0.5

	// adaptiveGradientGrowthEps treats gradients at or above this value as
	// "no measurable congestion", which triggers additive growth instead of
	// a shrink.
	adaptiveGradientGrowthEps = 0.99

	// adaptiveRttDownAlpha is the EWMA weight used while observed latency is
	// below the no-load baseline, so improvements are picked up quickly.
	adaptiveRttDownAlpha = 0.5

	// adaptiveRttUpBeta is the much smaller EWMA weight used while observed
	// latency is above the baseline, so a transient slowdown cannot
	// permanently inflate the no-load estimate (and thereby disable future
	// shrinkage). It also lets the baseline recover on its own.
	adaptiveRttUpBeta = 0.01

	// adaptiveFailureDecreaseFactor is the multiplicative decrease applied
	// when the window's failure rate crosses the configured threshold.
	adaptiveFailureDecreaseFactor = 0.8

	// adaptiveTimeoutDecreaseFactor is the stronger multiplicative decrease
	// applied when an individual handling duration gets close enough to the
	// nsqd message timeout that in-flight messages risk timing out and being
	// redelivered.
	adaptiveTimeoutDecreaseFactor = 0.7
)

// adaptiveLimiter continuously tunes the effective in-flight (RDY) budget of
// a Consumer based on observed handler latency and failure rate.
//
// It only ever restricts how many *new* messages the consumer requests from
// nsqd; messages already handed to a handler are never cancelled, so adjusting
// the limit cannot by itself cause a message to be redelivered. The limit is
// bounded below by a configured floor and above by Config.MaxInFlight, and any
// shrink is reversed additively as soon as sustained healthy windows are
// observed, so the consumer always grows back to the throughput it can handle.
type adaptiveLimiter struct {
	mu sync.Mutex

	ceiling int
	floor   int
	limit   int

	msgTimeout       time.Duration
	failureRate      float64
	growthStep       float64
	timeoutRatio     float64
	rttNoLoad        time.Duration
	hasBaseline      bool
	windowCount      int
	windowFailures   int
	windowLatency    time.Duration
	windowMaxLatency time.Duration
}

func newAdaptiveLimiter(config *Config) *adaptiveLimiter {
	ceiling := config.MaxInFlight
	return &adaptiveLimiter{
		ceiling:      ceiling,
		floor:        config.AdaptiveMinInFlight,
		limit:        ceiling,
		msgTimeout:   config.MsgTimeout,
		failureRate:  config.AdaptiveFailureRateThreshold,
		growthStep:   config.AdaptiveGrowthStep,
		timeoutRatio: config.AdaptiveMsgTimeoutRatio,
	}
}

// observe records the handling result of a single message. It is safe for
// concurrent use because handler goroutines finish messages concurrently.
func (l *adaptiveLimiter) observe(handlingDuration time.Duration, success bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.windowCount++
	if !success {
		l.windowFailures++
	}
	l.windowLatency += handlingDuration
	if handlingDuration > l.windowMaxLatency {
		l.windowMaxLatency = handlingDuration
	}
}

// evaluate consumes the current window's observations and returns the
// (already applied) effective in-flight limit.
func (l *adaptiveLimiter) evaluate() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	count := l.windowCount
	failures := l.windowFailures
	totalLatency := l.windowLatency
	maxLatency := l.windowMaxLatency
	l.resetWindowLocked()

	// A zero ceiling means message flow was paused externally; stay paused.
	if l.ceiling <= 0 {
		l.limit = 0
		return l.limit
	}

	// Not enough signal this window: keep the current limit and wait for a
	// statistically meaningful window rather than reacting to noise.
	if count < adaptiveMinWindowSamples {
		return l.limit
	}

	meanLatency := totalLatency / time.Duration(count)
	l.updateBaselineLocked(meanLatency)

	next := l.limit
	switch {
	case l.msgTimeout > 0 &&
		float64(maxLatency) >= l.timeoutRatio*float64(l.msgTimeout):
		// A handler is about to exceed the nsqd message timeout: shed load
		// hard to stop in-flight messages from being redelivered.
		next = l.decreaseLocked(adaptiveTimeoutDecreaseFactor)
	case float64(failures)/float64(count) >= l.failureRate:
		next = l.decreaseLocked(adaptiveFailureDecreaseFactor)
	default:
		gradient := 1.0
		if meanLatency > 0 {
			gradient = float64(l.rttNoLoad) / float64(meanLatency)
			if gradient < adaptiveGradientMin {
				gradient = adaptiveGradientMin
			}
			if gradient > 1 {
				gradient = 1
			}
		}
		if gradient < adaptiveGradientGrowthEps {
			// Latency is elevated relative to no-load: shrink proportionally.
			next = int(float64(l.limit) * gradient)
		} else {
			// Healthy window: probe for more capacity additively (AIMD).
			step := int(math.Ceil(float64(l.limit) * l.growthStep))
			if step < 1 {
				step = 1
			}
			next = l.limit + step
		}
	}

	l.limit = l.clampLocked(next)
	return l.limit
}

// setCeiling updates the upper bound (Config.MaxInFlight, or a value supplied
// via ChangeMaxInFlight) and immediately re-clamps the active limit.
func (l *adaptiveLimiter) setCeiling(ceiling int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ceiling = ceiling
	l.limit = l.clampLocked(l.limit)
}

func (l *adaptiveLimiter) currentLimit() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.limit
}

// adaptiveSnapshot is a point-in-time view of limiter state for stats/logging.
type adaptiveSnapshot struct {
	Limit     int
	Ceiling   int
	RTTNoLoad time.Duration
}

func (l *adaptiveLimiter) snapshot() adaptiveSnapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	return adaptiveSnapshot{Limit: l.limit, Ceiling: l.ceiling, RTTNoLoad: l.rttNoLoad}
}

func (l *adaptiveLimiter) decreaseLocked(factor float64) int {
	next := int(float64(l.limit) * factor)
	// Multiplicative decrease must always make progress even when the limit
	// is small enough that int() truncation would be a no-op.
	if next >= l.limit {
		next = l.limit - 1
	}
	return next
}

func (l *adaptiveLimiter) clampLocked(value int) int {
	if l.ceiling <= l.floor {
		// The externally configured ceiling dropped below the floor (a
		// ceiling of 0 pauses message flow entirely).
		return l.ceiling
	}
	if value < l.floor {
		value = l.floor
	}
	if value > l.ceiling {
		value = l.ceiling
	}
	return value
}

func (l *adaptiveLimiter) resetWindowLocked() {
	l.windowCount = 0
	l.windowFailures = 0
	l.windowLatency = 0
	l.windowMaxLatency = 0
}

// updateBaselineLocked maintains an EWMA of the best-case (no-load) handling
// latency. It moves down quickly when handlers get faster and only drifts up
// very slowly, so a temporary downstream stall cannot anchor the baseline at a
// high value and block future shrinkage.
func (l *adaptiveLimiter) updateBaselineLocked(meanLatency time.Duration) {
	if !l.hasBaseline {
		l.rttNoLoad = meanLatency
		l.hasBaseline = true
		return
	}
	if meanLatency < l.rttNoLoad {
		l.rttNoLoad += time.Duration(adaptiveRttDownAlpha * float64(meanLatency-l.rttNoLoad))
	} else {
		l.rttNoLoad += time.Duration(adaptiveRttUpBeta * float64(meanLatency-l.rttNoLoad))
	}
}
