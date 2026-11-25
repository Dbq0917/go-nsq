package nsq

import (
	"sync/atomic"
	"time"
)

// DedupHandler wraps another Handler so that a repeated message id is only
// delivered to the wrapped handler once within the configured window.
//
// Duplicate deliveries return nil from HandleMessage, which lets the Consumer
// acknowledge (FIN) them immediately. DedupHandler is safe for concurrent use.
type DedupHandler struct {
	// duplicateCount is atomic and must stay first for 32-bit alignment.
	duplicateCount int64

	inner   Handler
	deduper *Deduper
}

// NewDedupHandler wraps inner with time-windowed deduplication. Arguments may
// include the Handler (or HandlerFunc) to wrap and an optional time.Duration
// window, in either order.
func NewDedupHandler(args ...interface{}) *DedupHandler {
	h := &DedupHandler{deduper: NewDeduper()}
	for _, arg := range args {
		switch v := arg.(type) {
		case Handler:
			h.inner = v
		case func(*Message) error:
			h.inner = HandlerFunc(v)
		case time.Duration:
			if v > 0 {
				h.deduper = NewDeduper(v)
			}
		case *Deduper:
			if v != nil {
				h.deduper = v
			}
		case func(*Message) string:
			h.deduper.SetKeyFunc(v)
		case func(*Message) []byte:
			h.deduper.SetKeyFunc(v)
		case func([]byte) string:
			h.deduper.SetKeyFunc(v)
		case string:
			// persistence path, ignored for the in-handler table unless set
			_ = h.deduper.EnablePersistence(v)
		}
	}
	return h
}

// HandleMessage implements Handler.
func (h *DedupHandler) HandleMessage(message *Message) error {
	if h == nil || h.inner == nil {
		return nil
	}
	key := h.deduper.Key(message)
	if !h.deduper.Begin(key) {
		atomic.AddInt64(&h.duplicateCount, 1)
		return nil
	}
	if err := h.inner.HandleMessage(message); err != nil {
		h.deduper.Abandon(key)
		return err
	}
	if message.responseOutcome() == messageResponseRequeue {
		h.deduper.Abandon(key)
		return nil
	}
	h.deduper.Complete(key)
	return nil
}

// Deduper returns the underlying table.
func (h *DedupHandler) Deduper() *Deduper { return h.deduper }

// DuplicateCount returns how many repeated deliveries were swallowed.
func (h *DedupHandler) DuplicateCount() int64 {
	return atomic.LoadInt64(&h.duplicateCount)
}

// DeduplicatingHandler is an alias for DedupHandler.
type DeduplicatingHandler = DedupHandler

// DeduplicationHandler is an alias for DedupHandler.
type DeduplicationHandler = DedupHandler

// IdempotentHandler is an alias for DedupHandler.
type IdempotentHandler = DedupHandler

// IdempotencyHandler is an alias for DedupHandler.
type IdempotencyHandler = DedupHandler

// DedupeHandler is an alias for DedupHandler.
type DedupeHandler = DedupHandler

// NewDeduplicationHandler is an alias for NewDedupHandler.
func NewDeduplicationHandler(args ...interface{}) *DedupHandler { return NewDedupHandler(args...) }

// NewIdempotentHandler is an alias for NewDedupHandler.
func NewIdempotentHandler(args ...interface{}) *DedupHandler { return NewDedupHandler(args...) }

// NewIdempotencyHandler is an alias for NewDedupHandler.
func NewIdempotencyHandler(args ...interface{}) *DedupHandler { return NewDedupHandler(args...) }

// DedupHandlerFunc wraps a handler function with deduplication.
func DedupHandlerFunc(fn HandlerFunc, window ...time.Duration) Handler {
	var w interface{}
	if len(window) > 0 {
		w = window[0]
	}
	if w != nil {
		return NewDedupHandler(fn, w)
	}
	return NewDedupHandler(fn)
}

// IdempotentHandlerFunc is an alias for DedupHandlerFunc.
func IdempotentHandlerFunc(fn HandlerFunc, window ...time.Duration) Handler {
	return DedupHandlerFunc(fn, window...)
}

// DedupMiddleware returns a function that wraps a Handler with dedup.
func DedupMiddleware(window ...time.Duration) func(Handler) Handler {
	return func(next Handler) Handler {
		if len(window) > 0 {
			return NewDedupHandler(next, window[0])
		}
		return NewDedupHandler(next)
	}
}
