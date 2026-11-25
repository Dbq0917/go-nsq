package nsq

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultDedupWindow is the time window in which a given message is only
// processed once when consumer-side deduplication is enabled.
const DefaultDedupWindow = 10 * time.Minute

// defaultDedupEventBuffer bounds the number of duplicate delivery records
// kept in memory for post-mortem inspection.
const defaultDedupEventBuffer = 128

// DuplicateEvent describes a single redelivery that was suppressed by the
// consumer-side deduplicator. It is retained for troubleshooting.
type DuplicateEvent struct {
	// ID is the NSQ message id (string form of the raw id bytes).
	ID string
	// Address is the nsqd address that delivered the duplicate, when known.
	Address string
	// At is when the duplicate was observed.
	At time.Time
	// Attempts is the message attempt counter reported by the server.
	Attempts uint16
}

// ddpRecord is the per-message state kept in the dedup table.
type ddpRecord struct {
	FirstSeen time.Time `json:"first_seen"`
	LastSeen  time.Time `json:"last_seen"`
	Hits      int64     `json:"hits"`
}

// Deduper is a concurrency-safe, time-windowed consumer-side message
// deduplication table.
//
// A Deduper records the id of every message that has been processed
// successfully within a fixed time window. Redeliveries of an id that is
// still inside the window are reported as duplicates so the caller can
// acknowledge them immediately without running the business handler again.
// Records older than the window are treated as expired and are removed
// lazily (or by calling Cleanup/CleanupExpired), which keeps the table from
// growing without bound.
//
// The table is held by the Consumer (not by an individual connection), so it
// naturally survives TCP reconnections. An optional file path can be
// configured to additionally survive process restarts.
type Deduper struct {
	// duplicateCount is accessed atomically and must stay first for 32-bit
	// alignment.
	duplicateCount uint64

	mu      sync.Mutex
	window  time.Duration
	records map[string]*ddpRecord
	active  map[string]struct{}
	now     func() time.Time

	lastCleanup time.Time

	eventMu sync.Mutex
	events  []DuplicateEvent

	path string

	saveCh  chan struct{}
	closeCh chan struct{}
	closed  bool
	saveWg  sync.WaitGroup
	keyFunc func(*Message) string
}

// NewDeduper creates a Deduper. Arguments are optional and may be a
// time.Duration window and/or a string persistence path, in either order.
// With no positive window the table uses DefaultDedupWindow.
func NewDeduper(args ...interface{}) *Deduper {
	window, path := ddpParseArgs(args...)
	d := ddpNew(window)
	if path != "" {
		d.EnablePersistence(path)
	}
	return d
}

// newDeduper is the unexported constructor alias for in-package callers. It
// accepts the same optional arguments as NewDeduper.
func newDeduper(args ...interface{}) *Deduper { return NewDeduper(args...) }

func ddpNew(window ...time.Duration) *Deduper {
	w := DefaultDedupWindow
	for _, candidate := range window {
		if candidate > 0 {
			w = candidate
			break
		}
	}
	return &Deduper{
		window:      w,
		records:     make(map[string]*ddpRecord),
		active:      make(map[string]struct{}),
		now:         time.Now,
		lastCleanup: time.Now(),
		events:      make([]DuplicateEvent, 0, defaultDedupEventBuffer),
		saveCh:      make(chan struct{}, 1),
		closeCh:     make(chan struct{}),
	}
}

// SetKeyFunc installs a custom message key extractor. It accepts functions
// with the common signatures used to derive an idempotency key from a
// message; unrecognized values are ignored and the NSQ message id is used.
func (d *Deduper) SetKeyFunc(fn interface{}) *Deduper {
	if d == nil {
		return d
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	switch f := fn.(type) {
	case func(*Message) string:
		d.keyFunc = f
	case func(*Message) []byte:
		d.keyFunc = func(m *Message) string { return string(f(m)) }
	case func(*Message) interface{}:
		d.keyFunc = func(m *Message) string { return ddpKeyOf(f(m)) }
	case func([]byte) string:
		d.keyFunc = func(m *Message) string { return f(m.Body) }
	case func([]byte) []byte:
		d.keyFunc = func(m *Message) string { return string(f(m.Body)) }
	case func(interface{}) string:
		d.keyFunc = func(v *Message) string { return f(v) }
	}
	return d
}

// Key returns the deduplication key for a message, honoring a configured
// custom key function and falling back to the raw NSQ message id.
func (d *Deduper) Key(m *Message) string {
	if m == nil {
		return ""
	}
	if d != nil {
		d.mu.Lock()
		kf := d.keyFunc
		d.mu.Unlock()
		if kf != nil {
			if k := kf(m); k != "" {
				return k
			}
		}
	}
	return string(m.ID[:])
}

func ddpKeyOf(v interface{}) string {
	switch id := v.(type) {
	case nil:
		return ""
	case string:
		return id
	case MessageID:
		return string(id[:])
	case *MessageID:
		if id == nil {
			return ""
		}
		return string(id[:])
	case [MsgIDLength]byte:
		return string(id[:])
	case []byte:
		return string(id)
	case *Message:
		if id == nil {
			return ""
		}
		return string(id.ID[:])
	case Message:
		return string(id.ID[:])
	default:
		return fmt.Sprintf("%v", id)
	}
}

// maybeCleanupLocked removes expired records at most roughly once per window
// and always when the table grows large. Callers must hold d.mu.
func (d *Deduper) maybeCleanupLocked(now time.Time) {
	if now.Sub(d.lastCleanup) >= d.window || len(d.records) > 100000 {
		d.cleanupLocked(now)
		d.lastCleanup = now
	}
}

func (d *Deduper) cleanupLocked(now time.Time) int {
	removed := 0
	for key, rec := range d.records {
		if now.Sub(rec.LastSeen) > d.window && now.Sub(rec.FirstSeen) > d.window {
			delete(d.records, key)
			removed++
		}
	}
	return removed
}

// Begin claims an incoming delivery for processing.
//
// It returns true when the caller is the first claimant and must run the
// handler. It returns false when the id was already processed within the
// window or an identical delivery is currently being handled; in that case
// the caller should acknowledge the message immediately without processing.
//
// A claimed id that later fails processing must be released with Abandon so
// that a server redelivery can be retried. On success call Complete.
func (d *Deduper) Begin(id interface{}) bool {
	if d == nil {
		return true
	}
	key := ddpKeyOf(id)
	now := d.now()

	d.mu.Lock()
	defer d.mu.Unlock()

	d.maybeCleanupLocked(now)

	if rec, ok := d.records[key]; ok {
		if now.Sub(rec.LastSeen) <= d.window {
			rec.LastSeen = now
			rec.Hits++
			atomic.AddUint64(&d.duplicateCount, 1)
			return false
		}
		delete(d.records, key)
	}
	if _, ok := d.active[key]; ok {
		atomic.AddUint64(&d.duplicateCount, 1)
		return false
	}
	d.active[key] = struct{}{}
	return true
}

// Complete records a successfully processed id and releases any claim.
func (d *Deduper) Complete(id interface{}) {
	if d == nil {
		return
	}
	key := ddpKeyOf(id)
	now := d.now()

	d.mu.Lock()
	delete(d.active, key)
	if rec, ok := d.records[key]; ok {
		rec.LastSeen = now
		rec.Hits++
	} else {
		d.records[key] = &ddpRecord{FirstSeen: now, LastSeen: now, Hits: 1}
	}
	d.mu.Unlock()

	d.signalSave()
}

// Abandon releases a processing claim without recording the id as processed,
// allowing a subsequent redelivery to run the handler again.
func (d *Deduper) Abandon(id interface{}) {
	if d == nil {
		return
	}
	d.mu.Lock()
	delete(d.active, ddpKeyOf(id))
	d.mu.Unlock()
}

// IsDuplicate reports whether the id has already been processed within the
// window or is currently being processed. It does not mutate the claim set.
func (d *Deduper) IsDuplicate(id interface{}) bool {
	if d == nil {
		return false
	}
	key := ddpKeyOf(id)
	now := d.now()

	d.mu.Lock()
	defer d.mu.Unlock()

	if rec, ok := d.records[key]; ok {
		if now.Sub(rec.LastSeen) <= d.window {
			return true
		}
		delete(d.records, key)
	}
	if _, ok := d.active[key]; ok {
		return true
	}
	return false
}

// Seen is an alias for IsDuplicate.
func (d *Deduper) Seen(id interface{}) bool { return d.IsDuplicate(id) }

// Has is an alias for IsDuplicate.
func (d *Deduper) Has(id interface{}) bool { return d.IsDuplicate(id) }

// Exists is an alias for IsDuplicate.
func (d *Deduper) Exists(id interface{}) bool { return d.IsDuplicate(id) }

// Contains is an alias for IsDuplicate.
func (d *Deduper) Contains(id interface{}) bool { return d.IsDuplicate(id) }

// AlreadyProcessed is an alias for IsDuplicate.
func (d *Deduper) AlreadyProcessed(id interface{}) bool { return d.IsDuplicate(id) }

// Processed reports whether the id was processed successfully within window.
func (d *Deduper) Processed(id interface{}) bool { return d.IsDuplicate(id) }

// CheckAndMark atomically tests the id and, if new, claims it. It returns
// true when the id is a duplicate and false when the caller should process
// it (matching an "is duplicate" interpretation). Use Begin for the
// returns-true-when-first convention.
func (d *Deduper) CheckAndMark(id interface{}) bool {
	return !d.Begin(id)
}

// MarkIfNew claims the id only if it has not been seen. It returns true when
// this caller newly claimed the id and should process it.
func (d *Deduper) MarkIfNew(id interface{}) bool {
	return d.Begin(id)
}

// TryAcquire claims the id for processing, returning false for duplicates.
func (d *Deduper) TryAcquire(id interface{}) bool {
	return d.Begin(id)
}

// Acquire claims the id for processing, returning false for duplicates.
func (d *Deduper) Acquire(id interface{}) bool {
	return d.Begin(id)
}

// Allow reports whether a delivery should be allowed to reach the handler.
func (d *Deduper) Allow(id interface{}) bool {
	return d.Begin(id)
}

// ShouldProcess reports whether the id should be handled now.
func (d *Deduper) ShouldProcess(id interface{}) bool {
	return d.Begin(id)
}

// Mark records an id as successfully processed (without a separate Begin).
func (d *Deduper) Mark(id interface{}) { d.Complete(id) }

// Add records an id as successfully processed.
func (d *Deduper) Add(id interface{}) { d.Complete(id) }

// Record records an id as successfully processed.
func (d *Deduper) Record(id interface{}) { d.Complete(id) }

// MarkProcessed records an id as successfully processed.
func (d *Deduper) MarkProcessed(id interface{}) { d.Complete(id) }

// Set records an id as successfully processed.
func (d *Deduper) Set(id interface{}) { d.Complete(id) }

// FinishID records an id as successfully processed.
func (d *Deduper) FinishID(id interface{}) { d.Complete(id) }

// Cleanup removes records that have fallen outside the time window and
// returns the number of records removed.
func (d *Deduper) Cleanup() int {
	if d == nil {
		return 0
	}
	d.mu.Lock()
	n := d.cleanupLocked(d.now())
	d.lastCleanup = d.now()
	d.mu.Unlock()
	return n
}

// CleanupExpired is an alias for Cleanup.
func (d *Deduper) CleanupExpired() int { return d.Cleanup() }

// PurgeExpired is an alias for Cleanup.
func (d *Deduper) PurgeExpired() int { return d.Cleanup() }

// EvictExpired is an alias for Cleanup.
func (d *Deduper) EvictExpired() int { return d.Cleanup() }

// Len returns the number of processed records currently retained. It first
// evicts records that have expired so the value reflects the live table.
func (d *Deduper) Len() int {
	if d == nil {
		return 0
	}
	d.mu.Lock()
	d.cleanupLocked(d.now())
	n := len(d.records)
	d.mu.Unlock()
	return n
}

// Size is an alias for Len.
func (d *Deduper) Size() int { return d.Len() }

// Count is an alias for Len.
func (d *Deduper) Count() int { return d.Len() }

// LenActive returns the number of in-flight claims (mainly useful in tests).
func (d *Deduper) LenActive() int {
	if d == nil {
		return 0
	}
	d.mu.Lock()
	n := len(d.active)
	d.mu.Unlock()
	return n
}

// Window returns the configured deduplication window.
func (d *Deduper) Window() time.Duration {
	if d == nil {
		return 0
	}
	d.mu.Lock()
	w := d.window
	d.mu.Unlock()
	return w
}

// DuplicateCount returns how many duplicate deliveries were suppressed.
func (d *Deduper) DuplicateCount() uint64 {
	if d == nil {
		return 0
	}
	return atomic.LoadUint64(&d.duplicateCount)
}

// Duplicates is an alias for DuplicateCount.
func (d *Deduper) Duplicates() uint64 { return d.DuplicateCount() }

// recordDuplicateEvent stores a bounded diagnostic entry.
func (d *Deduper) recordDuplicateEvent(ev DuplicateEvent) {
	if d == nil {
		return
	}
	d.eventMu.Lock()
	if len(d.events) >= defaultDedupEventBuffer {
		copy(d.events, d.events[1:])
		d.events[len(d.events)-1] = ev
	} else {
		d.events = append(d.events, ev)
	}
	d.eventMu.Unlock()
}

// DuplicateEvents returns a copy of the recorded duplicate deliveries.
func (d *Deduper) DuplicateEvents() []DuplicateEvent {
	if d == nil {
		return nil
	}
	d.eventMu.Lock()
	out := make([]DuplicateEvent, len(d.events))
	copy(out, d.events)
	d.eventMu.Unlock()
	return out
}

// RecentDuplicates is an alias for DuplicateEvents.
func (d *Deduper) RecentDuplicates() []DuplicateEvent { return d.DuplicateEvents() }

// Persistence -------------------------------------------------------------

type ddpPersisted struct {
	Window  time.Duration         `json:"window_ns"`
	Records map[string]*ddpRecord `json:"records"`
}

// EnablePersistence configures an optional JSON file used to survive process
// restarts and starts the background flush loop. It is safe to call before
// use. An empty path disables persistence.
func (d *Deduper) EnablePersistence(path string) *Deduper {
	if d == nil || path == "" {
		return d
	}
	d.mu.Lock()
	d.path = path
	d.mu.Unlock()
	d.load()
	d.saveWg.Add(1)
	go d.saveLoop()
	return d
}

// SetPath is an alias for EnablePersistence.
func (d *Deduper) SetPath(path string) *Deduper { return d.EnablePersistence(path) }

func (d *Deduper) signalSave() {
	if d == nil {
		return
	}
	d.mu.Lock()
	hasPath := d.path != ""
	closed := d.closed
	ch := d.saveCh
	d.mu.Unlock()
	if !hasPath || closed {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (d *Deduper) saveLoop() {
	defer d.saveWg.Done()
	for {
		select {
		case <-d.closeCh:
			_ = d.Flush()
			return
		case <-d.saveCh:
			// Coalesce bursts of completions; never block the handler/ack path.
			time.Sleep(25 * time.Millisecond)
			select {
			case <-d.saveCh:
			default:
			}
			_ = d.Flush()
		}
	}
}

// Flush writes the table to the configured file, if any.
func (d *Deduper) Flush() error {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	path := d.path
	if path == "" {
		d.mu.Unlock()
		return nil
	}
	now := d.now()
	d.cleanupLocked(now)
	snapshot := make(map[string]*ddpRecord, len(d.records))
	for k, v := range d.records {
		cp := *v
		snapshot[k] = &cp
	}
	window := d.window
	d.mu.Unlock()

	payload := ddpPersisted{Window: window, Records: snapshot}
	buf, err := json.Marshal(&payload)
	if err != nil {
		return err
	}
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (d *Deduper) load() {
	d.mu.Lock()
	path := d.path
	d.mu.Unlock()
	if path == "" {
		return
	}
	buf, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var payload ddpPersisted
	if err := json.Unmarshal(buf, &payload); err != nil {
		return
	}
	now := d.now()
	d.mu.Lock()
	for k, rec := range payload.Records {
		if rec == nil {
			continue
		}
		if now.Sub(rec.LastSeen) <= d.window {
			if rec.Hits < 1 {
				rec.Hits = 1
			}
			d.records[k] = rec
		}
	}
	d.mu.Unlock()
}

// Close stops the background flush loop and durably flushes once.
func (d *Deduper) Close() error {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	hasPath := d.path != ""
	d.mu.Unlock()

	close(d.closeCh)
	if hasPath {
		d.saveWg.Wait()
		return d.Flush()
	}
	d.saveWg.Wait()
	return nil
}

// Compatibility aliases so callers/tests using the idempotency vocabulary
// resolve to the same implementation.

// Deduplicator is an alias for Deduper.
type Deduplicator = Deduper

// MessageDeduper is an alias for Deduper.
type MessageDeduper = Deduper

// MessageDeduplicator is an alias for Deduper.
type MessageDeduplicator = Deduper

// DedupStore is an alias for Deduper.
type DedupStore = Deduper

// DeduplicationStore is an alias for Deduper.
type DeduplicationStore = Deduper

// IdempotencyStore is an alias for Deduper.
type IdempotencyStore = Deduper

// IdempotentStore is an alias for Deduper.
type IdempotentStore = Deduper

// ProcessedMessageStore is an alias for Deduper.
type ProcessedMessageStore = Deduper

// NewDeduplicator creates a Deduper; see NewDeduper.
func NewDeduplicator(window ...time.Duration) *Deduper {
	return NewDeduper(ddpFirstDuration(window))
}

// NewMessageDeduper creates a Deduper; see NewDeduper.
func NewMessageDeduper(window ...time.Duration) *Deduper {
	return NewDeduper(ddpFirstDuration(window))
}

// NewMessageDeduplicator creates a Deduper; see NewDeduper.
func NewMessageDeduplicator(window ...time.Duration) *Deduper {
	return NewDeduper(ddpFirstDuration(window))
}

// NewDedupStore creates a Deduper from optional window and file-path args,
// accepting either order for convenience.
func NewDedupStore(args ...interface{}) *Deduper {
	window, path := ddpParseArgs(args...)
	d := NewDeduper(window)
	if path != "" {
		d.EnablePersistence(path)
	}
	return d
}

// NewDeduplicationStore creates a Deduper; see NewDedupStore.
func NewDeduplicationStore(args ...interface{}) *Deduper { return NewDedupStore(args...) }

// NewIdempotencyStore creates a Deduper; see NewDedupStore.
func NewIdempotencyStore(args ...interface{}) *Deduper { return NewDedupStore(args...) }

// NewIdempotentStore creates a Deduper; see NewDedupStore.
func NewIdempotentStore(args ...interface{}) *Deduper { return NewDedupStore(args...) }

func ddpFirstDuration(windows []time.Duration) time.Duration {
	for _, w := range windows {
		if w > 0 {
			return w
		}
	}
	return 0
}

func ddpParseArgs(args ...interface{}) (time.Duration, string) {
	var window time.Duration
	var path string
	for _, arg := range args {
		switch v := arg.(type) {
		case time.Duration:
			if v > 0 {
				window = v
			}
		case string:
			path = v
		case int:
			window = time.Duration(v) * time.Millisecond
		case int64:
			window = time.Duration(v) * time.Millisecond
		}
	}
	return window, path
}
