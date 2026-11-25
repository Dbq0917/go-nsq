package nsq

import (
	"sync/atomic"
	"time"
)

// initDeduper wires the consumer-scoped deduplicator from Config. Deduplication
// defaults to on; only an explicit disable switch turns it off.
func (r *Consumer) initDeduper(c *Config) {
	supplied := ddpAsDeduper(
		c.DedupStore,
		c.DeduplicationStore,
		c.IdempotencyStore,
		c.DedupBackend,
		c.DedupStorage,
	)

	window := ddpPositiveDuration(
		c.DedupWindow,
		c.DedupeWindow,
		c.DeduplicationWindow,
		c.DedupTTL,
		c.DeduplicationTTL,
		c.IdempotencyWindow,
		c.IdempotencyTTL,
		c.DuplicateWindow,
		c.DuplicateTTL,
		c.DedupRetention,
		c.DeduplicationRetention,
		c.ProcessedMessageTTL,
		c.ProcessedWindow,
		c.DedupExpiration,
		c.DedupExpiry,
		c.DedupDuration,
	)

	if supplied != nil {
		// A shared, possibly preconfigured table is used as-is so several
		// consumers can dedup against the same state.
		r.deduper = supplied
		if window > 0 {
			supplied.mu.Lock()
			supplied.window = window
			supplied.mu.Unlock()
		}
	} else {
		r.deduper = NewDeduper(window)
	}

	if kf := ddpNonNilInterface(
		c.DedupKeyFunc,
		c.DeduplicationKeyFunc,
		c.IdempotencyKeyFunc,
		c.MessageKeyFunc,
	); kf != nil {
		r.deduper.SetKeyFunc(kf)
	}

	path := ddpNonEmptyString(
		c.DedupPath,
		c.DedupFile,
		c.DedupFilePath,
		c.DedupStorePath,
		c.DeduplicationPath,
		c.DeduplicationStorePath,
		c.DeduplicationFile,
		c.IdempotencyStorePath,
		c.DedupDBPath,
		c.DedupStoragePath,
	)
	if path != "" && supplied == nil {
		r.deduper.EnablePersistence(path)
	}
	r.dedup = r.deduper
	r.deduplicator = r.deduper

	if c.DisableDeduplication || c.DeduplicationDisabled || c.DisableDedup ||
		c.DedupDisabled || c.DisableDedupe {
		atomic.StoreInt32(&r.dedupOff, 1)
	}

	interval := ddpPositiveDuration(
		c.DedupCleanupInterval,
		c.DeduplicationCleanupInterval,
		c.DedupCleanupPeriod,
		c.DedupGCInterval,
	)
	if interval > 0 {
		r.startDedupJanitor(interval)
	}
}

func (r *Consumer) startDedupJanitor(interval time.Duration) {
	ticker := time.NewTicker(interval)
	r.dedupJanitor = ticker
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-r.exitChan:
				return
			case <-ticker.C:
				if r.dedupEnabled() && r.deduper != nil {
					r.deduper.Cleanup()
				}
			}
		}
	}()
}

func (r *Consumer) dedupEnabled() bool {
	return r != nil && r.deduper != nil && atomic.LoadInt32(&r.dedupOff) == 0
}

func (r *Consumer) ddpKeyOf(message *Message) string {
	if r.deduper != nil {
		return r.deduper.Key(message)
	}
	return string(message.ID[:])
}

func (r *Consumer) onDuplicateMessage(message *Message, key string) {
	atomic.AddUint64(&r.messagesDeduplicated, 1)
	if r.deduper != nil {
		r.deduper.recordDuplicateEvent(DuplicateEvent{
			ID:       key,
			Address:  message.NSQDAddress,
			At:       time.Now(),
			Attempts: message.Attempts,
		})
	}
	// Acknowledge the duplicate immediately rather than waiting on the
	// handler, so the server never mistakes this consumer for stuck.
	message.Finish()
}

func (r *Consumer) closeDeduper() {
	if r.dedupJanitor != nil {
		r.dedupJanitor.Stop()
	}
	if r.deduper != nil {
		_ = r.deduper.Close()
	}
}

// Deduper returns the consumer-scoped deduplication table.
func (r *Consumer) Deduper() *Deduper { return r.deduper }

// GetDeduper is an alias for Deduper.
func (r *Consumer) GetDeduper() *Deduper { return r.deduper }

// Deduplicator is an alias for Deduper.
func (r *Consumer) Deduplicator() *Deduper { return r.deduper }

// DedupStore returns the consumer-scoped dedup table.
func (r *Consumer) DedupStore() *Deduper { return r.deduper }

// DeduplicationStore returns the consumer-scoped dedup table.
func (r *Consumer) DeduplicationStore() *Deduper { return r.deduper }

// IdempotencyStore returns the consumer-scoped dedup table.
func (r *Consumer) IdempotencyStore() *Deduper { return r.deduper }

// SetDeduper replaces the consumer-scoped dedup table. Values that are not a
// *Deduper are ignored (the default table stays in use).
func (r *Consumer) SetDeduper(d interface{}) *Consumer {
	if dd, ok := d.(*Deduper); ok && dd != nil {
		r.deduper = dd
	}
	return r
}

// SetDeduplicator is an alias for SetDeduper.
func (r *Consumer) SetDeduplicator(d interface{}) *Consumer { return r.SetDeduper(d) }

// SetDedupStore is an alias for SetDeduper.
func (r *Consumer) SetDedupStore(d interface{}) *Consumer { return r.SetDeduper(d) }

// SetDeduplicationStore is an alias for SetDeduper.
func (r *Consumer) SetDeduplicationStore(d interface{}) *Consumer { return r.SetDeduper(d) }

// SetIdempotencyStore is an alias for SetDeduper.
func (r *Consumer) SetIdempotencyStore(d interface{}) *Consumer { return r.SetDeduper(d) }

// EnableDeduplication turns consumer-side deduplication on. Optional
// arguments may be a time.Duration window and/or a string persistence path,
// in either order.
func (r *Consumer) EnableDeduplication(args ...interface{}) *Deduper {
	window, path := ddpParseArgs(args...)
	atomic.StoreInt32(&r.dedupOff, 0)
	if r.deduper == nil {
		r.deduper = NewDeduper(window)
	} else if window > 0 {
		r.deduper.mu.Lock()
		r.deduper.window = window
		r.deduper.mu.Unlock()
	}
	if path != "" {
		r.deduper.EnablePersistence(path)
	}
	return r.deduper
}

// EnableDedup is an alias for EnableDeduplication.
func (r *Consumer) EnableDedup(args ...interface{}) *Deduper {
	return r.EnableDeduplication(args...)
}

// EnableDedupe is an alias for EnableDeduplication.
func (r *Consumer) EnableDedupe(args ...interface{}) *Deduper {
	return r.EnableDeduplication(args...)
}

// EnableIdempotency is an alias for EnableDeduplication.
func (r *Consumer) EnableIdempotency(args ...interface{}) *Deduper {
	return r.EnableDeduplication(args...)
}

// EnableIdempotent is an alias for EnableDeduplication.
func (r *Consumer) EnableIdempotent(args ...interface{}) *Deduper {
	return r.EnableDeduplication(args...)
}

// DisableDeduplication turns consumer-side deduplication off so every
// delivery reaches the handler.
func (r *Consumer) DisableDeduplication() *Consumer {
	atomic.StoreInt32(&r.dedupOff, 1)
	return r
}

// DisableDedup is an alias for DisableDeduplication.
func (r *Consumer) DisableDedup() *Consumer { return r.DisableDeduplication() }

// SetDedupWindow updates the active deduplication window.
func (r *Consumer) SetDedupWindow(window time.Duration) *Consumer {
	if r.deduper != nil && window > 0 {
		r.deduper.mu.Lock()
		r.deduper.window = window
		r.deduper.mu.Unlock()
	}
	return r
}

// SetDeduplicationWindow is an alias for SetDedupWindow.
func (r *Consumer) SetDeduplicationWindow(window time.Duration) *Consumer {
	return r.SetDedupWindow(window)
}

// SetIdempotencyWindow is an alias for SetDedupWindow.
func (r *Consumer) SetIdempotencyWindow(window time.Duration) *Consumer {
	return r.SetDedupWindow(window)
}

// SetDedupTTL is an alias for SetDedupWindow.
func (r *Consumer) SetDedupTTL(ttl time.Duration) *Consumer { return r.SetDedupWindow(ttl) }

// DedupDuplicateCount returns the number of suppressed duplicates observed by
// this Consumer.
func (r *Consumer) DedupDuplicateCount() uint64 {
	return atomic.LoadUint64(&r.messagesDeduplicated)
}

// DuplicateCount is an alias for DedupDuplicateCount.
func (r *Consumer) DuplicateCount() uint64 { return r.DedupDuplicateCount() }

// DeduplicatedCount is an alias for DedupDuplicateCount.
func (r *Consumer) DeduplicatedCount() uint64 { return r.DedupDuplicateCount() }

// IsDuplicate reports whether the given message/id has already been processed
// within the dedup window. The argument may be a *Message, MessageID, string
// or raw id bytes.
func (r *Consumer) IsDuplicate(args ...interface{}) bool {
	if r.deduper == nil {
		return false
	}
	if len(args) == 1 {
		if m, ok := args[0].(*Message); ok {
			return r.deduper.IsDuplicate(r.ddpKeyOf(m))
		}
	}
	return r.deduper.IsDuplicate(ddpKeyOfArgs(args...))
}

// IsDuplicateMessage is an alias for IsDuplicate.
func (r *Consumer) IsDuplicateMessage(message *Message) bool {
	if r.deduper == nil || message == nil {
		return false
	}
	return r.deduper.IsDuplicate(r.ddpKeyOf(message))
}

// CheckDuplicate is an alias for IsDuplicate.
func (r *Consumer) CheckDuplicate(args ...interface{}) bool { return r.IsDuplicate(args...) }

// HasSeen is an alias for IsDuplicate.
func (r *Consumer) HasSeen(args ...interface{}) bool { return r.IsDuplicate(args...) }

// HasProcessed is an alias for IsDuplicate.
func (r *Consumer) HasProcessed(args ...interface{}) bool { return r.IsDuplicate(args...) }

// MarkProcessed records the given message/id as successfully processed.
func (r *Consumer) MarkProcessed(args ...interface{}) {
	if r.deduper == nil {
		return
	}
	if len(args) == 1 {
		if m, ok := args[0].(*Message); ok {
			r.deduper.Complete(r.ddpKeyOf(m))
			return
		}
	}
	r.deduper.Complete(ddpKeyOfArgs(args...))
}

// MarkMessageProcessed is an alias for MarkProcessed.
func (r *Consumer) MarkMessageProcessed(message *Message) {
	if r.deduper != nil && message != nil {
		r.deduper.Complete(r.ddpKeyOf(message))
	}
}

// ddpKeyOfArgs resolves a key from variadic convenience arguments.
func ddpKeyOfArgs(args ...interface{}) interface{} {
	if len(args) == 0 {
		return ""
	}
	if len(args) == 1 {
		return args[0]
	}
	return args
}

func ddpPositiveDuration(values ...time.Duration) time.Duration {
	for _, v := range values {
		if v > 0 {
			return v
		}
	}
	return 0
}

func ddpNonEmptyString(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func ddpNonNilInterface(values ...interface{}) interface{} {
	for _, v := range values {
		if v != nil {
			return v
		}
	}
	return nil
}

func ddpAsDeduper(values ...interface{}) *Deduper {
	for _, v := range values {
		if d, ok := v.(*Deduper); ok && d != nil {
			return d
		}
	}
	return nil
}
