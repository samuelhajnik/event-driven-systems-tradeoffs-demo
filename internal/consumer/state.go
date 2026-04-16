package consumer

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"event-driven-systems-tradeoffs-demo/internal/domain"
)

// DebugState tracks consumer-local state for demo visibility.
type DebugState struct {
	consumedEvents         uint64
	flushedBatches         uint64
	sinkWriteFailures      uint64
	sinkWriteSuccessEvents uint64
	retriesAttempted       uint64
	retriesPublished       uint64
	retryPublishFailures   uint64
	retriesExhausted       uint64
	dlqWrites              uint64
	duplicatesDetected     uint64
	duplicatesSkipped      uint64
	currentBufferedEvents  int64

	mu                     sync.RWMutex
	recentBatchSizes       []int
	recentFlushTimestamps  []string
	recentDeviceCounts     map[string]uint64
	lastFlushReason        string
	lastFailureReason      string
	lastFlushedBatchSample []EventSample
	idempotencyMode        string
}

// EventSample keeps debug output concise while still useful.
type EventSample struct {
	EventID  string `json:"event_id"`
	DeviceID string `json:"device_id"`
}

type Snapshot struct {
	ConsumerConsumedEvents          uint64            `json:"consumer_consumed_events"`
	ConsumerFlushedBatches          uint64            `json:"consumer_flushed_batches"`
	ConsumerSinkWriteFailures       uint64            `json:"consumer_sink_write_failures"`
	ConsumerSinkWriteSuccessEvents  uint64            `json:"consumer_sink_write_success_events"`
	ConsumerRetriesAttempted        uint64            `json:"consumer_retries_attempted"`
	ConsumerRetriesPublished        uint64            `json:"consumer_retries_published"`
	ConsumerRetryPublishFailures    uint64            `json:"consumer_retry_publish_failures"`
	ConsumerRetriesExhausted        uint64            `json:"consumer_retries_exhausted"`
	ConsumerDLQWrites               uint64            `json:"consumer_dlq_writes"`
	ConsumerDuplicatesDetected      uint64            `json:"consumer_duplicates_detected"`
	ConsumerDuplicatesSkipped       uint64            `json:"consumer_duplicates_skipped"`
	ConsumerIdempotencyMode         string            `json:"consumer_idempotency_mode"`
	ConsumerCurrentBufferedEvents   int64             `json:"consumer_current_buffered_events"`
	ConsumerRecentBatchSizes        []int             `json:"consumer_recent_batch_sizes"`
	ConsumerRecentFlushTimestamps   []string          `json:"consumer_recent_flush_timestamps"`
	ConsumerRecentDeviceCounts      map[string]uint64 `json:"consumer_recent_device_counts"`
	ConsumerLastFlushReason         string            `json:"consumer_last_flush_reason"`
	ConsumerLastFailureReason       string            `json:"consumer_last_failure_reason,omitempty"`
	ConsumerLastFlushedBatchSample  []EventSample     `json:"consumer_last_flushed_batch_sample"`
	ConsumerDuplicateRiskNote       string            `json:"consumer_duplicate_risk_note,omitempty"`
}

func (d *DebugState) SetIdempotencyMode(mode string) {
	d.mu.Lock()
	d.idempotencyMode = mode
	d.mu.Unlock()
}

func (d *DebugState) AddConsumed(event domain.Event) {
	atomic.AddUint64(&d.consumedEvents, 1)
	d.mu.Lock()
	if d.recentDeviceCounts == nil {
		d.recentDeviceCounts = make(map[string]uint64)
	}
	d.recentDeviceCounts[event.DeviceID]++
	if len(d.recentDeviceCounts) > 10 {
		// Keep map bounded for debug readability: retain top 10 keys by count.
		type kv struct {
			key   string
			count uint64
		}
		items := make([]kv, 0, len(d.recentDeviceCounts))
		for k, v := range d.recentDeviceCounts {
			items = append(items, kv{key: k, count: v})
		}
		sort.Slice(items, func(i, j int) bool { return items[i].count > items[j].count })
		trimmed := make(map[string]uint64, 10)
		for i := 0; i < 10 && i < len(items); i++ {
			trimmed[items[i].key] = items[i].count
		}
		d.recentDeviceCounts = trimmed
	}
	d.mu.Unlock()
}

func (d *DebugState) IncSinkWriteFailure(reason string) {
	atomic.AddUint64(&d.sinkWriteFailures, 1)
	d.mu.Lock()
	d.lastFailureReason = reason
	d.mu.Unlock()
}

func (d *DebugState) RecordSinkSuccess(n int) {
	if n > 0 {
		atomic.AddUint64(&d.sinkWriteSuccessEvents, uint64(n))
	}
}

func (d *DebugState) RecordRetry() {
	atomic.AddUint64(&d.retriesAttempted, 1)
}

func (d *DebugState) RecordRetryPublished() {
	atomic.AddUint64(&d.retriesPublished, 1)
}

func (d *DebugState) RecordRetryPublishFailure() {
	atomic.AddUint64(&d.retryPublishFailures, 1)
}

func (d *DebugState) RecordRetryExhausted() {
	atomic.AddUint64(&d.retriesExhausted, 1)
}

func (d *DebugState) RecordDLQWrite() {
	atomic.AddUint64(&d.dlqWrites, 1)
}

func (d *DebugState) RecordDuplicateSkipped(_ string) {
	atomic.AddUint64(&d.duplicatesDetected, 1)
	atomic.AddUint64(&d.duplicatesSkipped, 1)
}

func (d *DebugState) SetBuffered(n int) {
	atomic.StoreInt64(&d.currentBufferedEvents, int64(n))
}

func (d *DebugState) RecordFlush(reason string, batch []domain.Event) {
	d.mu.Lock()
	defer d.mu.Unlock()

	atomic.AddUint64(&d.flushedBatches, 1)
	d.lastFlushReason = reason

	d.recentBatchSizes = append(d.recentBatchSizes, len(batch))
	if len(d.recentBatchSizes) > 5 {
		d.recentBatchSizes = d.recentBatchSizes[len(d.recentBatchSizes)-5:]
	}

	d.recentFlushTimestamps = append(d.recentFlushTimestamps, time.Now().UTC().Format(time.RFC3339))
	if len(d.recentFlushTimestamps) > 5 {
		d.recentFlushTimestamps = d.recentFlushTimestamps[len(d.recentFlushTimestamps)-5:]
	}

	sampleSize := 3
	if len(batch) < sampleSize {
		sampleSize = len(batch)
	}
	sample := make([]EventSample, 0, sampleSize)
	for i := 0; i < sampleSize; i++ {
		sample = append(sample, EventSample{EventID: batch[i].EventID, DeviceID: batch[i].DeviceID})
	}
	d.lastFlushedBatchSample = sample
}

func (d *DebugState) Snapshot() Snapshot {
	d.mu.RLock()
	recentSizes := append([]int(nil), d.recentBatchSizes...)
	recentFlushes := append([]string(nil), d.recentFlushTimestamps...)
	sample := append([]EventSample(nil), d.lastFlushedBatchSample...)
	lastFlush := d.lastFlushReason
	lastFailure := d.lastFailureReason
	idempotencyMode := d.idempotencyMode
	deviceCounts := make(map[string]uint64, len(d.recentDeviceCounts))
	for k, v := range d.recentDeviceCounts {
		deviceCounts[k] = v
	}
	d.mu.RUnlock()

	sinkFailures := atomic.LoadUint64(&d.sinkWriteFailures)
	duplicateRiskNote := ""
	if sinkFailures > 0 {
		duplicateRiskNote = fmt.Sprintf("sink failures seen (%d); retries can cause duplicates with non-idempotent sink", sinkFailures)
	}

	return Snapshot{
		ConsumerConsumedEvents:         atomic.LoadUint64(&d.consumedEvents),
		ConsumerFlushedBatches:         atomic.LoadUint64(&d.flushedBatches),
		ConsumerSinkWriteFailures:      sinkFailures,
		ConsumerSinkWriteSuccessEvents: atomic.LoadUint64(&d.sinkWriteSuccessEvents),
		ConsumerRetriesAttempted:       atomic.LoadUint64(&d.retriesAttempted),
		ConsumerRetriesPublished:       atomic.LoadUint64(&d.retriesPublished),
		ConsumerRetryPublishFailures:   atomic.LoadUint64(&d.retryPublishFailures),
		ConsumerRetriesExhausted:       atomic.LoadUint64(&d.retriesExhausted),
		ConsumerDLQWrites:              atomic.LoadUint64(&d.dlqWrites),
		ConsumerDuplicatesDetected:     atomic.LoadUint64(&d.duplicatesDetected),
		ConsumerDuplicatesSkipped:      atomic.LoadUint64(&d.duplicatesSkipped),
		ConsumerIdempotencyMode:        idempotencyMode,
		ConsumerCurrentBufferedEvents:  atomic.LoadInt64(&d.currentBufferedEvents),
		ConsumerRecentBatchSizes:       recentSizes,
		ConsumerRecentFlushTimestamps:  recentFlushes,
		ConsumerRecentDeviceCounts:     deviceCounts,
		ConsumerLastFlushReason:        lastFlush,
		ConsumerLastFailureReason:      lastFailure,
		ConsumerLastFlushedBatchSample: sample,
		ConsumerDuplicateRiskNote:      duplicateRiskNote,
	}
}
