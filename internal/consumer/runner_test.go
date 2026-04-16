package consumer

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"event-driven-systems-tradeoffs-demo/internal/domain"
	"event-driven-systems-tradeoffs-demo/internal/stats"

	"github.com/segmentio/kafka-go"
)

type fakeReader struct {
	mu        sync.Mutex
	msgs      []kafka.Message
	committed map[string]bool
}

func newFakeReader(msgs []kafka.Message) *fakeReader {
	return &fakeReader{msgs: msgs, committed: map[string]bool{}}
}

func msgID(msg kafka.Message) string { return string(msg.Value) + "|" + msg.Topic }

func (r *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	for {
		r.mu.Lock()
		for _, msg := range r.msgs {
			if !r.committed[msgID(msg)] {
				r.mu.Unlock()
				return msg, nil
			}
		}
		r.mu.Unlock()
		if ctx.Err() != nil {
			return kafka.Message{}, ctx.Err()
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (r *fakeReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, msg := range msgs {
		r.committed[msgID(msg)] = true
	}
	return nil
}

func (r *fakeReader) appendMessage(msg kafka.Message) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs = append(r.msgs, msg)
}

func (r *fakeReader) committedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.committed)
}

type fakeRetryWriter struct {
	fail bool
	onOK func(kafka.Message)
}

func (w *fakeRetryWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	if w.fail {
		return context.DeadlineExceeded
	}
	if w.onOK != nil {
		for _, msg := range msgs {
			w.onOK(msg)
		}
	}
	return nil
}

type fakeSink struct {
	mu          sync.Mutex
	written     []domain.Event
	writeCalls  int
	successHook func()
}

func (s *fakeSink) WriteBatch(_ context.Context, batch []domain.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writeCalls++
	for _, ev := range batch {
		if ev.FailureMode == "permanent" {
			return context.DeadlineExceeded
		}
		if ev.FailureMode == "transient" && ev.RetryCount == 0 {
			return context.DeadlineExceeded
		}
	}
	s.written = append(s.written, batch...)
	if s.successHook != nil {
		s.successHook()
	}
	return nil
}

func mustMessage(t *testing.T, event domain.Event, topic string) kafka.Message {
	t.Helper()
	b, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return kafka.Message{Topic: topic, Key: []byte(event.DeviceID), Value: b}
}

func runWithTimeout(t *testing.T, fn func(ctx context.Context)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()
	fn(ctx)
}

func TestRunTransientRetrySuccess(t *testing.T) {
	runWithTimeout(t, func(ctx context.Context) {
		event := domain.Event{EventID: "e-transient", DeviceID: "d1", Timestamp: 1, EventType: "t", Payload: "p", FailureMode: "transient"}
		reader := newFakeReader([]kafka.Message{mustMessage(t, event, "events")})
		retryWriter := &fakeRetryWriter{
			onOK: func(msg kafka.Message) {
				reader.appendMessage(msg)
			},
		}
		sinker := &fakeSink{}
		counters := &stats.Counters{}
		debug := &DebugState{}
		ledger, err := NewLedger("off", filepath.Join(t.TempDir(), "ledger.jsonl"))
		if err != nil {
			t.Fatalf("ledger: %v", err)
		}
		dlq := NewDLQWriter(filepath.Join(t.TempDir(), "dlq.jsonl"))

		go func() {
			_ = Run(ctx, reader, retryWriter, dlq, ledger, 1, time.Hour, sinker, counters, debug, RunOptions{MaxRetries: 2, RetryTopic: "events.retry"})
		}()
		time.Sleep(200 * time.Millisecond)

		snap := debug.Snapshot()
		if snap.ConsumerRetriesAttempted == 0 {
			t.Fatalf("expected retries attempted > 0, got %d", snap.ConsumerRetriesAttempted)
		}
		if snap.ConsumerRetriesPublished == 0 {
			t.Fatalf("expected retries published > 0, got %d", snap.ConsumerRetriesPublished)
		}
		if snap.ConsumerRetryPublishFailures != 0 {
			t.Fatalf("expected retry publish failures = 0, got %d", snap.ConsumerRetryPublishFailures)
		}
		if snap.ConsumerFlushedBatches == 0 {
			t.Fatalf("expected flushed batches > 0 after transient retry success")
		}
		if snap.ConsumerDLQWrites != 0 {
			t.Fatalf("expected no dlq writes, got %d", snap.ConsumerDLQWrites)
		}
		if len(sinker.written) == 0 {
			t.Fatalf("expected eventual sink success")
		}
	})
}

func TestRunRetryPublishFailureDoesNotSilentlyDropBatch(t *testing.T) {
	runWithTimeout(t, func(ctx context.Context) {
		event := domain.Event{EventID: "e-retry-fail", DeviceID: "d1", Timestamp: 1, EventType: "t", Payload: "p", FailureMode: "transient"}
		reader := newFakeReader([]kafka.Message{mustMessage(t, event, "events")})
		retryWriter := &fakeRetryWriter{fail: true}
		sinker := &fakeSink{}
		counters := &stats.Counters{}
		debug := &DebugState{}
		ledger, _ := NewLedger("off", filepath.Join(t.TempDir(), "ledger.jsonl"))
		dlq := NewDLQWriter(filepath.Join(t.TempDir(), "dlq.jsonl"))

		_ = Run(ctx, reader, retryWriter, dlq, ledger, 1, time.Hour, sinker, counters, debug, RunOptions{MaxRetries: 2, RetryTopic: "events.retry"})

		if reader.committedCount() != 0 {
			t.Fatalf("expected no committed messages when retry publish fails, got %d", reader.committedCount())
		}
		if sinker.writeCalls == 0 {
			t.Fatalf("expected sink write attempts")
		}
		if debug.Snapshot().ConsumerCurrentBufferedEvents == 0 {
			t.Fatalf("expected buffered events to remain recoverable when retry publish fails")
		}
		snap := debug.Snapshot()
		if snap.ConsumerRetriesAttempted == 0 {
			t.Fatalf("expected retry attempts > 0 when publish fails")
		}
		if snap.ConsumerRetriesPublished != 0 {
			t.Fatalf("expected retry published = 0 on publish failure, got %d", snap.ConsumerRetriesPublished)
		}
		if snap.ConsumerRetryPublishFailures == 0 {
			t.Fatalf("expected retry publish failures > 0")
		}
		if snap.ConsumerFlushedBatches != 0 {
			t.Fatalf("failed flush must not count as successful flush")
		}
	})
}

func TestRunIdempotencyAfterFailedFirstAttempt(t *testing.T) {
	runWithTimeout(t, func(ctx context.Context) {
		event := domain.Event{EventID: "e-idem", DeviceID: "d1", Timestamp: 1, EventType: "t", Payload: "p", FailureMode: "transient"}
		reader := newFakeReader([]kafka.Message{mustMessage(t, event, "events")})
		retryWriter := &fakeRetryWriter{onOK: func(msg kafka.Message) { reader.appendMessage(msg) }}
		sinker := &fakeSink{}
		counters := &stats.Counters{}
		debug := &DebugState{}
		ledgerPath := filepath.Join(t.TempDir(), "ledger.jsonl")
		ledger, err := NewLedger("on", ledgerPath)
		if err != nil {
			t.Fatalf("ledger: %v", err)
		}
		dlq := NewDLQWriter(filepath.Join(t.TempDir(), "dlq.jsonl"))

		_ = Run(ctx, reader, retryWriter, dlq, ledger, 1, time.Hour, sinker, counters, debug, RunOptions{MaxRetries: 2, RetryTopic: "events.retry"})

		if len(sinker.written) != 1 {
			t.Fatalf("expected exactly one successful sink write with idempotency, got %d", len(sinker.written))
		}
		snap := debug.Snapshot()
		if snap.ConsumerDuplicatesSkipped != 0 {
			t.Fatalf("did not expect duplicate skip on failed-first-attempt path, got %d", snap.ConsumerDuplicatesSkipped)
		}
		if snap.ConsumerRetriesPublished == 0 {
			t.Fatalf("expected retry publish success in failed-first-attempt path")
		}
	})
}

func TestRunIdempotencyOffAllowsDuplicateWrites(t *testing.T) {
	runWithTimeout(t, func(ctx context.Context) {
		event1 := domain.Event{EventID: "dup-1", DeviceID: "d1", Timestamp: 1, EventType: "t", Payload: "p1"}
		event2 := domain.Event{EventID: "dup-1", DeviceID: "d1", Timestamp: 2, EventType: "t", Payload: "p2"}
		reader := newFakeReader([]kafka.Message{
			mustMessage(t, event1, "events"),
			mustMessage(t, event2, "events"),
		})
		retryWriter := &fakeRetryWriter{}
		sinker := &fakeSink{}
		counters := &stats.Counters{}
		debug := &DebugState{}
		ledger, _ := NewLedger("off", filepath.Join(t.TempDir(), "ledger.jsonl"))
		dlq := NewDLQWriter(filepath.Join(t.TempDir(), "dlq.jsonl"))

		_ = Run(ctx, reader, retryWriter, dlq, ledger, 10, 20*time.Millisecond, sinker, counters, debug, RunOptions{MaxRetries: 2, RetryTopic: "events.retry"})

		if len(sinker.written) < 2 {
			t.Fatalf("expected duplicate writes with idempotency off, got %d", len(sinker.written))
		}
		if debug.Snapshot().ConsumerDuplicatesSkipped != 0 {
			t.Fatalf("expected no skipped duplicates with idempotency off")
		}
	})
}

func TestRunIdempotencyOnSkipsDuplicateWritesAndCounts(t *testing.T) {
	runWithTimeout(t, func(ctx context.Context) {
		event1 := domain.Event{EventID: "dup-1", DeviceID: "d1", Timestamp: 1, EventType: "t", Payload: "p1"}
		event2 := domain.Event{EventID: "dup-1", DeviceID: "d1", Timestamp: 2, EventType: "t", Payload: "p2"}
		reader := newFakeReader([]kafka.Message{
			mustMessage(t, event1, "events"),
			mustMessage(t, event2, "events"),
		})
		retryWriter := &fakeRetryWriter{}
		sinker := &fakeSink{}
		counters := &stats.Counters{}
		debug := &DebugState{}
		ledger, _ := NewLedger("on", filepath.Join(t.TempDir(), "ledger.jsonl"))
		dlq := NewDLQWriter(filepath.Join(t.TempDir(), "dlq.jsonl"))

		_ = Run(ctx, reader, retryWriter, dlq, ledger, 10, 20*time.Millisecond, sinker, counters, debug, RunOptions{MaxRetries: 2, RetryTopic: "events.retry"})

		if len(sinker.written) >= 2 {
			t.Fatalf("expected cleaner sink output with idempotency on, got %d writes", len(sinker.written))
		}
		snap := debug.Snapshot()
		if snap.ConsumerDuplicatesDetected == 0 {
			t.Fatalf("expected duplicates detected counter > 0")
		}
		if snap.ConsumerDuplicatesSkipped == 0 {
			t.Fatalf("expected duplicates skipped counter > 0")
		}
	})
}

func TestRunPermanentFailureGoesToDLQ(t *testing.T) {
	runWithTimeout(t, func(ctx context.Context) {
		event := domain.Event{EventID: "e-perm", DeviceID: "d1", Timestamp: 1, EventType: "t", Payload: "p", FailureMode: "permanent"}
		reader := newFakeReader([]kafka.Message{mustMessage(t, event, "events")})
		retryWriter := &fakeRetryWriter{
			onOK: func(msg kafka.Message) {
				reader.appendMessage(msg)
			},
		}
		sinker := &fakeSink{}
		counters := &stats.Counters{}
		debug := &DebugState{}
		ledger, _ := NewLedger("off", filepath.Join(t.TempDir(), "ledger.jsonl"))
		dlqPath := filepath.Join(t.TempDir(), "dlq.jsonl")
		dlq := NewDLQWriter(dlqPath)

		_ = Run(ctx, reader, retryWriter, dlq, ledger, 1, time.Hour, sinker, counters, debug, RunOptions{MaxRetries: 2, RetryTopic: "events.retry"})

		snap := debug.Snapshot()
		if snap.ConsumerDLQWrites == 0 {
			t.Fatalf("expected dlq writes > 0")
		}
		if snap.ConsumerRetriesAttempted == 0 {
			t.Fatalf("expected retries to be attempted before dlq")
		}
		if len(sinker.written) != 0 {
			t.Fatalf("expected no successful sink write for permanent failure")
		}
		data, err := os.ReadFile(dlqPath)
		if err != nil {
			t.Fatalf("read dlq file: %v", err)
		}
		if len(data) == 0 {
			t.Fatalf("expected dlq file to contain records")
		}
		var record DLQRecord
		if err := json.Unmarshal(data, &record); err != nil {
			t.Fatalf("unmarshal dlq record: %v", err)
		}
		if record.Event.RetryCount != 2 {
			t.Fatalf("expected dlq event retry_count=2, got %d", record.Event.RetryCount)
		}
	})
}
