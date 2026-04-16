package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"event-driven-systems-tradeoffs-demo/internal/batch"
	"event-driven-systems-tradeoffs-demo/internal/domain"
	"event-driven-systems-tradeoffs-demo/internal/sink"
	"event-driven-systems-tradeoffs-demo/internal/stats"

	"github.com/segmentio/kafka-go"
)

type RunOptions struct {
	MaxRetries int
	MainTopic  string
	RetryTopic string
}

type messageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

type messageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

// Run consumes events from Kafka, batches them, writes to sink, and commits only on successful sink writes.
func Run(
	ctx context.Context,
	reader messageReader,
	retryWriter messageWriter,
	dlqWriter *DLQWriter,
	ledger *Ledger,
	batchSize int,
	flushInterval time.Duration,
	sinker sink.Sink,
	counters *stats.Counters,
	debug *DebugState,
	opts RunOptions,
) error {
	acc := batch.NewAccumulator(batchSize)
	ticker := batch.NewTicker(flushInterval)
	defer ticker.Stop()

	pendingMsgs := make([]kafka.Message, 0, batchSize)

	flush := func(flushCtx context.Context, reason string) error {
		chunk := acc.Snapshot()
		debug.SetBuffered(0)
		if len(chunk) == 0 {
			return nil
		}
		if len(pendingMsgs) < len(chunk) {
			debug.SetBuffered(acc.Len())
			return fmt.Errorf("pending message mismatch: pending=%d chunk=%d", len(pendingMsgs), len(chunk))
		}
		chunkMsgs := append([]kafka.Message(nil), pendingMsgs[:len(chunk)]...)
		finalize := func() {
			acc.Drop(len(chunk))
			pendingMsgs = pendingMsgs[len(chunk):]
			debug.SetBuffered(acc.Len())
		}

		filtered := make([]domain.Event, 0, len(chunk))
		seenInFlush := make(map[string]struct{}, len(chunk))
		for _, event := range chunk {
			if ledger.Enabled() {
				if event.EventID != "" {
					if _, ok := seenInFlush[event.EventID]; ok {
						log.Printf("[DUPLICATE DETECTED] event_id=%s source_topic=%s", event.EventID, event.SourceTopic)
						log.Printf("[DUPLICATE SKIPPED] event_id=%s idempotency_mode=%s", event.EventID, map[bool]string{true: "on", false: "off"}[ledger.Enabled()])
						debug.RecordDuplicateSkipped(event.EventID)
						continue
					}
				}
				if ledger.Seen(event.EventID) {
					log.Printf("[DUPLICATE DETECTED] event_id=%s source_topic=%s", event.EventID, event.SourceTopic)
					log.Printf("[DUPLICATE SKIPPED] event_id=%s idempotency_mode=%s", event.EventID, map[bool]string{true: "on", false: "off"}[ledger.Enabled()])
					debug.RecordDuplicateSkipped(event.EventID)
					continue
				}
			}
			filtered = append(filtered, event)
			if ledger.Enabled() && event.EventID != "" {
				seenInFlush[event.EventID] = struct{}{}
			}
		}

		if len(filtered) == 0 {
			if len(chunkMsgs) > 0 {
				if err := reader.CommitMessages(flushCtx, chunkMsgs...); err != nil {
					debug.SetBuffered(acc.Len())
					return fmt.Errorf("commit duplicate-only batch: %w", err)
				}
			}
			finalize()
			return nil
		}

		if err := sinker.WriteBatch(flushCtx, filtered); err != nil {
			counters.IncFailures()
			debug.IncSinkWriteFailure(fmt.Sprintf("%s: %v", reason, err))

			for _, event := range filtered {
				if event.RetryCount >= opts.MaxRetries {
					log.Printf("[DLQ ROUTE] event_id=%s reason=retry-exhausted retry_count=%d", event.EventID, event.RetryCount)
					debug.RecordRetryExhausted()
					if dlqErr := dlqWriter.Write(flushCtx, event, err.Error(), "retry-exhausted"); dlqErr != nil {
						return fmt.Errorf("write dlq: %w", dlqErr)
					}
					debug.RecordDLQWrite()
					continue
				}
				event.RetryCount++
				event.SourceTopic = opts.RetryTopic
				log.Printf("[RETRY ROUTE] event_id=%s retry_count=%d topic=%s", event.EventID, event.RetryCount, opts.RetryTopic)
				debug.RecordRetry()
				if retryErr := publishRetry(flushCtx, retryWriter, opts.RetryTopic, event); retryErr != nil {
					log.Printf("[RETRY PUBLISH FAILED] event_id=%s topic=%s error=%v", event.EventID, opts.RetryTopic, retryErr)
					debug.RecordRetryPublishFailure()
					debug.SetBuffered(acc.Len())
					return fmt.Errorf("publish retry: %w", retryErr)
				}
				log.Printf("[RETRY PUBLISH OK] event_id=%s topic=%s", event.EventID, opts.RetryTopic)
				debug.RecordRetryPublished()
			}
			if len(chunkMsgs) > 0 {
				if err := reader.CommitMessages(flushCtx, chunkMsgs...); err != nil {
					debug.SetBuffered(acc.Len())
					return fmt.Errorf("commit failed batch messages: %w", err)
				}
			}
			finalize()
			return nil
		}

		for _, event := range filtered {
			if err := ledger.Record(event.EventID); err != nil {
				debug.SetBuffered(acc.Len())
				return fmt.Errorf("idempotency ledger record: %w", err)
			}
		}

		debug.RecordSinkSuccess(len(filtered))
		if len(chunkMsgs) > 0 {
			if err := reader.CommitMessages(flushCtx, chunkMsgs...); err != nil {
				counters.IncFailures()
				debug.IncSinkWriteFailure(fmt.Sprintf("commit %s: %v", reason, err))
				debug.SetBuffered(acc.Len())
				return fmt.Errorf("commit batch (%s): %w", reason, err)
			}
		}
		finalize()

		counters.IncFlushed()
		debug.RecordFlush(reason, filtered)
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			// On shutdown, try one last flush with a fresh short-lived context.
			finalCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := flush(finalCtx, "shutdown-final-attempt"); err != nil {
				log.Printf("final shutdown flush failed: %v", err)
			}
			return nil
		case <-ticker.C:
			if err := flush(ctx, "time-threshold"); err != nil {
				log.Printf("flush on interval failed: %v", err)
			}
		default:
			fetchCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
			msg, err := reader.FetchMessage(fetchCtx)
			cancel()
			if err != nil {
				if fetchCtx.Err() != nil || ctx.Err() != nil {
					continue
				}
				return fmt.Errorf("fetch message: %w", err)
			}

			var event domain.Event
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				counters.IncFailures()
				if err := reader.CommitMessages(ctx, msg); err != nil {
					return fmt.Errorf("commit poison message: %w", err)
				}
				continue
			}
			if event.SourceTopic == "" {
				event.SourceTopic = msg.Topic
			}

			counters.IncConsumed(1)
			debug.AddConsumed(event)
			pendingMsgs = append(pendingMsgs, msg)
			if shouldFlush := acc.Add(event); shouldFlush {
				debug.SetBuffered(acc.Len())
				if err := flush(ctx, "size-threshold"); err != nil {
					log.Printf("flush on size failed: %v", err)
				}
			} else {
				debug.SetBuffered(acc.Len())
			}
		}
	}
}

func publishRetry(ctx context.Context, writer messageWriter, topic string, event domain.Event) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal retry event: %w", err)
	}
	msg := kafka.Message{
		Key:   []byte(event.DeviceID),
		Value: payload,
		Time:  time.Now().UTC(),
	}
	return writer.WriteMessages(ctx, msg)
}
