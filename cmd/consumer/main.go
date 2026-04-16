package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"event-driven-systems-tradeoffs-demo/internal/config"
	"event-driven-systems-tradeoffs-demo/internal/consumer"
	"event-driven-systems-tradeoffs-demo/internal/sink"
	"event-driven-systems-tradeoffs-demo/internal/stats"

	"github.com/segmentio/kafka-go"
)

func main() {
	cfg := config.Load()
	counters := &stats.Counters{}
	debug := &consumer.DebugState{}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.KafkaBrokers,
		GroupTopics: []string{cfg.KafkaTopic, cfg.KafkaRetryTopic},
		GroupID:     cfg.KafkaGroupID,
		MinBytes:    1,
		MaxBytes:    10e6,
	})
	defer reader.Close()
	retryWriter := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.KafkaBrokers...),
		Topic:                  cfg.KafkaRetryTopic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		Async:                  false,
		AllowAutoTopicCreation: true,
	}
	defer retryWriter.Close()

	sinker := sink.NewFileSink(cfg.SinkFilePath, cfg.FailEveryNBatches)
	dlqWriter := consumer.NewDLQWriter(cfg.DLQFilePath)
	ledger, err := consumer.NewLedger(cfg.IdempotencyMode, cfg.LedgerFilePath)
	if err != nil {
		log.Fatalf("failed to initialize idempotency ledger: %v", err)
	}
	debug.SetIdempotencyMode(cfg.IdempotencyMode)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	debugServer := &http.Server{
		Addr:              ":" + cfg.ConsumerDebugPort,
		Handler:           debugMux(debug),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("consumer debug endpoint listening on :%s", cfg.ConsumerDebugPort)
		if err := debugServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("consumer debug server error: %v", err)
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = debugServer.Shutdown(shutdownCtx)
	}()

	log.Printf(
		"consumer started: topics=[%s,%s] brokers=%v batch_size=%d flush_interval=%s sink=%s dlq=%s max_retries=%d idempotency=%s",
		cfg.KafkaTopic, cfg.KafkaRetryTopic, cfg.KafkaBrokers, cfg.BatchSize, cfg.BatchFlushInterval, cfg.SinkFilePath, cfg.DLQFilePath, cfg.MaxRetries, cfg.IdempotencyMode,
	)
	if err := consumer.Run(
		ctx, reader, retryWriter, dlqWriter, ledger, cfg.BatchSize, cfg.BatchFlushInterval, sinker, counters, debug,
		consumer.RunOptions{MaxRetries: cfg.MaxRetries, MainTopic: cfg.KafkaTopic, RetryTopic: cfg.KafkaRetryTopic},
	); err != nil {
		log.Fatalf("consumer failed: %v", err)
	}

	snap := counters.Snapshot()
	log.Printf("consumer exiting: consumed=%d flushed=%d failures=%d", snap.Consumed, snap.Flushed, snap.Failures)
}

func debugMux(state *consumer.DebugState) http.Handler {
	mux := http.NewServeMux()
	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(state.Snapshot())
	}

	// Keep the old path for convenience while making scope explicit with /consumer/debug/state.
	mux.HandleFunc("/debug/state", handler)
	mux.HandleFunc("/consumer/debug/state", handler)
	return mux
}
