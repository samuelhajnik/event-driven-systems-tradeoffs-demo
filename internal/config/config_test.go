package config

import (
	"os"
	"reflect"
	"testing"
	"time"
)

func TestLoadParsesEnvOverrides(t *testing.T) {
	t.Setenv("HTTP_PORT", "9090")
	t.Setenv("CONSUMER_DEBUG_PORT", "9091")
	t.Setenv("KAFKA_BROKERS", "a:1,b:2")
	t.Setenv("KAFKA_TOPIC", "demo-topic")
	t.Setenv("KAFKA_RETRY_TOPIC", "demo-topic.retry")
	t.Setenv("KAFKA_GROUP_ID", "group-x")
	t.Setenv("BATCH_SIZE", "12")
	t.Setenv("BATCH_FLUSH_INTERVAL", "3s")
	t.Setenv("SINK_FILE_PATH", "/tmp/demo.jsonl")
	t.Setenv("DLQ_FILE_PATH", "/tmp/dlq.jsonl")
	t.Setenv("FAIL_EVERY_N_BATCHES", "5")
	t.Setenv("MAX_RETRIES", "4")
	t.Setenv("IDEMPOTENCY_MODE", "on")
	t.Setenv("LEDGER_FILE_PATH", "/tmp/ledger.jsonl")

	cfg := Load()
	if cfg.HTTPPort != "9090" || cfg.ConsumerDebugPort != "9091" {
		t.Fatalf("unexpected ports: %+v", cfg)
	}
	if !reflect.DeepEqual(cfg.KafkaBrokers, []string{"a:1", "b:2"}) {
		t.Fatalf("unexpected brokers: %+v", cfg.KafkaBrokers)
	}
	if cfg.KafkaTopic != "demo-topic" || cfg.KafkaRetryTopic != "demo-topic.retry" || cfg.KafkaGroupID != "group-x" {
		t.Fatalf("unexpected kafka settings: %+v", cfg)
	}
	if cfg.BatchSize != 12 || cfg.BatchFlushInterval != 3*time.Second {
		t.Fatalf("unexpected batch settings: %+v", cfg)
	}
	if cfg.SinkFilePath != "/tmp/demo.jsonl" || cfg.DLQFilePath != "/tmp/dlq.jsonl" || cfg.FailEveryNBatches != 5 {
		t.Fatalf("unexpected sink/failure settings: %+v", cfg)
	}
	if cfg.MaxRetries != 4 || cfg.IdempotencyMode != "on" || cfg.LedgerFilePath != "/tmp/ledger.jsonl" {
		t.Fatalf("unexpected retry/idempotency settings: %+v", cfg)
	}
}

func TestLoadFallsBackOnInvalidValues(t *testing.T) {
	t.Setenv("BATCH_SIZE", "bad")
	t.Setenv("BATCH_FLUSH_INTERVAL", "nope")
	t.Setenv("FAIL_EVERY_N_BATCHES", "-1")
	t.Setenv("MAX_RETRIES", "-1")
	t.Setenv("IDEMPOTENCY_MODE", "weird")
	t.Setenv("KAFKA_BROKERS", ",,")
	_ = os.Setenv("HTTP_PORT", "")

	cfg := Load()
	if cfg.BatchSize != 10 {
		t.Fatalf("expected default batch size, got %d", cfg.BatchSize)
	}
	if cfg.BatchFlushInterval != 2*time.Second {
		t.Fatalf("expected default flush interval, got %s", cfg.BatchFlushInterval)
	}
	if cfg.FailEveryNBatches != 0 {
		t.Fatalf("expected default failure injection, got %d", cfg.FailEveryNBatches)
	}
	if cfg.MaxRetries != 2 {
		t.Fatalf("expected default max retries, got %d", cfg.MaxRetries)
	}
	if cfg.IdempotencyMode != "off" {
		t.Fatalf("expected idempotency mode off, got %s", cfg.IdempotencyMode)
	}
	if !reflect.DeepEqual(cfg.KafkaBrokers, []string{"localhost:9092"}) {
		t.Fatalf("unexpected broker fallback: %+v", cfg.KafkaBrokers)
	}
}
