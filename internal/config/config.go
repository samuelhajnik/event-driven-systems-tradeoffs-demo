package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds runtime settings for API and consumer processes.
type Config struct {
	HTTPPort           string
	ConsumerDebugPort  string
	KafkaBrokers       []string
	KafkaTopic         string
	KafkaRetryTopic    string
	KafkaGroupID       string
	BatchSize          int
	BatchFlushInterval time.Duration
	SinkFilePath       string
	DLQFilePath        string
	FailEveryNBatches  int
	MaxRetries         int
	IdempotencyMode    string
	LedgerFilePath     string
}

// Load returns config from environment with sensible local defaults.
func Load() Config {
	return Config{
		HTTPPort:           getenv("HTTP_PORT", "8080"),
		ConsumerDebugPort:  getenv("CONSUMER_DEBUG_PORT", "8081"),
		KafkaBrokers:       splitCSV(getenv("KAFKA_BROKERS", "localhost:9092")),
		KafkaTopic:         getenv("KAFKA_TOPIC", "events"),
		KafkaRetryTopic:    getenv("KAFKA_RETRY_TOPIC", "events.retry"),
		KafkaGroupID:       getenv("KAFKA_GROUP_ID", "event-pipeline-consumer"),
		BatchSize:          getenvInt("BATCH_SIZE", 10),
		BatchFlushInterval: getenvDuration("BATCH_FLUSH_INTERVAL", 2*time.Second),
		SinkFilePath:       getenv("SINK_FILE_PATH", "./data/events.jsonl"),
		DLQFilePath:        getenv("DLQ_FILE_PATH", "./data/dlq.jsonl"),
		FailEveryNBatches:  getenvInt("FAIL_EVERY_N_BATCHES", 0),
		MaxRetries:         getenvInt("MAX_RETRIES", 2),
		IdempotencyMode:    normalizeMode(getenv("IDEMPOTENCY_MODE", "off")),
		LedgerFilePath:     getenv("LEDGER_FILE_PATH", "./data/idempotency-ledger.jsonl"),
	}
}

func normalizeMode(v string) string {
	mode := strings.ToLower(strings.TrimSpace(v))
	if mode != "on" {
		return "off"
	}
	return "on"
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return fallback
	}
	return n
}

func getenvDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}

func splitCSV(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	if len(out) == 0 {
		return []string{"localhost:9092"}
	}
	return out
}
