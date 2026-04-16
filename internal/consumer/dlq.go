package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"event-driven-systems-tradeoffs-demo/internal/domain"
)

type DLQRecord struct {
	Event      domain.Event `json:"event"`
	Reason     string       `json:"reason"`
	RetryCount int          `json:"retry_count"`
	FailedAt   string       `json:"failed_at"`
	SourceFlow string       `json:"source_flow"`
}

type DLQWriter struct {
	path string
	mu   sync.Mutex
}

func NewDLQWriter(path string) *DLQWriter {
	return &DLQWriter{path: path}
}

func (w *DLQWriter) Write(_ context.Context, event domain.Event, reason string, sourceFlow string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := os.MkdirAll(filepath.Dir(w.path), 0o755); err != nil {
		return fmt.Errorf("create dlq directory: %w", err)
	}

	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open dlq file: %w", err)
	}
	defer f.Close()

	record := DLQRecord{
		Event:      event,
		Reason:     reason,
		RetryCount: event.RetryCount,
		FailedAt:   time.Now().UTC().Format(time.RFC3339),
		SourceFlow: sourceFlow,
	}
	if err := json.NewEncoder(f).Encode(record); err != nil {
		return fmt.Errorf("encode dlq record: %w", err)
	}
	return nil
}
