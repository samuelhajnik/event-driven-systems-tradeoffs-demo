package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"event-driven-systems-tradeoffs-demo/internal/domain"
)

// FileSink appends batches to a local JSONL file.
type FileSink struct {
	path             string
	failEveryNBatches int

	mu           sync.Mutex
	batchCounter int
}

func NewFileSink(path string, failEveryNBatches int) *FileSink {
	return &FileSink{path: path, failEveryNBatches: failEveryNBatches}
}

func (s *FileSink) WriteBatch(_ context.Context, batch []domain.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.batchCounter++
	if s.failEveryNBatches > 0 && s.batchCounter%s.failEveryNBatches == 0 {
		return fmt.Errorf("injected sink failure at batch %d", s.batchCounter)
	}
	for _, event := range batch {
		if event.FailureMode == "permanent" {
			return fmt.Errorf("permanent failure mode for event_id=%s", event.EventID)
		}
		if event.FailureMode == "transient" && event.RetryCount == 0 {
			return fmt.Errorf("transient failure mode for event_id=%s on first attempt", event.EventID)
		}
	}

	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("create sink directory: %w", err)
	}

	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open sink file: %w", err)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	for _, event := range batch {
		if err := enc.Encode(event); err != nil {
			return fmt.Errorf("encode event: %w", err)
		}
	}

	return nil
}
