package sink

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"event-driven-systems-tradeoffs-demo/internal/domain"
)

func TestFileSinkWritesJSONL(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "events.jsonl")
	s := NewFileSink(path, 0)

	batch := []domain.Event{{EventID: "e1", DeviceID: "d1", Timestamp: 1, EventType: "t", Payload: "p"}}
	if err := s.WriteBatch(context.Background(), batch); err != nil {
		t.Fatalf("write batch failed: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file failed: %v", err)
	}
	if !strings.Contains(string(data), "\"event_id\":\"e1\"") {
		t.Fatalf("expected event_id in sink output, got: %s", string(data))
	}
}

func TestFileSinkFailureInjection(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "events.jsonl")
	s := NewFileSink(path, 2)
	batch := []domain.Event{{EventID: "e1", DeviceID: "d1", Timestamp: 1, EventType: "t", Payload: "p"}}

	if err := s.WriteBatch(context.Background(), batch); err != nil {
		t.Fatalf("first write should succeed, got: %v", err)
	}
	if err := s.WriteBatch(context.Background(), batch); err == nil {
		t.Fatalf("second write should fail due to injection")
	}
}
