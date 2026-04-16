package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"event-driven-systems-tradeoffs-demo/internal/domain"
	"event-driven-systems-tradeoffs-demo/internal/ingest"
	"event-driven-systems-tradeoffs-demo/internal/stats"
)

// Handler serves API endpoints for ingest and API-local visibility.
type Handler struct {
	producer ingest.Producer
	counters *stats.Counters
}

// APILocalStats makes process-local scope explicit in payload shape.
type APILocalStats struct {
	APIReceivedEvents       uint64 `json:"api_received_events"`
	APIPublishedEvents      uint64 `json:"api_published_events"`
	APIPublishFailures      uint64 `json:"api_publish_failures"`
	APIValidationOrBadInput uint64 `json:"api_validation_or_bad_input"`
}

func NewHandler(producer ingest.Producer, counters *stats.Counters) *Handler {
	return &Handler{producer: producer, counters: counters}
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", h.health)
	mux.HandleFunc("/events", h.events)
	mux.HandleFunc("/stats", h.stats)
	return mux
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (h *Handler) stats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	snap := h.counters.Snapshot()
	resp := APILocalStats{
		APIReceivedEvents:       snap.Received,
		APIPublishedEvents:      snap.Published,
		APIPublishFailures:      snap.Failures,
		APIValidationOrBadInput: snap.Invalid,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *Handler) events(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event domain.Event
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		h.counters.IncInvalid()
		http.Error(w, "invalid JSON payload", http.StatusBadRequest)
		return
	}

	if err := validate(event); err != nil {
		h.counters.IncInvalid()
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	h.counters.IncReceived()
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := h.producer.Publish(ctx, event); err != nil {
		h.counters.IncFailures()
		http.Error(w, "failed to publish event", http.StatusBadGateway)
		return
	}

	h.counters.IncPublished()
	w.WriteHeader(http.StatusAccepted)
}

func validate(event domain.Event) error {
	if event.EventID == "" {
		return errors.New("event_id is required")
	}
	if event.DeviceID == "" {
		return errors.New("device_id is required")
	}
	if event.Timestamp == 0 {
		return errors.New("timestamp is required")
	}
	if event.EventType == "" {
		return errors.New("event_type is required")
	}
	if event.Payload == "" {
		return errors.New("payload is required")
	}
	if event.FailureMode != "" && event.FailureMode != "transient" && event.FailureMode != "permanent" {
		return errors.New("failure_mode must be one of: transient, permanent")
	}
	return nil
}
