package api

import (
	"testing"

	"event-driven-systems-tradeoffs-demo/internal/domain"
)

func TestValidateEvent(t *testing.T) {
	valid := domain.Event{
		EventID:   "evt-1",
		DeviceID:  "dev-1",
		Timestamp: 1710000000,
		EventType: "temperature",
		Payload:   "22.5",
	}
	if err := validate(valid); err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}

	cases := []struct {
		name  string
		event domain.Event
	}{
		{name: "missing event id", event: domain.Event{DeviceID: "d", Timestamp: 1, EventType: "t", Payload: "p"}},
		{name: "missing device id", event: domain.Event{EventID: "e", Timestamp: 1, EventType: "t", Payload: "p"}},
		{name: "missing timestamp", event: domain.Event{EventID: "e", DeviceID: "d", EventType: "t", Payload: "p"}},
		{name: "missing event type", event: domain.Event{EventID: "e", DeviceID: "d", Timestamp: 1, Payload: "p"}},
		{name: "missing payload", event: domain.Event{EventID: "e", DeviceID: "d", Timestamp: 1, EventType: "t"}},
		{name: "bad failure mode", event: domain.Event{EventID: "e", DeviceID: "d", Timestamp: 1, EventType: "t", Payload: "p", FailureMode: "unknown"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := validate(tc.event); err == nil {
				t.Fatalf("expected validation error for case %q", tc.name)
			}
		})
	}
}
