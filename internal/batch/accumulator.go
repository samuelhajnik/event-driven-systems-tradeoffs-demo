package batch

import (
	"time"

	"event-driven-systems-tradeoffs-demo/internal/domain"
)

// Accumulator collects events and emits flushes by size or interval.
type Accumulator struct {
	maxSize int
	events  []domain.Event
}

func NewAccumulator(maxSize int) *Accumulator {
	if maxSize <= 0 {
		maxSize = 1
	}
	return &Accumulator{maxSize: maxSize, events: make([]domain.Event, 0, maxSize)}
}

func (a *Accumulator) Add(event domain.Event) (flush bool) {
	a.events = append(a.events, event)
	return len(a.events) >= a.maxSize
}

func (a *Accumulator) Drain() []domain.Event {
	if len(a.events) == 0 {
		return nil
	}
	out := make([]domain.Event, len(a.events))
	copy(out, a.events)
	a.events = a.events[:0]
	return out
}

// Snapshot returns a copy of current buffered events without removing them.
func (a *Accumulator) Snapshot() []domain.Event {
	if len(a.events) == 0 {
		return nil
	}
	out := make([]domain.Event, len(a.events))
	copy(out, a.events)
	return out
}

// Drop removes the first n buffered events after a finalized outcome.
func (a *Accumulator) Drop(n int) {
	if n <= 0 || len(a.events) == 0 {
		return
	}
	if n >= len(a.events) {
		a.events = a.events[:0]
		return
	}
	a.events = a.events[n:]
}

func (a *Accumulator) Len() int {
	return len(a.events)
}

func NewTicker(interval time.Duration) *time.Ticker {
	if interval <= 0 {
		interval = time.Second
	}
	return time.NewTicker(interval)
}
