package batch

import (
	"testing"

	"event-driven-systems-tradeoffs-demo/internal/domain"
)

func TestAccumulatorFlushThreshold(t *testing.T) {
	acc := NewAccumulator(2)

	if flush := acc.Add(domain.Event{EventID: "1"}); flush {
		t.Fatalf("unexpected flush on first add")
	}
	if flush := acc.Add(domain.Event{EventID: "2"}); !flush {
		t.Fatalf("expected flush on threshold")
	}

	got := acc.Drain()
	if len(got) != 2 {
		t.Fatalf("expected 2 events, got %d", len(got))
	}
	if got[0].EventID != "1" || got[1].EventID != "2" {
		t.Fatalf("unexpected ordering in drained batch: %+v", got)
	}
}

func TestAccumulatorDefaultsToOne(t *testing.T) {
	acc := NewAccumulator(0)
	if flush := acc.Add(domain.Event{EventID: "x"}); !flush {
		t.Fatalf("expected flush when max size defaults to one")
	}
}

func TestAccumulatorSnapshotAndDrop(t *testing.T) {
	acc := NewAccumulator(10)
	_ = acc.Add(domain.Event{EventID: "1"})
	_ = acc.Add(domain.Event{EventID: "2"})
	_ = acc.Add(domain.Event{EventID: "3"})

	snap := acc.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("expected snapshot length 3, got %d", len(snap))
	}
	if acc.Len() != 3 {
		t.Fatalf("snapshot must not mutate accumulator")
	}

	acc.Drop(2)
	if acc.Len() != 1 {
		t.Fatalf("expected len 1 after drop, got %d", acc.Len())
	}
	rest := acc.Snapshot()
	if rest[0].EventID != "3" {
		t.Fatalf("expected remaining event 3, got %+v", rest)
	}
}
