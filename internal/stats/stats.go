package stats

import "sync/atomic"

// Counters tracks process-local activity metrics.
type Counters struct {
	received  uint64
	published uint64
	consumed  uint64
	flushed   uint64
	failures  uint64
	invalid   uint64
}

// Snapshot is a stable read of current counters.
type Snapshot struct {
	Received  uint64 `json:"received"`
	Published uint64 `json:"published"`
	Consumed  uint64 `json:"consumed"`
	Flushed   uint64 `json:"flushed"`
	Failures  uint64 `json:"failures"`
	Invalid   uint64 `json:"invalid"`
}

func (c *Counters) IncReceived()  { atomic.AddUint64(&c.received, 1) }
func (c *Counters) IncPublished() { atomic.AddUint64(&c.published, 1) }
func (c *Counters) IncInvalid()   { atomic.AddUint64(&c.invalid, 1) }
func (c *Counters) IncConsumed(n int) {
	if n > 0 {
		atomic.AddUint64(&c.consumed, uint64(n))
	}
}
func (c *Counters) IncFlushed()  { atomic.AddUint64(&c.flushed, 1) }
func (c *Counters) IncFailures() { atomic.AddUint64(&c.failures, 1) }

func (c *Counters) Snapshot() Snapshot {
	return Snapshot{
		Received:  atomic.LoadUint64(&c.received),
		Published: atomic.LoadUint64(&c.published),
		Consumed:  atomic.LoadUint64(&c.consumed),
		Flushed:   atomic.LoadUint64(&c.flushed),
		Failures:  atomic.LoadUint64(&c.failures),
		Invalid:   atomic.LoadUint64(&c.invalid),
	}
}
