package sink

import (
	"context"

	"event-driven-systems-tradeoffs-demo/internal/domain"
)

// Sink persists a flushed batch.
type Sink interface {
	WriteBatch(ctx context.Context, batch []domain.Event) error
}
