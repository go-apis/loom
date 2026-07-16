package loom

import (
	"context"
	"encoding/json"
	"sync"
)

// Envelope is the wire form of a published integration event. Ordering key
// = aggregate identity, so per-aggregate order survives the bus.
type Envelope struct {
	Service       string          `json:"service"`
	Namespace     string          `json:"namespace"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   string          `json:"aggregate_id"`
	Version       int             `json:"version"`
	GlobalSeq     int64           `json:"global_seq"`
	Type          string          `json:"type"`
	SchemaVersion int             `json:"schema_version"`
	At            string          `json:"at"`
	Meta          Metadata        `json:"meta"`
	Data          json.RawMessage `json:"data"`
}

func (e *Envelope) OrderingKey() string {
	return e.Service + "/" + e.Namespace + "/" + e.AggregateType + "/" + e.AggregateID
}

// Bus is the integration-event transport. Implementations must deliver
// at-least-once and should respect ordering keys; consumers are dedup'd by
// the process runner, never by the bus.
type Bus interface {
	Publish(ctx context.Context, env *Envelope) error
	// Subscribe registers a durable consumer group for one service. The
	// handler's error triggers redelivery per the implementation's policy.
	Subscribe(ctx context.Context, group string, handler func(ctx context.Context, env *Envelope) error) error
}

// MemoryBus is the in-process transport for tests and single-binary setups:
// synchronous fan-out, one redelivery attempt burden on the caller (the
// process runner owns retries and parking).
type MemoryBus struct {
	mu   sync.RWMutex
	subs []func(ctx context.Context, env *Envelope) error
}

func NewMemoryBus() *MemoryBus {
	return &MemoryBus{}
}

func (b *MemoryBus) Publish(ctx context.Context, env *Envelope) error {
	b.mu.RLock()
	subs := make([]func(ctx context.Context, env *Envelope) error, len(b.subs))
	copy(subs, b.subs)
	b.mu.RUnlock()
	for _, h := range subs {
		if err := h(ctx, env); err != nil {
			return err
		}
	}
	return nil
}

func (b *MemoryBus) Subscribe(ctx context.Context, group string, handler func(ctx context.Context, env *Envelope) error) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subs = append(b.subs, handler)
	return nil
}
