// Package loom is a schema-first event-sourcing runtime. Services declare
// their aggregates, commands, events, and reactions in a .loom schema;
// `loom generate` emits the typed code that this runtime executes. Nothing
// here uses reflection to route anything: the generated Registry is the
// single wiring point.
package loom

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Command targets one aggregate instance in one namespace. Generated
// command structs embed CommandBase and implement LoomCommand with their
// schema name — no reflection anywhere on the dispatch path.
type Command interface {
	CommandTarget() (namespace string, aggregateID uuid.UUID)
	LoomCommand() string
}

// DomainEvent is implemented by generated event structs.
type DomainEvent interface {
	LoomEvent() string
}

type CommandBase struct {
	AggregateID uuid.UUID `json:"aggregate_id"`
	Namespace   string    `json:"namespace"`
}

func (c CommandBase) CommandTarget() (string, uuid.UUID) {
	return c.Namespace, c.AggregateID
}

// Metadata travels with every event written in a unit of work.
// CorrelationID groups everything caused by one external trigger;
// CausationID names the immediate cause (a command dispatch or the event a
// policy/process reacted to).
type Metadata struct {
	CorrelationID string `json:"correlation_id,omitempty"`
	CausationID   string `json:"causation_id,omitempty"`
	Actor         string `json:"actor,omitempty"`
}

type metaKey struct{}

// WithMeta attaches metadata to the context for the next Dispatch.
func WithMeta(ctx context.Context, meta Metadata) context.Context {
	return context.WithValue(ctx, metaKey{}, meta)
}

func MetaFrom(ctx context.Context) Metadata {
	if m, ok := ctx.Value(metaKey{}).(Metadata); ok {
		return m
	}
	return Metadata{}
}

// Event is the stored envelope. Data holds the typed payload (a pointer to
// a generated event struct) after decode.
type Event struct {
	GlobalSeq     int64     `json:"global_seq"`
	Service       string    `json:"service"`
	Namespace     string    `json:"namespace"`
	AggregateType string    `json:"aggregate_type"`
	AggregateID   uuid.UUID `json:"aggregate_id"`
	Version       int       `json:"version"`
	Type          string    `json:"type"`
	SchemaVersion int       `json:"schema_version"`
	At            time.Time `json:"at"`
	Meta          Metadata  `json:"meta"`
	Data          any       `json:"data"`
}

// ConflictError is returned when an append loses an optimistic-concurrency
// race: another unit wrote the aggregate version this unit expected to
// write. Retry the whole command against fresh state.
type ConflictError struct {
	AggregateType string
	AggregateID   uuid.UUID
	Version       int
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("loom: version conflict on %s/%s at version %d", e.AggregateType, e.AggregateID, e.Version)
}

// UnknownEventError is returned when a stored or delivered event has no
// registration. It is never silently skipped: runners count and surface it.
type UnknownEventError struct{ Type string }

func (e *UnknownEventError) Error() string {
	return fmt.Sprintf("loom: unknown event type %q", e.Type)
}

// ContractError is returned when a command handler emits an event its
// schema contract does not declare.
type ContractError struct {
	Command string
	Emitted string
}

func (e *ContractError) Error() string {
	return fmt.Sprintf("loom: command %s emitted undeclared event %s", e.Command, e.Emitted)
}

// AggregateState is implemented by generated aggregate state structs. Fold
// applies a typed event payload; unknown types are a bug in generated code
// and must error.
type AggregateState interface {
	Fold(eventType string, data any) error
}

// EntityState is implemented by generated read-model structs.
type EntityState interface {
	Fold(eventType string, data any) error
}

// --- Registry: the generated wiring ---

type Registry struct {
	Service     string
	Aggregates  []*AggregateDef
	Records     []*RecordDef
	Events      []*EventDef
	Policies    []*ReactorDef
	Processes   []*ReactorDef
	Projections []*ProjectionDef
	Uploads     []*UploadDef
}

type AggregateDef struct {
	Name          string
	SnapshotEvery int // 0 = disabled
	// StatePII names state fields encrypted at rest in snapshots.
	StatePII []string
	NewState func() AggregateState
	Commands []*CommandDef
}

type CommandDef struct {
	Name  string
	New   func() Command
	Emits []string // the contract: the only event types Handle may return
	// PII names payload fields sealed wherever the command rests (timers,
	// batch items).
	PII    []string
	Handle func(ctx context.Context, state AggregateState, cmd Command) ([]any, error)
}

// RecordDef backs state-of-record objects that are NOT event-sourced
// (ledgers, balances, accumulators): commands mutate state directly and the
// state row is the source of truth. Handlers may still emit events — they
// land in the log as announcements (projections, processes, outbox all see
// them) but never rebuild the record.
type RecordDef struct {
	Name string
	// StatePII names state fields encrypted at rest in the record row.
	StatePII []string
	NewState func() any
	Commands []*RecordCommandDef
}

type RecordCommandDef struct {
	Name   string
	New    func() Command
	Emits  []string
	PII    []string
	Handle func(ctx context.Context, state any, cmd Command) ([]any, error)
}

type EventDef struct {
	Name          string
	SchemaVersion int
	Publish       bool
	Service       string // owning service; empty = local
	Aliases       []string
	// PII names payload fields encrypted at rest in the log.
	PII []string
	New func() any
}

// ReactorDef backs both policies (in-transaction, local events only) and
// processes (async, checkpointed on the local log or dedup'd off the bus).
type ReactorDef struct {
	Name   string
	Events []string
	// Subs carries the per-subscription dispatch contracts — the same
	// lists the generated React enforces — so the console can draw the
	// full topology at runtime.
	Subs []SubscriptionDef
	// Effects are the declared journaled external calls this process may
	// perform via loom.Once (processes only).
	Effects []string
	React   func(ctx context.Context, evt *Event) ([]Command, error)
}

type SubscriptionDef struct {
	Event      string
	Dispatches []string
}

type ProjectionDef struct {
	Name   string
	Events []string
	Entity string
	// PII names entity state fields encrypted at rest in the read model.
	PII      []string
	NewState func() EntityState
	// EntityID picks the read-model row an event lands in; generated code
	// defaults to the event's aggregate id, or the `key(field)` payload
	// field for keyed subscriptions.
	EntityID func(evt *Event) uuid.UUID
	// Fold, when set (@fold projections), replaces the state's generated
	// assignment fold with the hand-written one wired through Impl.
	Fold func(state EntityState, evt *Event) error
}

func (r *Registry) aggregateForCommand(cmdName string) (*AggregateDef, *CommandDef) {
	for _, agg := range r.Aggregates {
		for _, c := range agg.Commands {
			if c.Name == cmdName {
				return agg, c
			}
		}
	}
	return nil, nil
}

func (r *Registry) recordForCommand(cmdName string) (*RecordDef, *RecordCommandDef) {
	for _, rec := range r.Records {
		for _, c := range rec.Commands {
			if c.Name == cmdName {
				return rec, c
			}
		}
	}
	return nil, nil
}

func (r *Registry) eventDef(name string) *EventDef {
	for _, e := range r.Events {
		if e.Name == name {
			return e
		}
		for _, a := range e.Aliases {
			if a == name {
				return e
			}
		}
	}
	return nil
}
