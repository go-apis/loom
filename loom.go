// Package loom is a schema-first event-sourcing runtime. Services declare
// their aggregates, commands, events, and reactions in a .loom schema;
// `loom generate` emits the typed code that this runtime executes. Nothing
// here uses reflection to route anything: the generated Registry is the
// single wiring point.
package loom

import (
	"context"
	"encoding/json"
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
	Tables      []*TableDef
	Joins       []*JoinDef
	Enums       []*EnumDef
}

// EnumDef mirrors a schema `enum`: the closed value set behind a
// generated named string type. The gateway serves these as real GraphQL
// enum types; the runtime treats the values as strings.
type EnumDef struct {
	Name   string
	Values []string
}

// JoinDef is a schema-declared gateway edge between entities (`join` in
// the DSL). The runtime ignores these — the GraphQL gateway wires them
// into resolvers when it composes services, erroring if the target
// entity isn't mounted.
type JoinDef struct {
	OnEntity string // the entity the field hangs off
	Field    string
	Service  string // owning service of the target; "" = same service
	Entity   string // target entity
	List     bool
	// Via: single joins follow this local field to the target's row id;
	// list joins collect target rows whose Via field equals this row's id.
	Via string
}

// TableDef backs an @table entity: the read model lives in its own typed
// table (one real column per state field) instead of the shared jsonb doc
// table. Generated code owns the shape — DDL, columns, and the typed value
// extractor — so the runtime never reflects.
type TableDef struct {
	Entity string
	// Name is the SQL table name (loom_t_<service>_<entity>).
	Name string
	// DDL is the full CREATE TABLE IF NOT EXISTS statement, also written to
	// loomgen/tables_gen.sql for review. Migrate executes it and then diffs
	// live columns against Columns (additive only, never a drop).
	DDL string
	// Columns lists the state columns in order, excluding the meta columns
	// (service, namespace, id, updated_at).
	Columns []TableColumn
	// Values extracts the column values from the entity state, aligned with
	// Columns. JSON-typed columns are pre-marshaled via JSONValue.
	Values func(state EntityState) []any
}

type TableColumn struct {
	// Name is both the column name and the state field's json name.
	Name string
	// Type is the Postgres type as written in the DDL (bigint, text, uuid,
	// timestamptz, double precision, boolean, jsonb).
	Type string
}

// JSONValue marshals a jsonb-column value explicitly, so strings and byte
// slices are never mistaken for pre-encoded JSON by the driver. A marshal
// error is returned as the value itself: the write then fails loudly and
// the runner retries — never a silent drop.
func JSONValue(v any) any {
	raw, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("loom: JSONValue: %w", err)
	}
	return json.RawMessage(raw)
}

type AggregateDef struct {
	Name          string
	SnapshotEvery int // 0 = disabled
	// StatePII names state fields encrypted at rest in snapshots.
	StatePII []string
	// StateSecret names @secret state fields: sealed like StatePII, and
	// additionally redacted to a fingerprint on every HTTP read. Always a
	// subset of StatePII.
	StateSecret []string
	NewState    func() AggregateState
	Commands    []*CommandDef
}

type CommandDef struct {
	Name  string
	New   func() Command
	Emits []string // the contract: the only event types Handle may return
	// PII names payload fields sealed wherever the command rests (timers,
	// batch items).
	PII []string
	// Roles (@role) gates the command at the GraphQL gateway: the caller
	// must hold one of these roles in the target namespace. In-process
	// Dispatch and the service's own HTTP API are not gated.
	Roles []string
	// Required names the schema-required payload fields (snake case) —
	// the gateway serves exactly these as NonNull inputs, matching the
	// emitted SDL. Go value-ness can't say it: optional strings and
	// required strings are both plain `string`.
	Required []string
	Handle   func(ctx context.Context, state AggregateState, cmd Command) ([]any, error)
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
	// StateSecret names @secret state fields — see AggregateDef.StateSecret.
	StateSecret []string
	NewState    func() any
	Commands    []*RecordCommandDef
}

type RecordCommandDef struct {
	Name  string
	New   func() Command
	Emits []string
	PII   []string
	// Roles — see CommandDef.Roles.
	Roles []string
	// Required — see CommandDef.Required.
	Required []string
	Handle   func(ctx context.Context, state any, cmd Command) ([]any, error)
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
	// Upcasts maps a schema version to the hop that lifts a payload of
	// that version one version forward (raw JSON in, raw JSON out). decode
	// chains hops until SchemaVersion. Declaring any upcast makes version
	// handling strict for this event: a stored version with no path to
	// current is a loud UpcastError, never a silent zero-value fold.
	Upcasts map[int]UpcastFunc
}

// UpcastFunc is one hand-written migration hop: the payload JSON of one
// schema version in, the payload JSON of the next version out.
type UpcastFunc func(data []byte) ([]byte, error)

// UpcastError is returned when a stored event cannot be lifted to the
// registry's current schema version — a missing hop, a failing hop, or a
// stored version newer than the registry (deploy skew).
type UpcastError struct {
	Type   string
	Stored int // the stored schema_version
	At     int // the hop that failed or is missing (Stored > current: 0)
	To     int // the registry's current version
	Err    error
}

func (e *UpcastError) Error() string {
	switch {
	case e.Err != nil:
		return fmt.Sprintf("loom: upcast %s v%d→v%d (stored v%d, current v%d): %v", e.Type, e.At, e.At+1, e.Stored, e.To, e.Err)
	case e.Stored > e.To:
		return fmt.Sprintf("loom: stored %s is v%d but this registry only knows v%d — deploy skew?", e.Type, e.Stored, e.To)
	default:
		return fmt.Sprintf("loom: no upcast lifts %s from v%d (stored v%d, current v%d)", e.Type, e.At, e.Stored, e.To)
	}
}

func (e *UpcastError) Unwrap() error { return e.Err }

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

func (r *Registry) aggregateDef(name string) *AggregateDef {
	for _, agg := range r.Aggregates {
		if agg.Name == name {
			return agg
		}
	}
	return nil
}

func (r *Registry) recordDef(name string) *RecordDef {
	for _, rec := range r.Records {
		if rec.Name == name {
			return rec
		}
	}
	return nil
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

func (r *Registry) tableFor(entity string) *TableDef {
	for _, t := range r.Tables {
		if t.Entity == entity {
			return t
		}
	}
	return nil
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
