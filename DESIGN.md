# Loom design

The design of record is the es-v2 tracking issue (Design v2). This file
covers what's implemented and the decisions embedded in the code.

## SDL grammar

```
schema      := "service" IDENT decl*
decl        := aggregate | record | entity | event | consume | policy
             | process | projection | type
aggregate   := "aggregate" IDENT directives? "{" (state | command | event)* "}"
record      := "record" IDENT "{" (state | command | event)* "}"
state       := "state" fields
command     := "command" IDENT fields? "->" identList
event       := "event" IDENT directives? fields?
consume     := "consume" IDENT "." IDENT fields?
policy      := "policy" IDENT "{" on* "}"
process     := "process" IDENT "{" on* "}"
projection  := "projection" IDENT "->" IDENT "{" ("on" eventRef)* "}"
on          := "on" eventRef ("->" identList)?
eventRef    := IDENT | IDENT "." IDENT          // qualified = foreign
type        := "type" IDENT fields
entity      := "entity" IDENT fields
fields      := "{" (IDENT ":" ftype "!"?)* "}"
ftype       := builtin ("(" IDENT ")")? | IDENT | "[" ftype "]"     ("?" = nullable)
builtin     := string int float bool uuid timestamp bytes any map
directives  := "@snapshot(N)" | "@publish" | "@v(N)" | "@alias(A, B)"
```

Rules enforced at parse/validate time:

- aggregate commands must emit (records' may not: the state write is the
  effect); emits/dispatches must name declared things
- policies cannot subscribe to foreign events (they run in the producing
  transaction — use a process)
- nested object literals are forbidden: declare a `type` (keeps generated
  code flat and contracts referencable across languages)
- foreign events (`consume`/qualified refs) are implicitly published
  contracts

## Generated code

`loom generate` writes `loomgen/` (always regenerated): typed structs for
commands (embedding `loom.CommandBase`), events (with `LoomEvent()`),
aggregate/entity states with `Fold` (field-name merge between event and
state, optionality-bridged), and `NewRegistry(Impl)` wiring everything into
`loom.Registry`. Stubs (once, then yours): one file per aggregate and
reactor implementing the generated interfaces, plus `registry.go`.

No reflection anywhere: names come from generated methods, routing from
generated switches, folds from generated assignments.

## Runtime

- **Unit of work** (`Dispatch`): load (snapshot + fold in version order),
  handle, enforce the emit contract, append with the stream's UNIQUE
  constraint as the optimistic-concurrency guard (typed `ConflictError`,
  automatic retry against fresh state), run subscribed policies in the same
  transaction (depth-capped), write outbox rows for published events,
  snapshot every N.
- **Global sequence**: `loom_events.global_seq` (identity). Projections and
  local processes are checkpointed catch-up readers over it — rebuildable,
  no bus, no publish-to-self hack for async self-handling.
- **Outbox relay**: the one component ported by design from the old
  runtime: advisory-lock election, SKIP LOCKED claims, insert-order drain,
  per-aggregate ordering keys.
- **Processes**: local events from the log; foreign events from the bus
  with consumer-side dedup (`loom_dedup`). Retries with backoff, then loud
  parking to `loom_dead_letters`. Silent drops are structurally impossible.
- **Metadata**: correlation ids propagate across dispatches and the bus;
  causation records the triggering event. Both are columns, not folklore.
- **Read models**: one `loom_entities` jsonb doc table (typed per-entity
  tables are a perf milestone, not a semantic change).
- **Records** (`loom_records`): state-of-record persistence for the
  ledger/balance class. Row-locked writes, version per write; emitted
  events enter the log with the record's stream identity (projections and
  processes see them) but never rebuild the record. Deliberately a separate
  table from `loom_entities`: projection Rebuild truncates entities and
  must never touch records.
- **Timers** (`loom_timers`): durable scheduled commands, written in the
  scheduling unit's transaction. Keyed idempotently (default: command type
  + target) so redelivered reactions overwrite; `loom.CancelTimer` deletes
  by the same key. SKIP LOCKED claims, retry then park.
- **The log as a timeseries**: `global_seq` is monotonic and rows are
  append-only, so time queries are sequence-range queries. A BRIN index on
  `at` makes time-window scans cheap at ~zero write cost, and
  `(service, correlation_id)` serves trace-a-flow lookups. Deliberately no
  TimescaleDB dependency (unavailable on Cloud SQL); if volume ever
  demands it, native partitioning by `global_seq` range and rollup tables
  are the escalation path, not an engine change.
- **HTTP API** (`api.go` / `query.go`): the registry drives a complete
  mounted surface — command dispatch, filtered entity/record queries
  (validated field names, parameterized values, typed numeric/bool
  comparisons), log browsing, ops stats. Transport-level auth stays with
  the deployment.

## Not yet built (tracked on the issue)

Console (M2 equivalent), real bus providers (Pub/Sub with old-envelope
compat), persisted process state + timeouts, upcasters beyond aliases,
given/when/then harness generation, `loom extract` (legacy on-ramp,
returning from tag `m1-extraction`), replay-parity harness, table migrator,
OTel, TypeScript target.
