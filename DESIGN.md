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
process     := "process" IDENT "{" (on | effect)* "}"
effect      := "effect" IDENT
projection  := "projection" IDENT "->" IDENT "{" ("on" eventRef)* "}"
on          := "on" eventRef ("->" identList)?
eventRef    := IDENT | IDENT "." IDENT          // qualified = foreign
type        := "type" IDENT fields
entity      := "entity" IDENT fields
fields      := "{" (IDENT ":" ftype "!"? "@pii"?)* "}"
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
- effects are declared on processes only (a policy runs in the producing
  transaction and must not touch the outside world); `loom.Once` refuses
  undeclared keys — a typo'd key would be a fresh journal identity and a
  repeated call, so declaration is the safety net, not ceremony
- `@pii` lives only on top-level fields of local unpublished events and
  aggregate/record/entity states: commands are transient inputs, published
  events cross the bus in plaintext (keep PII on a private event — the
  ten99 private/published pair pattern), foreign events belong to another
  service's keys, and named types would smuggle PII anywhere

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
- **Bus providers**: `MemoryBus` (in-process, tests/single-binary) and
  `gpub` (Google Cloud Pub/Sub: one shared topic, subscription per
  consumer group, per-aggregate ordering keys, ResumePublish after a
  failed keyed publish, ordering disabled under the emulator whose ordered
  backlog is broken — lessons carried over from the old provider).
  Undecodable messages ack-and-log (nacking garbage redelivers forever);
  handler errors nack, and loom's dedup + parking make at-least-once safe.
  A `Codec` seam exists for bridging the old eventsourcing envelope during
  the (parked) six-service migration.
- **Processes**: local events from the log; foreign events from the bus
  with consumer-side dedup (`loom_dedup`). Retries with backoff, then loud
  parking to `loom_dead_letters`. Silent drops are structurally impossible.
- **Metadata**: correlation ids propagate across dispatches and the bus;
  causation records the triggering event. Both are columns, not folklore.
- **Context-injected reads**: every handler/reaction invocation carries
  read access (`loom.Load`/`GetRecord`/`GetEntity` on the ctx), so
  implementations never hold a client and registries wire without the
  set-client-after-New dance. Reads only — dispatch from inside a handler
  would nest units of work; reactions return commands.
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
- **Batches** (`loom_batches`/`loom_batch_items`): durable chunked fan-out
  of many commands (CopyFrom insert, SKIP LOCKED chunk claims, stale-claim
  reclaim). Per-item outcomes are recorded (at-least-once per item — use
  deterministic ids); the batch row is live progress, streamable over SSE.
  Reactions enqueue atomically via `loom.AsBatch`.
- **Effects** (`loom_effects`): journaled external calls — the outbox's
  cousin for the request/response direction. `loom.Once(ctx, key, fn)` in a
  process reaction claims a row (committed before fn runs), settles it with
  the JSON result or the error, and replays `done` results on every retry
  or redelivery. An unsettled claim (crash between call and record) is *in
  doubt*: the runtime refuses to re-run it and parks the reaction — an
  operator resolves it (recording what actually happened on the other side)
  and redrives the dead letter. At-most-once with loud ambiguity, the
  strongest guarantee non-idempotent APIs admit. Failed settles re-run on
  retry: an error return is the handler asserting the call did not happen.
- **PII encryption** (`loom_keys`): `@pii` fields are sealed with
  AES-256-GCM under a per-stream data key wrapped by a `KeyWrapper`
  (`LocalKeys` master key today; a KMS wrapper is the same interface).
  Sealing happens at every rest boundary — log, snapshots, read models,
  records, parked dead letters — and opening happens on folds and typed
  reads (`Load`/`Entity`/`Record` and their GET endpoints). Raw list
  queries and the log browser return ciphertext as stored, and jsonb
  filters can't match PII fields (keep a plain derived field like
  `tin_last4` for that). `Shred(ns, id)` deletes the key: every copy of
  that stream's PII becomes permanently unreadable and decodes as zero
  values, so replays and rebuilds keep working, redacted — erasure for an
  append-only store. Cached keys are evicted cluster-wide via the LISTEN
  channel.
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
- **Streaming = SSE, deliberately not gRPC** (`stream.go`): every streaming
  need in the domain is one-directional (server → client) — progress, log
  tails, state watches — which SSE serves over plain HTTP with native
  browser support and no proxy/Workers friction. The schema is already the
  typed contract, the bus already handles service-to-service, so gRPC
  would buy bidirectional streaming nobody needs at real compatibility
  cost. Resumability rides the global sequence (SSE id = seq →
  Last-Event-ID reconnect). Cross-instance wake-ups via pg LISTEN/NOTIFY
  (`listenLoop`), which also makes runners multi-instance-responsive. A
  gRPC transport can be added from the same registry later if a genuine
  internal-RPC need appears.

## Not yet built (tracked on the issue)

Console (M2 equivalent), old-envelope compat codec for gpub (migration
on-ramp), persisted process state + timeouts, upcasters beyond aliases,
given/when/then harness generation, `loom extract` (legacy on-ramp,
returning from tag `m1-extraction`), replay-parity harness, table migrator,
OTel, TypeScript target.
