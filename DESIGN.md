# Loom design

The design of record is the es-v2 tracking issue (Design v2). This file
covers what's implemented and the decisions embedded in the code.

## SDL grammar

```
schema      := "service" IDENT decl*
decl        := aggregate | record | entity | series | event | consume
             | policy | process | projection | type | upcast
aggregate   := "aggregate" IDENT directives? "{" (state | command | event | upload)* "}"
record      := "record" IDENT "{" (state | command | event | upload)* "}"
state       := "state" fields
command     := "command" IDENT fields? "->" identList
event       := "event" IDENT directives? fields?
upload      := "upload" IDENT "{" ("on" ("started"|"uploaded") "->" IDENT)* "}"
consume     := "consume" IDENT "." IDENT fields?
upcast      := "upcast" eventRef "@from" "(" NUM ("," NUM)* ")"
policy      := "policy" IDENT "{" on* "}"
process     := "process" IDENT directives? "{" (on | effect)* "}"
effect      := "effect" IDENT "@idempotent"?
projection  := "projection" IDENT "->" IDENT "@fold"? "{" projOn* "}"
projOn      := "on" eventRef ("key" "(" IDENT ")")?
on          := "on" eventRef ("->" identList)?
eventRef    := IDENT | IDENT "." IDENT          // qualified = foreign
type        := "type" IDENT fields
entity      := "entity" IDENT "@table"? fields
series      := "series" IDENT "@time" "(" IDENT ")" "@dim" "(" identList ")"
               ("@key" "(" identList ")")? fields
fields      := "{" (IDENT ":" ftype "!"? "@pii"?)* "}"
ftype       := builtin ("(" IDENT ")")? | IDENT | "[" ftype "]"     ("?" = nullable)
builtin     := string int float bool uuid timestamp bytes any map file
directives  := "@snapshot(N)" | "@publish" | "@v(N)" | "@alias(A, B)"
             | "@retired"
             | "@from(head|origin)"      // process start position
             | "@retry(N, DUR..DUR)"     // process durable retry: max, min..max backoff
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
- `@from` is a process's start position on its first run and belongs to
  processes only (a policy runs inside the producing transaction and has
  no start position): `@from(head)` — the default when omitted — means a
  process with no checkpoint row starts at the log's head and reacts only
  to what happens after it is deployed; `@from(origin)` is the explicit
  opt-in to replaying the whole log (a backfilling process). A process
  that already has a checkpoint row keeps it, so a redeploy is untouched.
  Projections are not affected: they always fold from the origin by design
- `@retry(max, min..max)` is a process's durable retry policy and belongs
  to processes only (a policy fails with the producing transaction):
  `max` is a positive count of durable attempts, `min`/`max` are Go
  durations (`100ms`, `5s`, `5m`) with `0 < min <= max`. The range is one
  directive argument, `5s..5m`. It lands on `schema.Reactor.Retry` and the
  generated `ReactorDef.Retry` (`loom.RetryPolicy`, bounds emitted as
  nanoseconds). Without it a process keeps the fixed in-process attempts,
  then parks — opt-in, not a default change
- effects are declared on processes only (a policy runs in the producing
  transaction and must not touch the outside world); `loom.Once` refuses
  undeclared keys — a typo'd key would be a fresh journal identity and a
  repeated call, so declaration is the safety net, not ceremony
- `@idempotent` is the only directive an effect takes, with no arguments;
  it lands on `schema.Reactor.Idempotent` and the generated
  `ReactorDef.IdempotentEffects`, beside the unchanged `Effects` list
- `@pii` lives only on top-level fields of local unpublished events and
  aggregate/record/entity states: commands are transient inputs, published
  events cross the bus in plaintext (keep PII on a private event — the
  ten99 private/published pair pattern), foreign events belong to another
  service's keys, and named types would smuggle PII anywhere
- series fields: `@time` names a required timestamp field; every `@dim`
  and `@key` field must be a required scalar (they become NOT NULL
  identity columns); `@pii`/`@secret` are rejected (typed columns, and a
  bulk-appended observation has no stream to key a DEK on); the name
  shares the query surface with aggregates, records, and entities
- `upcast X @from(n)` declares a hand-written migration hop n → n+1 for
  stored events behind the current `@v`; hops are code-first (raw JSON in,
  raw JSON of the next version out, generated `EventUpcasts` interface +
  once-only stub) and chain at the decode chokepoint, so folds and
  reactions only ever see the current shape. Coverage must be contiguous
  up to the current version (a gap would strand older rows). Declaring
  any upcast turns on strict version handling for that event — an
  unliftable stored version (below coverage, failing hop, or NEWER than
  the registry: deploy skew) is a loud `UpcastError`, never a silent
  zero-value fold; events without upcasts keep the permissive
  unmarshal-as-is behavior so additive changes stay free. Commands at
  rest (timers, batch items) have no stored version — command-shape
  migration is deliberately out of scope
- an `upload` needs an `on uploaded` command; both lifecycle commands must
  belong to the enclosing aggregate/record and carry exactly one required
  `file` field (loom fills it); upload names are service-unique — they
  become API surface (`create{Name}Upload`)
- a projection's `key(field)` must name a required uuid field on that
  event's payload — a missing key would route to the nil row
- `event X @retired` keeps a declaration alive for its stored rows alone:
  the payload struct stays generated, so replays and rebuilds still decode
  X and then fold nothing (folds are generated from what commands emit,
  and a retired event is emitted by nothing) — decode-skip, cleanly.
  Nothing may emit it, subscribe to it (policy, process, or projection
  `on`), or `upcast` it: all four are validation errors. Deleting the
  declaration instead is what leaves the log holding a type nothing can
  name, which `Migrate` now refuses (see Runtime)

## Generated code

`loom generate` writes `loomgen/` (always regenerated): typed structs for
commands (embedding `loom.CommandBase`), events (with `LoomEvent()`),
aggregate/entity states with `Fold` (field-name merge between event and
state, optionality-bridged), and `NewRegistry(Impl)` wiring everything into
`loom.Registry`. Stubs (once, then yours): one file per aggregate and
reactor implementing the generated interfaces, plus `registry.go`.
`layout: folders` puts stubs in per-kind packages (aggregates/, records/,
policies/, processes/) with registry.go at the root; flat stays the
default. Dependency structs are the user's to place — a leaf package like
processes/ is the natural home, since registry.go (root) imports the kind
packages, never the reverse.

No reflection anywhere: names come from generated methods, routing from
generated switches, folds from generated assignments.

## Runtime

- **Unit of work** (`Dispatch`): load (snapshot + fold in version order),
  handle, enforce the emit contract, append with the stream's UNIQUE
  constraint as the optimistic-concurrency guard (typed `ConflictError`,
  automatic retry against fresh state), run subscribed policies in the same
  transaction (depth-capped), write outbox rows for published events,
  snapshot every N.
- **Lazy decode**: the local log read (`readLog`) decrypts every row but
  decodes none: the payload rides `Event.raw` and each runner decodes only
  the types its own subscription check already let through, into a copy
  (the fan-out buffer hands one `*Event` to every runner at once, so
  decoding must never write to the shared pointer). Decoding eagerly for
  everyone meant one row of an undeclared or `@retired` type failed the
  whole batch for every runner sharing the buffer, including the ones that
  never look at that type. Aggregate replay (`loadState`) and bus
  deliveries (`eventFromEnvelope`, already subscription-filtered) decode
  as they always did. The cost of lazy decode is that an undeclared type
  is now invisible until something subscribes to it, so `Migrate` closes
  the gap: after the DDL and table/series diff it reads
  `SELECT DISTINCT type FROM loom_events` for the service and fails if any
  stored type is absent from the registry (active or `@retired`). Loud
  once at deploy time beats every runner tripping on it forever.
- **Global sequence**: `loom_events.global_seq` (identity). Projections and
  local processes are checkpointed catch-up readers over it — rebuildable,
  no bus, no publish-to-self hack for async self-handling. Both are elected
  so a scaled-out service runs one of each: a projection step by a
  transaction-scoped advisory lock (folds + checkpoint in one tx); the
  local-event processes by a per-service leader holding a session-scoped
  lock on a dedicated connection (`election.go`). Not per event: a
  reaction is an external call, and a lock that pinned a pool connection
  through it doubled every process step's connection use and starved
  projections and Dispatch (measured: projection lag ×3). A lease costs
  one connection, only while leading; hand-over is at-least-once.
- **Process start position**: a missing checkpoint row reads as sequence
  0, so a process deployed into a service with history would react to all
  of it on its first run. `Start` closes that: before a process runner can
  step, it inserts the runner's checkpoint at the current head unless the
  process declares `@from(origin)` (`ON CONFLICT DO NOTHING` — an existing
  row, from a redeploy or another instance, is never moved). Projections
  are deliberately exempt: folding from the origin is what a read model
  is.
- **A projection halts; it does not retry forever or skip.** An event a
  projection cannot decode, fold, or write stops it at that event: the
  events before it commit, the checkpoint stops just short of it, and the
  same transaction records the stall on the `loom_checkpoints` row —
  `failing_seq` (the event), `attempts`, `last_error`, `stalled_since`
  (first failure only). Skipping would be worse than stopping: a read
  model folded out of order is wrong in ways no later event repairs.
  The runner backs off by attempts (twice the poll, doubling, capped at
  five minutes) instead of refolding the batch on every wake — the
  incident this replaces retried one unwritable row every 2s for six
  hours. The next step that gets past the event clears the stall. The
  way out is the operator's: fix the fold and `Rebuild`, or
  `POST /runners/{name}/skip` (`Client.SkipStalled`), which parks the
  event to `loom_dead_letters` in park's shape and advances the
  checkpoint past it in one transaction under the runner's lock.
  `GET /runners` carries `stalled`/`failing_seq`/`last_error`/
  `stalled_since`, so the read surface shows what skip acts on. Every
  runner is loud without being noisy: `runner step failed` logs at most
  once a minute per runner, with the failures since the last line; a
  runner whose checkpoint row still doesn't exist one poll after `Start`
  logs `runner never checkpointed` under the same ceiling (an idle
  runner seeds its row, so silence means it never got to step — a lock
  held elsewhere, a starved pool). Processes keep their own policy:
  retry, then park and move on. The four columns are additive; a
  consumer upgrading runs `Migrate` before the new runners start.
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
- **Durable retries** (`@retry(max, min..max)`, `retry.go`): the fixed
  in-process attempts (three, 100/200/300ms) cover a blip, not an outage
  of minutes. A process declaring `@retry` does not park when they run
  out: the reaction is written to `loom_timers` as a retry row —
  `command_type = 'loom:retry'`, key
  `loom:retry:process:<name>/<service>:<global_seq>` (one per process and
  event), `command` holding the event in dead-letter shape (@pii
  re-sealed), the durable attempts so far and the last error — and the
  timer runner's existing lease re-fires it (claim -> commit -> fire, see
  Timers below). Each durable attempt is one reaction, after a backoff of
  full jitter over `[min, min(min·2^(n-1), max)]`. The reaction runs with
  no transaction open; its outcome is written afterwards in a short
  transaction of its own, every write conditional on the row still being
  the version the lease claimed (leased `fire_at` and `command`).
  Success deletes the row (and marks a foreign event processed in
  `loom_dedup`); failure re-arms it with the next backoff; the `max`-th
  failure parks the event to `loom_dead_letters` in park's shape
  (attempts = 3 + max, runner `process:<name>`, redrivable as ever) and
  deletes the row in the same transaction. Keying by (process, event) makes it converge: arming
  inserts `ON CONFLICT DO NOTHING`, and a redelivery of an event a retry
  row already owns (a checkpoint rewound by a crash, a bus redelivery) is
  skipped rather than reacted to twice. Retry rows fire on any instance,
  like timers, not only the process leader — the lease is the election. **Visible while it waits**: between the immediate attempts
  and the eventual park the reaction is a `loom:retry` row on
  `GET /timers` (its `fire_at` is the next attempt; overdue flags a stuck
  runner) and counts in `/stats`' `timers_pending` and the timers gauge;
  each failed attempt logs `durable retry failed; re-arming` at WARN with
  the attempt and max, and each fire is a `loom.retry.fire` span. No
  schema change: the rows reuse `loom_timers`' columns.
- **Metadata**: correlation ids propagate across dispatches and the bus;
  causation records the triggering event. Both are columns, not folklore.
- **Observability**: OTel API only (no SDK dependency, no exporters, no
  config — a no-op until the deployment installs global providers). Spans
  at every execution seam; W3C trace context rides the envelope's `trace`
  field so consumers join the producing dispatch's trace across the bus;
  counters/histograms at the natural code points and DB-observed gauges
  (outbox depth/age, dead letters, timers, effects, per-runner lag) on
  the SDK's collection cycle. Correlation/causation ids ride spans as
  attributes. Logs are the same stance: loom logs through the `slog`
  handler the deployment gives it, and `loom.CloudLogHandler(w)` is an
  opt-in JSON handler a consumer's `main` wires when that deployment is
  Google Cloud — `level`→`severity` (DEBUG/INFO/WARNING/ERROR; slog's
  WARN is Cloud Logging's WARNING), `msg`→`message`, and the context's
  OTel trace promoted to Cloud Logging's trace/span fields. Without it a
  plain `slog.NewJSONHandler` writes `level`, which Cloud Logging does
  not read, and `severity>=ERROR` never finds `runner step failed`.
- **Context-injected reads**: every handler/reaction invocation carries
  read access (`loom.Load`/`GetRecord`/`GetEntity` on the ctx), so
  implementations never hold a client and registries wire without the
  set-client-after-New dance. Reads only — dispatch from inside a handler
  would nest units of work; reactions return commands.
- **Read models**: one `loom_entities` jsonb doc table by default; `@table`
  opts an entity into its own typed table (`loom_t_<service>_<entity>`, one
  real column per state field, non-scalars as jsonb). Generated code owns
  the shape — DDL, columns, typed upsert extractor — so the runtime never
  reflects; reads come back doc-shaped via `to_jsonb(row)` minus the meta
  columns, so folds and typed decodes share one path. `Migrate` applies the
  DDL plus an additive-only declarative diff: missing columns added, type
  drift a loud error, never an `ALTER TYPE` or drop (the remediation is
  drop table → `Migrate` → `Rebuild`; a storage swap is exactly the same
  move — a perf change, not a semantic one). `@table` + `@pii` is a
  validation error.
- **Records** (`loom_records`): state-of-record persistence for the
  ledger/balance class. Row-locked writes, version per write; emitted
  events enter the log with the record's stream identity (projections and
  processes see them) but never rebuild the record. Deliberately a separate
  table from `loom_entities`: projection Rebuild truncates entities and
  must never touch records.
- **Timers** (`loom_timers`): durable scheduled commands, written in the
  scheduling unit's transaction. Keyed idempotently (default: command type
  + target) so redelivered reactions overwrite; `loom.CancelTimer` deletes
  by the same key. The runner goes **claim -> commit -> fire**, and no
  transaction is open while it dispatches (ADR 0002). The claim is one
  autocommitted statement that leases a batch of due rows: `UPDATE …
  SET fire_at = now() + 5m WHERE (service, key) IN (SELECT … FOR UPDATE
  SKIP LOCKED LIMIT n) RETURNING …`. SKIP LOCKED separates concurrent
  pollers only while that statement runs; after it, the pushed-out
  `fire_at` keeps them apart. Each claimed timer then fires in its own
  unit of work (three in-process attempts). A fire that still fails is
  parked (runner `timer`). Either way the row is deleted afterwards,
  conditionally: `WHERE fire_at = <leased> AND command = <claimed>`. A
  reaction to the fired command that re-armed the same key has changed
  that row (`writeTimer`'s upsert) and has made the next firing, so the
  delete leaves it alone. Delivery is at-least-once. A process that dies
  between the claim and the delete leaves a leased row that comes due
  again when the lease expires. A fire that outlasts the lease may be
  repeated by another poller. A runner stopping mid-batch hands its unfired
  claims back at their original due time. Before this change the claim's
  FOR UPDATE was held across the fires. A same-key re-arm then waited on
  that lock while holding the namespace append lock, and the poller waited
  on the dispatch, which is a cycle Postgres cannot detect (runsheet,
  2026-09-25). A leased row shows on `GET /timers` with its lease expiry
  as `fire_at`. The same claim carries `@retry` processes' durable retry
  rows (`loom:retry`, above).
- **Batches** (`loom_batches`/`loom_batch_items`): durable chunked fan-out
  of many commands (CopyFrom insert, SKIP LOCKED chunk claims, stale-claim
  reclaim). Per-item outcomes are recorded (at-least-once per item — use
  deterministic ids); the batch row is live progress, streamable over SSE.
  Reactions enqueue atomically via `loom.AsBatch`.
- **Effects** (`loom_effects`): journaled external calls — the outbox's
  cousin for the request/response direction. `loom.Once(ctx, key, fn)` in a
  process reaction claims a row (committed before fn runs), settles it with
  the JSON result or the error, and replays `done` results on every retry
  or redelivery. The settle write runs on `context.WithoutCancel` of the
  reaction's ctx: once the call has returned, its outcome is a settled fact,
  so a per-step deadline that fires while the call was in flight records
  `failed` (or `done`) instead of leaving the row in doubt. An unsettled
  claim (crash between call and record) is *in doubt*. Before declaring
  it, the claim consults what the service told it: an `@idempotent` effect
  (the call is safe to repeat) is re-claimed like a failed one — attempts
  bumped, back to running — and re-runs; otherwise a `Config.Reconcile`
  hook registered for the effect name
  (`func(ctx, scope, key) (result, executed, err)`) is asked: executed
  settles the row `done` with the hook's result (on `WithoutCancel`, like
  every settle) and `Once` replays it; not executed re-claims and re-runs;
  an error keeps the doubt. With neither — the default for most effects —
  the runtime refuses to re-run it and parks the reaction with
  `EffectInDoubtError`: an operator resolves it (recording what actually
  happened on the other side) and redrives the dead letter, in one call
  with `POST /effects/resolve {…, "redrive": true}` /
  `ResolveEffect(…, ResolveOptions{Redrive: true})`. The redrive finds the
  letter from the effect's scope, `process:<name>/<service>:<global_seq>`:
  runner `process:<name>`, envelope `service` and `global_seq`. The resolve
  commits first and stands if the redrive fails (the letter stays; a
  plain redrive retries it); no letter yet (the reaction is still in its
  retries) is not an error — the next retry sees the resolution.
  At-most-once with loud ambiguity, the strongest guarantee non-idempotent
  APIs admit. Failed settles re-run on retry: an error return is the
  handler asserting the call did not happen.
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
- **Series** (`series.go`): the third persistence shape — append-only
  observations (price ticks, readings, scraped sales) at volumes the
  log must not carry, and with none of its guarantees needed: no
  commands, no events, no reactions, no outbox, no global_seq traffic.
  `AppendSeries` bulk-inserts with `ON CONFLICT DO NOTHING` on
  `(service, namespace, dims-or-keys, time)`, so any batch replays
  idempotently — the crawler's retry story is the insert itself.
  Queries are the raw time-range read (filters on real columns) and
  `date_trunc` buckets (count/avg/min/max/last, grouped by dims) —
  date_trunc rather than time_bucket so the SQL is engine-portable.
  The engine question the log settled stays settled here: the
  declaration is engine-neutral, and Migrate *detects* TimescaleDB —
  extension present, the table becomes a hypertable (`create_hypertable`,
  `migrate_data => TRUE`); absent (Cloud SQL), a BRIN time index rides
  the same escalation path as the log. Series data is NOT rebuildable
  (the table is the only copy), which is why the additive column diff
  reports incompatible drift for hand migration instead of the
  drop-Migrate-Rebuild remediation, and why Reset's sweep is the only
  thing that ever empties one.
- **HTTP API** (`api.go` / `query.go`): the registry drives a complete
  mounted surface — command dispatch, filtered entity/record queries
  (validated field names, parameterized values, typed numeric/bool
  comparisons), log browsing, ops stats. This surface is ops-private;
  the deployment guards it. The GATEWAY is the public edge and carries
  the authorization model: `graphql.Access{Namespaces, All, Mutate,
  Mutations}` resolved per request by a `Config.Auth` hook (or the
  deployment's own middleware via `WithAccess`) — namespace scoping on
  every read/write/subscription and, via `Protect`, on Files downloads
  and Streams watches; `All` is god mode, unlocking namespace-less
  cross-namespace list queries (`Query.AllNamespaces`). Authentication
  itself (JWT/API key/session parsing) stays the deployment's job —
  loom only consumes the resulting capability.
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

## Parent–child (1-*) across aggregates

The MassPayout problem: a parent with many child aggregates, and a UI
that wants to watch the children as a group. Aggregates are consistency
boundaries, so the parent must NOT fold its children (contention
hotspot, consistency lie). The relationship lives in three places:

1. Child events carry the parent id (`PaymentSettled { mass_payout_id }`).
2. Read models re-group children by parent — `key(field)` routes an
   event to the row named by that payload field instead of the event's
   own aggregate id, and `@fold` hands the projection's fold to a
   once-generated stub for shapes assignment folds can't express
   (`settled_count++`, per-child status maps). Checkpointing, the
   advisory lock, and rebuild stay framework-owned; hand-written folds
   must stay deterministic because rebuild refolds the whole log.
3. A process drives the parent's own transitions (`on PaymentSettled ->
   RecordPaymentOutcome`); the process runner serializes the commands,
   so no conflict storm.

The UI watches either the parent-keyed progress row (`{x}Changed`) or a
live filtered list: `{x}sChanged(namespace, where, order, limit)` is a
subscription form of every list query — requery on log wake-ups, diff,
re-send the whole list. No delta bookkeeping, so rows entering and
leaving the filter just work; size it with where/limit (tables, not
unbounded feeds).

## The gateway is the public surface

Services stay on the private network; the gateway serves everything a
UI needs, federating at the client level (it holds `*loom.Client`s, not
HTTP proxies): `/graphql` for queries, mutations, and `{x}Changed`
subscriptions — served over SSE on the same endpoint (`Accept:
text/event-stream`, graphql-sse-shaped `next`/`complete` events,
EventSource-compatible GET) via graphql-go's native `Subscribe` and the
exported `Client.Watch` wake-ups — plus `graphql.Files` for downloads
and `graphql.Streams` for raw resumable entity/aggregate watches. Only
watch paths pass through Streams; the ops surfaces (events log, stats,
console, redrive) never face the internet. GraphQL numbers: schema
`int` is int64 and emits/serves the `Long` scalar everywhere — money in
cents must not squeeze through a 32-bit Int (decided 2026-07-17).

## Uploads: large files without bytes in the domain

Files never enter the log or cross the bus — events carry a `FileRef`
(id, key, name, content type, size), bytes live behind the `BlobStore`
seam (gblob on GCS, `DirBlobStore` locally — same chunked-PUT/308
resumable protocol, so upload client code is identical in dev). The
runtime brokers the session: `POST /uploads` (or the gateway's
`create{Name}Upload` mutation) opens a resumable session server-side
and returns the session URL; the browser PUTs chunks straight to
storage. Zero file bytes transit the service — which also sidesteps
Cloud Run's request-size ceiling.

The lifecycle is domain-visible by declaration, not convention:

```
upload Contract {
  on started  -> RequestContract   // optional: session opened
  on uploaded -> AttachContract    // object finalized and verified
}
```

`on uploaded` is never driven by the client claiming completion: it
fires from storage's own finalize signal (GCS Object Finalize →
Pub/Sub → `gblob.Watch` → `FinalizeUpload`; the dev store calls it
synchronously on the last chunk). FinalizeUpload Stats the object and
builds the FileRef from what storage actually holds, then dispatches;
deliveries are at-least-once, dedup'd on the object key in
`loom_dedup` like foreign events. Fold caveat worth knowing: folds
merge by field name, so give the `started` event a different field
name than the state's — a merely-requested file must not read as
attached.

Object keys are stream-prefixed (`service/namespace/streamID/upload/
fileID`), which is Shred's lever: shredding a stream deletes its
objects outright alongside its data key (bytes are outside the DEK's
reach, so deletion, not crypto-shred). A GCS lifecycle rule on
incomplete sessions cleans up abandoned uploads; `@pii` on a `file`
field seals the *reference* at rest like any other field.

The one place the provider shows through is the chunk dialect the
browser speaks at the session URL, so `UploadSession` carries a
`protocol` discriminator (`gcs-resumable` today; `s3-multipart`
reserved for S3/R2). Clients switch on it — never assume the dialect —
so adding a Cloudflare R2 or S3 store later is additive: new store
package + new client adapter, no contract break.

Downloads keep the services private: `FileRef.downloadUrl` points at
the gateway's `graphql.Files` handler, which routes by the key's
service prefix and streams through the owning client — the gateway
federates at the client level, so no service HTTP surface is involved.
The per-service `GET /files` remains for internal/ops use. A signed-URL
upgrade later changes what ServeFile does (redirect instead of stream),
not who calls it.

GraphQL note: `FileRef.size` rides a `Long` scalar — GraphQL `Int` is
32-bit and files are not. Long now exists in the SDL contract and the
runtime gateway for future opt-in use by `int` fields.

## The console (M2)

`/console` on every service: an embedded, self-contained page over the
JSON endpoints. Overview (health cards, batches, event volumes), Design
(the registry as a document — commands' emit contracts, reactions'
dispatch contracts via `ReactorDef.Subs`, effects, PII markers), Events
(log browser), Issues (checkpoint lag per runner — the stuck-detection
signal — with a stalled projection's failing seq, error, and a skip
button over `POST /runners/{name}/skip`; in-doubt effect resolution,
dead-letter redrive, overdue timers).
Deliberately no framework UI dependency and no build step; a topology
graph and the Performance tab are the deferred follow-ons.

## Not yet built (tracked on the issue)

Console topology graph + Performance tab, old-envelope compat codec for
gpub (migration on-ramp), persisted process state + timeouts, upcasters
beyond aliases, given/when/then harness generation, `loom extract` (legacy
on-ramp, returning from tag `m1-extraction`), replay-parity harness, table
migrator, OTel, TypeScript target.
