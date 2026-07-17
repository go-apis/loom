# Loom design

The design of record is the es-v2 tracking issue (Design v2). This file
covers what's implemented and the decisions embedded in the code.

## SDL grammar

```
schema      := "service" IDENT decl*
decl        := aggregate | record | entity | event | consume | policy
             | process | projection | type
aggregate   := "aggregate" IDENT directives? "{" (state | command | event | upload)* "}"
record      := "record" IDENT "{" (state | command | event | upload)* "}"
state       := "state" fields
command     := "command" IDENT fields? "->" identList
event       := "event" IDENT directives? fields?
upload      := "upload" IDENT "{" ("on" ("started"|"uploaded") "->" IDENT)* "}"
consume     := "consume" IDENT "." IDENT fields?
policy      := "policy" IDENT "{" on* "}"
process     := "process" IDENT "{" (on | effect)* "}"
effect      := "effect" IDENT
projection  := "projection" IDENT "->" IDENT "@fold"? "{" projOn* "}"
projOn      := "on" eventRef ("key" "(" IDENT ")")?
on          := "on" eventRef ("->" identList)?
eventRef    := IDENT | IDENT "." IDENT          // qualified = foreign
type        := "type" IDENT fields
entity      := "entity" IDENT "@table"? fields
fields      := "{" (IDENT ":" ftype "!"? "@pii"?)* "}"
ftype       := builtin ("(" IDENT ")")? | IDENT | "[" ftype "]"     ("?" = nullable)
builtin     := string int float bool uuid timestamp bytes any map file
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
- an `upload` needs an `on uploaded` command; both lifecycle commands must
  belong to the enclosing aggregate/record and carry exactly one required
  `file` field (loom fills it); upload names are service-unique — they
  become API surface (`create{Name}Upload`)
- a projection's `key(field)` must name a required uuid field on that
  event's payload — a missing key would route to the nil row

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
signal, in-doubt effect resolution, dead-letter redrive, overdue timers).
Deliberately no framework UI dependency and no build step; a topology
graph and the Performance tab are the deferred follow-ons.

## Not yet built (tracked on the issue)

Console topology graph + Performance tab, old-envelope compat codec for
gpub (migration on-ramp), persisted process state + timeouts, upcasters
beyond aliases, given/when/then harness generation, `loom extract` (legacy
on-ramp, returning from tag `m1-extraction`), replay-parity harness, table
migrator, OTel, TypeScript target.
