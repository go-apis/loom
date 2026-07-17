# Loom — what's next

The framework backlog, roughly ordered. Each item has enough context to
pick up cold. Shipped so far (v0.19.0): runtime core, timers, records,
HTTP/SSE surface, OpenAPI/GraphQL emitters, batches (+AsBatchKeyed),
effects journal, @pii (states/events/commands) + crypto-shred, gpub on
pubsub/v2, folders layout, context-injected reads, console (Overview/
Design/Data/Events/Issues), GraphQL gateway + playground, loomtest,
uploads (`file` type + `upload` blocks, BlobStore seam, gblob resumable
GCS + Watch, DirBlobStore, shred deletes stream files), gateway-only
public surface (Files + Streams passthrough + {x}Changed subscriptions
over SSE on /graphql), Long everywhere (schema int emits/serves Long —
money is int64, decided 2026-07-17), live lists + 1-* (v0.18:
`{x}sChanged` filtered list subscriptions; projection `key(field)`
routing + `@fold` hand-written fold stubs — the masspayout shape),
`@table` typed per-entity tables (v0.19: opt-in `entity X @table` →
generated DDL + tables_gen.sql artifact + typed upserts; queries/ORDER BY
hit real columns; Migrate applies an additive-only declarative diff —
type drift errors with the drop→Migrate→Rebuild remediation; @table+@pii
rejected; non-scalars ride jsonb columns and reject filters loudly).

## 1. Console topology graph

The Design tab is tabular; the design of record wanted the drawn graph
(command → event → reaction → command, cross-service consumes). All data
is already in `/registry` (ReactorDef.Subs has dispatch contracts).
Self-contained: hand-rolled SVG layered layout in console.html — no
external libs (the console is dependency-free by rule). M6 adds the
Performance tab (throughput, lag, fold times) later.

## 2. Upcasters beyond aliases

Schema versions exist on events (`@v`) and aliases handle renames;
payload-shape migration doesn't exist. Shape: schema-declared
`upcast EventName @from(1) { ... }`? or generated hook interface the
decode path calls when stored schema_version < registry version. Decide
schema-first vs code-first; decode chokepoints are `decode()` +
`decodeCommand()`.

## 3. OTel

Spans for Dispatch/UoW, runners, effects, bus publish/consume; metrics
for the /stats numbers (outbox depth, lag, dead letters, effect states).
Correlation/causation ids already flow — join them to trace ids. Was a
day-one objective (#61 comment 3); becomes urgent with real deployments.

## 4. Old-envelope compat codec for gpub

`gpub.Codec` seam exists; implement the old eventsourcing Event JSON
(service/namespace/aggregate_id/type/by/timestamp/data/metadata — no
global_seq, so consumer dedup needs a service:aggregate:version fallback
key). Only needed when the six-service inflow migration unparks (M5,
parked in favor of ten99).

## Parked / later

- M5 migration machinery: `loom extract` on-ramp returns from tag
  `m1-extraction`; replay-parity harness (new runtime replays prod
  stream → identical state) gates any cutover.
- M6 performance: log partitioning by global_seq range + rollups if
  volume demands (deliberately no TimescaleDB — Cloud SQL).
- Domain/integration event split as a language feature (today: the
  private/published pair convention, e.g. TaxDocRecorded /
  RecipientDocumented in ten99).
- Foreign-event projections (would collapse ten99's RecipientMirror
  aggregate+process into a plain projection).
- TypeScript target for the schema (payloads are already JSON Schema).
- Gateway: auth hooks? — auth stays deployment middleware by design;
  revisit only if field-level authz becomes real.

## Upload follow-ons (small, when needed)

- `r2blob` (Cloudflare R2, S3-compatible) — Chris is eyeing a move of
  workers/file storage to Cloudflare. The contract is ready:
  `UploadSession.protocol` discriminates dialects and `s3-multipart`
  is reserved. What R2/S3 needs beyond a BlobStore impl: multipart has
  no single self-authenticating session URL, so the service must sign
  a presigned URL PER PART and accept a completion call — two new
  endpoints (`POST /uploads/{id}/parts?n=`, `POST
  /uploads/{id}/complete` with ETags), plus session state to remember
  the multipart upload id (a loom_uploads row or object metadata).
  Finalize signal: R2 event notifications ride Cloudflare Queues (no
  Pub/Sub) — either a Worker forwards them to an HTTP endpoint on the
  service (needs an authenticated /uploads/finalize hook) or the
  completion call doubles as the signal (acceptable: completion is
  server-side, not client say-so, since the service verifies via Stat
  before dispatching). UI: one new uploader adapter keyed off
  `protocol`; Uppy's @uppy/aws-s3 multipart plugin fits.

- Console: render `uploads` from /registry in the Design tab (data
  already served) and show them in the future topology graph.
- Signed download URLs on gblob (`GET /files` streams through the
  service today — fine for docs, wrong for video-sized reads).
- Upload progress SSE? The client knows its own progress; only needed
  if other sessions must watch an upload happen.

## Standing invariants (do not regress)

- No reflection on the dispatch path (boot-time schema building is fine).
- Stubs are never clobbered; loomgen always regenerates.
- Nothing is silently dropped: retries → loud parking, effects → in-doubt.
- Full TINs/@pii never cross the bus; published events reject @pii.
- File bytes never enter the log or the bus — events carry FileRefs;
  `on uploaded` fires only from storage's finalize signal, never from
  the client claiming completion.
- Console/playground stay embedded and dependency-free.
- Old lib (go-apis/eventsourcing) is frozen to bugfix-only.
