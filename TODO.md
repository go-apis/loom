# Loom — what's next

The framework backlog, roughly ordered. Each item has enough context to
pick up cold. Shipped so far (v0.24.0): runtime core, timers, records,
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
rejected; non-scalars ride jsonb columns and reject filters loudly),
console topology graph (v0.20: Design tab draws the flow — hand-rolled
layered SVG, longest-path layering + barycenter ordering, hover-to-trace,
foreign/upload edges dashed; uploads table added; tabs deep-link via
#hash), upcasters (v0.21: `upcast X @from(n)` — code-first hops via
generated EventUpcasts interface + stub, raw JSON n → n+1 chained in
decode(); contiguous-coverage validation; strict-when-declared:
unliftable or newer-than-registry stored versions are loud UpcastErrors,
no-upcast events keep permissive decode so additive changes stay free;
events only — commands at rest have no stored version, see Parked),
OTel (v0.22: API-only instrumentation, no-op without SDK providers —
spans at every seam incl. loom.effect outcomes; W3C trace context rides
Envelope.Trace so consumers join the producing dispatch's trace across
the bus; counters/histograms + DB-observed gauges incl. per-runner lag;
e2e proves one trace id across dispatch→publish→consume→reaction),
gateway auth (v0.23.x: Access{Namespaces, All, Mutate, Mutations}
resolved per request by a Config.Auth hook or WithAccess middleware;
namespace scoping on reads/writes/subscriptions/Files/Streams via
Protect; Namespace scalar on lists with "*" = every namespace, god
access only (Query.AllNamespaces); denials are per-field errors, failed
hook = 401; nil hook = open gateway for trusted mounts), generated
admin UI (v0.24: loomgraphql.UI("/graphql") — self-contained page built
from introspection at load: list views w/ filters + live {x}sChanged
toggle (fetch-streamed SSE so the Bearer header works), doc detail,
mutation forms from input types, token/namespace header incl. "*",
?token=&ns=&view= dev links), declared joins (v0.25: `join field ->
[service.Entity] via fk` on entities — gateway auto-wires single/list
resolvers at compose time, skips edges whose target service isn't
mounted, cfg.Joins still overrides; kills the hand-written Join closures
in ten99's main.go).
M6 still owes the Performance tab (throughput, lag, fold times).

## 1. Old-envelope compat codec for gpub

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
- Command-shape migration: timers/batch items store command JSON with no
  schema_version column; long-lived timers crossing a shape change rely
  on additive compatibility today. Needs a version column + a
  decodeCommand() hook if it ever bites.
- Projection Rebuild has no HTTP/console surface — the ten99 @table
  cutover needed hand-SQL (truncate + delete checkpoint row). Add
  POST /projections/{name}/rebuild + a console button.
- Foreign-event projections (would collapse ten99's RecipientMirror
  aggregate+process into a plain projection).
- TypeScript target for the schema (payloads are already JSON Schema).
- Gateway auth follow-ons: rate limits, per-field read masking (@pii
  fields for non-god callers?), playground header editor for tokens.

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
