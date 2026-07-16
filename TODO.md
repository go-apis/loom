# Loom — what's next

The framework backlog, roughly ordered. Each item has enough context to
pick up cold. Shipped so far (v0.15.0): runtime core, timers, records,
HTTP/SSE surface, OpenAPI/GraphQL emitters, batches (+AsBatchKeyed),
effects journal, @pii (states/events/commands) + crypto-shred, gpub on
pubsub/v2, folders layout, context-injected reads, console (Overview/
Design/Data/Events/Issues), GraphQL gateway + playground, loomtest.

## 1. `@table` — typed per-entity tables (the deferred perf milestone)

Read models live in the `loom_entities` jsonb doc table; filters compile
to `(data->>'x')::numeric` comparisons the GIN index can't accelerate.
Fine at current volume, wrong at scale and for SQL/BI ergonomics.
Deliberately deferred as "a perf milestone, not a semantic change":
projections are rebuildable, so switching storage = create table, reset
checkpoint, refold.

Shape: opt-in `entity FormList @table` → `loom generate` emits
`CREATE TABLE` DDL (schema knows every column and type), typed upsert
code for projectionStep, a declarative diff against the live shape (NOT
AutoMigrate — that lesson is paid for), and the query layer targets real
columns. Cheaper interim: generated SQL views over the doc table.

## 2. Gateway subscriptions

The SDL fragments declare `Subscription` (entity/aggregate watches); the
gateway deliberately doesn't serve them — graphql-go's subscription
support is weak. Options: bridge to the existing SSE endpoints
(documented status quo), graphql-transport-ws on the gateway backed by
the same pg LISTEN wake-ups the SSE streams use, or swap executor.
Decide when a UI actually wants live queries.

## 3. `Long` scalar — CONTRACT DECISION, needs Chris

GraphQL `Int` is 32-bit; schema `int` is int64. Cent totals past ~$21M
overflow. Flipping emitted `Int` → custom `Long` changes the contract for
existing clients — decide deliberately (all ints? opt-in `int(long)`
format? next contract rev?). Emitter (gen/graphql.go), runtime gateway,
and OpenAPI (int64 format already correct there) must move together.

## 4. Console topology graph

The Design tab is tabular; the design of record wanted the drawn graph
(command → event → reaction → command, cross-service consumes). All data
is already in `/registry` (ReactorDef.Subs has dispatch contracts).
Self-contained: hand-rolled SVG layered layout in console.html — no
external libs (the console is dependency-free by rule). M6 adds the
Performance tab (throughput, lag, fold times) later.

## 5. Upcasters beyond aliases

Schema versions exist on events (`@v`) and aliases handle renames;
payload-shape migration doesn't exist. Shape: schema-declared
`upcast EventName @from(1) { ... }`? or generated hook interface the
decode path calls when stored schema_version < registry version. Decide
schema-first vs code-first; decode chokepoints are `decode()` +
`decodeCommand()`.

## 6. OTel

Spans for Dispatch/UoW, runners, effects, bus publish/consume; metrics
for the /stats numbers (outbox depth, lag, dead letters, effect states).
Correlation/causation ids already flow — join them to trace ids. Was a
day-one objective (#61 comment 3); becomes urgent with real deployments.

## 7. Old-envelope compat codec for gpub

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

## Standing invariants (do not regress)

- No reflection on the dispatch path (boot-time schema building is fine).
- Stubs are never clobbered; loomgen always regenerates.
- Nothing is silently dropped: retries → loud parking, effects → in-doubt.
- Full TINs/@pii never cross the bus; published events reject @pii.
- Console/playground stay embedded and dependency-free.
- Old lib (go-apis/eventsourcing) is frozen to bugfix-only.
