# Loom

A schema-first event-sourcing framework for Go. You declare aggregates,
commands, events, and reactions in a `.loom` schema; `loom generate` emits
the typed models, folds, and registry; you implement the handler stubs; the
runtime executes them on Postgres. Change the schema and the compiler walks
you through what's left — the gqlgen loop, for event sourcing.

```
service orders

aggregate Order @snapshot(5) {
  state {
    status: string
    total_cents: int
  }

  command PlaceOrder {
    items: [OrderItem]!
    currency: string!
  } -> OrderPlaced

  event OrderPlaced @publish {
    status: string!
    total_cents: int!
  }
}

process shipOnPayment {
  on billing.InvoicePaid -> ShipOrder
}
```

## The loop

```sh
loom init orders        # loom.yml + schema/orders.loom
$EDITOR schema/orders.loom
loom generate           # models, folds, registry (regenerated) + stubs (yours)
go build ./...          # compile errors are your to-do list
```

Stubs are generated once and never rewritten — your business logic lives in
ordinary Go files implementing generated interfaces. Everything else
regenerates on every run.

Handlers read state through their context — `loom.Load`, `loom.GetRecord`,
`loom.GetEntity` — so handler structs hold only genuine external
dependencies, and there's no client to plumb into the registry. Reads are
the only injected capability: dispatching from inside a handler would nest
units of work, so reactions return commands instead.

Stubs land flat in the service root by default; `layout: folders` in
loom.yml gives each kind its own package instead —

```
myservice/
  loomgen/         # generated, regenerated every run
  aggregates/      # yours, generated once
  records/
  policies/
  processes/
  registry.go      # wires &aggregates.X{}, &processes.Y{}, …
```

## Execution semantics (there are exactly three)

| declaration | runs | guarantees |
|---|---|---|
| `policy` | inside the producing transaction | atomic with the triggering event; local events only |
| `process` | async | local events: checkpointed off the log (no bus); foreign events: bus + dedup; retries then loud parking to dead letters |
| `projection` | async | checkpointed catch-up over the global sequence; entity writes + checkpoint in one tx; `Rebuild()` refolds from history |

Plus two persistence shapes: `aggregate` (event-sourced: handlers return
events, state folds) and `record` (state-of-record: ledgers, balances —
handlers mutate state directly; emitted events are announcements into the
log, never a rebuild source).

Projections handle parent–child (1-\*) shapes without the parent
aggregate folding its children: `key(field)` routes an event onto the
row named by that payload field, and `@fold` hands the fold to a
once-generated stub for what assignment can't express:

```
projection massPayoutProgress -> MassPayoutProgress @fold {
  on MassPayoutStarted
  on PaymentSettled key(mass_payout_id)   // child event → parent's row
}
```

Read models live in a shared jsonb doc table by default. `@table` opts an
entity into its own typed table — one real column per state field, filters
and `ORDER BY` hitting real columns instead of `(data->>'x')::numeric`
casts, and the table is plain SQL for BI:

```
entity OrderSummary @table {
  status: string
  total_cents: int
  items: [OrderItem]     // non-scalars ride a jsonb column
}
```

`loom generate` emits the `CREATE TABLE` DDL (reviewable in
`loomgen/tables_gen.sql`) and a typed upsert; `Migrate` creates the table
and applies a declarative, additive-only diff — missing columns are added,
type drift is a loud error, nothing is ever altered or dropped behind your
back. Projections are rebuildable, so switching storage (or fixing drift)
is: drop table, `Migrate`, `Rebuild`. `@table` and `@pii` are incompatible
by design — sealed ciphertext in a typed column would serve neither.

Note `Migrate` is per service once `@table` is in play: the shared
`loom_*` DDL is identical from any client, but each service's typed
tables ride its own registry — deployments that share one database must
call `Migrate` on every service's client, not just one.

## Timers

Durable scheduled commands, written in the same transaction that decided
them. Reactions return them through the ordinary command channel:

```go
loom.After(&CancelOrder{...}, 5*time.Second)   // schedule (keyed, idempotent)
loom.CancelTimer(&CancelOrder{...})            // delete the pending timer
```

A runner fires due timers; a firing that keeps failing parks to dead
letters. Three-year W-8 expiries and 200ms test timers use the same
mechanism.

Commands declare what they emit (`-> OrderPlaced`) and reactions declare
what they dispatch — both are runtime-enforced contracts, so the schema can
never lie about the topology.

## Effects: calling the outside world

Everything above is safe to retry — which is exactly what makes an external
call (a tax filing, a payment capture) dangerous inside a process. Declare
the effect on the process and wrap the call in `loom.Once`:

```
process captureOnPaid {
  on InvoicePaid
  effect gateway_capture
}
```

```go
receipt, err := loom.Once(ctx, "gateway_capture", func(ctx context.Context) (string, error) {
    return gateway.Capture(ctx, invoice)
})
```

The claim commits before the call runs; the result is journaled; retries
and redeliveries replay the journaled result instead of calling again. If
the process crashes between call and record, the effect is *in doubt* —
the runtime refuses to guess, parks the reaction to dead letters, and an
operator settles it (`POST /effects/resolve` with what actually happened)
and redrives (`POST /dead_letters/{id}/redrive`). At-most-once, with loud
ambiguity instead of silent duplicates. Undeclared effect keys are runtime
errors — a typo must not mint a fresh journal identity.

## Event versions and upcasts

The log is append-only, so event shapes are forever — unless you migrate
on read. `@v(n)` versions an event's schema (stored per row); `@alias`
handles renames; `upcast` handles shape changes:

```
event OrderCancelled @v(2) {
  status: string!
  reason: string!      // new in v2
}

upcast OrderCancelled @from(1)
```

`loom generate` emits an `EventUpcasts` interface and a once-only stub;
you write the hop — payload JSON of v1 in, payload JSON of v2 out:

```go
func (u *Upcasts) OrderCancelledFromV1(data []byte) ([]byte, error) {
    var old struct{ Status string `json:"status"` }
    if err := json.Unmarshal(data, &old); err != nil { return nil, err }
    return json.Marshal(map[string]any{"status": old.Status, "reason": "unspecified"})
}
```

Hops chain at decode time (v1→v2→v3), so replays, projection catch-up,
bus deliveries, and redrives all see only the current shape; stored rows
are never rewritten. Keep hops deterministic — rebuilds run them again.
Declaring an upcast makes version handling strict for that event: a
stored version with no path to current (or one newer than the registry —
deploy skew) is a loud `UpcastError`, never a silent zero-value fold.
Events without upcasts keep the permissive unmarshal, so purely additive
changes don't need ceremony.

## PII: encrypted at rest, shreddable forever

Mark the fields that identify a person and give the client a key wrapper:

```
aggregate Payee {
  state {
    name: string
    tin: string @pii
    tin_last4: string
  }
  ...
}
```

```go
keys, _ := loom.LocalKeys(masterKey) // 32 bytes from your secret manager
cli, _ := loom.New(loom.Config{DB: db, Registry: reg, Keys: keys})
```

`@pii` fields are sealed with a per-stream data key everywhere they rest —
log, snapshots, read models, records, dead letters — and open on folds and
typed reads. `cli.Shred(ctx, ns, id)` (or `POST /shred`) deletes the key:
every copy of that stream's PII is permanently unreadable, reads and
rebuilds continue with the fields redacted to zero values. That's erasure
for an append-only store. Published events can't carry `@pii` (the schema
rejects it) — keep PII on private events and publish a scrubbed pair.
Filters can't match sealed fields; keep a plain derived field
(`tin_last4`) for lookups.

## Batches

`loom.AsBatch(cmds...)` from a reaction (enqueued atomically with the
triggering event), `cli.EnqueueBatch`, or `POST /batches` fan out thousands
of commands durably: chunked SKIP LOCKED claims, per-item outcomes, live
progress on the batch row (`GET /batches/{id}`, streamable). Delivery is
at-least-once per item — deterministic aggregate ids make redeliveries
converge.

## The service is the API

`cli.HTTPHandler()` mounts the whole service over HTTP, driven entirely by
the registry — no per-service handler code:

```
POST /commands/PlaceOrder                      dispatch (JSON body; 409 on conflict)
GET  /entities/OrderSummary?namespace=demo&status=shipped&total_cents.gte=1000
GET  /entities/OrderSummary/{id}               one row
GET  /records/LedgerEntry/{id}                 state-of-record reads
GET  /aggregates/Order/{id}                    folded state + version
GET  /events?correlation_id=...                log browser
GET  /events/stats?since=...                   counts by type
POST /batches                                  durable command fan-out
GET  /batches/{id}                             batch progress (+ /failures, /stream)
GET  /effects?status=running                   the effect journal
POST /effects/resolve                          settle an in-doubt effect
GET  /dead_letters                             parked deliveries
POST /dead_letters/{id}/redrive                re-run one parked delivery
POST /uploads                                  open a resumable file-upload session
GET  /files?key=...                            stream a stored file back
POST /shred                                    delete a stream's PII key and files (irreversible)
GET  /stats                                    outbox / dead letters / timers / effects health
GET  /console                                  the ops console (see below)
GET  /registry                                 the service as its schema sees it
GET  /runners                                  checkpoint lag per projection/process
GET  /timers                                   pending schedule (overdue flagged)
GET  /batches                                  recent batches
```

## Uploads: large files, chunked, event-sourced

Declare an upload on the aggregate/record that owns the file; its
lifecycle dispatches your commands, so the domain sees uploads as events
like everything else:

```
aggregate Order {
  state { contract: file? }
  command AttachContract { contract: file! } -> ContractAttached
  event ContractAttached { contract: file! }
  upload Contract {
    on uploaded -> AttachContract
  }
}
```

`POST /uploads` (or the gateway's `createContractUpload` mutation) opens a
resumable session and returns a URL the browser PUTs chunks to *directly*
— file bytes never transit the service, events carry a `FileRef`, and the
`on uploaded` command fires from storage's finalize signal (GCS → Pub/Sub
→ `gblob.Watch`), never from the client's say-so. Locally,
`loom.NewDirBlobStore` speaks the same chunked resumable protocol and
finalizes synchronously:

```go
store  := loom.NewDirBlobStore("data/blobs", "http://localhost:8099/blobs") // dev
// store, _ := gblob.New(ctx, gblob.Config{Bucket: "acme-uploads"})          // prod
cli, _ := loom.New(loom.Config{DB: pool, Registry: NewRegistry(), Blobs: store})
mux.Handle("/blobs/", http.StripPrefix("/blobs", store)) // dev store only
// prod: go store.Watch(ctx, projectID, "loom-uploads-orders", cli.FinalizeUpload)
```

Shred deletes a stream's files along with its data key.

## Testing: given/when/then

`loomtest` exercises generated handlers and reactions against the real
registry with no database — tests read like the schema:

```go
loomtest.Aggregate(t, reg, "Invoice").
    Given(&loomgen.InvoiceRaised{Status: "raised", AmountCents: 4000}).
    When(&loomgen.MarkInvoicePaid{CommandBase: base}).
    Then(&loomgen.InvoicePaid{Status: "paid", AmountCents: 4000})

loomtest.Reaction(t, reg, "raiseOnOrder").
    When("ns", orderID, &loomgen.OrderPlaced{TotalCents: 4000}).
    Then(&loomgen.RaiseInvoice{...})
```

`ThenNothing` asserts convergence no-ops, `ThenError` asserts guards, and
`Reading(...)` fakes `loom.Load` for reactions that read state. Effects,
projections, timers and batches stay e2e territory — this harness tests
domain decisions, not delivery.

## The gateway

One GraphQL endpoint over any number of loom services, built at boot from
their registries — no gqlgen build step, no resolver boilerplate, and it
matches the SDL contracts `loom graphql` emits (plus `id`/`namespace`/
`updatedAt` on rows):

```go
gateway, _ := loomgraphql.New(loomgraphql.Config{
    Services: []*loom.Client{recipientsCli, filingsCli},
    Joins: []loomgraphql.Join{{
        OnType: "FormList", Field: "recipient", Returns: "RecipientSummary",
        Resolve: func(ctx context.Context, src map[string]any) (any, error) { ... },
    }},
})
mux.Handle("/graphql", gateway)
mux.Handle("/files", loomgraphql.Files(recipientsCli, filingsCli)) // FileRef.downloadUrl points here
```

Mutations dispatch commands (`placeOrder(input: {...}) { status }`),
queries serve aggregates and filtered read-model lists, and Join fields
are the hand-written cross-service edges no generator should guess.
Subscriptions are served on the same endpoint over SSE — send
`Accept: text/event-stream` (GET with `?query=&variables=` works with
the browser's native EventSource) and each change arrives as an
`event: next` execution result. `{x}Changed(namespace, id)` watches one
doc; `{x}sChanged(namespace, where, order, limit)` is a live filtered
list — the whole fresh list on every change, which is how a masspayout
screen watches its payments settle. Raw
resumable watches also pass through via `loomgraphql.Streams`. Together
with `Files`, the gateway is the whole public surface: keep the
services' own HTTP handlers on the private network. Schema `int` fields
are served as the 64-bit `Long` scalar — GraphQL `Int` is 32-bit and
cent totals are not.

Because it's the public surface, the gateway carries the authorization
model. Authentication stays yours — parse a JWT, API key, or session in
the `Auth` hook (or your own middleware via `WithAccess`) — and return
an `Access` saying what the caller may do:

```go
gateway, _ := loomgraphql.New(loomgraphql.Config{
    Services: services,
    Auth: func(r *http.Request) (loomgraphql.Access, error) {
        claims, err := verify(r)               // your authentication
        if err != nil { return loomgraphql.Access{}, err }  // → 401
        if claims.Admin {
            return loomgraphql.Access{All: true, Mutate: true}, nil // god mode
        }
        return loomgraphql.Access{Namespaces: claims.Orgs, Mutate: true}, nil
    },
})
mux.Handle("/files", loomgraphql.Protect(auth, loomgraphql.Files(...)))
mux.Handle("/streams/", http.StripPrefix("/streams", loomgraphql.Protect(auth, loomgraphql.Streams(...))))
```

The gateway also ships a generated admin UI — a self-contained page
that introspects the schema at load time and builds itself from it, so
there is nothing to regenerate and nothing to drift:

```go
mux.Handle("/ui", loomgraphql.UI("/graphql"))
```

Entity list views with filters, ordering, and pagination; a **live**
toggle that rides the `{x}sChanged` subscription so rows update (and
flash) as events land; row-click doc detail; mutation forms generated
from the command input types; and a Bearer-token + namespace header, so
it's also the fastest way to *see* the Access model work — switch
tokens and watch namespaces appear and disappear (`"*"` included). Dev
links prefill: `/ui?token=…&ns=…&view=OrderSummary`.

`Namespaces` scopes every read, subscription, mutation, file download,
and raw watch; `Mutate` gates writes (optionally narrowed to specific
mutation fields via `Mutations`); `All` is god mode. List queries and
list subscriptions take the `Namespace` scalar, whose special value
`"*"` searches every namespace at once — god access only, and rows
carry their namespace (`orderSummarys(namespace: "*") { namespace … }`).
Denials are per-field GraphQL errors; a failed hook is a 401 before
anything executes. No hook = the open pre-auth gateway, for mounts
behind trusted middleware. The playground page itself always serves —
the queries it fires are enforced like any other.

## The console

Every service carries its ops UI: open `/console` on any mounted service
(tabs deep-link: `/console#design`).
**Overview** (outbox, dead letters, effects, batches, event volumes),
**Design** (the drawn topology — command → event → reaction → command,
cross-service consumes dashed, uploads and projections in place, hover a
node to trace its edges — plus the schema tables: aggregates, reactions
with dispatch contracts and effects, projections, uploads), **Data**
(browse read models and records with filters, fetch any row or aggregate
by id), **Events** (log browser: filter by type/aggregate/correlation,
inspect payloads), **Issues** (runner lag against the log head, in-doubt
effects with resolve, dead letters with redrive, overdue timers). One
embedded self-contained page over the JSON endpoints — no build step, no
external assets (the topology layout is ~80 lines of hand-rolled layered
SVG), auth is whatever wraps the mount.

Filters are `field=value` with `.gte .lte .gt .lt .ne .like` suffixes,
compiled to parameterized jsonb SQL; `order`, `limit`, `offset` paginate.
These per-service surfaces are ops-private by design — keep them off the
public edge (the gateway carries the public authorization model).

Streaming is SSE (browser-native, proxy/Workers friendly; deliberately not
gRPC — see DESIGN.md):

```
GET /events/stream?type=OrderPlaced&after_seq=0    live log tail, resumable
GET /entities/OrderSummary/{id}/stream             read-model watch
GET /aggregates/Order/{id}/stream                  state watch
```

The SSE id is the global sequence, so EventSource's automatic
Last-Event-ID reconnect resumes exactly where it left off. Instances wake
each other through pg LISTEN/NOTIFY.

## Observability

Loom instruments with the OpenTelemetry **API only**: no exporters, no
config knobs, everything a no-op until the deployment installs SDK
providers (`otel.SetTracerProvider` / `otel.SetMeterProvider`) — then it
all lights up.

Spans cover every execution seam: `loom.dispatch` (the unit of work, with
command names and correlation/causation ids), `loom.publish` (relay →
bus), `loom.consume` (bus → process, dedup included), projection and
process steps (only when there's work — idle polls are not traces),
`loom.timer.fire`, `loom.effect` (outcome: executed / replayed / failed /
in-doubt), batch items. Trace context rides the envelope (W3C
`traceparent` in a `trace` field), so a consumer's reaction **joins the
trace of the dispatch that published the event** — one trace from the
originating command through the bus to every downstream effect, across
services. Correlation ids ride every span as attributes, joining the
domain's own causality to trace ids.

Metrics mirror the `/stats` and `/runners` numbers: counters
(`loom.dispatch.count`, `.conflicts`, `loom.events.appended`,
`loom.outbox.published`, `loom.dead_letters.parked`,
`loom.consume.dedup_hits`, `loom.timers.fired`, `loom.batch.items`,
`loom.effects.calls`), histograms (`loom.dispatch.duration`,
`loom.runner.step.duration`), and DB-observed gauges on the SDK's
collection cycle (`loom.outbox.depth`, `.oldest_age`,
`loom.dead_letters.depth`, `loom.timers.pending`, `loom.effects.running`,
`.failed`, and `loom.runner.lag` per runner — the stuck-runner alarm as a
metric).

## Storage (schema v2)

Postgres via pgx, hand-written SQL, no ORM. Events carry a global sequence
(the backbone for catch-up readers), correlation/causation ids, and a
schema version. Optimistic concurrency is a typed `ConflictError` with
automatic dispatch retry. Published events go through a transactional
outbox (advisory-lock relay, SKIP LOCKED, ordering keys) to a pluggable
`Bus`: in-memory for tests and single binaries, Google Cloud Pub/Sub for
production:

```go
bus, _ := gpub.New(ctx, gpub.Config{ProjectID: "my-project"})
cli, _ := loom.New(loom.Config{DB: db, Bus: bus, Registry: reg})
```

One shared topic, a durable subscription per consumer group, ordering keys
per aggregate, nack-for-redelivery (loom's dedup and parking absorb
at-least-once). Runs against the Pub/Sub emulator with ordering disabled —
the emulator's ordered backlog is broken.

## Status

Fresh rebuild (design of record: the es-v2 tracking issue). Working today:
the runtime core, the toolchain, and a two-service example under
`internal/e2e` whose cross-service loop (order → invoice → payment → ship,
correlation intact across the bus, dedup, rebuild) runs as the E2E suite.
The M1 extraction/topology tooling lives at tag `m1-extraction` and returns
as the legacy migration on-ramp.

See [DESIGN.md](DESIGN.md) for the SDL grammar and runtime semantics,
[TODO.md](TODO.md) for what comes next, and
[go-apis/loom-example](https://github.com/go-apis/loom-example) for the
example as a standalone repo.
