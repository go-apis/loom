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
Subscriptions from the SDL fragments ride the services' SSE endpoints
instead of the gateway.

## The console

Every service carries its ops UI: open `/console` on any mounted service.
**Overview** (outbox, dead letters, effects, batches, event volumes),
**Design** (aggregates → commands → events, reactions with their dispatch
contracts and effects, projections — the schema as the runtime runs it),
**Data** (browse read models and records with filters, fetch any row or
aggregate by id), **Events** (log browser: filter by
type/aggregate/correlation, inspect payloads), **Issues** (runner lag
against the log head, in-doubt effects with resolve, dead letters with
redrive, overdue timers). One embedded
self-contained page over the JSON endpoints — no build step, no external
assets, auth is whatever wraps the mount.

Filters are `field=value` with `.gte .lte .gt .lt .ne .like` suffixes,
compiled to parameterized jsonb SQL; `order`, `limit`, `offset` paginate.
Auth is deliberately not Loom's job — mount behind your middleware.

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
