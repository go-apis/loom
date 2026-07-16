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

## Storage (schema v2)

Postgres via pgx, hand-written SQL, no ORM. Events carry a global sequence
(the backbone for catch-up readers), correlation/causation ids, and a
schema version. Optimistic concurrency is a typed `ConflictError` with
automatic dispatch retry. Published events go through a transactional
outbox (advisory-lock relay, SKIP LOCKED, ordering keys) to a pluggable
`Bus` — in-memory today; Pub/Sub next.

## Status

Fresh rebuild (design of record: the es-v2 tracking issue). Working today:
the runtime core, the toolchain, and a two-service example under
`internal/e2e` whose cross-service loop (order → invoice → payment → ship,
correlation intact across the bus, dedup, rebuild) runs as the E2E suite.
The M1 extraction/topology tooling lives at tag `m1-extraction` and returns
as the legacy migration on-ramp.

See [DESIGN.md](DESIGN.md) for the SDL grammar and runtime semantics, and
[go-apis/loom-example](https://github.com/go-apis/loom-example) for the
example as a standalone repo.
