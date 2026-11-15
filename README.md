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
loom init orders        # loom.yml (schema: schema/) + schema/orders.loom
$EDITOR schema/orders.loom
loom generate           # models, folds, registry (regenerated) + stubs (yours)
loom generate --check   # write nothing; exit non-zero naming stale or missing loomgen files
go build ./...          # compile errors are your to-do list
```

The schema is a directory. `schema: schema/` in loom.yml reads every
`*.loom` file under `schema/`, at any depth, as one schema: the files are
joined in path order and parsed once, so an aggregate in
`schema/billing/invoice.loom` can emit an event declared in
`schema/orders.loom`, and an error names the file and line
(`schema/billing/invoice.loom:12: …`). **A new aggregate is a new file** —
two features adding aggregates never edit the same file, so they never
conflict. `schema:` may also name a single file or a glob, and
`loom check schema/` checks a directory without generating.

Open every file with the service header. The first file *by path* must
start with `service orders` — and that is not necessarily the file you
think of as the home file: `billing/invoice.loom` sorts before
`orders.loom`. Later files may repeat the same header (a different name is
refused) or leave it out, but repeating it in every file means no rename
or new file can ever leave the first file headerless:

```
schema/
  billing/invoice.loom    # service orders; aggregate Invoice { … }
  orders.loom             # service orders; shared enums, types and events
  shipping/shipment.loom  # service orders; aggregate Shipment { … }
```

A declaration must end in the file it starts in.

A feature can add an enum member from its own file with `+=`; the home
file's values come first, then each extension in path order:

```
// schema/orders.loom
enum Priority { low normal }

// schema/expedite/expedite.loom
enum Priority += { high urgent }
```

A second plain `enum Priority { … }` is refused, so an accidental name
clash stays an error.

Stubs are generated once and never rewritten — your business logic lives in
ordinary Go files implementing generated interfaces. Everything else
regenerates on every run.

Handlers read state through their context — `loom.Load`, `loom.GetRecord`,
`loom.GetEntity` — so handler structs hold only genuine external
dependencies, and there's no client to plumb into the registry. Reads are
the only injected capability: dispatching from inside a handler would nest
units of work, so reactions return commands instead.

Closed value sets are `enum` declarations, referenced like named types:

```
enum TinStatus { unknown pending_match matched mismatched }
...
tin_status: TinStatus
```

An enum is its string value at rest and on the wire; generated code gets
a named string type with constants (`TinStatusMatched`) and `Valid()`,
commands with enum fields get a generated `Validate()` the dispatcher
runs before any handler (a value outside the set never reaches the
domain), the gateway serves real GraphQL enum types (invalid inputs die
at coercion), the SDL/OpenAPI contracts carry the value sets, and
`@table` columns stay `text`. Values must be legal GraphQL enum names —
`1099-NEC` stays a plain string.

An enum is declared once; any file may add members with
`enum TinStatus += { disputed }`. Values keep declaration order (the home
block's first, then each extension's in file-path order). An extension with
no home, a second home block of the same name, a value declared twice
across blocks, and an empty extension are refused with `path:line`.

Stubs land flat in the service root by default; `layout: folders` in
loom.yml gives each kind its own package instead —

```
myservice/
  loomgen/              # generated, regenerated every run
    loom_gen.go         # fixed: Impl, NewRegistry; names nothing in the schema
    upcasts_gen.go      # the EventUpcasts interface, once an upcast is declared
    look_gen.go         # the Look interface, once an aggregate declares @loop
    <aggregate>_gen.go  # one per aggregate and record: order_gen.go, …
    <name>_<kind>_gen.go  # one per policy, process, projection and
                        # schedule: ship_on_payment_process_gen.go, …
    <schema file>_types_gen.go  # one per .loom file: its enums, types,
                        # shared events, entities and series
    models_gen.go       # only what no schema file declares
    tables_gen.sql      # DDL for @table and series
  aggregates/           # yours, generated once
  records/
  policies/
  processes/
  registry.go           # impl.X(&aggregates.X{}), impl.Y(&processes.Y{}), …
```

Each aggregate and record has its own `loomgen/<snake name>_gen.go`
(`LedgerEntry` → `ledger_entry_gen.go`). It holds the aggregate's commands,
the events only it emits, its state and fold, its `Handlers` interface, and
its binding method on `Impl`. It registers itself with `NewRegistry`.
Each policy, process and `@fold` projection has its own file the same way
(`shipOnPayment` → `ship_on_payment_process_gen.go`): its `Reactions` or
`Folds` interface, its binding method and its definition. A projection
loom folds itself and a schedule each have a file with their definition
alone. Enums, types, shared events, `@table` entities and series are
registered by their schema file's `_types_gen.go`. No file lists the
whole registry: `loom_gen.go` holds `Impl` and `NewRegistry`, and reads
what each file registered for itself.
The fixed files name none of them. Adding one adds only its own
file, so two branches that each add an aggregate or a process do not
conflict in
`loomgen/` ([ADR 0008](docs/adr/0008-one-generated-file-per-aggregate.md)).
`registry.go` binds each one by its method:

```go
impl := loomgen.Impl{}
impl.Order(&Order{})                 // one call per aggregate and record
impl.ShipOnPayment(&ShipOnPayment{}) // and per policy, process and @fold projection
return loomgen.NewRegistry(impl)
```

`NewRegistry` adds aggregates, then records, policies, processes and
`@fold` projections, each kind in name order, so policies reacting to one
event run in name order. One whose
implementation was never bound panics at boot, naming the call to add
(`loomgen: aggregate Payee has no handlers — call impl.Payee(...)`).

## Wiring by convention

Under `layout: folders` every declaration's implementation has an
obvious home — `aggregates.Order`, `processes.ShipOnPayment` — and a
hand-kept `registry.go` adds only a line per declaration saying so: a
list every new declaration edits, and two branches adding one each
collide on. Set `wire:` and loom writes that line itself, in a file of
its own per declaration:

```yaml
schema: schema/*.loom
layout: folders
wire: wire
```

```
wire/order_aggregate_gen.go          impl.Order(&aggregates.Order{})
wire/shiponpayment_process_gen.go    impl.ShipOnPayment(processes.NewShipOnPayment(deps))
wire/wire_gen.go                     Impl(deps), Configure
```

The implementation of a declaration `X` is, in its kind's package, the
constructor `NewX` when there is one — taking nothing, or the one
dependency type every constructor of the service shares, which becomes
`wire.Deps` — and otherwise the type `X`. `registry.go` is written once
and builds the registry from `wire.Impl(deps)`; adding an aggregate or
a process adds a file and edits none. What a convention cannot say is a
`Configure` in a hand-written file of the package, run on the
implementation before it is bound:

```go
// wire/goal.go
func init() {
    Configure(func(g *aggregates.Goal, s *processes.Services) {
        g.InProgress = func(ctx context.Context, ns string, project uuid.UUID) ([]uuid.UUID, error) {
            return processes.GoalsInProgress(ctx, s, ns, project)
        }
    })
}
```

## Keeping loomgen current

A loom bump can change what `loom generate` emits, so the generated files
can go stale without a compile error. Put this in CI, in each directory
with a loom.yml:

```sh
go tool loom generate --check
```

It renders every loomgen file in memory, writes nothing, and exits non-zero
naming each file whose bytes differ from disk or that is missing. Stubs
(the files that are yours) are never compared. To fix a failure, regenerate
and commit:

```sh
go tool loom generate
```

Rendering is deterministic: the same schema gives the same files, byte for
byte. `generate` deletes a generated file that the schema no longer
produces, such as a renamed or removed aggregate's `<name>_gen.go`, and
prints `removed <path>`. `--check` names such a file stale. It recognises
loom's files by their `Code generated by loom generate` header and never
touches a file without it, or a stub.

If a rebase or merge conflicts under `loomgen/`, resolve the `.loom` files,
take either side of each generated file, regenerate, and commit what the
generator wrote. Never hand-merge generated code.

**Upgrading to one file per declaration.** The first regenerate moves each
aggregate's, record's, policy's, process's and `@fold` projection's code
into its own file. `Impl` loses the field it had for each, so a
`registry.go` that sets them no longer compiles. Move each to a binding
call of the same name; only `Upcasts` stays a field:

```go
// before
return loomgen.NewRegistry(loomgen.Impl{Order: &Order{}, ShipOnPayment: &ShipOnPayment{}})

// after
impl := loomgen.Impl{}
impl.Order(&Order{})
impl.ShipOnPayment(&ShipOnPayment{})
return loomgen.NewRegistry(impl)
```

Code that read a reactor back off the `Impl` (`impl.ShipOnPayment.X`) keeps
its own reference to it instead. Policies that react to the same event now
run in name order, where they ran in schema order. The regenerate also
removes `loomgen/registry_gen.go`: `Impl` and `NewRegistry` are in
`loom_gen.go` now, under the same names, and the registry's lists (enums,
types, projections, tables, schedules, series) come out in name order.

Or delete `registry.go` and run `go tool loom generate` to get a fresh stub.
Type names and the `Handlers` interfaces are unchanged.

`loom init` adds `tool github.com/go-apis/loom/cmd/loom` to the go.mod in
the current directory (go 1.24 or newer; run `go get -tool
github.com/go-apis/loom/cmd/loom@<version>` or `go mod tidy` so it has a
require line). The generator is then pinned by the same line a loom bump
moves, so the bump moves the generator's version with it. For an existing
service, add the directive yourself. See
[ADR 0005](docs/adr/0005-generator-is-pinned-and-checked.md).

### The agent skill

```sh
go tool loom skill          # writes .claude/skills/loom/SKILL.md at the repository root
go tool loom skill --check  # in CI, beside generate --check
```

Agents working in a loom service read this skill before they touch a
schema: the loop, the pinned generator, naming, effects. It is stamped
with the loom version that wrote it, so a bump rewrites it; `loom init`
writes it for a new service ([ADR 0007](docs/adr/0007-loom-writes-its-agent-skill.md)).

### Naming a reactor

Name a process or policy for what it does — `shipOnPayment`,
`productManifests` — not for the aggregate it serves. If a reactor does
export to an aggregate's or record's name, its `Impl` field takes its
kind (`FleetStallProcess`, `…Policy`, `…Projection`) so the package
compiles; its interface, stub and runner keep the declared name
([ADR 0006](docs/adr/0006-a-reactor-named-after-an-aggregate.md)).

## Execution semantics (there are exactly three)

| declaration | runs | guarantees |
|---|---|---|
| `policy` | inside the producing transaction | atomic with the triggering event; local events only |
| `process` | async | local events: checkpointed off the log (no bus), one instance at a time per runner; foreign events: bus + dedup; retries then loud parking to dead letters (`@retry(max, min..max)` re-arms an exhausted reaction as a durable timer first). A process with no checkpoint row starts at the log's head (`@from(head)`, the default): it reacts to what happens after it is deployed, never to the service's whole history. Declare `@from(origin)` to replay everything (a backfill) |
| `projection` | async | checkpointed catch-up over the global sequence, one instance at a time per runner; entity writes + checkpoint in one tx; `Rebuild()` refolds from history |

`Start` is safe on every instance of a scaled-out service. Each
projection step takes a transaction-scoped advisory lock (its folds and
checkpoint are one transaction anyway), so one instance folds at a time.
The local-event processes are led: one instance per service holds a
session-scoped advisory lock on a dedicated connection and its process
runners react; the rest yield — a reaction is an external call that must
not pin a pool connection, so the lease costs one connection, only while
leading. Postgres drops the lease with the session, so a crashed or
scaled-down leader is replaced at the followers' next poll; across that
hand-over the guarantee is the at-least-once processes carry anyway.
`Client.Leading()` says whether this instance leads. The relay and the
timer/batch runners claim with `SKIP LOCKED` and need no election.

A deploy moves the lease to the new build. The instance's generation
names the leader's lease session: `Config.Generation`, else
`LOOM_GENERATION`, which the deploy sets to its Unix time
(`gcloud run deploy … --update-env-vars LOOM_GENERATION=$(date +%s)`, on
every deploy), else on Cloud Run the revision's `createTime` from the
Cloud Run Admin API (the service account needs `run.revisions.get`). Not
the number in the revision's name: Cloud Run does not number revisions in
deploy order. A follower of a
strictly newer generation asks the leader to yield, the leader finishes
the reaction it is on and lets go, and a leader that does not answer
within two polls (a throttled or wedged instance) has its session ended.
An equal or older generation never takes the lease, and with no
generation the election is as above. See
[ADR 0010](docs/adr/0010-a-newer-generation-takes-the-process-lease.md).

A rollback is checked the other way. The store remembers the release
that first wrote each event type, and a build of an older release refuses
to `Start` against it with a `*loom.FloorError` naming the type and the
release to start instead, since it could not decode what is stored. Tell
loom the build's release with `Config.Release`, else `LOOM_RELEASE`: a
number that belongs to the code, the same every time one build is
deployed and larger for every later build (a build number, a commit
count). Not the generation, which a rollback's deploy raises. A build
that states none is never refused, and the floors it writes are zero,
which hold no build back. `Client.EventFloors` lists them. See
[ADR 0013](docs/adr/0013-an-event-type-has-a-release-floor.md).

Budget the pool for that: a started client parks one connection on
`LISTEN` for its whole life, plus the lease connection while it leads, and
everything else — `Dispatch`, `Load`, the readers, the relay — queues
behind what is left. `pgxpool` defaults to `max(NumCPU, 4)` connections,
so two services sharing one pool on a four-core host park all four and
deadlock. Size `MaxConns` for the services on the pool, not for the host.

A process reacts to what happens after it is deployed. On `Start`, a
process with no checkpoint row is checkpointed at the log's head before
its runner can take a step, so shipping a new process into a service
with years of history does not replay that history through it (and does
not perform its effects for it). Say so explicitly when you want the
opposite — a backfilling process:

```
process backfillTaxIds @from(origin) {   // @from(head) is the default
  on PayeeRegistered
}
```

Only a brand-new process is affected: a process that has checkpointed
before — a redeploy, a rolling restart — keeps the position it had.

A reaction that fails is retried three times in-process (100/200/300ms),
then parked to dead letters. That covers a blip, not a gateway that is
down for minutes. Declare `@retry` to keep trying durably first:

```
process captureOnPaid @retry(20, 5s..5m) {   // 20 more attempts, 5s..5m apart
  on InvoicePaid
  effect gateway_capture
}
```

Once the in-process attempts run out, the reaction becomes a `loom:retry`
row in `loom_timers` — keyed by process and event, so a redelivery joins
it rather than doubling it — and re-fires after an exponential, jittered
backoff between the bounds. It shows on `GET /timers` while it waits. Only
when the 20th durable attempt fails does the event park, exactly as it
would have without `@retry`. Journaled effects make the repeats safe: a
`done` capture replays, a failed one re-runs.
Projections are unaffected either way; folding from the origin is what a
read model is, and `Rebuild()` is how you replay one.

Plus three persistence shapes: `aggregate` (event-sourced: handlers return
events, state folds), `record` (state-of-record: ledgers, balances —
handlers mutate state directly; emitted events are announcements into the
log, never a rebuild source), and `series` (append-only time-series
observations — see below).

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

A list read that always filters and sorts the same way declares its index
in the entity block, columns in the order written:

```
entity OrderSummary @table {
  customer_id: uuid
  total_cents: int
  index (service, namespace, customer_id, updated_at desc nulls last)
}
```

The name defaults to `idx_<entity>_<columns>`. `loom generate` writes it
as `CREATE INDEX CONCURRENTLY IF NOT EXISTS …` into `tables_gen.sql`.
`Migrate` builds it concurrently on its own connection, so writes keep
flowing. It replaces an invalid index left behind by a failed build, and
does nothing when the index is already there and valid. `Rebuild` and
`Reset` empty tables with `DELETE`, so the index survives both
([ADR 0009](docs/adr/0009-a-projection-declares-its-indexes.md)).

Declare the read itself with `query` instead, and skip working out the
index by hand — the trailing tie-breaker column a paginated list needs
is easy to get wrong or leave out:

```
entity OrderSummary @table {
  status: string
  updated_at: time
  query (status, updated_at desc nulls last)
}
```

All columns but the last are equality filters; the last is the order
column. `query` desugars at parse time into the same index
`index (status, updated_at desc nulls last, id desc)` would, with `id`
appended automatically in the order column's direction as the stable
tie-breaker, then rides the same `tables_gen.sql`/`Migrate` pipeline as
`index`
([ADR 0011](docs/adr/0011-a-declared-query-generates-its-index.md)).

Note `Migrate` is per service once `@table` is in play: the shared
`loom_*` DDL is identical from any client, but each service's typed
tables ride its own registry — deployments that share one database must
call `Migrate` on every service's client, not just one.

## Series: time-series observations

Facts you record about the world — price ticks, readings, scraped sales
— are not domain decisions, and at their volumes they must never ride
the event log. `series` stores them in a typed per-series table, keyed
by dimensions plus time:

```
series PricePoint @time(recorded_at) @dim(product_id, grade) {
  product_id:  uuid!
  grade:       string!
  recorded_at: timestamp!
  price_cents: int!
}
```

No commands, no events, no reactions — appends go straight to the table
with `ON CONFLICT DO NOTHING` on the identity, so re-running a load
converges (`inserted` counts only what was new):

```go
inserted, err := cli.AppendSeries(ctx, "prices", rows...)   // rows: generated *PricePoint
```

Reads are the raw time-range query (filters compile to real columns)
and `date_trunc` buckets:

```go
points, _ := cli.QuerySeries(ctx, "PricePoint", loom.SeriesQuery{
    Namespace: "prices", Filters: []loom.Filter{{Field: "grade", Value: "psa10"}},
    Since: yearAgo, Ascending: true,
})
buckets, _ := cli.QuerySeriesBuckets(ctx, "PricePoint", loom.SeriesBucketQuery{
    Namespace: "prices", Value: "price_cents", Bucket: "month", By: []string{"grade"},
})
```

The declaration is engine-neutral: on a database with the TimescaleDB
extension, `Migrate` makes the table a hypertable; on plain Postgres
(Cloud SQL) it falls back to a BRIN time index. Same schema, same
queries either way. When the dims don't identify a row (an external
listing id does), `@key(external_id)` overrides the identity; the time
field is always part of it. Unlike read models, series data is NOT
rebuildable — the table is the only copy — so the additive migration
diff reports incompatible drift for hand migration instead of
drop-and-rebuild.

Buckets also take percentiles (`Percentiles: []float64{50, 95}` →
`bucket.Percentiles["p95"]`), and `Bucket: "all"` collapses the range
into one bucket per group — the summary-table shape: p50/p95 per dim
over a window in one query.

Observations that lose their value — request samples, error events —
declare how long they live:

```
series ReqSample @time(at) @dim(route) @key(sample_id) @retain(30d) {
  ...
}
```

On plain Postgres a `@retain` series is range-partitioned by day: the
runner creates the coming days' partitions hourly and drops the ones
past retention (DROP TABLE, never a bloating DELETE), and an append
into a day with no partition yet creates it first. On TimescaleDB it
is a hypertable with a retention policy. Adding `@retain` to a series
whose table already exists unpartitioned is reported as drift for a
hand migration, like any other incompatible series change.

`Config.SeriesPartitionAhead` sets how many days ahead the partitions
are kept (default 7). A deployment that runs Migrate as the schema
owner and serves as a role with DML rights only works too: Migrate
leaves two `SECURITY DEFINER` functions per service,
`loom_series_partition_<service>(tbl, day)` and
`loom_series_maintain_<service>(ahead)`, with the service's @retain
tables and their retention written into them, and the serving process
creates and expires partitions through those — Postgres refuses even
`CREATE TABLE IF NOT EXISTS` on an existing partition to a role without
CREATE on the schema.

The gateway serves each series as `{name}s` (range query),
`{name}Buckets` (aggregation, with `percentiles` and `ALL`), and an
`append{Name}s` mutation, under the same namespace authorization as
everything else.

## Read your writes

Read models are async. A request that dispatches and then reads the
projection in the same breath — a sign-in that grants and then mints
the token — races the runner, and usually wins by milliseconds. Make
it explicit: take the log head after the dispatch, wait for the
projection to reach it, then read.

```go
head, _ := cli.Head(ctx)                                  // after the Dispatch
settle, cancel := context.WithTimeout(ctx, 2*time.Second) // never block a request forever
defer cancel()
if err := cli.AwaitProjection(settle, "grantIndex", head); err != nil {
    log.Warn("grant index lagging; minting from what is there", "error", err)
}
rows, _ := cli.QueryEntities(ctx, "GrantIndex", ...)
```

The projection may be running on another instance; the checkpoint is
polled with a short back-off.

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

### Schedules: a command on an interval

A timer fires once. For work that recurs — a watchdog's look, a sweep,
a reconcile — declare a schedule and loom owns the clock:

```
/// Looks at every project's flow.
schedule flowWatch every 5m -> TickFlowWatch @in(platform)
```

The command is dispatched at the deploy and every interval after (`30s`,
`5m`, `1h`, `1d`), to one stream: `@in` names its namespace, and its id
is `@id("uuid")` or, without one, derived from the service and the
schedule's name — the same on every build. A scheduled command carries
no payload, so it may declare no required field.

The difference from a command that re-arms itself with `loom.After` is
what happens when a tick goes wrong. A self-re-arming chain ends the
first time its reaction fails, is not deployed, or is refused, and then
nothing ticks until someone notices. A schedule's row is moved on after
every tick, whatever came of it: a failed tick is logged and the next
one is the retry. It fires on the newest generation alive, one instance
at a time. Ticks missed while the service was down are not made up —
it fires once when it can and every interval from there. A schedule the
newest build no longer declares stops. `Status` lists each with its
next time.

### A function on an interval

Recurring work that is not a command — a sweep of an outside system, a
report — is the same schedule with a function for a tick:

```go
err := cli.Every("githubBudget", time.Hour, func(ctx context.Context) error {
    return logGitHubBudget(ctx)
})
```

Call it before `Start`. The function is called at the start and every
interval after, on one instance of the newest generation alive, and
again at the next interval whatever came of the last call. It is for
work the service does once, wherever it runs. A cache each instance
keeps in its own memory is not that: every instance refreshes its own,
with a ticker of its own.

### Looking again until it is done

Some reactions have nothing to do yet: the deploy is still rolling out,
the channel does not exist, the payment has not settled. Return
`loom.NotYet` and the process's `@retry` looks again later:

```go
func (h *AwaitDeploy) OnDeployRequested(ctx context.Context, evt *loom.Event, d *DeployRequested) ([]loom.Command, error) {
    run, err := h.Runs.Get(ctx, d.RunID)   // a read: call it directly
    if err != nil {
        return nil, err                    // a failure: the usual retries
    }
    if !run.Finished {
        return nil, loom.NotYet("the deploy is still rolling out")
    }
    return []loom.Command{&RecordDeployOutcome{...}}, nil
}
```

```
process awaitDeploy @retry(40, 5s..5m) {
  on DeployRequested -> RecordDeployOutcome
}
```

`NotYet` is not a failure: the reaction is not tried three times at
once, nothing is logged as failing, and the durable timer starts
straight away, backing off between `@retry`'s bounds. When its attempts
run out the event is parked, saying how many times it looked — a thing
that never became ready is for a person. A process with no `@retry`
that answers `NotYet` is parked at once, saying so. Make the look
itself directly rather than through `loom.Once`: `Once` remembers a
result, and a remembered "not ready" would be the answer every time.

### Reacting again to what is already behind

A process reacts to an event once, as it passes. `ReplayProcess` hands
it events it has been past — for a process added after the things it
is about exist, or whose outside call was down while they went by:

```go
// once for every project that exists, not once per thing that ever happened to it
res, err := cli.ReplayProcess(ctx, "publishSection", loom.ReplayOptions{LatestPerAggregate: true})
```

Narrow it with `Types`, `Namespace` and `AggregateID`; `DryRun` counts
first. The reaction is the same one, with the same retries, its commands
dispatched and a failure parked (under `process:<name> (replay)`). Two
things differ from the process's own pass: its checkpoint does not move,
and its effects run again — they are journaled under the replay's id,
so `loom.Once` does not answer them from the first pass. A reaction
that converges replays safely; one that sends an email will send it
again, so narrow the replay to what needs it. Also on the service's
API: `POST /processes/{name}/replay`.

### Sending a command twice

A caller that does not hear back sends the command again. Most commands
take that well — the handler sees what the first one left — and some do
not: one that appends, one whose second run is a second charge. An
idempotency key says "this is the one I sent before":

```go
ctx, replayed := loom.WithIdempotencyKey(ctx, key)
err := cli.Dispatch(ctx, cmd)   // nil both times; replayed() says which
```

A dispatch under a key that has already committed, for the same command
and stream, does nothing. The key is written in the dispatch's own
transaction, so one that failed left none and runs when it is sent
again, and two sent at once resolve to one. Over the gateway the key is
the `Idempotency-Key` header and a repeat answers `status: duplicate`;
the MCP `dispatch` tool takes it as `key`. Keys are kept seven days.

## Effects: calling the outside world

Everything above is safe to retry — which is exactly what makes an external
call (a tax filing, a payment capture) dangerous inside a process. Declare
the effect on the process and wrap the call in `loom.Once`:

```
process captureOnPaid {
  on InvoicePaid
  effect gateway_capture
  effect receipt_email @idempotent   # safe to repeat: re-runs instead of doubting
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
and redrives (`POST /dead_letters/{id}/redrive`) — or both at once, with
`"redrive": true` on the resolve. At-most-once, with loud ambiguity instead
of silent duplicates. Undeclared effect keys are runtime errors — a typo
must not mint a fresh journal identity.

Two opt-ins resolve the doubt without a person. An effect marked
`@idempotent` declares the call safe to repeat, so an unsettled claim just
re-runs. A `Reconcile` hook asks the other side what happened before doubt
is declared:

```go
loom.Config{
    // ...
    Reconcile: map[string]loom.ReconcileFunc{
        "gateway_capture": func(ctx context.Context, scope, key string) (json.RawMessage, bool, error) {
            cap, found, err := gateway.FindCapture(ctx, scope+"/"+key) // the idempotency key the call sent
            if err != nil || !found {
                return nil, false, err // err: stays in doubt; not found: re-runs
            }
            return json.RawMessage(strconv.Quote(cap.ID)), true, nil // settles done; Once returns it
        },
    },
}
```

```
POST /effects/resolve
{"scope": "process:captureOnPaid/billing:1042", "key": "gateway_capture",
 "result": "cap_123", "redrive": true}
→ {"status": "resolved", "redriven": 7}
```

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

Retiring an event is the other direction. Deleting the declaration leaves
the log holding rows nothing can name, so mark it instead:

```
event PromotionExpired @retired {
  code: string!
}
```

The struct stays generated, so replays and rebuilds still decode old rows
and then fold nothing — but emitting it, subscribing to it (policy,
process, or projection), or upcasting it are all validation errors. Runners decode
lazily, only the types they subscribe to, so a retired event costs a
subscriber nothing. `Migrate` enforces the other half: it fails if the
service's log holds any type the registry doesn't declare, so a dropped
declaration is caught at deploy time rather than by whichever runner
happens to read that row.

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

`@secret` is `@pii` for credentials — tenant SMTP passwords, API keys,
webhook signing secrets. Everything above applies (sealed at rest,
shreddable, no published events), plus the value is **write-only outside
the process**: `GET /aggregates/...`, `GET /records/...`, their streams,
and the GraphQL gateway's get and changed-subscription fields all return
a stable fingerprint (`secret:sha256:xxxxxxxx`) instead of the plaintext,
so an admin console can show "configured" and detect rotation without ever
seeing the value. In-process reads (`Load`, `Record` in handlers,
processes, effects) see plaintext — that's where the credential gets used.
Entities can't project `@secret` fields at all (read models are queryable
by design); project an explicit boolean or the fingerprint instead.

```loom
record MailConfig {
  state {
    smtp_host: string
    smtp_user: string
    smtp_password: string @secret
  }
}
```

Data keys are wrapped by `Config.Keys`: `loom.LocalKeys` (32-byte master
key from your secret manager) for dev and self-hosting, or
[`gkms`](gkms/gkms.go) for Cloud KMS envelope wrapping — the master key
never leaves KMS, key use is IAM-gated and audit-logged, and loom's DEK
cache keeps it to ~one Decrypt per stream per process. Switch wrappers (or
rotate a master key) with `loom rewrap`: DEKs are re-wrapped in place,
sealed data untouched.

```sh
loom rewrap --db "$DSN" --from-local $OLD_HEX \
  --to-kms projects/P/locations/L/keyRings/R/cryptoKeys/K
```

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
POST /series/PricePoint                        bulk append observations (idempotent on identity)
GET  /series/PricePoint?namespace=...&since=   time-range read, filters on real columns
GET  /series/PricePoint/buckets?bucket=month&value=price_cents&by=grade   bucketed aggregates
GET  /events?correlation_id=...                log browser
GET  /events/stats?since=...                   counts by type
POST /batches                                  durable command fan-out
GET  /batches/{id}                             batch progress (+ /failures, /stream)
GET  /effects?status=running                   the effect journal
POST /effects/resolve                          settle an in-doubt effect ("redrive": true re-runs its dead letter too)
GET  /dead_letters                             parked deliveries
POST /dead_letters/{id}/redrive                re-run one parked delivery
POST /uploads                                  open a resumable file-upload session
GET  /files?key=...                            stream a stored file back
POST /shred                                    delete a stream's PII key and files (irreversible)
GET  /stats                                    outbox / dead letters / timers / effects health
GET  /console                                  the ops console (see below)
GET  /registry                                 the service as its schema sees it
GET  /runners                                  checkpoint lag per projection/process, and a stalled projection's failing seq/error
POST /runners/projection:{name}/skip           park a stalled projection's failing event to dead letters and move past it
GET  /timers                                   pending schedule and @retry rows (overdue flagged)
GET  /batches                                  recent batches
```

`QueryEntities`/`QueryRecords` take an opt-in `Query.Fields []string`: name
the state fields a list read actually needs and the SQL selects only those
columns instead of the whole row — nil/empty keeps the existing whole-row
read. GraphQL's list fields (`{x}s`, `{x}sChanged`) opt in automatically
from the query's own selection set
([ADR 0009](docs/adr/0009-a-list-read-selects-only-asked-fields.md)).

A list read answers one page: at most `loom.MaxPage` (500) rows, in
`OrderBy` and then id. The next page is the same query with `After` set
to the cursor of the last row (`Row.Cursor(orderBy)`), so rows that tie
on the order field are each read once
([ADR 0014](docs/adr/0014-list-reads-page-by-cursor.md)). To read a
whole list, let loom walk it:

```go
// every row, or an error and none: never part of the list
rows, err := loom.Every(ctx, cli.ListEntities("GoalIndex"), loom.Query{Namespace: ns})

// a page at a time, for a list too long to hold
for page, err := range loom.Pages(ctx, cli.ListEntities("WorkItemIndex"), loom.Query{Namespace: ns, OrderBy: "seq"}) {
    if err != nil { return err }
    ...
}
```

`cli.ListRecords(name)` is the same for a record, and any
`func(ctx, loom.Query) ([]loom.Row, error)` is a `loom.ListRead`. A row
that changes place in the order while the pages are read is read where
it stands: order by a field that does not change, or leave `OrderBy`
empty in `Every`, which reads ascending by `updated_at` so a row written
to meanwhile is met again and answered once.

In a test, a fake list read answers `loomtest.Page(rows, q)`: the rows
after the one `q.After` names, at most `q.Limit`.

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

## Loops, waits and approvals

Some aggregates need looking after, not just reacting to: a goal that
must keep moving, an order that must not sit unpaid. Mark the aggregate
`@loop` and loom calls its look for every live instance — at once when
it is created, within a second of any event for it, and at least every
minute:

```
aggregate Order @loop {
  ...
}
```

```go
func (l *Look) LookOrder(ctx context.Context) ([]loom.Command, error) {
    told := loom.LookingAt(ctx)           // which order, and why it was called
    state, _, _ := loom.Load(ctx, "Order", told.Namespace, told.ID)
    order := state.(*loomgen.Order)
    switch {
    case order.Shipped:
        loom.LookDone(ctx)                // nothing left to look after
        return nil, nil
    case order.Paid:
        return []loom.Command{&loomgen.ShipOrder{...}}, nil
    }
    return nil, nil                       // nothing to do yet
}
```

A look reads how things stand and answers the one next step, so calling
it again changes nothing: it is level-triggered, like a controller. The
commands it returns run as one unit of work. `loom.LookAgain(ctx, d)`
asks for the next look sooner than the minute.

**A wait** says the loop is waiting on someone: a name, an owner and a
deadline, none of them optional.

```go
loom.WaitFor(ctx, loom.Wait{Name: "payment", Owner: "customer:" + id, Deadline: told.At.Add(48 * time.Hour)})
```

When the deadline passes with the wait still open, the look is called
and told so, once: `told.ExpiredWait("payment")`. `loom.StopWaiting`
ends a wait that has been answered. Outside a look, `Client.Wait` and
`Client.EndWait` do the same.

**An approval** is a wait on a person's yes or no, kept as a record:

```go
loom.AskApproval(ctx, loom.Approval{
    Name: "refund", What: "Refund order " + id + " in full",
    Role: "support-lead", Deadline: told.At.Add(4 * time.Hour),
    AtDeadline: loom.AtDeadlineReject,   // or approve, or expire
})
```

`Client.Approvals` lists what is waiting on a role, and
`Client.AnswerApproval` takes the answer from someone who holds it. With
no answer by the deadline the approval becomes what it declared. Either
way the look is called and reads the outcome:

```go
if refund, ok := told.Approval("refund"); ok && refund.Status == loom.ApprovalApproved { ... }
```

Through the gateway, `approvals` and `answerApproval` are served for
any service with a `@loop` aggregate, and the caller must hold the
approval's role in the namespace.

`loomtest.Loop` tests a look with no database: when it would next be
called, what it is told, and what it decides.

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

For those, and for anything else that needs the store, `loomtest.Postgres(t)`
gives the test a database of its own on a real server: it creates one on
the server `LOOM_TEST_PG` names, answers a pool on it, and drops it when
the test ends (the test is skipped when there is no server to reach).

```go
pool := loomtest.Postgres(t)
cli, _ := loom.New(loom.Config{DB: pool, Registry: NewRegistry(), Release: 5})
_ = cli.Migrate(ctx)
```

## Testing: the access contract

A rule that holds for the principals someone thought to test and is
skipped for one nobody ran is the bug an ordinary test misses. Package
`contract` runs them all. Give it the gateway, the kinds of principal
the service's policy tells apart, and the namespaces the test seeded:

```go
contract.Run(t, contract.Suite{
    Gateway: gateway, // loom/graphql.New(...), over a seeded store
    Principals: []contract.Principal{
        {Name: "person", Access: person},
        {Name: "org admin", Access: orgAdmin},
        {Name: "estate staff", Access: staff},
    },
    Namespaces: []string{"acme", "globex"},
    Labels:     map[string]string{archived.String(): "the archived project"},
    Golden:     "testdata/contract.json",
})
```

It reads the gateway's own schema, so every query and mutation is in
it the moment it exists, and records for each principal:

- **who may run each operation**, by the gateway's own probe
  (`Executor.Allowed`);
- **which rows each list shows it**, in each namespace, by label or id,
  or the refusal it got;
- **which fields are hidden from it**, per type.

That record is the contract, a JSON file committed beside the code.
`Run` fails for every operation and principal that now differs from it,
naming both: `query projects as estate staff in acme: shown [the
archived project, the live project]; the contract says shown [the live
project]`. Record it the first time, and after a change that is meant,
with `LOOM_CONTRACT_UPDATE=1`, and read the diff: the file is where a
reviewer sees what each kind of caller is shown. Give seeded rows ids
that are the same on every run, or a label.

Mutations and reads that take more than a namespace are probed, not
run: the contract says who may run them, not what they answer.

A client that publishes what it asks of the gateway is checked the same
way. `contract.ReadAsks` reads a file of the queries and mutations it
sends and the fields it reads on each type (the shape of runsheet's
console manifest, `/.well-known/gateway-asks.json`); set it as
`Suite.Asks` and every ask must be served to every principal in
`AsksFor`. An asked query the policy refuses a principal, one the
gateway does not have, and a field hidden from a principal each fail by
name: `the client asks query parcels, which is refused as org admin`.
`AsksExcept` excuses what the client asks only on behalf of some.

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
queries serve aggregates and filtered read-model lists. Cross-entity
edges are declared in the schema with `join` — the gateway wires the
resolvers at compose time:

```
entity FormList @table {
  recipient_id: uuid
  join recipient -> recipients.RecipientSummary via recipient_id
}

entity RecipientSummary @table {
  join forms -> [filings.FormList] via recipient_id
}
```

A single join follows the local `via` field to the target's row id (a
dangling or null reference resolves to null); a `[list]` join collects
the target rows whose `via` field equals this row's id. Targets may
live in another service (`service.Entity`) — the edge only materializes
on a gateway that mounts both sides, so partial gateways stay valid,
same-namespace scoping applies, and same-service targets are validated
by `loom check`. The `Joins` config hook remains for resolvers no
declaration can express — hand-written entries land after declared ones,
so they can override a field.

Joins compose recursively (every joined row carries `id`/`namespace`,
so the target's own joins keep resolving — cycles included), which is
why the gateway bounds query nesting: `Config.MaxDepth` (default
`DefaultMaxDepth` = 20, deep enough for GraphiQL's introspection;
negative disables) rejects over-deep requests before execution. Within
one execution a per-request loader memoizes join lookups, so a cyclic
traversal or a list sharing one foreign key reads each row once —
`Client.Entities` (the batched `Entity`) is there when a hot path wants
one round-trip for many rows. Subscriptions skip the memo: their
context outlives a single execution.
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

Coarse `Mutate` can't express "owner in acme, viewer in globex" — that
per-namespace granularity is `@role`, declared on the command itself so
the write rules live in the schema, not in a hand-kept table in the
deployment:

```
command ShipOrder @role(owner, shipper) -> OrderShipped
```

The Auth hook maps each caller's memberships to `Access.Roles`
(namespace → role; `"*"` grants everywhere), and the gateway demands
one of the declared roles in the mutation's *target* namespace. Role
names are yours — loom compares strings, nothing more. Ungated commands
are unchanged, and an `All` access with no `Roles` map keeps god-mode
semantics, so existing deployments don't move. Everything else stays
where it was: `@role` gates only the GraphQL gateway — in-process
`Dispatch` (policies, processes, timers) and the service's own HTTP API
never consult it — and state-dependent rules ("only placed orders
ship") stay in the command handler. `loom graphql` stamps the gate on
the SDL artifact as a `@role(anyOf: [...])` field directive, so
consumers can read the contract.

A role says what a caller is in a namespace. `@allow` says what the
caller is at all, and what it owns:

```
aggregate Parcel @table @allow(kind: admin) @allow(kind: courier, depot_id: caller.depot) {
  state { depot_id: uuid  label: string }
  command RelabelParcel @allow(kind: admin) @allow(kind: courier, depot_id: caller.depot) {
    label: string!
  } -> ParcelRelabelled
}
```

Each `@allow` is one rule and any one may hold. `attr: value` compares
an attribute your `Auth` hook put in `Access.Attrs` (`kind: courier`);
`field: caller.attr` demands that a field of the thing operated on
equals a caller attribute. On an aggregate, record or entity the rules
gate its reads; on a command, the command. The gateway does the
comparing:

- **A command** is checked against the stored state. Its payload counts
  only where the state has no value — a thing being created — so a
  caller cannot claim another owner's thing by what it sends.
- **A get** is refused unless the stored doc is the caller's.
- **A list**, and its subscription, is narrowed to the caller's rows
  whatever else it filters by.

A field can be for fewer callers than the record it is on:

```
aggregate Project @allow(kind: admin) @allow(kind: session, id: caller.project) {
  state {
    name: string
    billing_email: string @allow(kind: admin)
  }
}
```

A session reads its project and is not shown `billing_email`. It is
not handed an empty one either: the field resolves to null and the
response says why, at the field's path and under a code a client can
test —

```json
{ "data": { "project": { "name": "Foghorn", "billingEmail": null } },
  "errors": [{ "message": "hidden: Project.billingEmail is shown when kind is admin",
               "path": ["project", "billingEmail"],
               "extensions": { "code": "FIELD_HIDDEN" } }] }
```

— so a page written once for every caller still loads for the ones a
field is not for. A caller may not filter or order a list by a field
hidden from it, which would read the value back a comparison at a
time. A field's rule compares the caller alone (`attr: value`); whose
record it is stays a rule on the record. That makes what is hidden the
same on every record a caller reads, so the MCP tools can say it
before the caller asks: `describe` marks the field, a default `get` or
`list` leaves it out and says so beside the data, and asking for it by
name is refused. This is the one case in which the gateway answers in
part: a `Policy` that forbids a field still denies the whole operation.

An upload answers to the same gates. Opening one asks for its commands
to be run — by the store, when the bytes land — so `create<Name>Upload`
is decided against each of those commands' `@role` and `@allow`, for
the caller opening it.

`@allow` sits beside `@role`: both must pass. Attribute names and
values are yours. A scoped caller with no `Attrs` meets no rule, so
adding `@allow` fails closed until `Auth` grants attributes; `All`
access with no `Attrs` map keeps god-mode semantics. A custom `Policy`
sees the rules as `Decision.Rules` (with `List` and a lazy `Target`)
and inherits the checks by delegating to `DefaultPolicy`. Like `@role`,
`@allow` gates only the gateway — and so the MCP tools — never
in-process `Dispatch`.

`loom access` prints all of this as one document — every command and
generated read under the name the gateway serves, with its roles, its
rules read aloud and whether it is planned first. Check it in, and
`loom access --check` fails a build whose schema says otherwise, so a
change to who may run what shows in the pull request that makes it.

The gateway also records who ran a command. Set `Access.Actor` in your
`Auth` hook (`user:<id>`, `session:<job>` — your ids) and it is stamped
on the metadata of every command the request dispatches, over GraphQL
and through the MCP tools, so the events say who caused them. A planned
command's events name whoever ran the plan, with `plan:<id>` as their
cause.

Some commands should not be run on a caller's first word. Mark one
`@plan` and the gateway holds it back:

```
/// Throw a parcel away. It cannot be brought back.
command DiscardParcel @plan -> ParcelDiscarded
```

Its mutation now changes nothing. It works out what running the
command would lead to and answers a `Plan`: the events, the target and
its version, an expiry, and an id.
`runPlan(namespace, id)` runs that plan — exactly the command that was
planned, since the plan's id is all it takes:

- **Once.** The plan is spent when it runs, whether or not the command
  then succeeds.
- **Only if nothing moved.** The command runs only while its target is
  at the version it was planned against; otherwise the plan is stale,
  and is planned again.
- **For a while.** A plan expires (`loom.DefaultPlanTTL`, 15 minutes).
- **By whoever may run the command.** Planning and running each answer
  to the command's own `@role` and `@allow`, for the caller of that
  step. Each is its own `Decision` — `Stage` is `StagePlan`, then
  `StageRun` — so a `Policy` can let an agent plan what only a person
  may run.

A command its handler refuses is refused at planning, with the
handler's error. Otherwise the plan runs the command, its policies and
its processes on a transaction it rolls back, and answers what that
came to:

- **Every event, in order.** The command's own, then those of the
  commands its reactions decide on. Each names the command that emits
  it and the reaction that dispatches that command (`cause`: "policy
  postLedger", "process coolAnvil"), and whether it goes to the bus.
- **What each reaction decided.** Beside each event is what reacts to
  it, with an `outcome`. A policy is `decided`, with the `commands` it
  dispatches: run at once, set as a timer (with when it fires), or
  queued as a batch. Timers and batches are shown and not followed. A
  policy that fails, or whose command is refused, fails the plan, as it
  would fail the command.
- **Processes, as far as the outside.** A process is run too, and what
  it dispatches is run in its turn. One that reaches an outside call
  (`loom.Once`) stops there with outcome `calls` and the call's name:
  the call is not made, so what the process does with its answer is not
  known. One that returns an error or dispatches a refused command is
  `fails`; `loom.NotYet` is `not_yet`. A plan follows processes ten
  deep and for two seconds; past that they are `not_followed`.
- **Who else reacts.** Projections with the read model they update, and
  other services' processes on the same gateway that consume the event,
  are named from the schema and not run.

Three things to know. A plan runs your handlers and reactions for
something that has not happened, so anything they do outside the
transaction must go through `loom.Once`, or check `loom.Planning(ctx)`
first; `Client.Dispatch` under a plan's context is refused. A real
process runs after the commit, beside the others; a plan runs them one
after another on its own transaction. And `runPlan` checks that the
command's own target has not moved, not everything its reactions read,
so what they decide at the run may differ from the plan. `@plan` holds
back only the gateway, and so the MCP tools; in-process `Dispatch`
(processes, timers, your own code) runs the command as any other, and
`Client.Plan` / `Client.RunPlan` are there to call directly.

When flat roles run out — "platform staff bypass everything", "this
client must be tagged to the caller's org" — set a `Policy`: the OPA
shape with Go as the rule language. Every operation (reads, lists,
subscriptions, mutations, uploads, file downloads, raw watches)
resolves to one `Decision` — kind, field, target namespace, args, the
requested selection paths (`Fields`, dotted through nested selections
and joins, fragments resolved), the `@role` contract, and the caller's
`Access` including an opaque `Claims` slot the Auth hook fills — and
the policy answers it. Rules can range over argument *values*
(`d.Args`: "only staff grant the owner role") and over what the query
asks to *read* (`d.Fields`: "support never sees `reason`"); forbidding
a field denies the operation whole, so callers re-shape their query and
partial results never happen. A set
policy *replaces* the built-in checks (`Access` becomes input, not
verdict); `DefaultPolicy` is those built-ins exported, so a policy
special-cases what it must and delegates the rest:

```go
gateway, _ := loomgraphql.New(loomgraphql.Config{
    Services: services,
    Auth:     auth,      // still authenticates; now also attaches Claims
    Policy: func(ctx context.Context, d loomgraphql.Decision) error {
        if c, ok := d.Access.Claims.(*Claims); ok && c.Staff != "" {
            return nil                      // staff: everything
        }
        return loomgraphql.DefaultPolicy(d) // everyone else: stock rules
    },
})
mux.Handle("/files", loomgraphql.ProtectWith(auth, policy, loomgraphql.Files(...)))
```

The policy runs with the request context, so relationship rules can
load aggregates or entities through a loom client before answering.
`ProtectWith` carries the same policy to the file and stream mounts.

What no generator can guess — derived views ("my orgs"), row-scoped
lists a policy can't filter, composite operations — mounts as a
hand-written root field:

```go
Fields: []loomgraphql.Field{{
    On: "Query", Name: "myOrgs", Returns: "Map", List: true,
    Resolve: func(ctx context.Context, args map[string]any) (any, error) {
        a, _ := loomgraphql.AccessFrom(ctx)   // the caller, claims included
        return orgsFor(ctx, a.Claims), nil
    },
}}
```

`Returns` names any composed type or a scalar (`Map` for free-form
shapes); `Docs` renders loom rows the way generated resolvers do.
Custom fields answer to the same Decision gate as everything else —
under `DefaultPolicy` their empty namespace passes only god access, so
scoped callers reach one only when a policy explicitly allows it. They
are composed at the deployment, so the emitted SDL artifact does not
carry them.

### Agents: the gateway over MCP

`mcp.New` serves the same composed schema to agents over the Model
Context Protocol:

```go
gateway, _ := graphql.New(graphql.Config{Services: services, Auth: auth, Policy: policy})
agents, _ := mcp.New(mcp.Config{Gateway: gateway, Name: "orders"})
mux.Handle("/graphql", gateway)
mux.Handle("/mcp", agents)
```

An agent gets five tools whatever the schema holds: `search` finds a
command or read by name, `describe` says what it takes and returns,
`dispatch` runs a command, `get` reads one record and `list` reads many.
Every call is one GraphQL operation run in-process through the gateway,
so `Auth`, `Policy`, `@role`, the depth limit and `@secret` redaction
apply exactly as they do over HTTP, and a hand-written `Field` is
reachable like a generated one. `Config.Unauthenticated` lets a
deployment name its OAuth resource metadata in the 401 it answers.

A client with no token has to be told how to get one. Give the server
a `SignIn` and mount it with `mcp.Mount`:

```go
err := mcp.Mount(mux, "/mcp", mcp.Config{
    Gateway: gateway,
    SignIn: &mcp.SignIn{
        Resource:             "https://api.example.com/mcp",
        AuthorizationServers: []string{"https://auth.example.com"},
    },
})
```

A request with no usable token is then answered 401 naming the sign-in
document (`/.well-known/oauth-protected-resource/mcp`, RFC 9728), which
names the authorization servers — the two reads the MCP authorization
spec has a client make. loom only points: it issues no token, and the
gateway's `Auth` hook still decides who a bearer is, including that the
token was issued for this server.

`search` and `describe` show a caller only what it could run. Each
operation is put to the gateway's own `Policy` as a probe — a
`Decision` with `Probe` set, the operation's `@role` and `@allow`
contract, a namespace the caller has, and no arguments — so a member is
not shown a command only an owner may run, and a read-only caller is
shown no commands. A custom `Policy` that cannot tell without arguments
should allow a probe: it decides only what is listed, and the call is
decided again in full when it comes.

What an agent reads about an operation is what the schema's doc
comments say. A `///` block directly above an aggregate, record, entity,
type, enum, command or field is its doc; the gateway serves it as the
GraphQL description, `search` matches on it and `describe` shows it:

```
/// Place a new order for a customer. The total is worked out from
/// the items.
command PlaceOrder {
  /// ISO 4217 code the prices are in, for example USD.
  currency: string!
} -> OrderPlaced
```

Plain `//` comments stay notes for whoever reads the schema and are
never published: a doc reaches every caller of the gateway, so writing
one is deliberate. A schema with no `///` generates exactly what it did
before.

### What the background work is doing

`Client.Status(ctx)` answers for the whole service from the database,
whichever instance is asked:

- **The lease:** whether the service needs a leader (it does when a
  process reacts to its own events), whether one holds it, the leader's
  generation and revision, whether it is yielding to a newer deploy, and
  the answering instance's own build. A leader on an older generation
  than the instance answering is a deploy whose hand-over has not
  happened.
- **Instances:** the builds with a live instance, newest first. More
  than one line after a deploy is the old revision still running. Only
  the newest generation alive fires timers, runs batches, folds
  projections, publishes the outbox and consumes the bus, so an old
  instance kept for a traffic tag never runs a held command with old
  code, never steps a projection past events only the new build folds,
  and hands a bus event back rather than handle it. A new instance announces itself, so older
  ones stand down at once; if the newer build's instances go away, the
  older ones take the work up again within a few beats. `HereNewest`
  says which kind answered.
- **Runners:** every projection and process with its position, its
  `Lag` in log positions, and `BehindSince` — when the oldest event it
  has not handled was written. Lag says how far behind; `BehindSince`
  says for how long.
- **What is waiting:** timers pending and overdue, effects running and
  failed, the outbox's depth and oldest row, dead letters, held plans.

Set `graphql.Config.Status` and the gateway serves it as
`serviceStatus`, one entry per composed service, with
`runners(onlyBehind: true)` for what needs attention. It names no
tenant, so under `DefaultPolicy` only `All` access reads it; let your
operators in from your `Policy` by its field name. Through `mcp.New` an
agent finds it with `search` and reads it with `list`.

## Audit: a signed export of the log

The log is the record of what a service did. `Client.ExportEvents`
writes it out for someone who must check it without the service:

```go
summary, err := cli.ExportEvents(ctx, file, loom.ExportOptions{
    Namespace: "acme",                        // or every namespace
    Signer:    loom.Ed25519Signer("audit-2026", key),
})
```

The export is lines of JSON: a header, one line an event in log order,
and a footer. Each event line carries a hash chained from the line
before, and the footer — the count and the last chain — is signed. So
`loom.VerifyExport`, or from a shell

```
loom verify-export --key audit.pub export.jsonl
verified: 1204 events of orders (namespace acme), ending at 88213, exported 2026-11-15T05:00:00Z, signed by audit-2026
```

fails on an event that was changed, dropped, added or moved, naming the
first one that does not check, and on a file cut short or signed by
another key. `Signer` is an interface: a product signs with the key its
identity service issued it. Events are exported as stored, so a `@pii`
field leaves sealed.

## Settings

A value the service's behaviour reads and a person may change while it
runs — a cap, a switch, a choice — is declared in the schema:

```
/// How many builds run at once.
setting build_cap: int @default(2)

setting tier: Tier @default(gold)   // an enum
setting paused: bool                // no default: the type's zero
```

Types are `string`, `int`, `bool`, `float` and enums. `loom generate`
writes a typed reader for each (`loomgen.SettingBuildCap(ctx, cli,
namespace)`), and the client reads and changes them:

```go
cap, _ := loomgen.SettingBuildCap(ctx, cli, "acme")           // 2
err := cli.SetSetting(ctx, "acme", "build_cap", 4, "two more runners arrived")
err = cli.SetSetting(ctx, loom.EveryNamespace, "build_cap", 3, "the new default for all")
err = cli.ResetSetting(ctx, "acme", "build_cap", "back with everyone else")
```

- **A change says why.** An empty reason is `loom.ErrNoReason`, and
  nothing changes.
- **A change is kept.** `Client.SettingHistory` lists who made each
  (the context's `Metadata.Actor`), when, why, and the value before and
  after.
- **A namespace reads its own value**, else the one set for every
  namespace (`"*"`), else the default; `Client.SettingValue` says which.
- **A value of the wrong type is refused**, as is a setting the schema
  does not declare.

Through the gateway, a service that declares a setting gets `settings`
and `settingHistory` to read and `setSetting` and `resetSetting` to
change, under the gateway's own rules, with the caller as the actor —
and so through the MCP tools too.

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
inspect payloads), **Issues** (runner lag against the log head, a stalled
projection's failing event and error with skip, in-doubt
effects with resolve, dead letters with redrive, overdue timers). One
embedded self-contained page over the JSON endpoints — no build step, no
external assets (the topology layout is ~80 lines of hand-rolled layered
SVG), auth is whatever wraps the mount.

Filters are `field=value` with `.gte .lte .gt .lt .ne .like` suffixes,
compiled to parameterized jsonb SQL; `order` and `limit` shape a page,
and `after_id` with `after_value` (or `after_null=1`) reads the page
past a row.
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

Logs go through whatever `slog` handler you put in `Config.Logger`. On
Google Cloud, wire `loom.CloudLogHandler` — a plain JSON handler writes
`level`, which Cloud Logging ignores, so every line lands at DEFAULT
severity and `severity>=ERROR` never finds `runner step failed`:

```go
slog.SetDefault(slog.New(loom.CloudLogHandler(os.Stdout)))
```

It renames `level` → `severity` (DEBUG/INFO/WARNING/ERROR — slog's WARN
is Cloud Logging's WARNING) and `msg` → `message`, and promotes the
context's OTel trace onto the entry (`logging.googleapis.com/trace`,
`/spanId`, `/trace_sampled`), so a log line links to the span that wrote
it — `projects/<id>/traces/...` when `GOOGLE_CLOUD_PROJECT` is set, the
bare trace id otherwise. `CloudLogHandlerOptions(w, opts)` takes the JSON
handler's options (a minimum level, `AddSource`); your own `ReplaceAttr`
runs after the mapping. Adopting it is the consumer's call — loom's
default is still `slog.Default()`.

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
[TODO.md](TODO.md) for what comes next, [CHANGELOG.md](CHANGELOG.md) for
what changed in each release, and
[go-apis/loom-example](https://github.com/go-apis/loom-example) for the
example as a standalone repo.
