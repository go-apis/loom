---
name: loom
description: Working in a service built on go-apis/loom — editing `.loom` schema files (aggregates, records, events, commands, policies, processes, projections, entities, effects, timers), the generated `loomgen` package, the handler and reaction stubs, `loom.Once` effects, upcasts, or running `loom generate`. Use before changing a schema, adding an aggregate or process, regenerating, or writing a reaction that calls the outside world.
metadata:
  loom-version: "{{LOOM_VERSION}}"
  written-by: "loom skill"
---

# loom

Schema-first event sourcing: you declare the domain in `.loom` files, `loom
generate` writes the `loomgen` package (models, registry, SQL), and you
write the stubs it leaves you. This skill is written by `loom skill` from
the loom version this repo pins ({{LOOM_VERSION}}); `go tool loom skill
--check` says when it is stale.

## The loop

1. Edit the schema. It is a directory (`schema:` in `loom.yml`), one file
   per aggregate or feature; a new aggregate is a new file. Shared enums
   are extended from a feature's own file with `enum X += { value }`,
   never by editing the home declaration.
2. Regenerate with the pinned generator, from the directory holding
   `loom.yml`:

   ```sh
   go tool loom generate
   ```

   Never `go run github.com/go-apis/loom/cmd/loom@<version>` or `@latest`:
   another version writes other bytes. The repo pins it with
   `tool github.com/go-apis/loom/cmd/loom` in go.mod.
3. Implement what the compiler asks for: new commands get stubs in
   `aggregates/` (or the flat package), new reactions in `processes/`,
   `policies/`, `projections/`. Stubs are yours and written once; loom
   never rewrites them. That includes `registry.go`. For a new aggregate
   or record, add its binding call there yourself, `impl.Order(&Order{})`.
   For a new reactor, add its field to the `Impl` literal.
   Each aggregate and record has its own `loomgen/<snake name>_gen.go`.
   The shared `registry_gen.go` never names one, so a new aggregate adds
   one generated file and changes none. Each schema file has its own
   `loomgen/<schema file>_types_gen.go` for the enums, types, entities
   and shared events it declares, so declare a type in the schema file
   of the feature it belongs to, not in one catch-all file.
4. Check before you push:

   ```sh
   go tool loom generate --check   # exits 1 naming each stale loomgen file
   go build ./... && go vet ./...
   ```

## Never

- **Never hand-edit `loomgen/`.** It is the generator's. A hand edit is
  undone by the next `loom generate` — anyone's, including an automated
  rebase — and `generate --check` fails on it.
- **Never name a reactor after an aggregate or record.** A process,
  policy or projection is named for what it does: `shipOnPayment`,
  `productManifests`, `threadReply`, `mergeBatchSweep`. Not
  `process fleetStall` beside `aggregate FleetStall`. loom compiles the
  collision by suffixing the Impl field (`FleetStallProcess`, ADR 0006),
  but it is a smell, and renaming a reactor later is not free (below).
- **Never rename a running process or projection casually.** Its name is
  its runner's checkpoint. A renamed process is a new runner and starts
  at the log's head (`@from(head)` is the default): events between the
  old checkpoint and the deploy are never reacted to. A renamed
  projection rebuilds from the origin.
- **Never delete an event declaration.** Mark it `@retired`; the log
  keeps rows that must still decode. `Migrate` refuses a log holding a
  type the registry does not declare.

## Writing handlers

- An aggregate handler returns events; state is only ever folded from
  them. A command that changes nothing returns `nil, nil` (converge) —
  repeat-safe commands are the norm.
- **A fold copies event fields onto state fields of the same name.** An
  event that changes a list must carry the whole list as it stands after
  the change, not just the new element: `ItemAdded{Item, Items, Count}`,
  where `Items` is the full slice.
- A command's `timestamp` field is a `time.Time` in Go; `timestamp?` is a
  `*time.Time`. Check `IsZero()` on the first, `nil` on the second.
- A `record` handler mutates state directly; its events are
  announcements, never a rebuild source.
- Commands declare what they emit (`-> A, B`) and reactions what they
  dispatch (`on X -> C, D`). Both are enforced at runtime: emitting or
  dispatching anything undeclared is an error.

## Reactions

| | runs | use for |
|---|---|---|
| `policy` | inside the producing transaction | atomic follow-ups on local events |
| `process` | async, checkpointed, one instance per runner | everything that waits, reads widely or calls out |
| `projection` | async catch-up | read models (`entity`; `@table` for typed columns) |

- A process reacts to events after it is deployed unless declared
  `@from(origin)`.
- A failing reaction retries three times in-process, then parks to dead
  letters; `@retry(n, min..max)` keeps trying durably first.
- A process that re-arms itself on a timer does it from the reaction
  that handles the tick (`loom.After`), never synchronously inside the
  timer's dispatch.
- A `@table` list read that filters and sorts the same way on every
  page gets an index in the schema, not by hand — and not hand-derived
  from the read either: declare the read itself with `query
  (org_id, updated_at desc nulls last)` inside the entity block, filter
  columns then the order column. `query` desugars into the index that
  read needs, including the trailing `id` tie-breaker a paginated list
  must have, so the direction is never worked out (or gotten wrong) by
  hand. `index (col [desc] [nulls first|last], ...)` still exists for an
  index that isn't backing one declared read. `Migrate` builds either
  `CONCURRENTLY`; `Rebuild` keeps both.

## Calling the outside world: effects

```
process captureOnPaid {
  on InvoicePaid
  effect gateway_capture
  effect receipt_email @idempotent
}
```

```go
id, err := loom.Once(ctx, "gateway_capture", func(ctx context.Context) (string, error) { … })
// several calls of one kind: "gateway_capture/<invoice id>/<attempt>"
```

- **The first segment of a `loom.Once` key must be an effect the process
  declares.** An undeclared key is a runtime error and the reaction parks
  ("does not declare effect …"); add `effect <name>` to the process.
- The result is journaled: a retry replays it instead of calling again.
  A crash between the call and the record leaves the effect in doubt and
  the reaction parks, unless the effect is `@idempotent` or has a
  `Reconcile` hook.
- Make each retry its own key when a retry must really call again
  (`…/<attempt>`).

## Doc comments are published

A `///` block directly above an aggregate, record, entity, type, enum,
command or field is its doc: the gateway serves it as the GraphQL
description, and agents calling the service over MCP search it and read
it. Write one on every command, in the caller's terms: what it does and
when to use it. Keep reasons, history and implementation notes in plain
`//` comments, which are never published.

## Commands that cannot be undone take @plan

A command that deletes, archives, revokes or otherwise cannot be taken
back is declared `@plan`. At the gateway its mutation then answers a
plan of what running it would lead to and changes nothing; `runPlan`
with the plan's id runs it, once, while its target is unchanged.

The plan runs the handler, the policies and the processes on a
transaction it rolls back, to say what each would decide. So a handler
or reaction may be run for something that never happens. Return
commands; never dispatch them yourself. Make every outside call through
`loom.Once`, which a plan does not call. Anything else that reaches
outside the transaction (a counter, a cache, a log of record) checks
`loom.Planning(ctx)` first.

## A change to who may see what shows in the contract

A service with a gateway keeps an access contract: a test calling
`contract.Run` with every kind of principal its policy tells apart, and
a recorded `contract.json`. When you change a policy, an `@allow`, an
`@role` or a list's filter, run that test. If it fails, the failure
names the operation and the principal whose view changed. Re-record it
(`LOOM_CONTRACT_UPDATE=1`) only when that change is the one you meant,
and say so in the pull request: the file's diff is the review. A new
kind of principal goes into the suite the day the policy names it.

## Work that recurs is a schedule

A look, a sweep or a reconcile that runs every so often is declared
`schedule <name> every 5m -> <Command> @in(<namespace>)`. Do not write a
command that re-arms itself with `loom.After`: its chain ends the first
time a tick fails. A schedule ticks whatever came of the last one.

## Waiting for something is NotYet

A reaction that has nothing to do yet returns `loom.NotYet("what it is
waiting for")` and its process declares `@retry(max, min..max)`. Do not
schedule your own timer to look again. Make the look directly, not
through `loom.Once`.

## Reading

- `cli.Load(ctx, "Aggregate", ns, id)` reads an aggregate's state from
  the log: use it right after a `Dispatch` when you must answer with the
  result. Read models (`cli.Entity`, `QueryEntities`) lag the log.
- `cli.AwaitProjection(ctx, projection, seq)` waits for a read model to
  reach a position when you must read your writes through it.

## Events change shape with upcasts

`@v(n)` on the event, `upcast X @from(n-1)` in the schema, and a
deterministic hop in the generated `Upcasts` stub. Purely additive fields
need no version.

## Testing

`loomtest` runs handlers and reactions against the real registry with no
database:

```go
loomtest.Aggregate(t, reg, "Invoice").
    Given(&loomgen.InvoiceRaised{…}).
    When(&loomgen.MarkInvoicePaid{CommandBase: base}).
    Then(&loomgen.InvoicePaid{…})
```

`ThenNothing` asserts a converging no-op, `ThenError` a guard. Effects,
projections, timers and batches are end-to-end territory.

- **A test that shells out to `go mod tidy` must not set `GOPROXY=off`.**
  `tidy` reads the `go.mod` of every version in the module graph, not
  just the ones a build already resolved, so it needs versions a prior
  `go build`/`go test` never fetched and never cached. `GOPROXY=off`
  fails on a fresh module cache with "module lookup disabled by
  GOPROXY=off", costing a session real time chasing what looks like a
  broken test. Let the subprocess inherit the environment's `GOPROXY`,
  or warm the cache first with `go mod download all` in the module it
  will tidy.

## When the generated code does not compile

- `X redeclared` in `registry_gen.go`: a reactor collides with an
  aggregate and the generator predates loom ADR 0006 — bump loom, do not
  edit the file.
- A missing method on `…Handlers` or `…Reactions`: a new command or
  subscription; implement the stub the compiler names.
- `generate --check` lists files: regenerate with `go tool loom generate`
  and commit them. An orphan (a renamed or removed aggregate's
  `_gen.go`) is listed as stale, and `generate` removes it.
- `unknown field Order in struct literal of type loomgen.Impl`: the
  `registry.go` predates per-aggregate registration (loom ADR 0008). Move
  each aggregate and record out of the literal into a binding call,
  `impl.Order(&Order{})`, and keep the reactor fields.
- A panic at boot, `loomgen: aggregate X has no handlers — call
  impl.X(...)`: `registry.go` never binds X. Add the call it names.

## When a rebase or merge conflicts in loomgen

The generated files are the generator's output, never a merge of two
people's lines. Two branches that each add an aggregate write disjoint
`<name>_gen.go` files and should not conflict. Two that change types in
different schema files write different `<schema file>_types_gen.go`
files. Two that change declarations in the same schema file, or a
reactor, a projection, a `@table`, an enum's values or a type's required
fields, can conflict: they rewrite the same `_types_gen.go`,
`registry_gen.go` or `tables_gen.sql`, and no line-by-line resolution of
those is right.

1. Resolve only the **sources** — the `.loom` schema files and
   `loom.yml`. A conflict there is real: keep both sides' declarations
   (with one schema file per aggregate or feature, two branches rarely
   touch the same file).
2. Take either side of every conflicted file under `loomgen/` (and the
   service's `registry_gen.go`) as it is — `git checkout --theirs` or
   `--ours`, it does not matter which.
3. Regenerate from the merged sources with the pinned generator, from
   the directory holding `loom.yml`, and check:

   ```sh
   go tool loom generate
   go tool loom generate --check
   go build ./... && go vet ./...
   ```

4. Commit what the generator wrote as the generated files' resolution.

Never hand-merge a `loomgen` conflict, and never resolve one by keeping
both sides' lines: the result compiles by accident or not at all, and the
next `loom generate` undoes it.
