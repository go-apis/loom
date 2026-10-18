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
   never rewrites them.
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

## When the generated code does not compile

- `X redeclared` in `registry_gen.go`: a reactor collides with an
  aggregate and the generator predates loom ADR 0006 — bump loom, do not
  edit the file.
- A missing method on `…Handlers` or `…Reactions`: a new command or
  subscription; implement the stub the compiler names.
- `generate --check` lists files: regenerate with `go tool loom generate`
  and commit them.
