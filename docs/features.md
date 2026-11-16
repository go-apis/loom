---
summary: What each loom feature does, the hand-written code it makes unnecessary, the release that brought it, and the `loom adopt` rule that finds the old pattern.
---

# Loom features, and what they make unnecessary

Every service on loom pins a loom version and gets whatever that version
does. This guide is what a service may be writing by hand that its loom
already does. Run `go tool loom adopt` in a service's repository to list
the places (file:line, the feature to use, the lines involved); it reads
only and is advice, not a check.

Each entry says what the feature does, what it makes unnecessary, the
release that brought it, and the `loom adopt` rule that finds the old
pattern where one exists. A release that adds a feature adds its entry
here and, where it can, an adopt rule (see the CHANGELOG).

## Work that recurs

### Schedules and `Client.Every`

**Since** v0.91.0 (`schedule`), v0.95.0 (`Client.Every`) · **adopt rules**
`ticker-goroutine`, `self-rearming-timer`

A command or a function run every so often, on one instance of the
newest generation alive. A tick that fails is retried at the next tick:
the series never ends because one link failed.

Makes unnecessary: a goroutine looping on `time.NewTicker` on every
instance (each doing the work, or racing to); a singleton "tick"
aggregate whose handler re-arms itself with `loom.After` (its chain ends
the first time a link fails, and comes back only on the next deploy);
a boot-time dispatch from `main` to start the chain.

```go
// before: every instance, failures only logged
t := time.NewTicker(time.Minute)
for range t.C { _ = sweep(ctx) }

// after: one instance, the next tick is the retry
cli.Every("client-sweep", time.Minute, sweep)
```

```
// or in the schema, when the work is a command
schedule clientSweep every 1m -> SweepClients @in(platform)
```

A cache each instance keeps in its own memory is not this: each instance
must refresh its own, and its ticker stays.

## Instances that wait or must be watched

### `@loop` aggregates, waits and approvals

**Since** v0.99.0 (`@loop`), v0.110.0 (per-instance runner, `WaitFor`),
v0.111.0 (approvals) · **adopt rules** `timer-policy`,
`self-rearming-timer`

A `@loop` aggregate's look is called for each live instance: at least
every minute, within a second of an event for it, and at the first
deadline among its waits. It reads how the instance stands and returns
the next step. A wait (`loom.WaitFor`) has a name, an owner and a
deadline, kept by loom with the instance; a passed deadline calls the
look, told the wait expired. An approval is a wait with a record: who may
answer, and what happens at the deadline.

Makes unnecessary: a `policy <name>Timer` block listing every event that
arms or cancels a timer; the handlers that arm and cancel it; the sweep
that finds instances whose timer was lost and re-arms them; chains of
events between processes standing in for "what should happen next".

```
// before: a timer armed and cancelled by hand
policy holdTimer {
  on ItemHeld -> ArmHold
  on ItemReleased -> CancelHold
}
```

```
// after: the look decides from how the item stands
aggregate Item @loop { … }
```

```go
func (l *Look) LookItem(ctx context.Context) ([]loom.Command, error) {
	told := loom.LookingAt(ctx)
	if _, late := told.ExpiredWait("hold"); late {
		return []loom.Command{&loomgen.ReleaseItem{…}}, nil
	}
	state, _, err := loom.Load(ctx, "Item", told.Namespace, told.ID)
	if err != nil {
		return nil, err
	}
	if item := state.(*loomgen.Item); item.Held {
		wait := loom.Wait{Name: "hold", Owner: "reviewer", Deadline: item.HeldAt.Add(time.Hour)}
		if err := loom.WaitFor(ctx, wait); err != nil {
			return nil, err
		}
	}
	return nil, nil
}
```

## Calling the outside world

### Effects: `loom.Once`

**Since** v0.7.0 · **adopt rule** none yet

A call to the outside world from a process reaction, made at most once
per reaction step and journaled: a retry of the reaction replays the
recorded result instead of calling again. Declared `effect <name>` on the
process; `@idempotent` or a `Reconcile` hook settles one left in doubt.

Makes unnecessary: idempotency keys and "already sent?" tables kept by
hand; a status field on an aggregate recording that a call was made.

```go
// before: an idempotency key and a "sent?" table kept by hand
sent, _ := db.WasSent(ctx, idemKey)
if !sent {
	if err := provider.Charge(ctx, order.AmountCents); err != nil {
		return err
	}
	db.MarkSent(ctx, idemKey)
}

// after: the process declares the effect; a replay doesn't call again
effect chargeCard

func (p *Checkout) OnOrderPlaced(ctx context.Context, evt *loom.Event, o *OrderPlaced) ([]loom.Command, error) {
	_, err := loom.Once(ctx, "chargeCard", func(ctx context.Context) (string, error) {
		return provider.Charge(ctx, o.AmountCents)
	})
	if err != nil {
		return nil, err
	}
	return nil, nil
}
```

### Durable retries and `NotYet`

**Since** v0.52.0 (`@retry`), v0.91.0 (`NotYet`) · **adopt rule** none yet

`@retry(max, min..max)` on a process retries a failing reaction with
backoff, durably, then dead-letters it. A reaction with nothing to do yet
returns `loom.NotYet("what it waits for")` and is retried the same way.

Makes unnecessary: retry loops with sleeps inside a reaction; a backoff
ladder kept as fields on an aggregate; a timer armed to look again.

```go
// before: a retry loop with sleeps inside the reaction
for attempt := 0; attempt < 40; attempt++ {
	state, err := runs.Get(ctx, d.RunID)
	if err == nil && state.Finished {
		return []loom.Command{&RecordDeployOutcome{...}}, nil
	}
	time.Sleep(backoff(attempt))
}
return nil, errors.New("deploy never finished")

// after: @retry owns the backoff and the dead letter
process awaitDeploy @retry(40, 5s..5m) {
  on DeployRequested -> RecordDeployOutcome
}
```

```go
func (h *AwaitDeploy) OnDeployRequested(ctx context.Context, evt *loom.Event, d *DeployRequested) ([]loom.Command, error) {
	state, err := h.Runs.Get(ctx, d.RunID) // a read: call it directly
	if err != nil {
		return nil, err // a failure: the usual retries
	}
	if !state.Finished {
		return nil, loom.NotYet("the deploy is still rolling out")
	}
	return []loom.Command{&RecordDeployOutcome{...}}, nil
}
```

## Values people change

### Settings

**Since** v0.103.0 · **adopt rule** none yet

`setting <name>: <type> @default(<value>)`, read with its generated
reader and changed with `SetSetting(…, reason)`: per namespace, with its
history and a required reason, served by the gateway.

Makes unnecessary: environment variables for values people change while
the service runs; a settings table of the service's own.

```
// before: an env var, changed only by redeploying
BUILD_CAP=2
```

```
// after: a setting, changed at runtime with a reason, and a history
setting build_cap: int @default(2)
```

```go
cap, err := loomgen.SettingBuildCap(ctx, cli, "acme")       // generated reader
err = cli.SetSetting(ctx, "acme", "build_cap", 4, "raised for the holiday surge")
```

## Reading

### `loom.Every` and `loom.Pages`

**Since** v0.112.0 · **adopt rule** none yet

Read every row of a list query, or page through it, without writing the
cursor loop: newest-first lists are read oldest first and reversed.

Makes unnecessary: hand-written `for cursor != ""` loops over a list read.

```go
// before: a hand-written cursor loop
q := loom.Query{Namespace: "acme", Limit: 100}
var all []loom.Row
for {
	rows, err := cli.QueryEntities(ctx, "OrderSummary", q)
	if err != nil {
		return err
	}
	all = append(all, rows...)
	if len(rows) < q.Limit {
		break
	}
	cursor, err := rows[len(rows)-1].Cursor(q.OrderBy)
	if err != nil {
		return err
	}
	q.After = &cursor
}

// after
all, err := loom.Every(ctx, cli.ListEntities("OrderSummary"), loom.Query{Namespace: "acme"})
```

## Data and audit

### `@pii`, data keys and shredding

**Since** v0.8.0 · **adopt rule** none yet

A field marked `@pii` is encrypted at rest under a key per stream;
shredding the stream's key makes the data unreadable everywhere, the log
included, and every instance drops its cached key.

Makes unnecessary: a key-encryption package of the service's own; erasing
personal data by rewriting events.

```
// before: plaintext in an append-only log; erasure means rewriting history
aggregate Payee @table {
  state {
    tin: string
  }
}
```

```
// after: sealed at rest, erased by dropping the key, not the event
aggregate Payee @table {
  state {
    tin: string @pii
  }
}
```

```go
err := cli.Shred(ctx, "acme", payeeID) // tin now reads as "" everywhere, no unshred
```

### Signed export

**Since** v0.102.0 · **adopt rule** none yet

`ExportEvents` writes the log as hash-chained JSON lines with a signed
footer; `loom verify-export` checks one without the service.

Makes unnecessary: audit dumps that cannot show nothing was changed,
dropped or reordered.

```go
// before: a dump nothing can check — a dropped or edited row looks the same
rows, _ := db.Query(ctx, "SELECT * FROM events ORDER BY global_seq")
json.NewEncoder(w).Encode(rows)

// after: each line's hash depends on the one before it, and a signed footer
summary, err := cli.ExportEvents(ctx, w, loom.ExportOptions{
	Namespace: "acme",
	Signer:    signer, // an ed25519 key the service holds
})
```

```
$ loom verify-export --key <ed25519 public key, base64 or a file> export.jsonl
```

## Serving

### The generated gateway, GraphQL and MCP server

**Since** v0.88.0 (MCP) · **adopt rule** none yet

From the schema: a GraphQL API with roles and `@allow` rules, OpenAPI,
an access document, and an MCP server whose tools are the service's
reads and commands under the same rules. `@plan` lets a caller preview a
command that cannot be undone.

Makes unnecessary: hand-written resolvers, REST handlers and MCP tools
over the same commands, and the access checks repeated in each.

```go
// before: the same rule, checked by hand in each surface
func registerParcelHandler(w http.ResponseWriter, r *http.Request) {
	if !isAdmin(r) && !isCourierForOwnDepot(r) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	// ...and the GraphQL resolver, and the MCP tool, each repeat the check
}
```

```
// after: one rule on the schema, enforced wherever the command is reached
command RegisterParcel @allow(kind: admin) @allow(kind: courier, depot_id: caller.depot) {
  ...
}

command DiscardParcel @plan @allow(kind: admin) @allow(kind: courier, depot_id: caller.depot) -> ParcelDiscarded
```

## Running

### Generations and the process lease

**Since** v0.86.0 (`LOOM_GENERATION`) · **adopt rule** none yet

Each deploy sets `LOOM_GENERATION`; the newest generation alive takes the
process lease and does the service's background work, and an older one
leaves it, so a deploy never runs two builds' processes at once.

Makes unnecessary: a leader lease of the service's own, and code that
terminates older leaders by hand.

```go
// before: a leader lease and a sweep that kills older leaders by hand
if acquireAdvisoryLock(ctx, "leader") {
	go runBackgroundWork(ctx)
}
// ...plus a sweep that finds the previous revision's process and kills it

// after: the deploy sets the generation, loom hands the lease over
// LOOM_GENERATION=$(date +%s) — the newest alive takes the lease; an
// older generation is asked to yield and, failing that, is terminated.
```
