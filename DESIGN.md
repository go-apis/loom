# Loom design

The design of record is the es-v2 tracking issue (Design v2). This file
covers what's implemented and the decisions embedded in the code.

## SDL grammar

A schema is one or more `.loom` files: a directory (every `*.loom` under it,
at any depth), a single file, or a glob. The files are joined in
slash-separated path order and parsed once as one token stream, so a
declaration in one file may refer to anything declared in another; errors
name `path:line:` ([ADR 0003](docs/adr/0003-schema-is-a-directory.md)).
"First" is by path (`billing/invoice.loom` sorts before `orders.loom`), so
the recommended layout opens every file with the same `service X`; a
declaration must end in the file it starts in.

```
schema      := file+                    // joined in path order, parsed once
file        := header? decl*            // header required on the first file by path; decls never span files
header      := "service" IDENT          // later files: omit, or repeat the same name
decl        := aggregate | record | entity | series | event | consume
             | policy | process | projection | type | upcast | enum
aggregate   := "aggregate" IDENT directives? "{" (state | command | event | upload)* "}"
record      := "record" IDENT "{" (state | command | event | upload)* "}"
state       := "state" fields
command     := "command" IDENT fields? "->" identList
event       := "event" IDENT directives? fields?
upload      := "upload" IDENT "{" ("on" ("started"|"uploaded") "->" IDENT)* "}"
consume     := "consume" IDENT "." IDENT fields?
upcast      := "upcast" eventRef "@from" "(" NUM ("," NUM)* ")"   // an unqualified name may be a command
policy      := "policy" IDENT "{" on* "}"
process     := "process" IDENT directives? "{" (on | effect)* "}"
effect      := "effect" IDENT "@idempotent"?
projection  := "projection" IDENT "->" IDENT "@fold"? "{" projOn* "}"
projOn      := "on" eventRef ("key" "(" IDENT ")")?
on          := "on" eventRef ("->" identList)?
eventRef    := IDENT | IDENT "." IDENT          // qualified = foreign
type        := "type" IDENT fields
enum        := "enum" IDENT "+="? "{" IDENT+ "}"     // += extends an enum declared once elsewhere
entity      := "entity" IDENT "@table"? "{" (field | index)* "}"   // index needs @table
index       := "index" "(" indexCol ("," indexCol)* ")"
indexCol    := IDENT "desc"? ("nulls" ("first" | "last"))?
series      := "series" IDENT "@time" "(" IDENT ")" "@dim" "(" identList ")"
               ("@key" "(" identList ")")? fields
fields      := "{" field* "}"
field       := IDENT ":" ftype "!"? "@pii"?
ftype       := builtin ("(" IDENT ")")? | IDENT | "[" ftype "]"     ("?" = nullable)
builtin     := string int float bool uuid timestamp bytes any map file
directives  := "@snapshot(N)" | "@publish" | "@v(N)" | "@alias(A, B)"
             | "@retired"
             | "@from(head|origin)"      // process start position
             | "@retry(N, DUR..DUR)"     // process durable retry: max, min..max backoff
             | "@loop"                   // aggregate only: one recurring look per type
```

Rules enforced at parse/validate time:

- aggregate commands must emit (records' may not: the state write is the
  effect); emits/dispatches must name declared things
- policies cannot subscribe to foreign events (they run in the producing
  transaction — use a process)
- nested object literals are forbidden: declare a `type` (keeps generated
  code flat and contracts referencable across languages)
- foreign events (`consume`/qualified refs) are implicitly published
  contracts
- `@from` is a process's start position on its first run and belongs to
  processes only (a policy runs inside the producing transaction and has
  no start position): `@from(head)` — the default when omitted — means a
  process with no checkpoint row starts at the log's head and reacts only
  to what happens after it is deployed; `@from(origin)` is the explicit
  opt-in to replaying the whole log (a backfilling process). A process
  that already has a checkpoint row keeps it, so a redeploy is untouched.
  Projections are not affected: they always fold from the origin by design
- `@retry(max, min..max)` is a process's durable retry policy and belongs
  to processes only (a policy fails with the producing transaction):
  `max` is a positive count of durable attempts, `min`/`max` are Go
  durations (`100ms`, `5s`, `5m`) with `0 < min <= max`. The range is one
  directive argument, `5s..5m`. It lands on `schema.Reactor.Retry` and the
  generated `ReactorDef.Retry` (`loom.RetryPolicy`, bounds emitted as
  nanoseconds). Without it a process keeps the fixed in-process attempts,
  then parks — opt-in, not a default change
- effects are declared on processes only (a policy runs in the producing
  transaction and must not touch the outside world); `loom.Once` refuses
  undeclared keys — a typo'd key would be a fresh journal identity and a
  repeated call, so declaration is the safety net, not ceremony
- `@idempotent` is the only directive an effect takes, with no arguments;
  it lands on `schema.Reactor.Idempotent` and the generated
  `ReactorDef.IdempotentEffects`, beside the unchanged `Effects` list
- `@pii` lives only on top-level fields of local unpublished events and
  aggregate/record/entity states: commands are transient inputs, published
  events cross the bus in plaintext (keep PII on a private event — the
  ten99 private/published pair pattern), foreign events belong to another
  service's keys, and named types would smuggle PII anywhere
- series fields: `@time` names a required timestamp field; every `@dim`
  and `@key` field must be a required scalar (they become NOT NULL
  identity columns); `@pii`/`@secret` are rejected (typed columns, and a
  bulk-appended observation has no stream to key a DEK on); the name
  shares the query surface with aggregates, records, and entities
- `upcast X @from(n)` declares a hand-written migration hop n → n+1 for
  stored events behind the current `@v`; hops are code-first (raw JSON in,
  raw JSON of the next version out, generated `EventUpcasts` interface +
  once-only stub) and chain at the decode chokepoint, so folds and
  reactions only ever see the current shape. Coverage must be contiguous
  up to the current version (a gap would strand older rows). Declaring
  any upcast turns on strict version handling for that event — an
  unliftable stored version (below coverage, failing hop, or NEWER than
  the registry: deploy skew) is a loud `UpcastError`, never a silent
  zero-value fold; events without upcasts keep the permissive
  unmarshal-as-is behavior so additive changes stay free
- a command takes `@v(n)` too, and `upcast X @from(n)` names a command
  when X is one (resolved once the whole schema is read; a name that is
  both a command and an event is refused). A command loom stores to run
  later — a timer (`At`/`After`, and the row a parked timer leaves in the
  dead letters), a batch item, a plan — rests with its version in
  `command_version`; reading it to run lifts it through the hops first
  (`decodeStoredCommand`, `storedcommands.go`), so a handler only ever
  sees the current shape. Rows from before the column read as 1, the
  only version a command had. Command decode is always strict: a stored
  version behind with no hop, or ahead (deploy skew), is an
  `UpcastError`. `Migrate` refuses while any timer, pending batch item or
  unexpired plan holds a version no declared hop brings forward, naming
  the command, the version and the count of stored rows; a schedule's
  row is exempt (rewritten in the current shape at start), and so is a
  stored version newer than the build (refusing would block a rollback).
  The hops are `CommandUpcasts`, beside `EventUpcasts`: its own
  interface in `upcasts_gen.go`, its own `Impl.CommandUpcasts` field and
  once-only stub (`command_upcasts.go`)
- `@loop` belongs to aggregates only (rejected on a record, naming the file
  and line); it declares one recurring look for the aggregate TYPE, not its
  instances. `loom generate` emits a service-wide `Look` interface — one
  `Look<Name>(ctx) ([]loom.Command, error)` method per looping aggregate —
  and a once-only stub, the same shape as `EventUpcasts`/`Upcasts`; the
  wait between calls is the runtime's to keep, not the implementation's to
  hand-roll ([ADR 0012](docs/adr/0012-a-loop-is-declared-per-aggregate-and-a-wait-is-built-in.md)).
  When to look is one rule that reads no clock and no store
  (`loom.LoopState.NextLook`, `loop.go`): no later than `LoopEvery` (a
  minute) after the last look, within `LoopSoon` (a second) of an event
  that has arrived since, and at the first deadline among the loop's
  `Wait`s (a name, an owner, a deadline). A `@loop` aggregate may declare
  `deadline <name> after <d> while <field> in (<values>)` on its own
  state: loom keeps that wait itself (`DeadlineWait(name)`, owner
  `loom`) — the look that finds the field in the values starts it, After
  from then; the look that finds it out ends it; when it expires the look
  is told once, and it does not start again until the field has left
  the values (`loom_loop_due.deadlines_fired`). A `@loop` aggregate may
  declare `agent <name> { kind: "<kind>", deadline: <d> }`: an agent step
  its look asks of Mesh, a job of that kind waited on for `<d>` (both
  required; refused on an aggregate that is not `@loop`; see "An agent
  step is a wait on Mesh" below, ADR 0017). A `@loop` aggregate may
  declare `@supervised_by(Parent, field)`: its instances are children of
  the Parent instance whose id `field` holds (the runner records it on the
  child's row at each look). A child that is parked, reaches its end
  (`LookDone`) or is told a wait ran out leaves a `ChildNote` on its
  parent's row and brings the parent's look to within `LoopSoon`, outside
  its own row lock so parent and child never wait on each other. The
  parent's look is told `Looking.Children` (its live children: failures,
  parked, last error, waits) and `Looking.ChildNotes` (each note once),
  and decides with ordinary commands, or `loom.RestartChild` (failures
  cleared, unparked, looked at at once, `restarted_at`/`restarted_by` on
  the child's row). A parent that fails its own looks is parked and told
  to its own supervisor: escalation is failing. A look reads why it was called
  `Wait`s (a name, an owner, a deadline). After looks that failed in a
  row the minute shortens to `loom.LoopBackoff(failures)`: a second after
  the first failure, doubling with each one after, never past the minute;
  `LoopState.AfterLook` counts them, and a look that succeeds clears
  them. A look that panics is recovered and counted like any failure.
  Past `Config.LoopFailureLimit` (10) failures in a row the instance is
  parked (`LoopState.ParkPast`): it holds the wait `loom.looks-failing`
  on its `operators` for a day and has no minute, so only an event, that
  wait's deadline, or `Client.EndWait` on it (which clears the count and
  looks at once) brings a look. `Client.LoopDue` and `Status.Loops`
  report the count, the last error and what is parked. A look reads why it was called
  with `loom.LookingAt(ctx)`: whether an event arrived, and which waits
  have expired. The part of a look that reads no clock and no store is
  one `loom.LookTurn` (`StartLook`, `Context`, `End`): the declared
  deadlines kept, what the look is told, what it says, and the state and
  next look after it. The runner makes every look through one, and
  `loomtest.Loop` / `loomtest.LoopOf` make the same turns with no
  database. Their `RunFor` / `RunUntil` sleep to each look the rule
  makes due and to each timer (`loomtest.Timers`, which holds what
  `loom.At` / `loom.After` schedule), so inside a `testing/synctest`
  bubble a test walks a backoff, a park, a day's wait or a week's
  approval in no real time and in the same order every run. An
  approval's deadline action and an answer's checks are
  `ApprovalRecord.PastDeadline` / `Answer`, and an outside wait's start
  and end `LoopState.Waited` / `WaitEnded`, shared the same way. Left
  out, being the store's: claims and leases, the newest-generation rule,
  a look's commands as one unit of work, the reactions they start, and
  supervisors' children; a durable `@retry`'s backoff is jittered
  (`math/rand`), so it is not walked in a bubble either
- an `upload` needs an `on uploaded` command; both lifecycle commands must
  belong to the enclosing aggregate/record and carry exactly one required
  `file` field (loom fills it); upload names are service-unique — they
  become API surface (`create{Name}Upload`)
- a projection's `key(field)` must name a required uuid field on that
  event's payload — a missing key would route to the nil row
- an aggregate's state folds every event its commands emit and every
  event declared in its block that is not `@retired`, emitted or not: a
  command deleted or changed to stop emitting an event leaves the event's
  fold in place, so its stored rows replay as before (ADR 0016)
- `event X @retired` keeps a declaration alive for its stored rows alone:
  the payload struct stays generated, so replays and rebuilds still decode
  X and then fold nothing (a retired event has no case in any fold) —
  decode-skip, cleanly.
  Nothing may emit it, subscribe to it (policy, process, or projection
  `on`), or `upcast` it: all four are validation errors. Deleting the
  declaration instead is what leaves the log holding a type nothing can
  name, which `Migrate` now refuses (see Runtime)
- `index (col [desc] [nulls first|last], ...)` inside an `entity X @table`
  block declares a secondary index on its typed table. Columns are the
  entity's state fields or the meta columns `service`, `namespace`, `id`,
  `updated_at`, in the order written. The SDL gives no name: it defaults
  deterministically to `idx_` + snake_case(entity) + `_` + each column
  joined by `_` (WorkItem's `index (service, namespace, org_id, updated_at
  desc nulls last)` is `idx_work_item_service_namespace_org_id_updated_at`).
  It lands on `schema.Entity.Indexes` (`schema.Index{Name, Columns
  []IndexColumn{Name, Desc, Nulls}}`). Validation refuses an index on an
  entity without `@table` (and so on an aggregate mirror), a column the
  entity does not have (naming the entity and the column), and two indexes
  with identical column lists. `index` is a keyword only when `(` follows,
  so a field named `index` still parses
  ([ADR 0009](docs/adr/0009-a-projection-declares-its-indexes.md))
- `query (filter_col, ..., order_col [desc] [nulls first|last])`, also
  inside an `entity X @table` block, declares the index a filtered,
  ordered, paginated list read needs without the author working out the
  index by hand: it desugars at parse time into the same `schema.Index`
  a hand-written `index (...)` line would produce, plus a trailing `id`
  column appended automatically in the order column's own direction, as
  the stable tie-breaker a list page needs to never split or repeat a
  row. `query (status, updated_at desc nulls last)` and
  `index (status, updated_at desc nulls last, id desc)` are
  indistinguishable once parsed — same columns, same default name — so
  nothing downstream of parsing (`TableDef.Indexes`, `tables_gen.sql`,
  `Migrate`) changed to support it. Restricted to entity `@table`, like
  `index`
  ([ADR 0011](docs/adr/0011-a-declared-query-generates-its-index.md))

## Generated code

`loom generate` writes `loomgen/` (always regenerated, deterministically): typed
structs for commands (embedding `loom.CommandBase`), events (with
`LoomEvent()`), aggregate/entity states with `Fold` (field-name merge between
event and state, optionality-bridged), and `NewRegistry(Impl)` wiring
everything into `loom.Registry`. Each aggregate and record gets its own
`<snake name>_gen.go`: its commands, the events only it emits, its state and
fold, its `Handlers` interface, its def function, and its binding method on
`Impl` (`impl.Order(h)`). It registers itself, in `init`, into a table that
`NewRegistry` reads in kind-then-name order. An owner left unbound panics at
boot with its name. Each policy, process and `@fold` projection gets a file
the same way, `<snake name>_<kind>_gen.go`: its `Reactions` or `Folds`
interface, its binding method on `Impl` (`impl.ShipOnPayment(h)`), and an
`init` that registers its definition. So policies reacting to one event run
in name order. Each schema file gets `<its name>_types_gen.go`
(`goal.loom` → `goal_types_gen.go`): the enums, named types, entities and
series it declares, and the events it declares that several aggregates emit
or none does. So an edit to one schema file's types changes that file's
generated file and no other schema file's. A name a Go file cannot carry is
rewritten, and two schema files of one name take a number, in path order.
`models_gen.go` holds only what no schema file declares: everything, for a
schema parsed from a lone unnamed source, and nothing otherwise.
No file holds the whole registry. A schema file's types file also registers
what it declares: its enums, its types' required fields and docs, its shared
events, its entities' joins and `@table` definitions, and its series. A
projection loom folds itself and a schedule each get a file holding their
definition alone (`<snake name>_projection_gen.go`,
`<snake name>_schedule_gen.go`). The fixed scaffolding is `loom_gen.go`:
`Impl`, `NewRegistry`, and the two tables the other files add themselves to
in `init` — declarations that need an implementation, and those that do
not. It names nothing the schema declares but the service. The two
implementations no declaration's file holds stay fields on `Impl`,
`Upcasts`, `CommandUpcasts` and `Look`, each present once the schema has
an event upcast, a command upcast or a `@loop`, with its interface in
`upcasts_gen.go` or `look_gen.go`. So a
schema change edits `loom_gen.go` only when the first of either arrives. `NewRegistry` puts every list in name order,
never init order.
`generate` removes a generated file the schema no longer
produces (ADR 0008). Stubs (once, then yours): one file per aggregate and
reactor implementing the generated interfaces, plus `registry.go`, which
binds each of them through its method on `Impl`.
`layout: folders` puts stubs in per-kind packages (aggregates/, records/,
policies/, processes/) with registry.go at the root; flat stays the
default. Dependency structs are the user's to place — a leaf package like
processes/ is the natural home, since registry.go (root) imports the kind
packages, never the reverse.

No reflection anywhere: names come from generated methods, routing from
generated switches, folds from generated assignments.

## Runtime

- **Unit of work** (`Dispatch`): load (snapshot + fold in version order),
  handle, enforce the emit contract, append with the stream's UNIQUE
  constraint as the optimistic-concurrency guard (typed `ConflictError`,
  automatic retry against fresh state), run subscribed policies in the same
  transaction (depth-capped), write outbox rows for published events,
  snapshot every N.
- **Lazy decode**: the local log read (`readLog`) decrypts every row but
  decodes none: the payload rides `Event.raw` and each runner decodes only
  the types its own subscription check already let through, into a copy
  (the fan-out buffer hands one `*Event` to every runner at once, so
  decoding must never write to the shared pointer). Decoding eagerly for
  everyone meant one row of an undeclared or `@retired` type failed the
  whole batch for every runner sharing the buffer, including the ones that
  never look at that type. Aggregate replay (`loadState`) and bus
  deliveries (`eventFromEnvelope`, already subscription-filtered) decode
  as they always did. The cost of lazy decode is that an undeclared type
  is now invisible until something subscribes to it, so `Migrate` closes
  the gap: after the DDL and table/series diff it reads
  `SELECT DISTINCT type FROM loom_events` for the service and fails if any
  stored type is absent from the registry (active or `@retired`). Loud
  once at deploy time beats every runner tripping on it forever.
- **Global sequence**: `loom_events.global_seq` (identity). Projections and
  local processes are checkpointed catch-up readers over it — rebuildable,
  no bus, no publish-to-self hack for async self-handling. Both are elected
  so a scaled-out service runs one of each: a projection step by a
  transaction-scoped advisory lock (folds + checkpoint in one tx); the
  local-event processes by a per-service leader holding a session-scoped
  lock on a dedicated connection (`election.go`). Not per event: a
  reaction is an external call, and a lock that pinned a pool connection
  through it doubled every process step's connection use and starved
  projections and Dispatch (measured: projection lag ×3). A lease costs
  one connection, only while leading; hand-over is at-least-once. A
  strictly newer generation (`Config.Generation`, else the deploy's
  `LOOM_GENERATION`, else the Cloud Run revision's createTime; never the
  revision name's number, which Cloud Run does not assign in deploy order)
  takes the lease: the leader yields between events, or has its
  session ended if it does not answer (ADR 0010).
- **A runner passes a run of events it ignores in one write.** A process
  reads the whole log and reacts to a few types. It wrote its checkpoint
  after every event it read, its own or not, so 84,000 events of one type
  cost each of a service's 205 processes 84,000 writes through six step
  slots: hours, during which nothing reacted to anything (runsheet,
  2026-11-18). A process step now writes after each event it subscribes
  to, as before, and once for each run of events it does not: to the
  last ignored event before the next subscribed one, so a reaction that
  fails leaves the checkpoint just short of its event. When a batch read
  ends inside such a run, one statement finds where the run ends, up to
  10,000 events on (`ignoredAhead`, `skip.go`), and the step passes all
  of it; a projection step does the same inside its transaction. The
  rule for what a checkpoint may hold is the old one: sequence S only
  when every subscribed event at or before S was handled, parked or
  handed to a durable retry. The look ahead is one statement, so one
  snapshot, and a service's appends commit in sequence order under its
  log lock, so nothing subscribed can appear behind what it answered
  (the same argument `readLog` rests on). A crash redelivers at most the
  one event whose reaction finished without its write, as it always
  did. The write that passes a run moves the checkpoint only from where
  the step left it (`passCheckpoint`): a step that has lost its place —
  an operator's reset, a second leader after a forced hand-over — passes
  nothing and reads again. Since ADR 0019 the write after a handled
  event is guarded too.
- **A runner writes its checkpoint only when it matters** (ADR 0019,
  `checkpoint.go`). Within a step, a run of ignored events was one
  write. With events arriving a few at a time, each runner still wrote
  about once an event: ~46 writes an event and 89% of the statements on
  a runsheet e2e shard (2026-11-21). A runner now holds its position in
  memory. It writes the row with every event it applies, at most every
  `Config.CheckpointEvery` (1s) while passing ignored events, and within
  that interval of going idle. Every write is conditional on the row it
  last knew: a write that misses means the row was moved (a rebuild, a
  skip, a reset, another instance), and the runner reads the row again.
  An idle step sends nothing when the fan-out shows nothing new. When
  the row is due a look, the step sends one statement with no
  transaction (`probeCheckpoint`: try-lock, row, any event past the
  position). The liveness bound for readers of `loom_checkpoints` is
  `CheckpointEvery` plus a step, over ignored events only.
  `AwaitProjection` counts only the events the projection folds.
- **A loop is looked at per instance.** Each instance of a `@loop`
  aggregate has a row in `loom_loop_due`: when it is next looked at, when
  it last was, when its newest event arrived, and its waits. The first
  event of an instance writes the row, due at once; every later event
  brings `next_look` to within `LoopSoon` on the appending transaction.
  The loop runner (`looprun.go`), on every instance of the newest
  generation, claims due rows under a lease, calls the aggregate's look
  — which reads which instance it is, and why it was called, with
  `LookingAt(ctx)` — runs the commands it answers with as one unit of
  work, and writes the next look by `LoopState.NextLook`. A look's own
  events count as arrived since it began, so it is looked at once more
  and finds nothing to do. `LookDone(ctx)` ends an instance's looks until
  an event arrives for it; `LookAgain(ctx, d)` asks for the next sooner
  than the minute. A look that fails is logged and looked at again by
  the rule (ADR 0012).
- **A loop that keeps acting without progress is backed off.** A look's
  own events call it again within `LoopSoon`, which is right once — it
  sees what its commands did — and wrong for ever: a look that sends a
  command on every call ran flat out (runsheet's item loop polled a
  deploy every 1.4s on 18 items for three hours, 2026-11-18). The row
  now tells the two kinds of event apart: `event_at` is any event,
  `outside_at` only one that no look of the instance's own caused (the
  unit of work of a look's commands is marked on its context, and
  `noteLoopEvents` leaves `outside_at` alone for it; answers and child
  notes set both). `acting` counts the looks in a row that sent the same
  commands and left what loom records as it was: the waits (name, owner,
  deadline) and the outcome (not failed, parked or done), with no event
  from outside in between (`actingAfter`, `loopturn.go`). "The same
  commands" is their types and payloads in order (`CommandsDigest`, kept
  in `acting_sent`): a look that sends something else than the one
  before is a loop taking its steps and is never held back, and one that
  sends the same again is polling. From the
  second such look (`LoopActingFrom`) the look's own events bring the
  next only to `LoopActingBackoff` after it — 2s, doubling, never past
  `LoopEvery` — and `LookAgain` is held to the same floor. An hour of it
  is 65 looks, not 3,600. Nothing else is held: an event from outside
  still brings the look within `LoopSoon` (the appending transaction
  moves `next_look` itself) and ends the run, a wait's deadline is kept,
  and a look that sends nothing, fails, or changes what is recorded ends
  it too. The look that begins a spell is logged at WARN and told to
  `Config.OnLoopActing(ctx, LoopActing)` — service, aggregate, namespace,
  id, the commands it sent, the next look — once per spell: the count
  passes two once, under the row's lock. It is the rule's, not a
  watchdog's: it runs on the instance that made the look, at most once
  (an instance that dies between the row's commit and the call does not
  repeat it). The incident's own look is caught: past its wait's
  deadline runsheet's item loop sent one `PollWorkItemDeploy`, the same
  each time, straight from the look, and ended a wait that was no longer
  there; before the deadline it restarted the wait with the deadline it
  already had, which changes nothing either. What the rule cannot see:
  a look that restarts a wait with a new deadline each time has changed
  what is recorded, one that alternates two commands sends something
  else each time, and one driven by a timer or a process it set off
  gets its events from outside.
- **A wait belongs to a loop, and has an owner and a deadline.** A
  `Wait` is a name, who is to answer (`Owner`) and when to stop waiting
  (`Deadline`); one with any of the three missing is refused, so nothing
  waits on nobody or for ever. It is kept with its instance's looks, in
  `loom_loop_due.waits`. A look starts one with `WaitFor(ctx, w)` and
  ends one that has been answered with `StopWaiting(ctx, name)`; code
  outside a look uses `Client.Wait` and `Client.EndWait`, which take the
  row's lock, so a wait started while a look runs is kept. A wait's
  deadline is a time the instance is looked at: when it passes with the
  wait still open, the instance's look is called and told the wait has
  expired (`Looking.Expired`), once, and the wait is dropped. A look that
  fails is told again (`wait.go`, ADR 0012).
- **An approval is a wait on a person's yes or no.** It says what is
  asked, the role that may answer, the deadline, and what it becomes at
  the deadline if nobody has answered (`approve`, `reject` or `expire`);
  one that leaves any of those out is refused. It is a row of
  `loom_approvals`, so what is waiting on whom can be listed
  (`Client.Approvals`), and a wait on its instance, so the loop is called
  when it is settled. `AnswerApproval` takes an answer from someone
  holding the role, before the deadline, records who and why, ends the
  wait and brings the instance's next look to now. At the deadline the
  runner gives an open approval its declared action, by `deadline`,
  before the look that is told the wait has expired. The look reads how
  it came out with `LookingAt(ctx).Approval(name)`. The gateway serves
  `approvals` and `answerApproval`, and checks the caller's role in the
  namespace (`approval.go`, `graphql/approvals.go`).
- **An agent step is a wait on Mesh.** A `@loop` aggregate declares
  `agent triage { kind: "triage", deadline: 30m }`; the generator writes
  `AggregateDef.Agents` and, beside the `Look` interface,
  `Ask<Aggregate><Agent>(ctx, loom.AgentAsk)` and a name constant. A
  look asks with it (or `loom.AskAgent(ctx, name, ask)`): after a look
  that did not fail, the runner records the request in `loom_agents`
  (status `requested`, attempt n, the ask, the deadline from the look),
  starts a wait `AgentWait(name)` owned by `mesh` until the deadline,
  and offers one Mesh job (`Config.Mesh`, `POST /v1/jobs`) under an
  idempotency key naming the service, instance, agent and attempt, with
  `refs.loom` naming them too. An ask while the request is open changes
  nothing, so a level-triggered look may ask on every look; a request
  with no job yet — the offer failed, or the process died between
  recording and offering — is offered again at each look under the same
  key, and Mesh converges on the same job. `Client.MeshHook` (mounted at
  the gateway with `graphql.MeshHooks`, routed by `refs.loom.service`)
  verifies `X-Mesh-Signature` under `Config.MeshHookSecret` (401 if
  not; 400 for a body that is not a feed entry); a `job.ended` entry for
  an open request records it `finished` (end `finished`) or `failed`
  (any other end) with Mesh's outcome, ends the wait and brings the next
  look to now; anything else is 200 and changes nothing, so a retried
  hook settles once. A `job.confirm` ask is answered `run` while the
  request is open and `cancel` once it is not. At the deadline the runner
  marks an open request `expired` and cancels its job before the look
  that is told the wait expired. The look reads how it came out with
  `LookingAt(ctx).Agent(name)`. `mesh.HTTPClient` follows Mesh's own API
  (mesh-api `server/jobs.go`, `server/cancel.go`, `conformance/`);
  `mesh.Fake` takes offers the same way and posts signed hooks, and
  `loomtest.LoopScenario.Mesh`/`AgentAnswered` test a look's agent
  decisions with no database (`agent.go`, `mesh/`, ADR 0017).
- **An event type has a floor.** The release that first writes an event
  type is recorded with that event, in `loom_event_floors`, on the
  appending transaction: first write wins. `Start` refuses, with a
  `*FloorError`, a build whose `Config.Release` (else `LOOM_RELEASE`) is
  older than any floor in the store, naming the type and the release to
  start instead, because it could not decode what its runners would
  read. A release is the code's number, the same on every deploy of one
  build; a generation is the deploy's, and a rollback gets a new one. A
  build that states no release writes floors of zero and is never
  refused; so are the types a log held before floors were kept, seeded
  at zero by the first `Migrate` (`floor.go`, ADR 0013).
- **The log can be exported, signed.** `Client.ExportEvents` writes a
  service's events in log order as lines of JSON — a header, one line an
  event, a footer — each event line carrying a SHA-256 chain over the
  line before it and its own bytes, and the footer (the count, the last
  sequence, the last chain) signed by a `Signer` the caller gives.
  `VerifyExport`, and `loom verify-export` on the command line, check it
  away from the service: a changed, dropped, added or moved event breaks
  the chain at that event, and a file cut short or added to does not
  match its footer. Events are exported as stored, so a `@pii` field
  leaves sealed (`export.go`).
- **A setting is declared, and every change says why.**
  `setting build_cap: int @default(2)` declares a value a person may
  change while the service runs: a `string`, `int`, `bool`, `float` or an
  enum, with a default. It is read per namespace (`Client.Setting`, or
  the typed `loomgen.SettingBuildCap`): the namespace's own value, else
  the one set for every namespace (`EveryNamespace`, `"*"`), else the
  default. `SetSetting` and `ResetSetting` refuse an empty reason
  (`ErrNoReason`) and a value of the wrong type, and write the value and
  a history row in one transaction: who (the context's actor), when,
  why, the value before and after (`loom_settings`,
  `loom_setting_changes`). The gateway serves `settings`,
  `settingHistory`, `setSetting` and `resetSetting` for any service that
  declares one, under its own rules (`settings.go`,
  `graphql/settings.go`).
- **One connection per caller.** Nothing waits for a pooled connection
  while it holds one: rows are read in full and closed before the
  events or rows in them are opened, a data-key read or mint runs on the
  caller's own connection or transaction, and a projection step reads the
  log on its own transaction. Holding one connection while waiting for a
  second deadlocks the pool once every connection is held that way, and
  under load that is a matter of time (readLog opening @pii fields with
  its rows still open did it on 2026-10-30). A handler's or policy's
  `loom.Load`/`GetRecord`/`GetEntity` runs on the dispatch's own
  transaction for the same reason: that transaction holds the log lock,
  the dispatches queued on the lock hold the pool, and a policy read on
  the pool hung mesh-api on 2026-10-30.
- **The log lock is waited for with a deadline** (`Config.LogLockWait`,
  default 15s). A unit holds it for milliseconds; one that holds it for
  longer is stuck on something outside its transaction, and unbounded,
  every dispatch behind it waits with a connection apiece until the
  service hangs. Past the wait the dispatch fails with a `*LogLockError`
  naming the holder, logged at ERROR, and its connection goes back.
- **Process start position**: a missing checkpoint row reads as sequence
  0, so a process deployed into a service with history would react to all
  of it on its first run. `Start` closes that: before a process runner can
  step, it inserts the runner's checkpoint at the current head unless the
  process declares `@from(origin)` (`ON CONFLICT DO NOTHING` — an existing
  row, from a redeploy or another instance, is never moved). Projections
  are deliberately exempt: folding from the origin is what a read model
  is.
- **A projection halts; it does not retry forever or skip.** An event a
  projection cannot decode, fold, or write stops it at that event: the
  events before it commit, the checkpoint stops just short of it, and the
  same transaction records the stall on the `loom_checkpoints` row —
  `failing_seq` (the event), `attempts`, `last_error`, `stalled_since`
  (first failure only). Skipping would be worse than stopping: a read
  model folded out of order is wrong in ways no later event repairs.
  The runner backs off by attempts (twice the poll, doubling, capped at
  five minutes) instead of refolding the batch on every wake — the
  incident this replaces retried one unwritable row every 2s for six
  hours. The next step that gets past the event clears the stall. The
  way out is the operator's: fix the fold and `Rebuild`, or
  `POST /runners/{name}/skip` (`Client.SkipStalled`), which parks the
  event to `loom_dead_letters` in park's shape and advances the
  checkpoint past it in one transaction under the runner's lock.
  `GET /runners` carries `stalled`/`failing_seq`/`last_error`/
  `stalled_since`, so the read surface shows what skip acts on. Every
  runner is loud without being noisy: `runner step failed` logs at most
  once a minute per runner, with the failures since the last line; a
  runner whose checkpoint row still doesn't exist one poll after `Start`
  logs `runner never checkpointed` under the same ceiling (an idle
  runner seeds its row, so silence means it never got to step — a lock
  held elsewhere, a starved pool). Processes keep their own policy:
  retry, then park and move on. The four columns are additive; a
  consumer upgrading runs `Migrate` before the new runners start.
- **Idle runners back off** (`Config.RunnerIdlePoll`, distinct from the
  stalled-runner backoff above — this one backs off a runner with
  nothing to do, not a failing one). A runner that had work or an error
  steps again after `Start`'s poll; `idleWait` doubles that on each
  idle round, up to `RunnerIdlePoll`. Zero (the default) keeps every
  round at the poll, as before. New events still wake a runner at once
  through the log fan-out, so the unwoken step backed off here is only
  the safety net for a wake it missed, not the common path.
- **Outbox relay**: the one component ported by design from the old
  runtime: advisory-lock election, SKIP LOCKED claims, insert-order drain,
  per-aggregate ordering keys.
- **Bus providers**: `MemoryBus` (in-process, tests/single-binary) and
  `gpub` (Google Cloud Pub/Sub: one shared topic, subscription per
  consumer group, per-aggregate ordering keys, ResumePublish after a
  failed keyed publish, ordering disabled under the emulator whose ordered
  backlog is broken — lessons carried over from the old provider).
  Undecodable messages ack-and-log (nacking garbage redelivers forever);
  handler errors nack, and loom's dedup + parking make at-least-once safe.
  A `Codec` seam exists for bridging the old eventsourcing envelope during
  the (parked) six-service migration.
- **Processes**: local events from the log; foreign events from the bus
  with consumer-side dedup (`loom_dedup`). Retries with backoff, then loud
  parking to `loom_dead_letters`. Silent drops are structurally impossible.
- **Durable retries** (`@retry(max, min..max)`, `retry.go`): the fixed
  in-process attempts (three, 100/200/300ms) cover a blip, not an outage
  of minutes. A process declaring `@retry` does not park when they run
  out: the reaction is written to `loom_timers` as a retry row —
  `command_type = 'loom:retry'`, key
  `loom:retry:process:<name>/<service>:<global_seq>` (one per process and
  event), `command` holding the event in dead-letter shape (@pii
  re-sealed), the durable attempts so far and the last error — and the
  timer runner's existing lease re-fires it (claim -> commit -> fire, see
  Timers below). Each durable attempt is one reaction, after a backoff of
  full jitter over `[min, min(min·2^(n-1), max)]`. The reaction runs with
  no transaction open; its outcome is written afterwards in a short
  transaction of its own, every write conditional on the row still being
  the version the lease claimed (leased `fire_at` and `command`).
  Success deletes the row (and marks a foreign event processed in
  `loom_dedup`); failure re-arms it with the next backoff; the `max`-th
  failure parks the event to `loom_dead_letters` in park's shape
  (attempts = 3 + max, runner `process:<name>`, redrivable as ever) and
  deletes the row in the same transaction. Keying by (process, event) makes it converge: arming
  inserts `ON CONFLICT DO NOTHING`, and a redelivery of an event a retry
  row already owns (a checkpoint rewound by a crash, a bus redelivery) is
  skipped rather than reacted to twice. Retry rows fire on any instance,
  like timers, not only the process leader — the lease is the election. **Visible while it waits**: between the immediate attempts
  and the eventual park the reaction is a `loom:retry` row on
  `GET /timers` (its `fire_at` is the next attempt; overdue flags a stuck
  runner) and counts in `/stats`' `timers_pending` and the timers gauge;
  each failed attempt logs `durable retry failed; re-arming` at WARN with
  the attempt and max, and each fire is a `loom.retry.fire` span. No
  schema change: the rows reuse `loom_timers`' columns.
- **Persistent errors** (`loom.Persistent(err)`, `IsPersistent`): a
  reaction failing with an error so marked will not pass by trying again,
  so it skips the immediate attempts, `@retry`'s backoff and the redrive
  ladder: it parks at once (attempts = 1) and is reported to
  `OnDeadLetter`. Every other error retries as before.
- **Background-work hooks** (`hooks.go`): `Config.OnDeadLetter`,
  `OnRunnerLag` and `OnEffectInDoubt` tell a service its background work
  went wrong instead of it polling. A watchdog loop runs every poll on the
  instance holding the process lease (a service with hooks and no local
  process stands for the lease too). Each hook is called at most once per
  occurrence, and "once" lives in the store — the row is claimed before
  the hook runs — so a change of leader does not repeat it. A hook runs
  on the watchdog goroutine; a panic is recovered and logged.
  - **Redrive ladder** (`Config.RedriveLadder []time.Duration`): a
    process or timer letter parks with `redrive_at = now + ladder[0]`;
    the watchdog redrives due letters (`RedriveDeadLetter`), and each
    failure bumps `redrives` and sets the next step, or `NULL` when the
    ladder is spent (or the error is Persistent). A letter whose redrive
    succeeds is deleted — the failure passed, nobody is told. Empty ladder
    (the default): no automatic redrive.
  - **OnDeadLetter(ctx, DeadLetter)**: called once for each letter with
    `redrive_at IS NULL` (ladder spent, Persistent, no ladder, or a runner
    with no redrive such as a skipped projection event), marking
    `notified_at`. Letters parked before the upgrade are reported once.
  - **OnRunnerLag(ctx, RunnerLag)**: a runner's lag is the age of the
    oldest event of a type it subscribes to past its checkpoint (zero when
    it has none to handle) — a time, so it reads the same for a busy and a
    quiet log, and an event of a type the runner ignores never counts.
    Past `RunnerLagThreshold` (default 1m) the hook is called once and
    `loom_checkpoints.lag_notified_at` set; back within it the mark clears
    and a later crossing is reported again. Projections and local-event
    processes; foreign (bus) subscriptions have no checkpoint.
  - **OnEffectInDoubt(ctx, EffectRecord)**: `Config.Reconcile` already
    settles an in-doubt effect (it asks the other side; `@idempotent`
    re-runs it), so this is the only new piece for effects. When a
    reaction finds a claim neither can settle it marks
    `loom_effects.in_doubt_at`; the watchdog reports each marked,
    still-running row once (`doubt_notified_at`). A re-claim clears both
    marks; `ResolveEffect` ends the doubt.
  Schema: `loom_dead_letters` gains `redrives`, `redrive_at`,
  `notified_at`; `loom_checkpoints` gains `lag_notified_at`;
  `loom_effects` gains `in_doubt_at`, `doubt_notified_at`. All additive.
  A fourth hook, `OnLoopActing`, is not the watchdog's. The loop runner
  calls it from the look that begins a spell of acting without progress
  (see "A loop that keeps acting without progress is backed off").
- **Metadata**: correlation ids propagate across dispatches and the bus;
  causation records the triggering event. Both are columns, not folklore.
- **Observability**: OTel API only (no SDK dependency, no exporters, no
  config — a no-op until the deployment installs global providers). Spans
  at every execution seam; W3C trace context rides the envelope's `trace`
  field so consumers join the producing dispatch's trace across the bus;
  counters/histograms at the natural code points and DB-observed gauges
  (outbox depth/age, dead letters, timers, effects, per-runner lag) on
  the SDK's collection cycle. Correlation/causation ids ride spans as
  attributes. Logs are the same stance: loom logs through the `slog`
  handler the deployment gives it, and `loom.CloudLogHandler(w)` is an
  opt-in JSON handler a consumer's `main` wires when that deployment is
  Google Cloud — `level`→`severity` (DEBUG/INFO/WARNING/ERROR; slog's
  WARN is Cloud Logging's WARNING), `msg`→`message`, and the context's
  OTel trace promoted to Cloud Logging's trace/span fields. Without it a
  plain `slog.NewJSONHandler` writes `level`, which Cloud Logging does
  not read, and `severity>=ERROR` never finds `runner step failed`.
- **Context-injected reads**: every handler/reaction invocation carries
  read access (`loom.Load`/`GetRecord`/`GetEntity` on the ctx), so
  implementations never hold a client and registries wire without the
  set-client-after-New dance. Reads only — dispatch from inside a handler
  would nest units of work; reactions return commands. Inside a unit of
  work the reads run on its transaction and see its writes so far; a
  process's reads run on the pool.
- **Read models**: one `loom_entities` jsonb doc table by default; `@table`
  opts an entity into its own typed table (`loom_t_<service>_<entity>`, one
  real column per state field, non-scalars as jsonb). Generated code owns
  the shape — DDL, columns, typed upsert extractor — so the runtime never
  reflects; reads come back doc-shaped via `to_jsonb(row)` minus the meta
  columns, so folds and typed decodes share one path. `Migrate` applies the
  DDL plus an additive-only declarative diff: missing columns added, type
  drift a loud error, never an `ALTER TYPE` or drop (the remediation is
  drop table → `Migrate` → `Rebuild`; a storage swap is exactly the same
  move — a perf change, not a semantic one). `@table` + `@pii` is a
  validation error.
- **Declared indexes**: a `@table` entity's SDL `index` lines generate
  `TableDef.Indexes` (`[]loom.TableIndex{Name, DDL}`), each DDL
  `CREATE INDEX CONCURRENTLY IF NOT EXISTS <name> ON <table> ("col" [DESC]
  [NULLS FIRST|LAST], ...);`, also written to `loomgen/tables_gen.sql`
  right after the table's CREATE TABLE. `Migrate` creates them after the
  table DDL and column diff (`migrateIndexes`), in table-name then
  declared order. CONCURRENTLY builds without blocking writes on a live
  table but cannot run in a transaction block, so each index runs
  autocommitted on its own `pool.Acquire`d connection. A failed or
  interrupted concurrent build leaves an invalid index (`pg_index.indisvalid
  = false`) that `IF NOT EXISTS` would skip forever and the planner never
  uses, so before each create `Migrate` looks the name up in `pg_index`:
  valid is a no-op (a second `Migrate` changes nothing), invalid is dropped
  `CONCURRENTLY` and created again, up to 3 attempts, then an error naming
  the index and table. An index is matched by name only — a changed
  column list gets a new default name, and the old index is left for the
  operator to drop, like an extra column. `Rebuild` and `Reset` empty
  tables with `DELETE`, never truncate or drop, so declared indexes (slow
  to build on a large table) survive both
  ([ADR 0009](docs/adr/0009-a-projection-declares-its-indexes.md)).
- **List reads select only the fields they ask**: `Query.Fields []string`
  is opt-in; nil/empty is the whole-row read, byte-identical SQL to before
  it existed. Named, it narrows a `@table` list read's SELECT to those
  real columns — an unasked TOASTed column is never read off disk — and
  narrows the jsonb-document path's projection the same way, though that
  path still reads the whole `data` value server-side, so its saving is on
  the wire and in decoding only. The meta fields (`id`, `namespace`,
  `updated_at`) always ride the `Row`; `Filters`/`OrderBy` may still name
  fields `Fields` leaves out. The GraphQL list resolvers (`{x}s`,
  `{x}sChanged`) opt in automatically from the list field's own selection
  ([ADR 0009](docs/adr/0009-a-list-read-selects-only-asked-fields.md)).
- **Records** (`loom_records`): state-of-record persistence for the
  ledger/balance class. Row-locked writes, version per write; emitted
  events enter the log with the record's stream identity (projections and
  processes see them) but never rebuild the record. Deliberately a separate
  table from `loom_entities`: projection Rebuild empties entities and
  must never touch records.
- **Timers** (`loom_timers`): durable scheduled commands, written in the
  scheduling unit's transaction. Keyed idempotently (default: command type
  + target) so redelivered reactions overwrite; `loom.CancelTimer` deletes
  by the same key. The runner goes **claim -> commit -> fire**, and no
  transaction is open while it dispatches (ADR 0002). The claim is one
  autocommitted statement that leases a batch of due rows: `UPDATE …
  SET fire_at = now() + 5m WHERE (service, key) IN (SELECT … FOR UPDATE
  SKIP LOCKED LIMIT n) RETURNING …`. SKIP LOCKED separates concurrent
  pollers only while that statement runs; after it, the pushed-out
  `fire_at` keeps them apart. Each claimed timer then fires in its own
  unit of work (three in-process attempts). A fire that still fails is
  parked (runner `timer`). Either way the row is deleted afterwards,
  conditionally: `WHERE fire_at = <leased> AND command = <claimed>`. A
  reaction to the fired command that re-armed the same key has changed
  that row (`writeTimer`'s upsert) and has made the next firing, so the
  delete leaves it alone. Delivery is at-least-once. A process that dies
  between the claim and the delete leaves a leased row that comes due
  again when the lease expires. A fire that outlasts the lease may be
  repeated by another poller. A runner stopping mid-batch hands its unfired
  claims back at their original due time. Before this change the claim's
  FOR UPDATE was held across the fires. A same-key re-arm then waited on
  that lock while holding the namespace append lock, and the poller waited
  on the dispatch, which is a cycle Postgres cannot detect (runsheet,
  2026-09-25). A leased row shows on `GET /timers` with its lease expiry
  as `fire_at`. The same claim carries `@retry` processes' durable retry
  rows (`loom:retry`, above).
- **Batches** (`loom_batches`/`loom_batch_items`): durable chunked fan-out
  of many commands (CopyFrom insert, SKIP LOCKED chunk claims, stale-claim
  reclaim). Per-item outcomes are recorded (at-least-once per item — use
  deterministic ids); the batch row is live progress, streamable over SSE.
  Reactions enqueue atomically via `loom.AsBatch`.
- **Effects** (`loom_effects`): journaled external calls — the outbox's
  cousin for the request/response direction. `loom.Once(ctx, key, fn)` in a
  process reaction claims a row (committed before fn runs), settles it with
  the JSON result or the error, and replays `done` results on every retry
  or redelivery. The settle write runs on `context.WithoutCancel` of the
  reaction's ctx: once the call has returned, its outcome is a settled fact,
  so a per-step deadline that fires while the call was in flight records
  `failed` (or `done`) instead of leaving the row in doubt. An unsettled
  claim (crash between call and record) is *in doubt*. Before declaring
  it, the claim consults what the service told it: an `@idempotent` effect
  (the call is safe to repeat) is re-claimed like a failed one — attempts
  bumped, back to running — and re-runs; otherwise a `Config.Reconcile`
  hook registered for the effect name
  (`func(ctx, scope, key) (result, executed, err)`) is asked: executed
  settles the row `done` with the hook's result (on `WithoutCancel`, like
  every settle) and `Once` replays it; not executed re-claims and re-runs;
  an error keeps the doubt. With neither — the default for most effects —
  the runtime refuses to re-run it and parks the reaction with
  `EffectInDoubtError`: an operator resolves it (recording what actually
  happened on the other side) and redrives the dead letter, in one call
  with `POST /effects/resolve {…, "redrive": true}` /
  `ResolveEffect(…, ResolveOptions{Redrive: true})`. The redrive finds the
  letter from the effect's scope, `process:<name>/<service>:<global_seq>`:
  runner `process:<name>`, envelope `service` and `global_seq`. The resolve
  commits first and stands if the redrive fails (the letter stays; a
  plain redrive retries it); no letter yet (the reaction is still in its
  retries) is not an error — the next retry sees the resolution.
  At-most-once with loud ambiguity, the strongest guarantee non-idempotent
  APIs admit. Failed settles re-run on retry: an error return is the
  handler asserting the call did not happen.
- **PII encryption** (`loom_keys`): `@pii` fields are sealed with
  AES-256-GCM under a per-stream data key wrapped by a `KeyWrapper`
  (`LocalKeys` master key today; a KMS wrapper is the same interface).
  Sealing happens at every rest boundary — log, snapshots, read models,
  records, parked dead letters — and opening happens on folds and typed
  reads (`Load`/`Entity`/`Record` and their GET endpoints). Raw list
  queries and the log browser return ciphertext as stored, and jsonb
  filters can't match PII fields (keep a plain derived field like
  `tin_last4` for that). `Shred(ns, id)` deletes the key: every copy of
  that stream's PII becomes permanently unreadable and decodes as zero
  values, so replays and rebuilds keep working, redacted — erasure for an
  append-only store. Cached keys are evicted cluster-wide via the LISTEN
  channel. A fork's stream has no key of its own: a `loom_key_aliases`
  row points it at its source's namespace, so one person's stream has
  one key however many forks copied it, and shredding the source
  redacts every fork (ADR 0018).
- **Forks** (`fork.go`, `compare.go`, ADR 0018): `ForkStream` copies an
  instance's events up to a version into another namespace through
  `appendEvents`, with the settled `loom_effects` of reactions to them
  re-keyed to the copies, so the service re-runs the instance from that
  step on today's code and `loom.Once` replays what the source recorded.
  Copies are caused `fork:<ns>/<id>@<seq>` and are not put on the bus.
  `CompareFork` lines the two runs up by position after the fork point
  and lists the steps whose command (`loom_events.command`), event or
  payload differ: a recorded run as a regression test of changed look or
  agent code. The gateway serves both with `Config.Fork`, to All access
  only.
- **The log as a timeseries**: `global_seq` is monotonic and rows are
  append-only, so time queries are sequence-range queries. A BRIN index on
  `at` makes time-window scans cheap at ~zero write cost, and
  `(service, correlation_id)` serves trace-a-flow lookups. Deliberately no
  TimescaleDB dependency (unavailable on Cloud SQL); if volume ever
  demands it, native partitioning by `global_seq` range and rollup tables
  are the escalation path, not an engine change.
- **Series** (`series.go`): the third persistence shape — append-only
  observations (price ticks, readings, scraped sales) at volumes the
  log must not carry, and with none of its guarantees needed: no
  commands, no events, no reactions, no outbox, no global_seq traffic.
  `AppendSeries` bulk-inserts with `ON CONFLICT DO NOTHING` on
  `(service, namespace, dims-or-keys, time)`, so any batch replays
  idempotently — the crawler's retry story is the insert itself.
  Queries are the raw time-range read (filters on real columns) and
  `date_trunc` buckets (count/avg/min/max/last, grouped by dims) —
  date_trunc rather than time_bucket so the SQL is engine-portable.
  The engine question the log settled stays settled here: the
  declaration is engine-neutral, and Migrate *detects* TimescaleDB —
  extension present, the table becomes a hypertable (`create_hypertable`,
  `migrate_data => TRUE`); absent (Cloud SQL), a BRIN time index rides
  the same escalation path as the log. Series data is NOT rebuildable
  (the table is the only copy), which is why the additive column diff
  reports incompatible drift for hand migration instead of the
  drop-Migrate-Rebuild remediation, and why Reset's sweep is the only
  thing that ever empties one.
- **HTTP API** (`api.go` / `query.go`): the registry drives a complete
  mounted surface — command dispatch, filtered entity/record queries
  (validated field names, parameterized values, typed numeric/bool
  comparisons), log browsing, ops stats. This surface is ops-private;
  the deployment guards it. The GATEWAY is the public edge and carries
  the authorization model: `graphql.Access{Namespaces, All, Mutate,
  Mutations}` resolved per request by a `Config.Auth` hook (or the
  deployment's own middleware via `WithAccess`) — namespace scoping on
  every read/write/subscription and, via `Protect`, on Files downloads
  and Streams watches; `All` is god mode, unlocking namespace-less
  cross-namespace list queries (`Query.AllNamespaces`). Authentication
  itself (JWT/API key/session parsing) stays the deployment's job —
  loom only consumes the resulting capability.
- **Streaming = SSE, deliberately not gRPC** (`stream.go`): every streaming
  need in the domain is one-directional (server → client) — progress, log
  tails, state watches — which SSE serves over plain HTTP with native
  browser support and no proxy/Workers friction. The schema is already the
  typed contract, the bus already handles service-to-service, so gRPC
  would buy bidirectional streaming nobody needs at real compatibility
  cost. Resumability rides the global sequence (SSE id = seq →
  Last-Event-ID reconnect). Cross-instance wake-ups via pg LISTEN/NOTIFY
  (`listenLoop`), which also makes runners multi-instance-responsive. The
  NOTIFY is sent after an append commits, never inside it (`notifyLog`):
  a transaction that has queued a NOTIFY takes a cluster-wide lock at
  commit, so every append serialised on it. Notifies coalesce — one
  pending at a time, so the appends that commit while one is in flight
  share the next — and a lost one (a crash between commit and notify) is
  a lost wake the other instances' poll recovers. A
  gRPC transport can be added from the same registry later if a genuine
  internal-RPC need appears.

## Parent–child (1-*) across aggregates

The MassPayout problem: a parent with many child aggregates, and a UI
that wants to watch the children as a group. Aggregates are consistency
boundaries, so the parent must NOT fold its children (contention
hotspot, consistency lie). The relationship lives in three places:

1. Child events carry the parent id (`PaymentSettled { mass_payout_id }`).
2. Read models re-group children by parent — `key(field)` routes an
   event to the row named by that payload field instead of the event's
   own aggregate id, and `@fold` hands the projection's fold to a
   once-generated stub for shapes assignment folds can't express
   (`settled_count++`, per-child status maps). Checkpointing, the
   advisory lock, and rebuild stay framework-owned; hand-written folds
   must stay deterministic because rebuild refolds the whole log.
3. A process drives the parent's own transitions (`on PaymentSettled ->
   RecordPaymentOutcome`); the process runner serializes the commands,
   so no conflict storm.

The UI watches either the parent-keyed progress row (`{x}Changed`) or a
live filtered list: `{x}sChanged(namespace, where, order, limit)` is a
subscription form of every list query — requery on log wake-ups, diff,
re-send the whole list. No delta bookkeeping, so rows entering and
leaving the filter just work; size it with where/limit (tables, not
unbounded feeds).

## The gateway is the public surface

Services stay on the private network; the gateway serves everything a
UI needs, federating at the client level (it holds `*loom.Client`s, not
HTTP proxies): `/graphql` for queries, mutations, and `{x}Changed`
subscriptions — served over SSE on the same endpoint (`Accept:
text/event-stream`, graphql-sse-shaped `next`/`complete` events,
EventSource-compatible GET) via graphql-go's native `Subscribe` and the
exported `Client.Watch` wake-ups — plus `graphql.Files` for downloads
and `graphql.Streams` for raw resumable entity/aggregate watches. Only
watch paths pass through Streams; the ops surfaces (events log, stats,
console, redrive) never face the internet. GraphQL numbers: schema
`int` is int64 and emits/serves the `Long` scalar everywhere — money in
cents must not squeeze through a 32-bit Int (decided 2026-07-17).

The gateway serves agents too. `graphql.New` builds the MCP server
(package `mcp`: search, describe, dispatch, get, list) over its own
executor, and the handler answers any path ending in `/mcp` with it and
any path under `/.well-known/oauth-protected-resource` with the RFC 9728
sign-in document naming `Config.Issuer`; every other path is GraphQL.
Mount it at the root (`mux.Handle("/", gateway)`) and a service serves
MCP with no wiring of its own; `MCPOptions.Off` opts out (both paths
404). Each tool call is one in-process operation of the same gateway,
so Auth, Policy, `@role`, `@allow`, the depth limit and redaction apply
exactly as to an HTTP caller. The Executor contract lives in
`internal/gateway` so `graphql` can import `mcp` without a cycle;
`graphql.Executor` and `graphql.Result` are aliases of it.

The MCP server speaks MCP 2026-07-28 (go-sdk v1.8.0, stateless
streamable HTTP): no `initialize`, no `Mcp-Session-Id`, `server/discover`,
the version in each request's `_meta`, `resultType` on every result.
Clients on 2025-11-25 and earlier, with `initialize` and a session id,
are still served. It advertises tools (no `listChanged`: the five never
change) and nothing else. Not yet mapped: an approval or wait a command
opens answering `input_required`, and a loop instance's work as a task
(`io.modelcontextprotocol/tasks`) — both need the dispatch to wait on a
look that runs after the command commits.

Input nullability follows the schema, not Go: a GraphQL input field is
NonNull exactly when the schema marks it required, at a command's top
level (`CommandDef.Required`) and inside a nested type
(`Registry.Types`) alike, so the served schema matches the emitted SDL;
enums stay nullable
([ADR 0004](docs/adr/0004-nested-input-nullability-follows-the-schema.md)).

## Uploads: large files without bytes in the domain

Files never enter the log or cross the bus — events carry a `FileRef`
(id, key, name, content type, size), bytes live behind the `BlobStore`
seam (gblob on GCS, `DirBlobStore` locally — same chunked-PUT/308
resumable protocol, so upload client code is identical in dev). The
runtime brokers the session: `POST /uploads` (or the gateway's
`create{Name}Upload` mutation) opens a resumable session server-side
and returns the session URL; the browser PUTs chunks straight to
storage. Zero file bytes transit the service — which also sidesteps
Cloud Run's request-size ceiling.

The lifecycle is domain-visible by declaration, not convention:

```
upload Contract {
  on started  -> RequestContract   // optional: session opened
  on uploaded -> AttachContract    // object finalized and verified
}
```

`on uploaded` is never driven by the client claiming completion: it
fires from storage's own finalize signal (GCS Object Finalize →
Pub/Sub → `gblob.Watch` → `FinalizeUpload`; the dev store calls it
synchronously on the last chunk). FinalizeUpload Stats the object and
builds the FileRef from what storage actually holds, then dispatches;
deliveries are at-least-once, dedup'd on the object key in
`loom_dedup` like foreign events. Fold caveat worth knowing: folds
merge by field name, so give the `started` event a different field
name than the state's — a merely-requested file must not read as
attached.

Object keys are stream-prefixed (`service/namespace/streamID/upload/
fileID`), which is Shred's lever: shredding a stream deletes its
objects outright alongside its data key (bytes are outside the DEK's
reach, so deletion, not crypto-shred). A GCS lifecycle rule on
incomplete sessions cleans up abandoned uploads; `@pii` on a `file`
field seals the *reference* at rest like any other field.

The one place the provider shows through is the chunk dialect the
browser speaks at the session URL, so `UploadSession` carries a
`protocol` discriminator (`gcs-resumable` today; `s3-multipart`
reserved for S3/R2). Clients switch on it — never assume the dialect —
so adding a Cloudflare R2 or S3 store later is additive: new store
package + new client adapter, no contract break.

Downloads keep the services private: `FileRef.downloadUrl` points at
the gateway's `graphql.Files` handler, which routes by the key's
service prefix and streams through the owning client — the gateway
federates at the client level, so no service HTTP surface is involved.
The per-service `GET /files` remains for internal/ops use. A signed-URL
upgrade later changes what ServeFile does (redirect instead of stream),
not who calls it.

GraphQL note: `FileRef.size` rides a `Long` scalar — GraphQL `Int` is
32-bit and files are not. Long now exists in the SDL contract and the
runtime gateway for future opt-in use by `int` fields.

## The console (M2)

`/console` on every service: an embedded, self-contained page over the
JSON endpoints. Overview (health cards, batches, event volumes), Design
(the registry as a document — commands' emit contracts, reactions'
dispatch contracts via `ReactorDef.Subs`, effects, PII markers), Events
(log browser), Issues (checkpoint lag per runner — the stuck-detection
signal — with a stalled projection's failing seq, error, and a skip
button over `POST /runners/{name}/skip`; in-doubt effect resolution,
dead-letter redrive, overdue timers).
Deliberately no framework UI dependency and no build step; a topology
graph and the Performance tab are the deferred follow-ons.

## Not yet built (tracked on the issue)

Console topology graph + Performance tab, old-envelope compat codec for
gpub (migration on-ramp), persisted process state + timeouts, upcasters
beyond aliases, given/when/then harness generation, `loom extract` (legacy
on-ramp, returning from tag `m1-extraction`), replay-parity harness, table
migrator, OTel, TypeScript target.
