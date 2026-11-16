---
summary: Notable changes to loom by release, newest first; starts at v0.54.0, the timer-lock incident fix.
---

# Changelog

Notable changes to loom, newest first. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Every merge to
`main` is tagged automatically ([ADR 0001](docs/adr/0001-ci-and-release-tagging.md)):
the next MINOR by default, or the next PATCH for a PR labelled `patch`. This
file starts at v0.54.0. For earlier releases, see the git history and tags.

## [Unreleased]

### Changed

- **An aggregate folds every event it declares, emitted or not.** The
  generated `Fold` has a case for every event the aggregate's commands
  emit and for every event declared in its block that is not
  `@retired`. Before, a case lasted only while some command emitted the
  event: deleting the last emitter silently dropped it, and stored rows
  of the event replayed as if they were not there. To stop producing an
  event, delete its emitter and keep the declaration; its rows fold as
  before. `@retired` is still the only way to take a fold away. No new
  annotation.
  ([ADR 0016](docs/adr/0016-an-aggregate-folds-every-event-it-declares.md))

  **Migration:** regenerate `loomgen/`. An aggregate that declares an
  event no command emits gets that event's fold case back. runsheet
  gets the `KindProbe*` cases on `KindBreaker` and `KindFailureRecorded`
  on `KindFailureWindow`. If its state should no longer take the
  event's fields, mark the event `@retired`. A snapshot taken while a
  fold was missing does not refold the earlier rows; a replay from
  version 0 does.
- **Projections fold a batch with one read and one write.** A projection
  step read (and locked) its row and wrote it back for every event: two
  round trips each. It now reads every row a batch touches at once, folds
  the batch in memory in order, and writes each changed row once. A
  backlog of 2,000 events folds at ~7,800 events/s, up from ~1,470
  (`BenchmarkPerfProjectionCatchUp`). `@table` projections keep the
  per-event fold.
- **A load reads no snapshot for an aggregate without `@snapshot`.** One
  round trip fewer on every load and dispatch: a dispatch to a new stream
  ~1.69 ms from ~2.14 ms, a load of a 500-event stream ~1.03 ms from
  ~1.38 ms (`internal/e2e/perf_bench_test.go`, local Postgres).

### Added

- **`docs/features.md` and `loom adopt`: what a service is missing.**
  The guide says, for each feature, what it makes unnecessary and the
  release that brought it; `go tool loom adopt` lists the places a
  service still writes that code by hand (rules `ticker-goroutine`,
  `self-rearming-timer`, `timer-policy`). From here on, a release that
  adds a feature adds its entry to the guide and, where it can, an adopt
  rule, and names the rule here.
- **`loomgen.NewPartialRegistry`: the registry a test builds.** It is
  the whole registry from an `Impl` that need not bind everything: a
  declaration nothing is bound to is there with its definition and no
  implementation, and panics only when loom calls it, where
  `NewRegistry` panics at once naming the first it misses. A test binds
  the one handler it tests, or none to read the schema's definitions; a
  service starts on `NewRegistry`. (v0.116.0 left an unbound declaration
  out.) **Migration:** regenerate `loomgen/`.

- **Bindings chain, and are read back.** A binding method answers its
  `Impl`, so one expression builds one —
  `*new(loomgen.Impl).Order(h).ShipOnPayment(p)` — where a composite
  literal did before `Impl` lost its fields. `loomgen.BoundBy(&impl)`
  reads what is bound, by the binding methods' own names, nil when
  nothing is: a caller wraps a handler before the `Impl` becomes a
  registry with `impl.Order(logged{loomgen.BoundBy(&impl).Order()})`.
  **Migration:** regenerate `loomgen/`; calls that ignore the result are
  unchanged.

- **`loomtest.Page`: a fake list read that pages as the store does.**
  `loomtest.Page(rows, q)` answers the page of `rows` after the row
  `q.After` names, at most `q.Limit` of them. It stands where a test's
  fake sliced by `Query.Offset`, so code that walks a list with
  `loom.Every` or `loom.Pages` is tested through the same cursor.

- **`loom.Every` and `loom.Pages`: a list read walked to its end.**
  `Every(ctx, read, q)` answers every row of a list however many pages
  that takes, or an error and no rows; `Pages` yields a page at a time.
  Both page by cursor, so they replace the `Offset` loops the keyset
  read ended. `Client.ListEntities(name)` and `Client.ListRecords(name)`
  are the reads to hand them, and `loom.MaxPage` (500) names the most
  rows one read answers. With no `OrderBy`, `Every` reads ascending by
  `updated_at`, so a row written to while the pages are read is met
  again and answered once (`every.go`).

- **Approvals: what is asked, who may answer, until when, and what then.**
  A loop asks a person with `loom.AskApproval(ctx, loom.Approval{Name,
  What, Role, Deadline, AtDeadline})` (or `Client.AskApproval`). The
  approval is a record that `Client.Approvals` lists as waiting on its
  role. `Client.AnswerApproval` takes a yes or no from someone holding
  the role, before the deadline; with no answer by then the approval
  becomes what it declared — `approve`, `reject` or `expire` — by
  `deadline`. Either way its instance's look is called and reads the
  outcome with `loom.LookingAt(ctx).Approval(name)`. The gateway serves
  `approvals` and `answerApproval` for a service with a `@loop`
  aggregate, and checks the caller's role. New table `loom_approvals`.
  ([ADR 0012](docs/adr/0012-a-loop-is-declared-per-aggregate-and-a-wait-is-built-in.md))

- **A wait: an owner, a deadline, and a loop it calls.** A loop that is
  waiting on someone says so: `loom.WaitFor(ctx, loom.Wait{Name, Owner,
  Deadline})` in a look, or `Client.Wait` from outside one. The wait is
  kept with its instance's looks. When the deadline passes with the
  wait still open, the instance's look is called and told the wait has
  expired and who was to answer (`loom.LookingAt(ctx).ExpiredWait`),
  once. `loom.StopWaiting(ctx, name)` and `Client.EndWait` end a wait
  that has been answered. A wait with no name, owner or deadline is
  refused.
  ([ADR 0012](docs/adr/0012-a-loop-is-declared-per-aggregate-and-a-wait-is-built-in.md))

- **A `@loop` aggregate's look runs, for every live instance.** Each
  instance has a row in the new `loom_loop_due` table. Its first event
  makes it due at once, every later event brings its next look to
  within a second, and with no event it is looked at again within a
  minute. The look learns which instance it is from
  `loom.LookingAt(ctx)` (`Aggregate`, `Namespace`, `ID`) and returns the
  commands for the next step, which run as one unit of work.
  `loom.LookDone(ctx)` ends an instance's looks until an event arrives
  for it; `loom.LookAgain(ctx, d)` asks for the next one sooner.
  `Client.LoopDue` reads when an instance is next looked at. Looks run
  on every instance of the newest generation, one runner per instance
  at a time. Regenerate `loomgen/`: an aggregate's definition now
  carries its look.
  ([ADR 0012](docs/adr/0012-a-loop-is-declared-per-aggregate-and-a-wait-is-built-in.md))

- **A loop's scheduling rule, and `loomtest.Loop`.** When a `@loop`
  aggregate's look is next made is one rule that reads no clock and no
  store, `loom.LoopState.NextLook`: no later than a minute after the last
  look (`LoopEvery`), within a second of an event that has arrived since
  (`LoopSoon`), and at the first deadline among the loop's waits. A
  `loom.Wait` has a name, an owner and a deadline, and a look reads why
  it was called with `loom.LookingAt(ctx)`: whether an event arrived and
  which waits have expired. `loomtest.Loop` runs a look under that rule
  with no database, handling the commands it dispatches so a second look
  sees what the first did. Nothing calls a look in a running service
  yet.
  ([ADR 0012](docs/adr/0012-a-loop-is-declared-per-aggregate-and-a-wait-is-built-in.md))

- **`loomtest.Postgres`: a database of its own for a test.** For what
  the given/when/then harness cannot show because it needs the store — a
  client's `Migrate` and `Start`, dispatches that commit, runners —
  `loomtest.Postgres(t)` creates a fresh database on the server
  `LOOM_TEST_PG` names, answers a pool on it sized for a test, and drops
  it when the test ends. With no server to reach the test is skipped.
  loom's own end-to-end tests use it.

- **Settings: declared, typed, and every change with a reason.**
  `setting build_cap: int @default(2)` declares a value a person may
  change while the service runs (`string`, `int`, `bool`, `float` or an
  enum). `loom generate` writes a typed reader, `loomgen.SettingBuildCap`.
  A namespace reads its own value, else the one set for every namespace
  (`loom.EveryNamespace`), else the default. `Client.SetSetting` and
  `ResetSetting` refuse an empty reason (`loom.ErrNoReason`) and a value
  of the wrong type, and keep every change: who, when, why, and the
  value before and after (`Client.SettingHistory`). The gateway serves
  `settings`, `settingHistory`, `setSetting` and `resetSetting` for any
  service that declares a setting, under its own rules. New tables
  `loom_settings` and `loom_setting_changes`. Regenerate `loomgen/`.

- **A signed export of a service's events, for audit.**
  `Client.ExportEvents(ctx, w, ExportOptions{Signer})` writes the log,
  or one namespace or a range of it, as lines of JSON: each event
  carries a hash chained from the one before, and the footer — the
  count and the last chain — is signed. `loom.VerifyExport`, and
  `loom verify-export --key <public key> <file>` on the command line,
  check an export away from the service and name the first event that
  does not check: a changed, dropped, added or moved event, a file cut
  short, or another key's signature all fail. `Ed25519Signer` and
  `Ed25519Verifier` cover a key in hand; a product signs with the key
  its identity service issued by implementing `Signer`. Events are
  exported as stored: a `@pii` field leaves sealed.

- **A build older than a stored event type refuses to start.** The store
  records, with each event type's first event, the release that wrote it
  (`loom_event_floors`; first write wins). `Start` returns a
  `*loom.FloorError` for a build whose release is older than any of
  them, naming the event type and the release to start instead: the
  rule "never roll back past new event types", enforced.
  `Config.Release` (else `LOOM_RELEASE`) states the build's release — a
  number of the code, the same on every deploy of one build, unlike
  `Config.Generation`. `Client.EventFloors` lists the floors. A build
  that states no release writes floors of zero and is never refused,
  and the first `Migrate` seeds a floor of zero for every type already
  in the log, with one pass over it.
  ([ADR 0013](docs/adr/0013-an-event-type-has-a-release-floor.md))

- **Package `contract`: every operation, as every principal.**
  `contract.Run(t, Suite{Gateway, Principals, Namespaces, Golden})` reads
  the gateway's own schema and, for each kind of principal the service
  names, records who may run each query and mutation, which rows each
  list shows it in each namespace, and which fields are hidden from it.
  The record is a JSON file committed beside the code; `Run` fails for
  every operation and principal that differs from it, naming both, until
  it is recorded again with `LOOM_CONTRACT_UPDATE=1`. A policy that skips
  a rule for one kind of principal changes that principal's rows and
  fails. With `Suite.Asks` (`contract.ReadAsks`, the shape of a client's
  published `gateway-asks.json`) every query, mutation and field the
  client asks must be served to every principal it serves; a refusal
  names the ask and the principal. Mutations and reads that take more
  than a namespace are probed, not run.

- **`wire:` in loom.yml: implementations wired by convention.** With
  `layout: folders`, `wire: wire` has `loom generate` write one wiring
  file per declaration into a generated package —
  `wire/order_aggregate_gen.go`, `wire/notify_process_gen.go` — each
  binding its implementation: the constructor `New<Name>` in its kind's
  package when there is one, taking nothing or the one dependency type
  the service's constructors share (`wire.Deps`), otherwise the type
  `<Name>`. `registry.go` only builds the registry from `wire.Impl(deps)`
  and no declaration edits it, so two branches that each add an
  aggregate touch disjoint files. `wire.Configure(func(*aggregates.Goal,
  wire.Deps))` in a hand-written file of the package is the wiring no
  convention says. A removed declaration's wiring file is removed, and
  `--check` covers the package.
- **`Client.Every`: a function on an interval.** The schedule for
  recurring work that is not a command. It is called at the start and
  every interval after, on one instance of the newest generation alive,
  and again at the next interval whatever came of the last call: an
  error or a panic is logged. `Status.Schedules` lists it. It is for
  work the service does once wherever it runs, not for a cache each
  instance keeps in its own memory.
- **Idempotency keys.** `loom.WithIdempotencyKey(ctx, key)` makes the
  next `Dispatch` run only if no dispatch of the same command to the
  same stream has committed under the key; a repeat does nothing and
  says so. The key is recorded in the dispatch's own transaction, so a
  dispatch that failed left none and runs when sent again, and two sent
  at once resolve to one. The gateway reads the `Idempotency-Key`
  header and answers a repeat `status: duplicate`; the MCP `dispatch`
  tool takes `key`. New table `loom_idempotency`; keys are kept seven
  days.

### Changed

- **`loom.Every` keeps every row of a newest-first read.** With
  `OrderBy: "-updated_at"` it reads oldest first and answers in reverse:
  walked from the newest down, a row written to while the pages were
  read moved behind the walk and was lost.

- **List reads page by cursor, with an id tie-breaker.** Every list
  read orders by its column and then by id, so rows that tie on the
  column have one order, and pages past a row with a keyset in place of
  `OFFSET`: a page walk visits every row once however many tie (a tie of
  113 rows stalled runsheet's merge queue on 17 Oct). `loom.Query.Offset`
  is replaced by `Query.After *Cursor`, built from a page's last row
  with `Row.Cursor(orderBy)`. Through the gateway, each row of a list
  carries a `cursor` and the list takes `after: String`; over HTTP, the
  list endpoints take `after_id` with `after_value` or `after_null=1`.
  ([ADR 0014](docs/adr/0014-list-reads-page-by-cursor.md))

  **Migration:** a caller that pages with `offset` moves to the cursor.
  The gateway's lists no longer accept `offset`, so a query that passes
  it fails validation; one that passes only `limit` is unaffected.
  Series lists keep `offset`.

- **No generated file lists the whole registry.** `loomgen/registry_gen.go`
  is no longer written. Every declaration registers itself from the
  file that holds it: enums, types, shared events, `@table` entities
  and series from their schema file's `_types_gen.go`; a projection loom
  folds itself from `<snake name>_projection_gen.go`; a schedule from
  `<snake name>_schedule_gen.go`. What is left is fixed: `loom_gen.go`
  holds `Impl` and `NewRegistry` and names nothing the schema declares.
  The `EventUpcasts` and `Look` interfaces move to `upcasts_gen.go` and
  `look_gen.go`, each written once the schema has an upcast or a
  `@loop`. So a change to a schema edits no file that another,
  unrelated change also edits, bar those two lists.
  `NewRegistry` puts every list of the registry in name order.

  **Migration:** regenerate `loomgen/`; `generate` removes the old
  `registry_gen.go`. `Impl`, `NewRegistry`, `EventUpcasts` and `Look`
  keep their names, so `registry.go` needs nothing beyond the binding
  calls described in the entry below.

- **Policies, processes and `@fold` projections register themselves.**
  Each gets its own generated file, `loomgen/<snake name>_<kind>_gen.go`
  (`shipOnPayment` → `ship_on_payment_process_gen.go`), holding its
  `Reactions` or `Folds` interface, a binding method on `Impl` and its
  definition, as aggregates and records already had. `registry_gen.go`
  no longer names any of them, so adding a policy or a process adds one
  generated file and changes none. One never bound panics when the
  registry is built, naming the call to add, where a nil field used to
  fail at its first event.

  **Migration:** regenerate `loomgen/`. `Impl` loses its field for each
  policy, process and `@fold` projection: in `registry.go`, move each
  out of the literal into a call of the same name —
  `impl.ShipOnPayment(&ShipOnPayment{})` for
  `Impl{ShipOnPayment: &ShipOnPayment{}}`. `Upcasts` stays a field. A
  service using `wire:` only regenerates. Policies that react to the
  same event now run in name order, where they ran in schema order.

- **Each schema file's types are generated into their own file.**
  `loom generate` writes the enums, named types, entities and series a
  `.loom` file declares, and the events it declares that no single
  aggregate or record owns, into `loomgen/<schema file>_types_gen.go`
  (`goal.loom` → `goal_types_gen.go`) instead of the shared
  `models_gen.go`. Two changes to types in different schema files no
  longer touch the same generated file. `models_gen.go` stays, holding
  only what no schema file declares: nothing, for a schema read from
  files. An event one aggregate or record emits still lives in that
  owner's file. The package, the type names and the events' wire format
  are unchanged, so callers need no edits. `schema.Enum`, `NamedType`,
  `Event`, `Entity` and `Series` gain `File`, the path of the schema
  file that declares each.

  **Migration:** regenerate `loomgen/` and commit the new
  `_types_gen.go` files with the emptied `models_gen.go`.
  `registry_gen.go` still lists every enum's values and every type's
  required fields, so two changes to those can still meet there.

- **A plan says what its reactions decide.** `Client.Plan` now runs the
  command, its policies and its processes on a transaction it rolls
  back, where it used to run the handler alone and list who reacts from
  the schema. `Plan.Events` is every event the run would lead to, in
  order — the command's own, then those of the commands its reactions
  dispatch — each with the command that emits it, the reaction that
  caused it (`Cause`) and the stream it is written to. Each
  `PlannedReaction` carries an `Outcome`: `decided`, with the
  `Commands` it dispatches (run, timer with its time, cancelled timer,
  batch); `calls`, for a process that reached a `loom.Once` call, which
  a plan does not make; `fails`; `not_yet`; or `not_followed`, past ten
  processes deep or two seconds. A policy that fails now fails the
  plan, as it fails the command. The gateway's `Plan` type and the MCP
  `dispatch` answer carry the same. A plan takes no log lock, so it
  holds up no dispatch but one to a stream it has written to.

  On upgrade: a plan now runs reaction code for something that has not
  happened. A reaction that reaches outside its transaction other than
  through `loom.Once` checks the new `loom.Planning(ctx)`;
  `Client.Dispatch` under a plan's context is refused. `runPlan` still
  checks only the command's own target, so what reactions decide at the
  run may differ from the plan.

- **The newest build alive publishes the outbox and consumes the bus.**
  On an in-process bus a relay delivers to its own instance's
  consumers, so an older build's relay handed every event to the older
  build's code; the relay now runs on the newest generation alive. A
  consumer of another service's events on a superseded instance hands
  the event back (`loom.ErrSuperseded`) before it looks at it, where it
  used to ack a type only the newer build subscribes to, park one it
  could not decode, or react with old code. `gpub` nacks it without a
  warning and stops pulling for three seconds, so an older instance
  does not slow delivery to the newer one by pulling and handing back.
  This closes the gap left open when timers, batches and projections
  moved to the newest generation.

- **A plan names what reacts.** Each event of a `Plan` now carries
  `Reactions`: this service's policies with the commands they may
  dispatch, its processes with the outside calls they declare, its
  projections with the read model they update — and `Published`, for
  an event that goes to the bus. Through a gateway, the other composed
  services' processes that consume the event are listed too, by
  service. It is read from the schema and nothing is run: a plan says
  who reacts, not what each would decide. `Client.Reactions` and
  `Client.ConsumedReactions` answer the same for any event.

- **`@allow` on a field: who is shown it.** A state or entity field
  takes `@allow(kind: admin)`. A caller no rule holds for still reads
  the record; that field resolves to null, and the response carries an
  error at the field's path with `extensions.code` `FIELD_HIDDEN`, so
  "hidden from you" is never read as "not set". The caller may not
  filter or order a list by a field hidden from it. A field's rule
  compares the caller alone, so what is hidden is the same on every
  record: the MCP tools mark it in `describe`, leave it out of a
  default `get` and `list` with a note beside the data saying so, and
  refuse it by name. `graphql.Executor.Hidden` answers a type's hidden
  fields for a caller. This is the one case in which the gateway
  answers in part; a `Policy` that forbids a field still denies the
  operation. **Migration:** regenerate `loomgen/`.

- **`PreviewDeleteStream`: what a stream delete would remove.** The
  same counts `DeleteStream` reports — events, snapshots, outbox rows,
  read-model rows, the projections left to rebuild — with nothing
  removed: it runs the delete's own statements in a transaction it
  rolls back, and shreds no key. `POST /streams/delete` takes
  `dry_run: true`.

### Fixed

- **`loom generate` writes no root `registry.go` when the registry is
  built elsewhere.** A service that keeps its registry in a package of
  its own (Tripline's `store/registry.go`) got a second `registry.go` at
  the root on every regenerate. A non-test file anywhere in the module
  that calls the generated package's `NewRegistry` is now reported kept
  in its place.

- **`loom generate` writes no stub for a type implemented in a file named
  otherwise.** A stub is named after its declaration (`order.go` for
  `Order`); an implementation the owner keeps in `model.go` got a second
  `order.go` declaring `Order` on every regenerate, which did not
  compile. The generator now reads the stub's directory first, and a
  file there that declares the stub's type or function is reported kept.

- **Two processes migrating one database no longer collide.** `Migrate`
  holds a lock for the database while it runs. `CREATE ... IF NOT EXISTS`
  does not stop two sessions creating the same table, or the same day's
  partition of a `@retain` series, from colliding in the catalog
  (`relation ... already exists`, or a duplicate key in
  `pg_type_typname_nsp_index`), so a rollout's instances or a test
  suite's packages migrating one empty database at once could fail.

- **An upload answers to its commands' gates.** `create<Name>Upload`
  was decided on the namespace alone, so a caller a command's `@role`
  or `@allow` refuses could still have the command run by opening an
  upload: the store dispatches it when the bytes land. The upload
  mutation now asks each of its commands' gates of the caller, with
  `@allow` own-fields read from the stored state. A schema whose
  upload dispatches a `@plan` command is refused: there is no caller
  at that point to show a plan to.

- **`schedule`: a command on an interval, with loom owning the clock.**
  `schedule flowWatch every 5m -> TickFlowWatch @in(platform)`
  dispatches the command at the deploy and every interval after, to one
  stream: `@in` names its namespace, `@id("uuid")` its id, or one is
  derived from the service and the schedule's name. It rides the timer
  table, on the newest generation alive, and its row is moved on after
  every tick whether the command succeeded or not, so a tick that fails
  does not end the series — which a command re-arming itself with
  `loom.After` cannot promise. A failed tick is logged, not parked.
  Missed ticks are not made up. `Status.Schedules` lists each with its
  next time. **Migration:** regenerate `loomgen/`.
- **`loom.NotYet`: look again until it is done.** A reaction that
  returns `loom.NotYet(reason)` is not a failure: it is looked at again
  later under its process's `@retry`, without the three immediate
  attempts and without failure logs. When the attempts run out the
  event is parked, saying how many times it looked. A process with no
  `@retry` that answers `NotYet` is parked at once, saying so.
- **`Client.ReplayProcess`: hand a process events it has been past.**
  For a process added after the things it is about exist, or whose
  outside call was down while they went by. Narrow it by event type,
  namespace or stream; `LatestPerAggregate` reacts once per existing
  thing; `DryRun` counts. The process's checkpoint does not move, and
  the replay's effects run again under its own id rather than being
  answered from the journal. Also `POST /processes/{name}/replay`.
- **The gateway records who ran a command.** `Access.Actor`, set by the
  `Auth` hook, is stamped on the metadata of every command a request
  dispatches, over GraphQL and through the MCP tools, so its events
  name the caller. A planned command's events name whoever ran the
  plan, with `plan:<id>` as their cause.
- **MCP sign-in discovery.** `mcp.Config.SignIn` names the endpoint and
  the authorization servers whose tokens it takes. A request with no
  usable token is answered 401 naming the sign-in document (RFC 9728),
  and `mcp.Mount` serves both the endpoint and that document. loom
  issues no token: the gateway's `Auth` hook still decides who a
  bearer is.
- **`loom access`: who may run what, as a document.** Every command
  and generated read under the name the gateway serves, with its
  `@role` names, its `@allow` rules read aloud and whether it is
  planned first. `--check` fails when the checked-in file is missing or
  stale; `loom graphql` and `loom openapi` take `--check` too.

- **The gateway serves agents over MCP.** `mcp.New(mcp.Config{Gateway: h})`
  mounts five tools over the handler `graphql.New` returned: `search`,
  `describe`, `dispatch`, `get` and `list`. They are the same for every
  schema, so a service with hundreds of commands gives an agent five
  tools, and they reach every command and read the composed schema has,
  hand-written `Field`s included. Each call is one GraphQL operation run
  in-process through the new `graphql.Executor` (`graphql.AsExecutor`),
  so it meets the gateway's `Auth`, `Policy`, `@role` gates, depth limit
  and redaction as an HTTP caller's does; the package holds no rules of
  its own. Streamable HTTP, stateless.
- **Doc comments.** A `///` block directly above an aggregate, record,
  entity, type, enum, command or field is its doc. The generator carries
  it into the registry (`Doc`, `FieldDocs`, `EntityDoc`,
  `EntityFieldDocs`), the gateway serves it as the GraphQL description,
  and the MCP tools search and show it. Plain `//` comments are notes
  and are never published. A schema with no `///` generates what it did
  before; the JSON artifacts (`loom openapi`, `loom graphql`) do not
  carry docs.
- **`@allow`: rules by what the caller is.** Beside `@role`, an
  aggregate, record, entity or command takes `@allow(...)` rules:
  `@allow(kind: admin) @allow(kind: session, project_id: caller.project)`.
  `attr: value` compares one of the caller's new `Access.Attrs`;
  `field: caller.attr` demands that a field of the thing operated on
  equals a caller attribute. The gateway checks the field against the
  stored state (a command's payload counts only where the state has
  none, so a caller cannot claim another owner's thing), refuses a get
  the caller does not own, and narrows a list and its subscription to
  the rows it does. Rules ride `Decision.Rules`, `Decision.List` and
  `Decision.Target`; `DefaultPolicy` enforces them, and a scoped caller
  with no `Attrs` meets no rule, so adding `@allow` fails closed.
- **A caller is shown what it could run.** The MCP tools' `search` and
  `describe` list an operation only when the caller could run it in
  some namespace it has. Each is asked of the gateway's own Policy as a
  probe (`Decision.Probe`, `graphql.Executor.Allowed`): no arguments,
  no target, nothing run. It is a listing aid — the call itself is
  decided again in full.
- **`@plan`: see a command's effect before causing it.** A command
  marked `@plan` is not run by its gateway mutation: the mutation
  answers a `Plan` — the events the command's handler decided it would
  emit, with nothing written — and `runPlan(namespace, id)` runs that
  plan: the same command, once, and only while its target is at the
  version it was planned against. A stale, spent or expired plan is
  refused. Both steps answer to the command's own gate, each as its own
  `Decision` (`Stage` plan, then run), so a `Policy` can let an agent
  plan what only a person may run. In core: `Client.Plan`,
  `Client.RunPlan`, `Client.PlannedCommand`, the `loom_plans` table
  (created by `Migrate`). In-process `Dispatch` is never held back. The
  MCP `dispatch` tool returns a command's whole answer, so a plan
  arrives with its events.
- **`Client.Status`: what the background work is doing.** One read of
  which instance holds the process lease (its generation and revision,
  whether it is yielding, whether the service needs a leader at all),
  every projection and process with its lag and — new — `BehindSince`,
  when the oldest event it has not handled was written, and what is
  waiting: timers pending and overdue, effects running and failed, the
  outbox, dead letters, held plans. It is read from the database, so
  any instance answers for the whole service. `graphql.Config.Status`
  serves it as the `serviceStatus` query (`runners(onlyBehind: true)`
  for what needs attention), to All access under `DefaultPolicy`, and
  so to the MCP tools. `GET /runners` is the same read and gains
  `behind_since`.
- **Idle runners back off: `Config.RunnerIdlePoll`.** A projection or
  process runner with nothing to do stepped every `Start` poll, and
  each idle step is a transaction. Runners are woken at once by new
  events through the log fan-out, so the unwoken step is a safety net:
  with `RunnerIdlePoll` set it stays at the poll after a round with
  work or an error and doubles on each idle round up to
  `RunnerIdlePoll`. Zero (the default) keeps today's behaviour.
  runsheet's e2e rig measured one settled rig at 1,277 transactions/s
  before and 202 after (5s, poll 200ms).

### Changed

- **Timers fire on the newest generation alive.** They fired on every
  instance, so after a deploy the previous revision's instances — idled
  with CPU allocated, kept for a traffic tag — went on firing timers
  and running the commands they hold with the old code. Every instance
  with a generation now keeps a row in `loom_instances`, refreshed each
  beat (the poll interval, between 0.1 s and 5 s), and fires timers
  only while no instance seen in the last four beats is of a newer
  generation. It is liveness, not a high-water mark: shift traffic back
  to an older revision and, once the newer one's instances are gone,
  the older ones fire again. `Status.Instances` (`serviceStatus {
  instances }`) lists the live builds. With no generation nothing
  changes: every instance fires.
- **Batches and projections follow the same rule.** Batch items hold
  commands, as timers do. A projection is folded by whichever instance
  takes its step lock, and an instance of an older build stepped over
  events only the newer build's projection subscribes to, moving the
  checkpoint past them: the read model stayed short of those events
  until rebuilt. Both now run only on the newest generation alive. A
  new instance announces itself on the service's channel after its
  first beat, so an older build stands down at once rather than at its
  next beat. `Status.HereNewest` says whether the answering instance
  is of the newest generation alive. Not covered: bus consumers, which
  still run on every instance (see the pull request).

### Fixed

- **A generation comes from the deploy, not the revision name.** v0.85.0
  ordered builds by the sequence in Cloud Run's `K_REVISION`, and Cloud
  Run does not number revisions in deploy order: on 2026-11-01
  `latchkey-00355-7rg` was deployed hours after `latchkey-00380-xih`,
  and 380 kept the process lease. A generation is now
  `Config.Generation`, else `LOOM_GENERATION` (set by the deploy, by
  convention `$(date +%s)`, on every deploy), else on Cloud Run the
  revision's `createTime` from the Cloud Run Admin API (needs
  `run.revisions.get`; any failure is no generation). The lease is named
  `loom lease g<generation> <revision>` and asked for with
  `<service> g<generation>`, so a v0.85.0 instance never takes it or
  is yielded to, and a deploy generation takes the lease from a v0.85.0
  leader, asking in its terms so it yields cooperatively. Deploy recipes
  should add `--update-env-vars LOOM_GENERATION=$(date +%s)`.
  `RevisionSequence` is deprecated. ADR 0010, amendment 2026-11-01.

### Fixed

- **A deploy moves the process lease to the new build.** The lease is a
  session advisory lock, and an instance of the previous revision that
  stayed alive (a traffic tag, CPU always allocated, a deleted revision
  running on) kept it, so the processes ran the old code for up to ~15
  minutes after a deploy, or indefinitely. An instance now has a
  generation, `Config.Generation`, defaulting to the sequence of Cloud
  Run's `K_REVISION` (`Config.Revision`), and names it on its lease
  session (`application_name` `loom lease <generation> <revision>`). A
  follower of a strictly newer generation asks the leader to yield
  (`NOTIFY loom.process_lease`); the leader stops between events, waits
  for the reaction in flight (at most 30s) and releases, and the
  follower, trying every poll/20, leads within milliseconds. A leader
  that does not answer within two polls has its session ended
  (`pg_terminate_backend`), a hand-over as at-least-once as a crash.
  Equal or older generations, and leaders whose session names none (an
  older loom), are never preempted; with no generation (no K_REVISION,
  or a negative Generation) nothing changes. No migration. See
  [ADR 0010](docs/adr/0010-a-newer-generation-takes-the-process-lease.md).

### Fixed

- **A handler's or policy's reads run on the dispatch's transaction.**
  `loom.Load`, `GetRecord` and `GetEntity` from inside a command handler
  or a policy went to the pool while the dispatch's transaction was open.
  That transaction holds the service's log lock from its first append to
  its commit, and every other dispatch waits on the lock holding a pooled
  connection of its own: once those waiters held every free connection,
  the holder's read never got one and nothing moved. mesh-api hung that
  way on 2026-10-30 (a hand-back's policy read the Lease record with the
  lock held; 20 minutes, every database route). The reads now run on the
  unit's own transaction, so they also see what the unit has written so
  far (a policy reacting to `InvoicePaid` loads the invoice as paid). A
  process reacts outside any transaction and still reads on the pool.

### Added

- **`Config.LogLockWait` bounds the wait for the log lock (default
  15s).** Past it, the dispatch fails with a `*LogLockError` naming the
  session that holds the lock (pid, state, transaction start, last
  statement), logs it at ERROR, and gives its connection back. A unit of
  work stuck while it holds the lock now shows as errors within seconds
  instead of a silent hang, and the connections the waiters return let a
  holder that waits on the pool go on.

### Fixed

- **Opening or sealing @pii fields no longer deadlocks a full pool.**
  The data-key read ran on the pool while the caller still held a
  connection: `readLog` and `Load` opened each event with the log rows
  still open, `Entities` did the same over its rows, and every write
  (dispatch, record, projection fold, timer, batch, dead letter) sealed
  through the pool from inside its transaction. A projection step also
  read the log on the pool from inside its own transaction. Each of these
  held one connection while waiting for a second, so once every pooled
  connection was held that way nothing returned. Now rows are read in
  full and closed before anything is opened, and the key is read (and
  minted) on the connection or transaction the caller already holds. A
  key minted inside a transaction commits or rolls back with the writes
  it seals, and is cached only after it is committed. Nothing about the
  stored data changes.

### Added

- **@retain series upkeep works for a DML-only serving role.** Migrate,
  run as the schema owner, creates `loom_series_partition_<service>` and
  `loom_series_maintain_<service>` (`SECURITY DEFINER`, search path and
  UTC pinned, tables and retention written into the body), and
  `MaintainSeries` and the append-path repair use them when they exist.
  Before, a serving role without CREATE on the schema failed every
  hourly maintenance pass (Postgres checks CREATE even for `CREATE TABLE
  IF NOT EXISTS` on an existing partition), so partitions ran out and
  appends past the last day failed. `Config.SeriesPartitionAhead` sets
  the days kept ahead (default 7, the old constant).

- **A `@table` projection declares its indexes.** Inside
  `entity X @table { ... }`, `index (col [desc] [nulls first|last], ...)`
  declares a secondary index over state fields and the meta columns
  (`service`, `namespace`, `id`, `updated_at`). The name defaults to
  `idx_<entity>_<columns>`. An unknown column, an index on an entity
  without `@table` and a duplicate column list are validation errors.
  `loom generate` emits the indexes as `TableDef.Indexes`
  (`loom.TableIndex{Name, DDL}`) and into `loomgen/tables_gen.sql`.
  `generate --check` reports an index-only mismatch. `Migrate` builds each
  index with `CREATE INDEX CONCURRENTLY` on its own connection, outside
  any transaction. It drops and retries an invalid index left by a failed
  build (up to 3 attempts, then an error naming the index), and running it
  twice is a no-op. On runsheet production, an index on
  `(service, namespace, org_id, updated_at DESC NULLS LAST)` took page reads
  of `loom_t_platform_work_item_index` from 79,390 to 1,282 buffers per call
  ([ADR 0009](docs/adr/0009-a-projection-declares-its-indexes.md)).
  **Migration:** regenerate `loomgen/`.

- **A declared query generates its index.** Inside the same
  `entity X @table { ... }` block, `query (filter_col, ..., order_col
  [desc] [nulls first|last])` declares the index a filtered, ordered,
  paginated list read needs, without working out the index by hand.
  All columns but the last are equality filters; the last carries the
  order and a trailing `id` column, appended automatically in the same
  direction as the stable tie-breaker a list page needs so no row is
  split or repeated across pages. `query (status, updated_at desc
  nulls last)` desugars at parse time into the same `schema.Index`
  `index (status, updated_at desc nulls last, id desc)` would, so it
  rides ADR 0009's generator and `Migrate` pipeline unchanged. Restricted
  to entity `@table`, like `index`
  ([ADR 0011](docs/adr/0011-a-declared-query-generates-its-index.md)).
  **Migration:** regenerate `loomgen/`.

- **A list read can ask for only the fields it needs.** `Query.Fields
  []string` is opt-in on `QueryEntities`/`QueryRecords`; nil or empty
  keeps the existing whole-row read, byte-identical. Named, a `@table`
  list read's SELECT narrows to those columns, so an unasked TOASTed
  column is never read — measured at a 0.0060 buffer ratio for a page of
  50 rows against an 8192-byte column
  (`docs/measurements/list-read-buffers.md`). The GraphQL list fields
  (`{x}s`, `{x}sChanged`) opt in automatically from the query's own
  selection set
  ([ADR 0009](docs/adr/0009-a-list-read-selects-only-asked-fields.md)).

### Changed

- **`Reset` empties tables with `DELETE` instead of `TRUNCATE`**, as
  `Rebuild` already did. Indexes survive either way, and `Reset` no longer
  takes an `ACCESS EXCLUSIVE` lock. On a very large store it is slower and
  leaves dead tuples for autovacuum.

- **`loom generate` writes one file per aggregate and record, and adding
  one changes no existing generated file.** Each aggregate and record has
  its own `loomgen/<snake name>_gen.go`, holding its commands, the events
  only it emits, its state and fold, its `Handlers` interface, its def
  function and its binding method on `Impl`. It registers itself with
  `NewRegistry`, which adds aggregates, then records, in name order.
  `models_gen.go` and `registry_gen.go` keep only what is shared and name
  no aggregate or record, so two branches that each add an aggregate no
  longer conflict in `loomgen/`. Output is byte-for-byte deterministic
  ([ADR 0008](docs/adr/0008-one-generated-file-per-aggregate.md)).
  **Migration:** `Impl` no longer has a field per aggregate and record.
  A `registry.go` that sets them stops compiling after you regenerate.
  Bind each aggregate and record with its method, and keep the reactor
  fields in the literal:

  ```go
  impl := loomgen.Impl{ShipOnPayment: &ShipOnPayment{}}
  impl.Order(&Order{})
  return loomgen.NewRegistry(impl)
  ```

  Alternatively, delete `registry.go` and regenerate for a new stub. An
  aggregate or record left unbound panics at `NewRegistry`, naming the
  call to add. No type or `Handlers` interface is renamed. Commit the
  regenerated `loomgen/`, and `generate --check` passes.
- **`loom generate` removes orphaned generated files.** A `_gen.go` or
  `_gen.sql` that has loom's header but that the schema no longer
  produces is deleted and reported as `removed <path>`. A renamed or
  deleted aggregate's file is the usual case. `generate --check` names
  such a file stale. Files without the header, and stubs, are never
  touched.

### Added

- The agent skill (`loom skill`) gains "When a rebase or merge conflicts in loomgen": resolve only the `.loom` sources, take either side of the generated files, regenerate with `go tool loom generate`, check, and commit the generator's output. Never hand-merge a loomgen conflict.
- **`loom skill` writes a Claude Code skill for loom into the repository.**
  `.claude/skills/loom/SKILL.md` at the repository root, stamped with the
  loom version: the loop, the pinned generator, the naming and effect rules,
  reading after a dispatch. `loom init` writes it too; `loom skill --check`
  fails when it is missing or stale ([ADR 0007](docs/adr/0007-loom-writes-its-agent-skill.md)).

### Fixed

- **A process, policy or `@fold` projection named after an aggregate
  compiles.** `aggregate FleetStall` beside `process fleetStall` used to
  generate two `Impl` fields called `FleetStall`. The reactor's field now
  takes its kind (`FleetStallProcess`, `…Policy`, `…Projection`); its
  interface, stub type and runner name are unchanged. Name reactors for what
  they do, not for their aggregate ([ADR 0006](docs/adr/0006-a-reactor-named-after-an-aggregate.md)).

### Added

- **`loom generate --check`.** Renders every loomgen file in memory, writes
  nothing, and exits non-zero naming each file that differs from disk or is
  missing. Stubs are never compared. Run `go tool loom generate --check` in
  CI ([ADR 0005](docs/adr/0005-generator-is-pinned-and-checked.md)).
- **`loom init` pins the generator.** If the current directory has a go.mod
  (go 1.24 or newer), init adds `tool github.com/go-apis/loom/cmd/loom`,
  idempotently, so a loom bump's require line moves the generator.
- **Enums extend across files: `enum X += { … }`.** A feature can add a
  member to an enum declared in another file. Values keep declaration order
  (home first, then extensions in path order). An extension with no home,
  a second home declaration, a value declared twice, and an empty extension
  are refused with `path:line` positions. Schemas without `+=` parse as
  before ([ADR 0003](docs/adr/0003-schema-is-a-directory.md)).
- **A schema is a directory of files, so a new aggregate is a new file.**
  `schema:` in loom.yml may name a directory (every `*.loom` under it, at any
  depth), a single file, or a glob, and `loom check` takes directories. The
  files are joined in path order and parsed once, so a file may refer to an
  event, type, or enum declared in another. Errors name `path:line:`. The
  first file by path opens with `service X`; later files may repeat the
  same name or omit it, and a different name is refused. Since "first" is
  by path, the README's layout opens every file with the header. A
  declaration that runs past the end of its file is refused. The old per-file merge (which
  dropped enums and series from every file after the first, and could not
  resolve cross-file references) is gone. Library: `sdl.ParseFiles`,
  `sdl.ParseDir`, `sdl.ParseFS`, `sdl.ParsePaths`; `sdl.Parse` is unchanged.
  `loom init` now writes `schema: schema/`
  ([ADR 0003](docs/adr/0003-schema-is-a-directory.md)).

### Fixed

- **A nested type's optional list is nullable in GraphQL, as the schema
  says.** A schema `type` used inside a command, such as runsheet's
  `type NodeScaling { accounts: [NodeScalingAccount]? }`, was served as
  `accounts: [NodeScalingAccountInput!]!`. A client that omitted the list
  was refused with `got invalid value`. The generator now writes each
  type's required list to `Registry.Types` (`loom.TypeDef`), and the
  gateway makes a nested input field NonNull only when the schema requires
  it, as it already did for top-level command fields. An omitted optional
  list reaches the command as a nil slice. Regenerate to pick this up: a
  registry without `Types` keeps the old pointer-based rule
  ([ADR 0004](docs/adr/0004-nested-input-nullability-follows-the-schema.md)).
- e2e proof of the fix above: `TestGraphQLNestedOptionalList` drives the
  e2e orders schema's `Fulfilment` type (`windows: [FulfilmentWindow]!`,
  `gifts: [GiftNote]?`) through `PlaceOrder` on the live gateway. It checks
  introspection, the emitted SDL, and that an omitted or null `gifts`
  arrives as a nil slice.

## [v0.54.0] — 2026-09-25

### Fixed

- **Timer firing no longer deadlocks against event appends. This was the
  runsheet incident of 2026-09-25, 13:14–13:55Z, on loom v0.50.0.** For 41
  minutes, every event append in runsheet's `runsheet` namespace hung until
  Cloud Run cut the request at 60s. That took down the node's reports,
  `setGoal` and GitHub webhooks. One poller transaction sat idle in
  transaction for 1,533s.

  The cause was `fireDueTimers`. It selected a batch of due timers
  `FOR UPDATE SKIP LOCKED` and then fired each one, a command dispatch on
  another connection, while the claiming transaction still held the row locks.
  runsheet's integrator re-arms the same key on every look
  (`loom.After(PollIntegration)`). The re-arm's upsert waited on the poller's
  row lock while it held the namespace append lock
  (`pg_advisory_xact_lock(hashtext('loom_log_'||ns))`). The poller's next fire
  then waited on that append lock. One edge of the cycle is an
  application-level wait, so Postgres's deadlock detector could not see it,
  and the cycle lasted until a transaction was killed.

  The fix is **claim → commit → fire**:
  - Due timers are claimed by a 5-minute **lease** in one autocommitted
    statement that pushes `fire_at` forward. No lock is held while a timer
    fires.
  - After the fire, the row is deleted **conditionally**, only if its
    `fire_at` and `command` still match the claim. A same-key re-arm made by
    the fired command's reactions is therefore kept as the next firing.
  - Durable `@retry` rows (`loom:retry`) get the same fix. `fireRetry` runs
    the reaction with no transaction open, then records success, re-arm or
    park in one short conditional transaction.
  - A fire that keeps failing is **parked** to dead letters (runner `timer`)
    in the same transaction as its delete, so nothing is lost. A claim that
    is never completed, for example after a crash, comes due again when its
    lease expires. Delivery stays at-least-once, as before. A runner that
    stops mid-batch hands its unfired claims back.

  Regression tests with a real Postgres are in
  `internal/e2e/timer_rearm_test.go` and `internal/e2e/retry_test.go`. They
  fire, re-arm the same key in the reaction, and must complete within 5s.

### Changed

- New rule: **no runner holds a claim transaction open across a dispatch**.
  See [ADR 0002](docs/adr/0002-no-transaction-across-a-dispatch.md). Audit
  of the other runners:
  - The batch runner (`batch.go`) already claims, commits and then
    dispatches, so it is safe.
  - The process runners (`runner.go`) wrap no reaction in a transaction, so
    they are safe.
  - The outbox relay (`relay.go`) still holds its transaction across
    `bus.Publish`. This is a **documented exception**: publishing is not a
    command dispatch, and it takes none of the locks the relay holds.
- A leased timer appears on `GET /timers` with its lease expiry as
  `fire_at`.

### Upgrading

- No schema or API change. Consumers on v0.50.0–v0.53.0 should bump to
  v0.54.0 or later (`go get github.com/go-apis/loom@v0.54.0`). runsheet
  takes this release through its loom bump.

[v0.54.0]: https://github.com/go-apis/loom/releases/tag/v0.54.0
