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
