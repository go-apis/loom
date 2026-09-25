---
summary: Notable changes to loom by release, newest first; starts at v0.54.0, the timer-lock incident fix.
---

# Changelog

Notable changes to loom, newest first. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Every merge to
`main` is tagged automatically ([ADR 0001](docs/adr/0001-ci-and-release-tagging.md)):
the next MINOR by default, or the next PATCH for a PR labelled `patch`. This
file starts at v0.54.0. For earlier releases, see the git history and tags.

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
