---
title: "ADR 0002 — No runner holds a claim transaction open across a dispatch"
status: accepted
date: 2026-09-25
summary: Timer, retry, batch and process runners claim work, commit, and only then dispatch; due timers are claimed by a 5-minute lease and deleted conditionally so a same-key re-arm survives; the outbox relay's transaction across bus.Publish is a documented exception.
---

# ADR 0002 — No runner holds a claim transaction open across a dispatch

## Context

On 2026-09-25, from 13:14 to 13:55Z, every event append in runsheet's
`runsheet` namespace hung (loom v0.50.0). Cloud Run cut the requests at 60s,
including the node's reports, setGoal and GitHub webhooks. One poller
transaction sat idle in transaction for 1,533s.

The cause was `fireDueTimers`. It opened a transaction, selected a batch of
due `loom_timers` rows `FOR UPDATE SKIP LOCKED`, and then fired each timer.
Firing a timer is a `Dispatch`, which runs its own unit of work on another
connection. The runner deleted each fired row, and the claiming transaction
kept holding the row locks until the whole batch had fired. runsheet's
integrator re-arms `loom.After(PollIntegration)` on every look, so the fired
command's reaction re-armed the same key. That produced this cycle:

- The re-arm's `INSERT … ON CONFLICT (service, key) DO UPDATE` waited on the
  poller's row lock. It did so while holding the namespace append lock
  (`pg_advisory_xact_lock(hashtext('loom_log_'||ns))`, taken in
  `appendEvents`).
- The poller waited on the dispatch. For a process re-arm, the poller's next
  fire waited on the append lock.

One edge of that cycle is a Go call waiting for another Go call, not a lock
wait. Postgres's deadlock detector cannot see it, so nothing broke the cycle
until a transaction was killed. `retry.go`'s `fireRetry` had the same shape:
durable `@retry` rows were claimed in the same batch, and `c.react`, which
dispatches, ran inside the claim transaction.

## Decision

**No runner holds a transaction that claimed work open while it dispatches a
command.** A runner claims its work, commits the claim, and only then
dispatches. Whatever it records afterwards (a delete, a re-arm, a park) goes
in a new short transaction. Any lock a dispatch might need is then held by
nobody who is waiting on that dispatch.

### Timers: claim by lease, delete conditionally

`fireDueTimers` claims a batch with one autocommitted statement:

```sql
UPDATE loom_timers t SET fire_at = now() + interval '5 minutes'
FROM (SELECT service, key, fire_at FROM loom_timers
      WHERE service = $1 AND fire_at <= now()
      ORDER BY fire_at LIMIT $2 FOR UPDATE SKIP LOCKED) due
WHERE t.service = due.service AND t.key = due.key
RETURNING t.key, t.command_type, t.command, t.meta, t.fire_at, due.fire_at
```

SKIP LOCKED keeps concurrent pollers apart only while this statement runs.
After it commits, the pushed-out `fire_at` (the lease) keeps them apart and no
lock is held. The runner then fires each timer. When a fire fails its
in-process attempts, the timer is parked to dead letters (runner `timer`), as
before. In both cases the row is then deleted conditionally:

```sql
DELETE FROM loom_timers
WHERE service = $1 AND key = $2 AND fire_at = <leased fire_at> AND command = <claimed command>
```

A reaction that re-armed the same key has already changed `fire_at` and
`command` through `writeTimer`'s upsert. That row is the next firing, so the
delete leaves it alone. The park and the delete for a failed fire commit
together.

We chose the lease over the goal statement's `DELETE … RETURNING` claim,
which would delete the rows and commit before firing. With that claim, a
process that died after the commit and before the fire would lose those
timers. The lease keeps at-least-once delivery: a claimed row that is never
deleted comes due again when its lease expires. The costs are:

- A crash delays the re-fire by up to the lease (5 minutes).
- A fire that runs longer than the lease can be fired a second time by
  another poller. That is the same duplicate a crash could always cause,
  which is why the fired commands already have to converge.

When the runner stops mid-batch, it releases its unfired claims back to their
original `fire_at`, conditional on the lease, so a deploy does not hold them
for the full lease. A leased row shows on `GET /timers` with its lease expiry
as `fire_at`.

### Durable retry rows

`@retry` rows (`command_type = 'loom:retry'`) are claimed by the same lease.
`fireRetry` runs `c.react` with no transaction open. It then records the
outcome in one short transaction, and every write in it is conditional on the
leased `fire_at` and `command`:

- On success: mark a foreign event processed and delete the row.
- On failure: re-arm the row (update `command` and `fire_at` with the next
  backoff).
- On the `max`-th failure, or a row that cannot be fired at all: park the
  event and delete the row.

### Audit of the other runners

- **Batches** (`batch.go` `batchStep`): the step already claims items in a
  transaction, commits, and then dispatches each one. Per-item outcomes are
  single statements. No change.
- **Process runners** (`runner.go` `processStep` and `subscribeForeign`): no
  transaction surrounds a reaction. The checkpoint read, the dedup check,
  `markProcessed` and `writeCheckpoint` are single statements that run before
  or after `react`. No change.
- **Outbox relay** (`relay.go` `drainBatch`): **the exception.** The relay
  holds its transaction (the per-service `loom_relay_` advisory lock, plus
  `FOR UPDATE SKIP LOCKED` on the service's outbox rows) across
  `bus.Publish`. Publishing is not a command dispatch and takes no append
  lock. Holding the rows until `published_at` is written keeps the outbox
  ordered and exactly-claimed.

  With the in-process `MemoryBus`, `Publish` calls subscribers inline, so in
  that case a consuming service's reaction does dispatch inside the relay's
  transaction. It still cannot close the cycle. The consumer is another
  service (`subscribeForeign` takes only foreign events), so its dispatch
  takes its own service's append lock and writes its own outbox and timer
  rows. None of that is something the relay holds. Network buses return from
  `Publish` on the broker's ack. This case is documented, not changed. If the
  relay ever comes to dispatch commands, or the bus comes to share a service's
  locks, this exception has to be revisited.

## Consequences

- Timers keep at-least-once delivery. A crash between the claim and the
  delete delays the re-fire by up to 5 minutes instead of re-firing on the
  next poll.
- A same-key re-arm by the fired command's reactions, whether from a policy
  inside the fire's transaction or a process after it, completes and
  survives. `internal/e2e/timer_rearm_test.go` pins this. On the old claim,
  the policy case hangs until the test's 5s deadline, and the lock probes in
  that file and in `retry_test.go` fail with `55P03`.
- New runners follow the same rule: claim, commit, then dispatch. A lock taken
  to claim work is never held across a call that can wait on another
  connection.
