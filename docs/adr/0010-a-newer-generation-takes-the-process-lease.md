---
title: "ADR 0010 — a newer generation takes the process lease"
status: accepted
date: 2026-11-01
summary: An instance with a generation (Config.Generation, else LOOM_GENERATION set by the deploy, else the Cloud Run revision's createTime; amended 2026-11-01, no longer the sequence in K_REVISION) names it on its process-lease session; a follower of a strictly newer generation asks the leader to yield (NOTIFY), the leader drains its in-flight reaction and releases, and a leader that does not answer within two polls has its session ended; equal, older or unnamed never preempts; no generation is the election as before.
---

# ADR 0010 — a newer generation takes the process lease

## Context

A service's local-event processes run on one elected instance: the one
holding a session-scoped advisory lock,
`pg_try_advisory_lock(hashtext('loom_processes_<service>'))`, until its
session ends (`election.go`). A session lock knows nothing of builds.
After a deploy, an instance of the previous revision that is still alive
keeps the lease, and the processes go on running the old code and
configuration:

- Cloud Run keeps an old revision's instance for a traffic tag, idles it
  for up to ~15 minutes with CPU always allocated, and a deleted
  revision's instance can run on for many minutes.
- mesh-api: the old backend was ended by hand (`pg_terminate_backend`)
  after every deploy.
- Latchkey: tagged old revisions held the lease.
- runsheet: on 2026-10-31 a candidate revision with no Mesh v1 client
  held the lease for sixteen minutes, and the canary's validations were
  never offered. runsheet/runsheet#5776 worked around it in the service
  (terminate an older revision's lease session, named by
  `application_name = K_REVISION`) and named the real fix: generation-aware
  election in loom.

## Decision

**An instance may have a generation, and a strictly newer one takes the
lease.**

### What a generation is

`Config.Generation int64` orders this instance's build against the
service's other instances. Any number that grows with each deploy works:
a build number, the deploy's Unix time. When it is zero (the default)
loom takes the sequence of `Config.Revision`, which defaults to
`K_REVISION`: Cloud Run names a revision `<service>-<sequence>-<suffix>`
(`mesh-api-00021-j42` is 21) and numbers a service's revisions in the
order they are created (`loom.RevisionSequence`). A negative Generation
turns generations off; a revision name not of that shape (a custom
`--revision-suffix`, a local run) gives none.

Cloud Run's sequence is the default because every Cloud Run service has
it with nothing to wire. It is not the only option because it only
orders revisions of one Cloud Run service and is lost with a custom
revision suffix. An explicit generation covers those. Every instance of
one loom service must use the same scheme: a build number and a revision
sequence do not compare.

### How the lease changes hands

1. **The leader names its session.** On taking the lock, a leader with a
   generation sets its session's `application_name` to
   `loom lease <generation> <revision>` and `LISTEN`s on
   `loom.process_lease`. It resets both before the connection goes back
   to the pool. No table and no migration: the lease and its generation
   live on the one session, and `pg_stat_activity` shows them to an
   operator.
2. **A newer follower asks.** A follower with a generation that fails to
   take the lock finds the holder (`pg_locks` joined to
   `pg_stat_activity`, the int4 key split into classid/objid) and parses
   its name. Only when the holder's generation is strictly lower does it
   act: it sends `pg_notify('loom.process_lease', '<service>
   <generation>')` (again every poll while it waits) and tries the lock
   every `fastPoll` (poll/20, clamped to 10–250ms) instead of every poll.
3. **The leader yields cooperatively.** On a notification from a newer
   generation of its service, the leader renames its session
   `loom lease <generation> yielding <revision>`, clears its leader flag,
   and waits for the process steps in flight to finish, at most
   `handoverDrain` (30s). A process step reads the flag before each event
   and stops at the last event it checkpointed, so the wait is at most
   one reaction. The leader then unlocks and stays out of the election
   for two polls so the newer follower, polling fast, takes the lock.
4. **A leader that does not answer is ended.** If the holder's session
   is not marked yielding two polls after the follower first saw it (or
   is still held `handoverDrain` plus two polls after it said it was
   yielding), the follower calls `pg_terminate_backend` on it. That is a
   crash as far as the processes are concerned: the session and the lock
   go, the old leader notices on its next read of the session, and the
   hand-over is at-least-once as it has always been. This covers a
   CPU-throttled or wedged instance, which cannot answer a notification.

**What never moves the lease:** a follower of an equal or older
generation; a follower with no generation; a holder whose session does
not carry a loom lease name (an older loom, an instance with no
generation). So an older revision woken later follows for good while a
newer instance is alive. If every newer instance goes, an older one
takes the free lock as before: an old build running the processes is
better than none.

## Consequences

- After a deploy the new revision's first instance leads within
  milliseconds of the old leader's in-flight reaction finishing (a
  live old leader), or within about two polls (10s at the usual 5s poll)
  of a throttled or wedged one. Before, it was until the old instance
  went: up to ~15 minutes, or indefinitely for a tagged revision.
- A cooperative hand-over has no overlap: the old leader stops between
  events and the new one starts from its checkpoint. A forced one
  overlaps exactly as a crash does today (the old instance may finish
  the reaction it was on; effects are journaled and redeliveries
  converge). Process checkpoints are still written per event, unguarded,
  so an overlapping old leader can move one back: the events in between
  are delivered again, which at-least-once already allows.
- A rollback by shifting traffic to an older revision does not move the
  lease while an instance of the newer revision is alive (it never did:
  the lease stayed wherever it was). To roll the processes back promptly,
  deploy the old image as a new revision, or stop the newer revision's
  instances.
- The first deploy of a service onto this loom still finds a leader that
  does not name a generation (the previous loom); that one hand-over is
  as before. Every deploy after it is generation-aware.
- `pg_terminate_backend` needs the follower's role to be the holder's (or
  a member of `pg_signal_backend`); otherwise the forced step logs a
  warning and the lease waits for the session to end as before.
- Unit tests: `RevisionSequence`, lease names, payloads. e2e tests on real
  Postgres (`internal/e2e/generation_test.go`): a newer generation takes
  the lease from a live leader mid-reaction (exactly one capture per
  invoice, nothing parked); an older, equal or explicit-equal generation
  never takes it; a wedged leader's session is ended after two polls; no
  generation, or a holder with none, is the election as before.

## Amendment 2026-11-01 — a generation comes from the deploy, not the revision name

### What went wrong

The Decision took Cloud Run's revision sequence as the default
generation, on the belief that Cloud Run numbers a service's revisions
in the order they are created. It does not. On 2026-11-01 a deploy of
latchkey-prod at 18:55 AEDT made `latchkey-00380-xih`, and a later one
at 22:41 AEDT made `latchkey-00355-7rg`. v0.85.0 took 355 for the older
build: 380 kept the lease (`application_name` `loom lease 380
latchkey-00380-xih`) while 355 served the traffic, so the processes ran
the old code until 380 was shut down by hand. The sequence is not a
generation, and nothing in a revision's name is.

### What a generation is now

It must grow with every deploy and be the same on every instance of one
revision. In order (`generation.go`):

1. **`Config.Generation`**, when positive: a service that computes its
   own.
2. **`LOOM_GENERATION`**, when set: the deploy sets it, by convention to
   its Unix time in seconds, with every deploy:
   `gcloud run deploy … --update-env-vars LOOM_GENERATION=$(date +%s)`.
3. **The Cloud Run revision's `createTime`**, in Unix seconds, when
   neither is set and the instance is on Cloud Run (`K_SERVICE` and
   `K_REVISION`). loom asks the Cloud Run Admin API once, when the
   election starts (`GET run.googleapis.com/v2/projects/<n>/locations/
   <region>/services/<K_SERVICE>/revisions/<K_REVISION>`, with the
   metadata server's region and token, at most 10s). The service account
   needs `run.revisions.get` on the service (`roles/run.viewer`).
   Cloud Run stamps it, so it is monotonic per deploy with nothing to
   wire, and every instance of a revision reads the same value.
4. **None** otherwise, and whenever the above fails: an unparseable
   `LOOM_GENERATION`, no metadata server, a 403 from Cloud Run, no
   `createTime`. None is the election as before this ADR: never takes the
   lease, never has it taken. A failure is logged at start
   (`process lease: no generation`), never guessed around.

A negative `Config.Generation` or `LOOM_GENERATION` turns generations
off. `LOOM_GENERATION` and `createTime` are both Unix seconds, so they
compare: a service that starts or stops setting the variable still
orders a deploy against the one before it (the deploy's `date +%s`
precedes the revision's creation by the length of the deploy, so only
overlapping deploys could misorder).

Why `LOOM_GENERATION` comes before `createTime` though `createTime` needs
no recipe: it works without the IAM grant, off Cloud Run and in tests,
costs no call at start, and is what the deploy says. Its trap is that
`--update-env-vars` carries the variable into the next revision, so a
deploy that forgets to set it gets the previous value: an equal
generation, which never preempts (the safe failure, the election as
before). The recipe must set it on every deploy. The alternative, the
larger of the two, was rejected: a lookup that fails on one revision and
not on another would order them by different sources and could let the
older one win.

`RevisionSequence` stays exported, deprecated.

### Like compares with like

A leader now names its lease `loom lease g<generation> <revision>`, and
asks with the payload `<service> g<generation>`. The `g` marks a deploy
generation; v0.85.0 wrote a bare revision sequence. So:

- **A v0.85.0 follower never takes this build's lease.** It parses the
  third field as a bare integer, fails on `g…`, and treats the holder as
  one with no generation.
- **This build's leader never yields to a v0.85.0 follower.** A bare
  number on the lease channel is a revision sequence and does not
  compare with a deploy generation; it is ignored, however large.
- **Any deploy generation takes the lease from a v0.85.0 leader**, of
  whatever revision number. Every build with a deploy generation is a
  later loom than v0.85.0, and the v0.85.0 leader's number orders
  nothing. It is asked in its own terms: the bare number
  `max(generation, its sequence + 1)`, which v0.85.0 yields to
  cooperatively (drain, unlock); one that does not answer is ended
  after two polls like any wedged leader.
- **Equal generations never preempt**: instances of one revision, and an
  env-only redeploy that kept `LOOM_GENERATION`.

The one ordering this gives up: a rollback by deploying a pre-fix
(v0.85.0) image as a new revision, while an instance of a fixed build
with a generation is alive, has the fixed build take the lease back.
Stop the newer revision's instances, as the Decision already says for
rollbacks.

### Rolling out

- **The first deploy onto this loom needs no hand-over** when it has a
  generation (`LOOM_GENERATION` set, or the IAM grant for `createTime`):
  its first instance asks the v0.85.0 leader by bare number and the
  v0.85.0 leader yields cooperatively. From a loom older than v0.85.0
  (a leader whose session names no generation) the hand-over is as
  before: that leader keeps the lease until its session ends.
- A deploy with no generation at all (neither set nor readable) leaves
  the lease where it is, as before this ADR.
- No migration.

Tests: unit (`election_unit_test.go`, `generation_unit_test.go`):
sources and their order, fails-safe lookups against a fake metadata
server and Admin API, lease names in both encodings, v0.85.0's own
parsers against this build's name and payloads, non-monotonic revision
names, equal generations. e2e (`internal/e2e/generation_test.go`): the
later deploy with the lower revision number (355 over 380) takes the
lease mid-reaction; the earlier one, an equal one on another revision,
and a revision name alone never do; a v0.85.0 follower's request does
not move it; a v0.85.0 leader hands over cooperatively to the first
deploy with a generation, or is ended when it does not answer.
