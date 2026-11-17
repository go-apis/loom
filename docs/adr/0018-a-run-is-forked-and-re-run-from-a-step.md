---
title: "ADR 0018 — a run is forked and re-run from a step"
status: accepted
date: 2026-11-17
summary: Client.ForkStream copies an instance's events up to a version into another namespace with the settled effects of reactions to them, so the service re-runs it from that step on today's code and loom.Once replays what was recorded; a fork seals @pii under its source's one data key (loom_key_aliases), CompareFork lists the commands and events where a re-run parts from its source, and the gateway serves both to All access only.
---

# ADR 0018 — a run is forked and re-run from a step

## Context

An instance's log already says how it got where it is. Re-running a
failed goal, item or job from the step that failed after a fix has
meant hand SQL or a redrive of whatever happens to be parked; trying a
changed look or agent step against real runs has meant waiting for new
ones. Draft PR #167 prototyped a fork; it refused `@pii` streams and
had no comparison and no served surface.

## Decision

- **A fork is a copy of a stream's events into another namespace.**
  `Client.ForkStream(ctx, ForkOptions{Aggregate, Namespace, ID, UpTo,
  ToNamespace})` copies the events up to version `UpTo` (0 for all) in
  one unit of work through `appendEvents`, so the copies are ordinary
  events of the fork's stream: its `@loop` is looked at
  (`loom_loop_due`), its processes react, its projections fold, all on
  the code deployed now. Each copy keeps its type, schema version, time,
  actor, correlation and command; its cause is
  `fork:<namespace>/<id>@<seq>` (`ForkedFrom` reads it back). The source
  is not touched, and copies are not put on the bus.
- **Recorded effects replay.** Every settled `loom_effects` row of a
  process reaction to a copied event is copied, re-keyed from scope
  `process:<name>/<service>:<old seq>` to the copy's seq, so `loom.Once`
  in the fork's reaction returns the recorded result. Only steps past
  the fork point reach the outside world.
- **@pii: the fork shares its source's data key.** A
  `loom_key_aliases` row `(service, namespace, stream_id) ->
  of_namespace` says where a stream's key lives; `dek` resolves it
  (`keyHome`) before reading, minting or caching a key, and the copies
  are moved as stored, sealed. A fork of a fork points at the first
  source. So there is one key per person's stream however many forks
  hold copies: `Shred` of the source deletes it and every fork reads
  redacted; `Shred` of a fork deletes that fork's alias alone. Rejected:
  re-sealing the copies under a fresh key for the fork, which would
  leave the person's data readable after the source's shred unless
  shred learned to find every fork (the alias is that knowledge, held
  once); and copying the wrapped key, which is the same problem.
  Aliases never change once written, so they are cached; ForkStream
  sends `keys:<ns>:<id>` on the LISTEN channel after commit to evict a
  cached "no alias" on every instance.
- **Compare is the eval.** `Client.CompareFork(ctx,
  CompareOptions{Aggregate, Namespace, ID, ToNamespace, Ignore})` reads
  both runs after the fork point (the fork's last copied version) and
  lines them up by position: each `Step` is a version, the command
  whose handling wrote it, its event, what issued the command
  (loop, evt, timer, plan) and its payload with @pii opened and the
  `Ignore` fields left out. A `Difference` names `command`, `event`,
  `data`, or `missing`. To know the command, `loom_events` gains a
  `command` column written by every append from a command
  (`Event.Command`).
- **Served to operators only.** `graphql.Config.Fork` adds the
  `forkStream` mutation and `compareFork` query, and MCP offers them
  with the gateway's other operations. As `serviceStatus` does, their
  Decision names no namespace (a fork reads one tenant's namespace and
  writes another), so `DefaultPolicy` passes them only for `All` access,
  and `forkStream` also needs `Mutate`; a Policy lets other operators in
  by Field name. A caller without them is not shown them in search and
  is refused `access denied`.

## Consequences

- Refused: an `@table` aggregate (its row is written by command
  handling, which a fork does not run), the source's own namespace, a
  stream that exists in the target, an empty source.
- One stream at a time: forking several related streams together is not
  built. Uploaded files are not copied (they live outside the key).
- Comparison is positional, not an alignment: a run that inserts one
  extra step differs at every later step. A command that wrote no event
  leaves no step, so it is not seen.
- Events written before the `command` column read an empty command.
- `TestForkReplaysARecordedRun`, `TestForkALoopInstanceMidRun`,
  `TestForkSharesTheSourcesDataKey`,
  `TestForkCompareALoopRunWithAChangedLook` and
  `TestForkServedToOperatorsOnly` (internal/e2e) hold this.
