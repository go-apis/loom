---
title: "ADR 0017 — a loop asks an agent through Mesh"
status: accepted
date: 2026-11-17
summary: A @loop aggregate declares `agent <name> { kind, deadline }`; its look asks, loom records the request in loom_agents, offers one Mesh job under an idempotency key, waits on mesh until the deadline, and settles the request from Mesh's signed job.ended hook or expires it, so the look reads the outcome with LookingAt(ctx).Agent(name).
---

# ADR 0017 — a loop asks an agent through Mesh

## Context

Mesh runs AI agent sessions as jobs. A product offers a job of a kind it
publishes (`POST /v1/jobs`, with an idempotency key), and Mesh reports
each change to the job on a hook signed with the product's secret
(`X-Mesh-Signature: sha256=<hex HMAC-SHA256 of the body>`), each body a
feed entry with a `seq`, an `event` and, on `job.ended`, an `outcome`.
runsheet does this by hand: a client, a hook handler that verifies and
dedupes, an aggregate per job, and a sweeper for jobs that never come
back. A loom service whose loop wants an agent's judgement would write
all of it again.

A `@loop` aggregate already has what the step needs: a look that
decides, waits with an owner and a deadline (ADR 0012), and approvals,
which are the same shape for a person (a record plus a wait).

## Decision

- **The schema declares the step on the aggregate**, beside `deadline`
  and `upload`:

  ```
  aggregate Ticket @loop {
    ...
    agent triage { kind: "triage", deadline: 30m }
  }
  ```

  Inside the aggregate block, not a top-level `agent triage on Ticket`,
  because the step belongs to one aggregate's look and the other
  per-aggregate loop declarations (`deadline`, `@supervised_by`) live
  there. `{ key: value }` is the form `kind` and `deadline` read best in,
  and leaves room for more keys. Both are required; an agent on an
  aggregate that is not `@loop` is refused, as is an unknown key, an
  empty kind, a deadline that is not a duration, and a name declared
  twice.
- **A look asks; loom records, offers and waits.** The generated
  `Ask<Aggregate><Agent>(ctx, loom.AgentAsk)` (or `loom.AskAgent`)
  notes the ask. After a look that did not fail, the runner writes the
  request to `loom_agents` (one row per instance and agent: status
  `requested`, attempt, the ask, the deadline from the look), starts the
  wait `agent:<name>` owned by `mesh`, and offers the job. The record is
  the recorded fact, as `loom_approvals` is for approvals; no domain
  event is invented on the service's aggregate.
- **Exactly one job, by the idempotency key.** The key is
  `loom/<service>/<aggregate>/<namespace>/<id>/<agent>/<attempt>`
  (hashed past Mesh's 200 characters). An ask while the request is open
  changes nothing, so a level-triggered look may ask on every look. A
  request with no job id yet is offered again at each look under the
  same key, so a crash between recording and offering, or a lost answer,
  converges on the same job. `loom.Once` was not used: the request row
  already is the journal, and Mesh's own idempotency is the guarantee
  across the network.
- **The hook settles; the deadline expires.** `Client.MeshHook`
  verifies the signature under `Config.MeshHookSecret` (401), requires a
  feed entry (400), and reads `job.refs.loom`, which loom put on the
  offer, to find the request. `job.ended` with end `finished` records
  `finished`, any other end `failed`, with Mesh's outcome; the wait ends
  and the next look is now. Only an open request of the same attempt is
  settled, so a retried or replayed hook settles once and a late one
  after expiry is not read. `job.confirm` asks are answered `run` while
  the request is open and `cancel` after. At the deadline the runner
  marks an open request `expired` and cancels its job before the look
  that is told the wait expired. `graphql.MeshHooks` routes hooks among
  several services by `refs.loom.service`, so services stay private.
- **Mesh is an interface.** `loom.Mesh` (`OfferJob`, `CancelJob`) is
  `mesh.HTTPClient` against Mesh, following its API as the mesh-api project
  declares it, and runsheet's client implements it (`external/meshv1`), or
  `mesh.Fake` in tests, which converges on the key as Mesh does and posts
  signed hooks. `loomtest.LoopScenario.Mesh` and `AgentAnswered` test a
  look's agent decisions with no database.

## Consequences

- A service that asks agents sets `Config.Mesh`, `Config.MeshTenant`
  (or a tenant per ask) and `Config.MeshHookSecret`, and mounts the
  hook at its catalogue's `hooks.jobs` URL. Publishing the catalogue and
  setting the hook secret stay the product's own steps.
- The feed (`GET /v1/jobs/changes`) is not read: a hook Mesh gives up on
  leaves the request open until its deadline expires it. A catch-up
  reader is the next step if that proves too slow.
- One open request per agent per instance: a look that wants two
  answers of one kind at once declares two agents.
- Asking a settled agent again asks afresh (a new attempt and job), as
  an approval asked again is; a look that asks unconditionally after an
  answer would loop, and must read the outcome first.
- `TestALoopAsksAnAgentThroughMesh`, `TestAnAgentThatNeverAnswersExpires`
  and `TestTheMeshHookRefusesWhatIsNotAFeedEntry` (internal/e2e),
  `TestALookAsksAnAgentAndDecidesOnItsAnswer` and
  `TestALookDecidesFromARecordedAgentOutcome` (loomtest), and the
  `sdl`, `gen` and `mesh` tests hold this.
