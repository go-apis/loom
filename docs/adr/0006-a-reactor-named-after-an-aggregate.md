---
title: "ADR 0006 — A reactor named after an aggregate gets its kind on its Impl field, and reactors are named for what they do"
status: accepted
date: 2026-10-18
summary: When a process, policy or @fold projection exports to the same name as an aggregate or record, its Impl field (and only that) takes its kind as a suffix — FleetStallProcess — so the schema compiles; the convention is that a reactor is named for what it does, never for the aggregate it serves.
---

# ADR 0006 — A reactor named after an aggregate gets its kind on its Impl field, and reactors are named for what they do

## Context

The generated `Impl` struct has one field per aggregate and record (named
after it) and one per policy, process and `@fold` projection (named after
its exported name). A schema that declares `aggregate FleetStall` and
`process fleetStall` gets two fields called `FleetStall`, and loomgen does
not compile. No release has handled it.

runsheet met this three times (`fleetStall`, `orgMembership`,
`outcomeSnapshot`, 2026-10-02 onwards) and hand-edited
`registry_gen.go` to rename the process fields `…Process`. Every
`loom generate` after that wrote the colliding names back. On 2026-10-18 a
rebase session regenerated, and runsheet's main stopped compiling and
deploying for two and a half hours.

## Decision

- **The Impl field takes the reactor's kind when an aggregate or record
  holds its name.** A process becomes `<Name>Process`, a policy
  `<Name>Policy`, a `@fold` projection `<Name>Projection`. The
  `On<Event>` calls through the field and the `registry.go` stub's wiring
  follow. Nothing else moves: the `<Name>Reactions` or `<Name>Folds`
  interface, the stub type in `processes/` (or `policies/`,
  `projections/`) and the runner's name keep the declared name. A running
  reactor is never renamed, so its checkpoint and its effect journal stay
  where they are.
- **The convention: a reactor is named for what it does.** A process or
  policy is a verb phrase or the plural of what it reacts across —
  `shipOnPayment`, `scheduleAutoCancel`, `productManifests`,
  `threadReply` — never the name of the aggregate it serves. The suffix is
  the safety net for schemas that already broke the convention, not a
  naming scheme to lean on.

## Consequences

- runsheet's three hand edits become what the generator writes, so
  `loom generate --check` (ADR 0005) passes there, and a regeneration
  cannot break its build this way again.
- A new reactor that collides compiles. Following the convention it
  should not collide, and a consumer that wants the convention enforced
  checks its own schema, as runsheet does.
- A reactor that stops colliding (its aggregate renamed or retired) loses
  the suffix at the next generate. That is a field rename in loomgen and
  in the consumer's `registry.go` stub, which the compiler names.
