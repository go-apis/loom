---
title: "ADR 0013 — an event type has a release floor"
status: accepted
date: 2026-11-15
summary: The store records, in loom_event_floors, the Config.Release (else LOOM_RELEASE) that first wrote each event type, on the transaction appending its first event; Start refuses with a *FloorError a build whose release is older than any floor, naming the type and the release to start instead. A release numbers the code and is the same on every deploy of one build, unlike a generation. No release stated means floors of zero and no refusal; types already in the log are seeded at zero by the first Migrate.
---

# ADR 0013 — an event type has a release floor

## Context

A build decodes the event types its registry knows. Roll a service back
past the release that introduced a type, and the old build meets stored
events it has no struct for: its projections and processes park them or
stall, and what it then writes is decided without them. "Never roll back
past new event types" was a rule people remembered, and it has broken
runsheet when they did not.

The generation (ADR 0010) cannot say this. It orders deploys, and a
rollback is a new deploy: the old build arrives with the newest
generation.

## Decision

- **A build states its release.** `Config.Release`, else `LOOM_RELEASE`:
  a number that belongs to the code. It is the same every time one build
  is deployed and larger for every later build — a build number, a
  commit count.
- **The first write of an event type records its floor.** `appendEvents`
  inserts `(service, event_type, release)` into `loom_event_floors` with
  `ON CONFLICT DO NOTHING`, on the transaction appending the event. The
  floor commits with the type's first event or not at all, and a later
  release writing the same type does not move it. A client remembers the
  types whose floor it has seen committed, so a type costs it two
  inserts in its life, not one per append.
- **`Start` checks.** A client whose release is older than any floor of
  its service returns a `*FloorError` before it starts a runner: the
  newest such floor, a type that release first wrote, and how many more
  types the build is too old for.
- **Zero holds nothing back.** A build that states no release writes
  floors of zero and is not checked. Event types already in a log when
  floors arrive get a floor of zero from the first `Migrate` — what
  wrote them first is not known — in one `SELECT DISTINCT` over the log,
  run only while the service has no floor at all.
- **A store without the table is left alone.** A client looks once for
  `loom_event_floors`; where `Migrate` has not run since floors arrived
  it records and checks nothing, and says so in the log, rather than
  fail every dispatch.

## Consequences

- A rollback past a new event type fails at boot with the release to
  deploy instead, where it used to start and misbehave later.
- Only `Start` is checked. A build that dispatches without starting
  (a one-off tool) is not refused.
- A floor is as good as the release numbers: two builds given one number
  are one release to the store.
- A product's own notion of a release floor (runsheet's gradual
  promotion and rollback) reads `Client.EventFloors`; it is not built
  here.
