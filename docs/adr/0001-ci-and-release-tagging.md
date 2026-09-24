---
title: "ADR 0001 — CI gates every PR; every merge to main makes a tag"
status: accepted
date: 2026-09-24
summary: CI runs go test ./... against Postgres on every PR and blocks red merges; every merge to main is tagged automatically, next MINOR by default, next PATCH when the PR is labelled `patch`.
---

# ADR 0001 — CI gates every PR; every merge to main makes a tag

## Context

loom has ten consumers (runsheet, wardroom, latchkey, grapevine, optrader,
ten99 on v0.49.0; tripline, purser, foghorn on v0.46.0). Until now nothing ran
the test suite on a PR, and releases were tagged by hand, so every Mesh goal
that needs a releasable loom was waiting on a person. This decision unblocks
those goals, per `specs/proposal-loom-for-adr-001.md` §3 (G0) in
runsheet/runsheet. It is loom's first release-process decision and its first
ADR.

## Decision

### CI gates every PR

`.github/workflows/ci.yml` runs `go test ./... -count=1` on every pull request
to `main` (and on every push to `main`) against a `postgres:16` service
container. The container's credentials and port match the DSN `internal/e2e`
defaults to (`postgres://postgres:mysecret@localhost:5432/postgres`), and a
readiness step checks that DSN before the tests run. Without that check, a
misconfigured service would make the e2e suite skip silently and the run would
still show green. The `go test` check is required on `main`, so a red run
blocks the merge.

### Every merge to main makes a tag

`.github/workflows/tag-on-merge.yml` runs on `pull_request` `closed` against
`main`. Its job runs only when `github.event.pull_request.merged == true`, so a
PR closed without merging never tags anything. It:

1. checks out full history and tags;
2. takes the latest tag matching `vX.Y.Z` exactly (by `sort -V`), or `v0.0.0`
   if there is none;
3. bumps **MINOR** by default (`vX.(Y+1).0`), or **PATCH** (`vX.Y.(Z+1)`) when
   the PR carries the `patch` label;
4. creates an **annotated** tag on the PR's `merge_commit_sha` and pushes it
   to `origin`.

Steps 2–3 are in `.github/scripts/next-tag.sh`, so the computation can be run
by hand against any clone. The job skips a merge commit that already has a
`vX.Y.Z` tag, and a single concurrency group runs merges one at a time. That
way two merges landing close together can't compute the same tag.

MINOR is the default because loom is pre-1.0. Under semver, any 0.x change may
be breaking, so the minor number is where changes go. `patch` is an opt-in
for fixes a consumer can pick up with no review. Any backward-incompatible
change still ships behind an opt-in, as it always has. The tag records when a
change landed; it doesn't promise the change is compatible.

### Starting point

The `v0.0.0` default only matters in a repo with no release tags; the first
merge there becomes `v0.1.0`. loom already has hand-cut tags up to `v0.49.0`,
so its first automated tag is `v0.50.0` (or `v0.49.1` if that PR is labelled
`patch`). The goal brief assumed loom had no tags; it does, and the numbering
continues from them rather than restarting.

## Consequences

- Every merge to `main` is a release. Consumers pick it up with a normal
  `go get github.com/go-apis/loom@vX.Y.Z` bump. Opening those bump PRs is each
  consumer's own goal (runsheet's half of G0), not loom's.
- The tag is pushed with `GITHUB_TOKEN`, which by GitHub's design does not
  trigger other workflows. Nothing currently listens for tag pushes; anything
  added later that does must be aware of this.
- Tags are never cut by hand for merged work. A mistaken bump is fixed by
  merging the next PR with the right label, not by moving or deleting a tag
  consumers may already have fetched.
