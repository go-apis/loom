---
title: Goal — loom has CI, and a merge makes a tag
kind: plan
status: draft
---

# Goal — loom has CI, and a merge makes a tag

Mesh goal `loom-release-path`, running.

## Statement

G0, loom's half. loom has no workflows: add CI running go test ./... with a Postgres service container, and tag on merge — every merge to main gets the next minor tag unless it carries a patch label. The bump PRs on consumers are runsheet's half, a separate goal. The brief is runsheet/runsheet specs/proposal-loom-for-adr-001.md (Prologue, project runsheet), section 3. loom has ten consumers (runsheet, wardroom, latchkey, grapevine, optrader, ten99 on v0.49.0; tripline, purser, foghorn on v0.46.0): every change is backward compatible or ships behind an opt-in, and the consumers' adoption is their own goals, not this one's.

## Acceptance

A PR's CI runs the full suite against Postgres and blocks the merge when red; merging a one-line PR produces the next tag without a person; a patch-labelled merge produces a patch tag.

## Items

| Seq | Title | Status | Branch | PR | Cost |
| --- | --- | --- | --- | --- | --- |
| 1 | CI: go test ./... against a Postgres service container | running | — | — | $1.44 |
| 2 | Tag on merge: next minor unless patch-labelled, plus the release-path ADR | pending | — | — | $0.00 |

## Log

- 2026-09-23 · item 1 · session started
- 2026-09-23 · item 1 · Done. Branch `mesh/loom-release-path-1` has three commits. ## What I did **`.github/workflows/ci.yml`** (new — loom had no `.github/` at all): - Triggers: `pull_request: branches: [main]` and `push: branches: [main]`. - `actions/checkout@v4` + `actions/setup-go@v5` with `go-version-file: go.mod` (picks up the repo's `go 1.26.4`). - `postgres:16` service, `POSTGRES_USER/PASSWORD/DB` = `postgres`/`mysecret`/`postgres`, `5432:5432`, `pg_isready` health check with 10 retries. - `go test ./... -count=1`. `LOOM_TEST_PG` is deliberately unset so `internal/e2e`'s `testDB` uses its built-in default, which the service container matches exactly. - One addition beyond the brief: a `pg_isready` preflight step against `localhost:5432` on the runner. Rationale below — it's load-bearing. ## How acceptance was checked Rule 5 forbids pushing, so I could not produce a GitHub Checks run; I verified locally against a `postgres:16` container configured identically to the service block (published on 55432 because latchkey's `postgres:17` holds 5432 on this box; `LOOM_TEST_PG` pointed at it). - **e2e actually executes, not skips**: `go test ./... -count=1` → all packages `ok`, `internal/e2e` at **21.5s**. Verbose: **35 PASS**, 3 SKIP (`TestPubSubBus`/`TestPubSubPush` self-skip without `PUBSUB_EMULATOR_HOST`, `TestSeriesHypertable` needs TimescaleDB) — all expected self-skips, none on the `no postgres available` path. - **Red suite fails the job**: committed `6431f75`, flipping `TestOrderBillingLoop`'s invoice assertion from `4000` to `4001` cents. Suite → `FAIL github.com/go-apis/loom/internal/e2e`, `go test` exit **1**. Reverted in `13904eb`; re-ran, exit **0**. Both commits are on the branch as the record. - **Triggers**: parsed the YAML, confirmed `{"pull_request": {"branches": ["main"]}, "push": {"branches": ["main"]}}`. ## One thing worth knowing Before the container had its port mapping, `go test ./...` printed `ok github.com/go-apis/loom/internal/e2e 0.017s` — **green, with every single e2e test skipped**. A green `go test ./...` is not by itself evidence the Postgres suite ran; the only tell is the package duration. That's why I added the `pg_isready` preflight: it turns a misconfigured service container into a loud red step instead of a silent all-skip green. If the goal's "blocks the merge when red" is to mean anything, that guard matters. No ADR/TRD exists in this repo and Prologue indexes no documents for `go-apis/loom`. This change touches no aggregate, policy, process, or public contract, and item 2's title already scopes the release-path ADR — so I wrote no design doc here.
