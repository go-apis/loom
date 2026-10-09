---
title: "ADR 0005 — The generator is pinned per consumer, and `generate --check` compares rendered bytes"
status: accepted
date: 2026-10-09
summary: A consumer pins the generator with a go.mod tool directive (added by `loom init`) so a loom bump moves it and is labelled [needs loomgen]; `loom generate --check` renders loomgen files in memory and compares bytes, never stubs.
---

# ADR 0005 — The generator is pinned per consumer, and `generate --check` compares rendered bytes

## Context

Most consumers' loom bump PRs merged without regenerating, and nothing
noticed. The platform's bump process writes only go.mod and go.sum, and
labels a PR `[needs loomgen]` only when the consumer pins the generator in
its own go.mod (runsheet ADR 007). Only runsheet did. For the others, a bump
looked complete even though `loom generate` at the new tag changes their
output, as v0.58's nested-input nullability does. Loom had no check mode and
`loom init` added no tool directive.

## Decision

- **The generator is pinned per consumer by a go.mod tool directive.**
  `loom init` adds `tool github.com/go-apis/loom/cmd/loom` to the go.mod in
  the current directory. The directive is added by text edit, with no network
  or go toolchain. If the directive is present (single line or in a
  `tool ( … )` block), go.mod stays byte-identical. A bump moves the require
  line, so the platform labels it `[needs loomgen]`, and `go tool loom` runs
  the pinned version.
- **go.mod below go 1.24 is not rewritten.** Tool directives need 1.24, and
  init does not bump the `go` line on its own: it says so and leaves go.mod
  alone. Raising the language version is the consumer's decision.
- **init does not add the require line.** That needs the network. It tells
  the user to run `go get -tool github.com/go-apis/loom/cmd/loom@<version>` or
  `go mod tidy`.
- **`loom generate --check` compares rendered bytes.** It renders the loomgen
  files (models, registry, tables and the other regenerated outputs) through
  the same path as `generate`, writes nothing, and exits non-zero naming each
  file that differs from disk or is missing. It never looks at stubs, which
  belong to the user and are written once.

## Consequences

- Consumers run `go tool loom generate --check` in CI, in each directory with
  a loom.yml, and regenerate with `go tool loom generate`.
- Existing consumers add the directive themselves, one at a time; that is
  follow-up work on each consumer's project.
- A stale file is found by content, so a formatting-only generator change
  also fails the check until regenerated. That is intended.
