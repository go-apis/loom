# ADR 0015 — `loom adopt` names the hand-written code a loom feature already covers

## Context

A consumer only learns a loom release freed it from code it wrote by
hand (a ticker goroutine, a chain of `loom.After` re-arms, a `policy
<name>Timer` block) by reading the changelog and remembering its own
service well enough to connect the two. In practice nobody does: a
service keeps its hand-rolled polling loop long after `Client.Every` (a
schedule, since v0.95.0) or a `@loop` aggregate with `WaitFor` (since
v0.110.0) could keep it instead, because nothing ever points at the
spot and says so.

## Decision

- **A new `adopt` package holds the rules.** Each `Rule` names what the
  repository wrote by hand, the feature to use instead and the loom
  release that brought it. `Scan(root)` walks a repository's `.go` and
  `.loom` files (skipping tests, generated code — `loomgen`,
  `*_gen.go` — vendor, node_modules, testdata and dot-directories) and
  returns a `Finding` per match: rule, file, line, the span of lines it
  covers, the feature and a one-line reading of what it found.
- **Three rules ship with it**: a goroutine looping on `time.NewTicker`
  (`Client.Every`, or a `schedule`, since v0.95.0); a tick that
  schedules its own next tick with `loom.After` (a `schedule`, or a
  `@loop` aggregate with `WaitFor`, since v0.110.0; a one-shot timeout
  is left alone); and a `policy <name>Timer` block arming or cancelling
  a timer on events (a `@loop` aggregate with `WaitFor`, since
  v0.110.0). Every rule names a feature this loom has, so a repository
  is only ever told about what the loom it pins can do.
- **`loom adopt [--root <repo root>] [--json]`** runs `Scan` and prints
  a report: each finding, then each matched rule's count and lines with
  its feature, release and advice, closing with a pointer to
  `docs/features.md` and the line "this is advice: nothing here is a
  fault." `--json` prints the findings instead. It reads only, never
  writes, and always exits 0 — a finding is something to look at, not a
  build failure.

## Consequences

- A release that adds a feature a hand-written pattern could retire
  adds its rule here (and its entry in `docs/features.md`, carried by a
  later item) in the same PR, so `loom adopt` only ever recommends what
  the pinned loom version actually has.
- Because it exits 0 and only reads, a consumer can run it in CI or an
  agent's own loop without it ever blocking a merge; it is a prompt to
  look, not a gate.
- `docs/features.md` — the rule-to-feature reference the report points
  readers to — does not exist yet; `adopt`'s own test
  (`TestEveryRuleIsDocumented`) skips until it is written by a later
  item, so the report's closing pointer is a promise, not yet a link.
