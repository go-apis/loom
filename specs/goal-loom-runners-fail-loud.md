---
title: Goal — A runner that cannot go on says so, and the rest go on
kind: plan
status: draft
---

# Goal — A runner that cannot go on says so, and the rest go on

Mesh goal `loom-runners-fail-loud`, running.

## Statement

G1: projection failure state on the checkpoint row, with backoff and a skip endpoint; lazy decode so a runner decodes only the events it subscribes to, an `@retired` event marker, and Migrate refusing unknown types in the log; an explicit start position for processes with from-head the default for a new one; ERROR on a never-checkpointed runner and failures logged at most once a minute; loom.CloudLogHandler writing `severity` so Cloud Logging's severity>=ERROR finds `runner step failed`. The brief is runsheet/runsheet specs/proposal-loom-for-adr-001.md (Prologue, project runsheet), section 3. loom has ten consumers (runsheet, wardroom, latchkey, grapevine, optrader, ten99 on v0.49.0; tripline, purser, foghorn on v0.46.0): every change is backward compatible or ships behind an opt-in, and the consumers' adoption is their own goals, not this one's.

## Acceptance

Tests: a fold that always fails leaves the runner stalled with the seq and error while other runners advance; a log holding an undeclared type fails Migrate and not the runners; a new process with effects performs none for events before it existed; the handler's JSON carries severity=ERROR for a failed step.

## Items

| Seq | Title | Status | Branch | PR | Cost |
| --- | --- | --- | --- | --- | --- |
| 1 | Lazy decode, @retired events, and Migrate guards against undeclared log types | merged | mesh/loom-runners-fail-loud-1 | https://github.com/go-apis/loom/pull/33 | $6.66 |
| 2 | Projection runners fail loud: stalled state, backoff, and a skip endpoint | claimed | — | — | $0.00 |
| 3 | Explicit process start position, @from(head) default for a new process | review | mesh/loom-runners-fail-loud-3 | https://github.com/go-apis/loom/pull/34 | $6.68 |
| 4 | loom.CloudLogHandler — severity for Cloud Logging | merged | mesh/loom-runners-fail-loud-4 | https://github.com/go-apis/loom/pull/31 | $2.43 |

## Log

- 2026-09-23 · item 4 · session started
- 2026-09-23 · item 4 · Done. Item 4 is complete on `mesh/loom-runners-fail-loud-4` (commit `50b45d9`). ## What I did **`cloudlog.go`** — new file, `loom.CloudLogHandler(w io.Writer) slog.Handler`: - Wraps `slog.NewJSONHandler` with a `ReplaceAttr` that renames `level` → `severity` (mapped by range to `DEBUG`/`INFO`/`WARNING`/`ERROR` — slog's WARN becomes Cloud Logging's WARNING) and `msg` → `message`. The rename only applies to the record's own built-ins: an application attribute called `level` carries a string rather than a `slog.Level` and is left alone, as is anything inside a group. - A thin `cloudLogHandler` wrapper does what `ReplaceAttr` can't — the trace lives on the context. When `trace.SpanContextFromContext` is valid it adds `logging.googleapis.com/trace`, `/spanId`, `/trace_sampled`. `GOOGLE_CLOUD_PROJECT` (set for you on Cloud Run/GAE/GKE) gives the `projects/<id>/traces/...` form Cloud Logging links on; without it the bare trace id still goes out. `WithAttrs`/`WithGroup` are overridden so the wrapper survives a `slog.With(...)` service logger. - `log/slog` + `os` + `go.opentelemetry.io/otel/trace`, which is already a direct dependency — no Cloud Logging client, no SDK, consistent with DESIGN.md's OTel-API-only stance. - I added a second constructor, `CloudLogHandlerOptions(w, opts)`, because the single-argument form gives no way to set a minimum level (Debug would be silently dropped). A caller's `ReplaceAttr` chains after the mapping. It is opt-in, as scoped: loom's default is still `slog.Default()`, and nothing in the package wires it. **Docs** — DESIGN.md's Observability bullet now names `CloudLogHandler` and what it maps; README's Observability section gets the wiring snippet and the reason (a plain JSON handler's `level` is invisible to `severity>=ERROR`). ## Acceptance `cloudlog_test.go` — seven tests, all passing. The acceptance test proper is `TestCloudLogHandlerSeverityForFailedStep`: it logs `Error("runner step failed", "runner", ..., "error", ...)` over the handler and asserts the JSON carries `"severity":"ERROR"` and `"message":"runner step failed"`, that `level`/`msg` are gone, and that the attributes survive. `TestCloudLogHandlerSeverityLevels` covers Debug→DEBUG, Info→INFO, Warn→WARNING, Error→ERROR (and a custom level above Error). The rest cover trace promotion, the project prefix, survival through `With`, application attrs, and `ReplaceAttr` chaining. `go test ./...` passes across every package, including the 20s Postgres-backed e2e suite. `go vet` and `gofmt` are clean on the new files (`bus.go`, `otel.go`, and `internal/e2e/aggtable_test.go` were already unformatted on `main`; I left them alone).
- 2026-09-24 · item 2 · session started
