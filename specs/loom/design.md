
## @loop (ADR 0012)

`@loop` is an aggregate-only directive (rejected on a record, naming the file and line) declaring one recurring look per aggregate TYPE, not per instance. `loom generate` emits a service-wide `Look` interface — one `Look<Name>(ctx) ([]loom.Command, error)` method per looping aggregate — and a once-only stub, the same shape as `EventUpcasts`/`Upcasts`. The wait between calls is meant to be the runtime's to keep rather than hand-rolled by the implementation, but the runtime that actually calls `Look<Name>` on an interval is not yet built — this directive currently reaches only the schema and the generated interface/stub. See go-apis/loom's `docs/adr/0012-a-loop-is-declared-per-aggregate-and-a-wait-is-built-in.md`.
