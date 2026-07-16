# Loom schema — M1 (extraction)

Loom is the schema-first layer over the es library: one schema file per
service declaring aggregates, commands, events, and handlers. Later
milestones generate the registry, typed handler interfaces, and read-model
persistence from it. **M1 only extracts** — it derives the schema from
existing service code and generates topology docs. No runtime changes.

## Two formats: SDL for humans, compiled schema for tools

- **`.loom` (SDL, sdl)** — the authoring surface: what people read,
  review, and (from M3 on) write. `loom extract` emits it; `loom compile`
  lowers it. Syntax highlighting lives in `vscode`.
- **Compiled schema (schema, YAML)** — the language-neutral
  interchange artifact with JSON Schema payloads. Nothing in it assumes Go:
  a future TypeScript target (Cloudflare Workers) consumes this, never the
  SDL. Go type names appear only under `go:` keys as extraction metadata —
  the one thing the SDL intentionally does not carry.

Both are deterministic: collections sorted, re-running extraction on
unchanged code is byte-identical, so schema drift shows up as a git diff.
The SDL is lossless over the compiled form (round-trip tested).

M1 records reality: sagas, event handlers, projectors, groups. The Phase-2
semantic names (Policy / Process / Projection) become new declaration
keywords later without a format break.

## The SDL (`<service>.loom`)

```
service payouts

aggregate MassPayout @snapshot(-1) {
  state {
    id: uuid!
    state: string!
    pending_payouts: [MassPayoutPayout?]
  }

  command NewMassPayout {
    aggregate_id: uuid!
    customers: [MassPayoutCustomer?]
  } -> MassPayoutCreated, MassPayoutPaymentsRequested

  event MassPayoutCreated { state: string!, ... }              // own, single emitter: nested
  event MassPayoutPaymentsRequested @publish { created_at: timestamp! }
}

consume profile.MassPayoutProfilesImported { mass_payout_id: uuid! }

saga massPayoutImportSaga {                       // @group(external) is the saga default
  on MassPayoutPaymentsRequested -> CompleteMassPayoutImport, ProcessMassPayout
  on profile.MassPayoutProfilesImported -> CompleteMassPayoutFinalize
}

projector paymentFeeProjector {                   // @group(internal) is the projector default
  on PaymentPreProcessing project PaymentFee
}

commands ledgerEntryHandler {                     // IsCommandHandler (no aggregate)
  command CreateLedgerEntry { ... }
}

type MassPayoutCustomer { email: string!, user_id: uuid!, ... }
```

Grammar notes:

- **Declarations**: `aggregate` (event-sourced) / `holder` / `entity`,
  `event`, `consume svc.Event` (foreign), `saga`, `handler` (event
  handler), `projector`, `commands` (command handler), `type` (shared
  payload type, JSON Schema `$defs` equivalent).
- **Fields**: `name: type` with `!` = required, `?` = nullable. Types:
  `string int float bool uuid timestamp bytes any map`, `[T]` arrays,
  inline `{ ... }` objects, `string(email)` for other formats, capitalized
  identifiers reference `type` declarations.
- **Directives**: `@snapshot(N)`, `@revision(s)`, `@project(false)`,
  `@publish`, `@alias(A, B)`, `@group(g)`, `@chunked`.
- `->` after a command lists emitted events; after `on`, dispatched
  commands. Both come from static body analysis (composite literals, one
  level of module helpers) and may under-report dynamically built values.
- Events emitted by exactly one aggregate nest inside it; `execution`
  (in-transaction vs async) is derived from the group, not written.

## Extraction rules (mirror of the lib's runtime reflection)

Source of truth: `es.NewRegistry` (registry.go) and the handle finders.

| Schema element | Detected by |
|---|---|
| registration list | args of the `es.NewRegistry(service, ...)` call; constructor calls resolved to their concrete returned type |
| classification | marker interfaces, same precedence as the registry type-switch: `IsSaga`, `IsProjector`, `IsEventHandler`, `IsCommandHandler`, `IsEvent`, `Aggregate` (has `Apply`), `Entity` |
| aggregate kind | embeds `BaseAggregateSourced` → sourced; `BaseAggregateHolder` → holder; else entity |
| aggregate config | `es:"..."` tag on the embedded `BaseAggregateSourced` field: bare name, `snapshot=N`, `rev=`, `project=false` (config_entity.go) |
| command handles | exported methods, name ≠ `Apply`, signature `(ctx, C) error` with C implementing `es.Command` (commandhandle.go) |
| saga handles | exported methods, name ≠ `Run`, `(ctx, *es.Event, D) ([]es.Command, error)` (sagahandle.go) |
| event-handler handles | exported methods, name ≠ `Run`, `(ctx, *es.Event, D) error` (eventhandlerhandle.go) |
| projector handles | exported methods, `(ctx, EntityT, D) error` (projectorhandle.go) |
| event config | `es:"..."` tag on embedded `BaseEvent`: bare name override, `publish[=bool]`, `service=X` (foreign), `alias=A,B` (config_event.go) |
| handler group | `es:"group=..."` on `BaseSaga`/`BaseProjector`/`BaseEventHandler`; defaults: projector `internal`, others `external` (config_eventhandler.go) |
| emitted events | `Apply(ctx, &events.X{...})` calls in aggregate command-handler bodies |
| chunked | a `unit.Dispatch` (or `es.NewDetachedUnit`) reachable inside a loop in the handler body, following module callees — the chunked-commit pattern |

Binding is by **type**, never method name — method names in service code are
conventionally the event name, but the lib routes on the parameter type, and
so does extraction.

## Cross-service linking (`loom docs`)

Each service's schema is self-contained; the docs generator joins them:

- a local event with `service: X` is matched to service X's event of the
  same name → consumer edge in the topology.
- **Drift detection:** the local redeclaration's payload fields must be a
  compatible subset (by JSON name + type) of the owner's payload; the owner
  event must exist and be `publish: true`. Violations land in the docs'
  drift report — this is the check that doesn't exist today.

## Reserved for later milestones (format-stable extensions)

- `events[].version` + `upcasts:` — payload schema versioning / upcaster
  chain (Phase 3; today only rename aliases exist).
- `handlers[].kind: policy | process | projection` — Phase 2 semantic
  replacements; `execution` already carries the in-tx/async split.
- `handlers[].state:` — persisted process-manager state (Phase 2).
- `metadata: correlation` requirements (Phase 3 translator).
- `projections[].checkpoint:` — positioned, rebuildable projections.
- `contracts:` — published integration-event module (Phase 3); JSON Schema
  payloads here are already the contract format.
