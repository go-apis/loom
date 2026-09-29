---
title: "ADR 0004 — GraphQL input nullability follows the schema's required list, nested too"
status: accepted
date: 2026-09-29
summary: A GraphQL input field is NonNull exactly when the schema marks it required, at a command's top level and inside a nested type alike; enums stay nullable; the registry carries each type's required list as loom.TypeDef, and a registry without Types falls back to Go pointer-ness.
---

# ADR 0004 — GraphQL input nullability follows the schema's required list, nested too

## Context

The gateway builds its GraphQL inputs by reflecting on the generated Go
structs. Go cannot tell `[T]?` from `[T]!`: the generator emits a plain
slice for both, and a plain value for optional and required scalars alike.

Top-level command inputs already avoided this. The generator emits
`CommandDef.Required` from the schema, and `commandInput` makes a field
NonNull exactly when it is in that list. Nested inputs, which are schema
`type`s used inside a command, did not. `nestedInput` read nullability from
Go pointer-ness, so every slice came out NonNull.

The emitted SDL (`loom graphql`) followed the schema in both positions, so
the served schema disagreed with the published one. runsheet found it on
2026-09-28. `type NodeScaling { accounts: [NodeScalingAccount]? }`, reached
through `command HeartbeatNode { scaling: NodeScaling? }`, was served as
`accounts: [NodeScalingAccountInput!]!`. A client that omitted the list, as
the SDL allows, was refused with `got invalid value`.

## Decision

- **GraphQL input NonNull follows the schema's required list in every
  position, top-level and nested.** A field is NonNull if and only if the
  schema marks it required. This is the same rule `commandInput` uses, and
  it matches the emitted SDL.
- **The registry carries each type's required list.** `loom.Registry.Types`
  holds a `loom.TypeDef{Name, Required}` for every schema `type`. The
  generator always writes the field, sorted by name, and writes it empty when
  the schema has no types. The gateway indexes it by type name, which is the
  Go type name `nestedInput` reflects.
- **Enums stay nullable** in both positions. Their zero value means "unset",
  and the generated `Validate()` rejects an empty required enum at dispatch.
- **A registry without Types falls back to Go pointer-ness.** A struct with
  no `TypeDef` keeps the old rule: pointer fields are nullable and value
  fields are NonNull. This covers hand-written structs and registries
  generated before this change.
- An absent or null optional field converts to an absent or nil value, so a
  `[]T` field unmarshals to a nil slice. No extra marker is needed.

## Consequences

- The gateway's public contract changes: nested optional fields are now
  nullable. The change only relaxes it. Every query that was valid before
  is still valid, and clients may now omit what the schema says is
  optional.
- A service must regenerate (`loom generate`) to get the fix. Until then its
  registry has no Types and its nested inputs behave as before.
- If two services declare a type with the same name, the gateway keeps
  one required list for it: the one from the service registered last. The
  input object itself is still built once and shared. Name collisions
  across services are already the services' problem to avoid.
- Output types are unchanged. Row and state fields stay nullable, as
  `objectFor` documents.
