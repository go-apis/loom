---
title: Goal — A schema's optional list inside a nested input type is nullable in GraphQL, as the schema says
kind: plan
status: running
summary: Nested optional lists are nullable in GraphQL, matching the schema; item 1 merged and tagged v0.58.0; item 3 adds e2e proof, in review.
---

# Goal — A schema's optional list inside a nested input type is nullable in GraphQL, as the schema says

## Statement

Found 2026-09-28 in runsheet: a schema type used as a nested input (`type NodeScaling { accounts: [NodeScalingAccount]? }` reached through `command HeartbeatNode { scaling: NodeScaling? }`) still came out as `accounts: [NodeScalingAccountInput!]!` in the GraphQL input — a client that omitted the list was refused with 'got invalid value'. The schema's `?` on a list was lost for nested inputs; top-level command fields use the schema's required list and are correct. The fix: nestedInput treats a slice field as nullable when the schema marked it optional, consulting the required list that top-level fields use. Registry.Types carries each type's required list; the generator emits it sorted by name. A nil slice is the natural absent value, so the converter accepts null/absent and leaves the slice nil.

## Acceptance

go test in loom covers the nested optional list (nullable in SDL/introspection, absent accepted, nil in Go) and the required list (NON_NULL); a tagged release exists and runsheet's bump PR appears.

## Items

| Seq | Title | Status | Branch | PR |
| --- | --- | --- | --- | --- |
| 1 | Nested input NonNull follows the schema's required list: Registry.Types, generator emission, nestedInput, ADR 0004; unit tests TestNestedInputFollowsSchemaRequired and TestTypeDefsCarryRequired | merged | mesh/nested-optional-lists-are-nullable-1 | https://github.com/go-apis/loom/pull/50 |
| 2 | e2e: a nested type's optional list is nullable through the live gateway and an input omitting it is accepted | failed (three evaluations) | mesh/nested-optional-lists-are-nullable-2 | — |
| 3 | e2e: a nested type's optional list is nullable through the live gateway, an omitted or null list is accepted and arrives as a nil slice; e2e test TestGraphQLNestedOptionalList | open | mesh/nested-optional-lists-are-nullable-3 | https://github.com/go-apis/loom/pull/53 |

## Release

- **v0.58.0** tagged on 2026-09-29 at commit efcc4a3 (item 1 merged, the nestedInput fix). Runsheet's bump PR https://github.com/runsheet/runsheet/pull/1288 was opened by loom-bump-on-tag.
- When item 3 (PR #53) merges, a new tag will be created with the e2e test, completing the proof.
- **Runsheet regeneration required:** runsheet must run `loom generate` to regenerate its registry with Types before the fix takes effect there. Until then its registry has no Types and nested inputs behave as before (pointer-based nullability).

## Models

- Item 1 (PR #50): Claude Opus
- Item 3 (PR #53): Claude Opus

## Log

- 2026-09-28 · Finding in runsheet: nested optional list in NodeScaling served as NON_NULL, client refused
- 2026-09-29 · Item 1 · PR #50 merged (commit efcc4a3), Registry.Types implementation, nestedInput fix, ADR 0004 decision; tagged v0.58.0; runsheet loom-bump-on-tag opened PR #1288
- 2026-09-29 · Item 3 · PR #53 in review (commits bdb57d3, 76de3a5, 1ef1e7a), e2e test TestGraphQLNestedOptionalList proves nested optional lists are nullable through the live gateway

## Design

Consult [ADR 0004 — GraphQL input nullability follows the schema's required list, nested too](docs/adr/0004-nested-input-nullability-follows-the-schema.md): a GraphQL input NonNull follows the schema's required list in every position, top-level and nested alike. The registry carries each type's required list as loom.TypeDef.
