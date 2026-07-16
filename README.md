# Loom

Schema-first tooling for [go-apis/eventsourcing](https://github.com/go-apis/eventsourcing):
one `.loom` schema per service declaring its aggregates, commands, events,
and handlers — extracted from existing code today, generating registries and
typed handler interfaces tomorrow.

Status: **M1 (extraction)**. `loom extract` derives a schema from a service
without any runtime change, and `loom docs` turns a set of schemas into a
system map: per-service pages, a cross-service mermaid topology, an event
catalog, and a foreign-event drift report.

## Usage

```sh
go run github.com/go-apis/loom/cmd/loom extract --dir path/to/service --service payouts
go run github.com/go-apis/loom/cmd/loom docs --schemas . --out docs
go run github.com/go-apis/loom/cmd/loom compile --in payouts.loom   # lower to YAML
```

## Layout

| package | what |
|---|---|
| `schema` | the compiled schema: the language-neutral interchange form (YAML, JSON Schema payloads) |
| `sdl` | the `.loom` language: lexer, parser, emitter — the human surface, lossless over `schema` |
| `extract` | static analysis of a Go service, mirroring the es library's runtime registration rules |
| `docs` | cross-service linker + system-map generator |
| `cmd/loom` | the CLI |
| `vscode` | VS Code language support for `.loom` |

See [DESIGN.md](DESIGN.md) for the SDL grammar and extraction rules.
`testdata/users` is a copy of the es library's example service, pinned to
the published lib, used by the round-trip and extraction tests.
