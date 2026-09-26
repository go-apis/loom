---
title: "ADR 0003 — A loom schema is a directory of files, parsed as one"
status: accepted
date: 2026-09-26
summary: loom.yml's schema may be a directory; every *.loom under it is lexed per file, joined in slash-path order and parsed once, with file:line errors; the first file declares the service and later files may omit or repeat it, never change it.
---

# ADR 0003 — A loom schema is a directory of files, parsed as one

## Context

A service's schema was one file in practice. `loom generate` did glob
several files, but it ran `sdl.Parse` on each file separately and then merged
the results. Because `Parse` validates, a file could not refer to an event or
type declared in another file. The merge also dropped enums and series from
every file after the first, and parse errors said only `line N`, without
naming the file.

So every feature edited the same file. runsheet's `platform.loom` is 9,000
lines and has 346 same-place insertion hunks across open branches. It is the
most-conflicted file in the estate. Of 370 Mesh reworks, 282 were merge
conflicts, and 49% of the conflict hunks were two sides inserting at the same
place in a registry-style file. Git cannot merge two insertions at one point.
If each feature adds a new file instead, the conflict goes away.

## Decision

- **A schema is a set of files parsed as one.** `sdl.ParseFiles` lexes each
  file on its own, so every token carries its file and line. It then joins
  the token streams and parses once. `Sort` and `Validate` run once, over the
  whole schema. A declaration may refer to anything in any file.
- **Files are ordered by path.** `sdl.ParseDir` walks a directory
  recursively, collects every `*.loom` file, and sorts them by
  slash-separated relative path. The order does not depend on how the file
  system lists them, and the output of `Sort` does not depend on the order at
  all. A glob in loom.yml is sorted the same way. loom.yml's `schema:` may
  name a directory, a single file, or a glob. `loom check` takes files or
  directories.
- **Positions are `path:line`.** An error at a token that came from a named
  file reads `path:line: message`. `sdl.Parse(src)` is `ParseFiles` over one
  unnamed file, so its errors keep the `line N:` form.
- **The service header.** The first file (by path) must open with
  `service X`. A later file may open with `service X` too, which keeps a file
  readable on its own, or leave the header out. A later file that names a
  different service is refused at its `path:line`. `service` is only a header
  at the top of a file. Anywhere else it is an unexpected token.

## Consequences

- A new aggregate is a new file. Features stop inserting at the same place
  in one file, so their branches stop conflicting there.
- Splitting a single-file schema into a directory changes nothing
  downstream. The declarations are the same, and `Sort` makes the compiled
  schema, and so the generated code, identical.
- A declaration must still be unique across all the files: two files that
  declare the same aggregate are refused, as they were in one file. Letting
  a feature add a member to an enum declared elsewhere is a separate
  decision (goal `schema-is-many-files`, item 2).
- Directive errors that are not raised at a token (for example, an unknown
  `@directive` on an aggregate) still name the declaration rather than a
  position. That is unchanged by this decision.
