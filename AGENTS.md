# Agent instructions for go-apis/loom

- Smoke-testing a new `cmd/<name>` with `go run ./cmd/<name>` or
  `go build ./cmd/<name>` (no `-o`) leaves a compiled binary named
  `<name>` sitting in the repo root or the package directory; this repo
  has no `.gitignore`, so a stray binary lands in the next commit. Build
  with `go build -o /dev/null ./cmd/...` when smoke-testing, or add a
  `.gitignore` entry for the binary's name, before committing.
