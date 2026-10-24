# Agent instructions for go-apis/loom

- A test that shells out to `go mod tidy` must not set `GOPROXY=off`:
  `tidy` reads the `go.mod` files of every version in the module graph,
  which a normal build never caches, so it fails on a cold module cache.
  Let the subprocess inherit the environment's `GOPROXY`, or warm the
  cache first with `go mod download all`.
