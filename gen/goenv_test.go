package gen_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// The compile checks (vetGenerated) run `go mod tidy` and `go vet` in a
// temporary module, and tidy fetches test-only dependencies of
// dependencies that a build of loom never needs. A Mesh node mounts its
// shared module cache read-only into validation pods, so those fetches
// failed with "read-only file system" and the checks went red for a reason
// that had nothing to do with the generator.
//
// goEnv keeps the inherited cache when it is writable. When it is not,
// the commands get a module cache of this package's own (removed when the
// package's tests end), and the read-only cache serves as a read-through
// proxy in front of the inherited GOPROXY, so modules already there cost
// no download.

var (
	goEnvOnce sync.Once
	goEnvVars []string
	goEnvErr  error
	goEnvTemp string // the private module cache, if one was made
	goFlags   string // GOFLAGS the commands run with
)

func TestMain(m *testing.M) {
	code := m.Run()
	if goEnvTemp != "" {
		_ = os.RemoveAll(goEnvTemp) // writable: -modcacherw
	}
	os.Exit(code)
}

// goEnv is the environment for a go command a test runs, with extra
// GOFLAGS added to the ones it already carries.
func goEnv(t *testing.T, flags ...string) []string {
	t.Helper()
	goEnvOnce.Do(func() { goEnvVars, goEnvErr = buildGoEnv() })
	if goEnvErr != nil {
		t.Fatal(goEnvErr)
	}
	env := append([]string(nil), goEnvVars...)
	if len(flags) > 0 { // exec keeps the last of duplicate keys
		env = append(env, "GOFLAGS="+strings.TrimSpace(goFlags+" "+strings.Join(flags, " ")))
	}
	return env
}

func buildGoEnv() ([]string, error) {
	out, err := exec.Command("go", "env", "GOMODCACHE", "GOPROXY", "GOFLAGS").Output()
	if err != nil {
		return nil, fmt.Errorf("go env: %w", err)
	}
	lines := strings.Split(strings.TrimRight(string(out), "\n"), "\n")
	for len(lines) < 3 {
		lines = append(lines, "")
	}
	modcache, proxy, flags := lines[0], lines[1], lines[2]
	goFlags = flags
	if modcache == "" || writable(modcache) {
		return os.Environ(), nil
	}
	tmp, err := os.MkdirTemp("", "loom-gen-modcache-")
	if err != nil {
		return nil, err
	}
	goEnvTemp = tmp
	if proxy == "" {
		proxy = "https://proxy.golang.org,direct"
	}
	if proxy != "off" {
		// a module missing from the file proxy is a 404, which falls
		// through to the next entry
		proxy = "file://" + filepath.ToSlash(filepath.Join(modcache, "cache", "download")) + "," + proxy
	}
	goFlags = strings.TrimSpace(flags + " -modcacherw")
	return append(os.Environ(),
		"GOMODCACHE="+tmp,
		"GOPROXY="+proxy,
		"GOFLAGS="+goFlags,
	), nil
}

// writable reports whether go could write into the module cache: the
// download lock files live under cache/download.
func writable(modcache string) bool {
	dir := filepath.Join(modcache, "cache", "download")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return false
	}
	f, err := os.CreateTemp(dir, ".loom-gen-probe-")
	if err != nil {
		return false
	}
	name := f.Name()
	f.Close()
	os.Remove(name)
	return true
}
