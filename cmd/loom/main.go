// Command loom is the schema-first toolchain:
//
//	loom init <service>   scaffold loom.yml + schema/<service>.loom
//	loom generate         regenerate models/registry + missing stubs
//	loom generate --check write nothing; list stale/missing generated files, exit 1 if any
//	loom rewrap           re-wrap sealed data keys under a new KeyWrapper
package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"gopkg.in/yaml.v3"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/gen"
	"github.com/go-apis/loom/gkms"
	"github.com/go-apis/loom/schema"
	"github.com/go-apis/loom/sdl"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "init":
		err = runInit(os.Args[2:])
	case "generate":
		err = runGenerate(os.Args[2:], os.Stdout)
	case "check":
		err = runCheck(os.Args[2:])
	case "openapi":
		err = runEmit(os.Args[2:], "openapi.json", gen.OpenAPI)
	case "graphql":
		err = runEmit(os.Args[2:], "", gen.GraphQL)
	case "rewrap":
		err = runRewrap(os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "loom:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: loom init <service>")
	fmt.Fprintln(os.Stderr, "       loom generate [--dir <service dir>] [--check]")
	fmt.Fprintln(os.Stderr, "       loom check <schema.loom|schema dir ...>")
	fmt.Fprintln(os.Stderr, "       loom openapi [--dir <service dir>] [--out openapi.json]")
	fmt.Fprintln(os.Stderr, "       loom graphql [--dir <service dir>] [--out <service>.graphqls]")
	fmt.Fprintln(os.Stderr, "       loom rewrap --db <dsn> (--from-local <hex>|--from-kms <key>) (--to-local <hex>|--to-kms <key>)")
	os.Exit(2)
}

// runRewrap re-wraps loom_keys rows from one KeyWrapper to another —
// master-key rotation, or moving LocalKeys → Cloud KMS. Sealed data is
// untouched; only the DEK wrapping changes.
func runRewrap(args []string) error {
	fs := flag.NewFlagSet("rewrap", flag.ExitOnError)
	dsn := fs.String("db", "", "Postgres DSN (required)")
	fromLocal := fs.String("from-local", "", "current master key (64 hex chars)")
	fromKMS := fs.String("from-kms", "", "current Cloud KMS crypto key resource name")
	toLocal := fs.String("to-local", "", "new master key (64 hex chars)")
	toKMS := fs.String("to-kms", "", "new Cloud KMS crypto key resource name")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dsn == "" {
		return fmt.Errorf("rewrap: --db is required")
	}
	ctx := context.Background()
	wrapper := func(local, kms, side string) (loom.KeyWrapper, error) {
		switch {
		case local != "" && kms != "":
			return nil, fmt.Errorf("rewrap: give exactly one of --%s-local / --%s-kms", side, side)
		case local != "":
			key, err := hex.DecodeString(local)
			if err != nil {
				return nil, fmt.Errorf("rewrap: --%s-local is not hex: %w", side, err)
			}
			return loom.LocalKeys(key)
		case kms != "":
			return gkms.New(ctx, kms)
		default:
			return nil, fmt.Errorf("rewrap: give one of --%s-local / --%s-kms", side, side)
		}
	}
	from, err := wrapper(*fromLocal, *fromKMS, "from")
	if err != nil {
		return err
	}
	to, err := wrapper(*toLocal, *toKMS, "to")
	if err != nil {
		return err
	}
	pool, err := pgxpool.New(ctx, *dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	n, err := loom.Rewrap(ctx, pool, from, to)
	if err != nil {
		return fmt.Errorf("rewrap: %d keys done, then: %w", n, err)
	}
	fmt.Printf("rewrapped %d data keys\n", n)
	return nil
}

// runEmit powers the schema projections (OpenAPI, GraphQL SDL): load the
// service's schema via loom.yml, run the emitter, write the artifact.
func runEmit(args []string, defaultOut string, emit func(*schema.Schema) ([]byte, error)) error {
	fs := flag.NewFlagSet("emit", flag.ExitOnError)
	dir := fs.String("dir", ".", "service directory (where loom.yml lives)")
	out := fs.String("out", "", "output file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	raw, err := os.ReadFile(filepath.Join(*dir, "loom.yml"))
	if err != nil {
		return fmt.Errorf("no loom.yml in %s: %w", *dir, err)
	}
	var cfg config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return fmt.Errorf("loom.yml: %w", err)
	}
	s, err := loadSchemas(*dir, cfg.Schema)
	if err != nil {
		return err
	}
	data, err := emit(s)
	if err != nil {
		return err
	}
	name := defaultOut
	if name == "" {
		name = s.Service + ".graphqls"
	}
	if *out != "" {
		name = *out
	} else {
		name = filepath.Join(*dir, name)
	}
	if err := os.WriteFile(name, data, 0o644); err != nil {
		return err
	}
	fmt.Println("wrote", name)
	return nil
}

// runCheck parses and validates schemas without generating anything — for
// designing schemas before their services exist. Each argument is one
// schema: a .loom file, or a directory of them.
func runCheck(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("check wants schema files or directories")
	}
	for _, path := range args {
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		var s *schema.Schema
		if info.IsDir() {
			s, err = sdl.ParseDir(path)
		} else {
			s, err = sdl.ParsePaths([]string{path})
		}
		if err != nil {
			return err
		}
		fmt.Printf("%s: ok — service %s: %d aggregates, %d events, %d policies, %d processes, %d projections\n",
			path, s.Service, len(s.Aggregates), len(s.Events), len(s.Policies), len(s.Processes), len(s.Projections))
	}
	return nil
}

// config is loom.yml. Everything except schema has a default.
type config struct {
	// Schema is a directory of .loom files, a single file, or a glob.
	Schema    string `yaml:"schema"`
	Module    string `yaml:"module,omitempty"`
	Package   string `yaml:"package,omitempty"`
	Generated string `yaml:"generated,omitempty"`
	// Layout: "flat" (default) or "folders" (stubs in aggregates/,
	// records/, policies/, processes/ packages).
	Layout string `yaml:"layout,omitempty"`
}

func runInit(args []string) error {
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("init wants exactly one argument: the service name")
	}
	service := fs.Arg(0)

	if _, err := os.Stat("loom.yml"); err == nil {
		return fmt.Errorf("loom.yml already exists")
	}
	if err := os.MkdirAll("schema", 0o755); err != nil {
		return err
	}
	cfgYaml := "schema: schema/\n" // every *.loom under schema/; a new aggregate is a new file
	if err := os.WriteFile("loom.yml", []byte(cfgYaml), 0o644); err != nil {
		return err
	}
	skeleton := fmt.Sprintf(`service %s

// Declare aggregates, events, and reactions, then run: loom generate
//
// aggregate Thing {
//   state {
//     status: string
//   }
//   command CreateThing -> ThingCreated
//   event ThingCreated { status: string! }
// }
`, service)
	schemaPath := filepath.Join("schema", service+".loom")
	if err := os.WriteFile(schemaPath, []byte(skeleton), 0o644); err != nil {
		return err
	}
	fmt.Printf("initialised %s: loom.yml + %s\n", service, schemaPath)
	return pinGenerator("go.mod", os.Stdout)
}

const loomTool = "github.com/go-apis/loom/cmd/loom"

// pinGenerator makes sure the go.mod at path carries a tool directive for the
// generator, so the version a loom bump moves is the generator's version too.
// A missing go.mod is not an error; a go.mod below go 1.24 is left alone.
func pinGenerator(path string, out io.Writer) error {
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var inTool bool
	var goVer string
	for _, line := range strings.Split(string(raw), "\n") {
		f := strings.Fields(strings.SplitN(line, "//", 2)[0])
		switch {
		case len(f) == 0:
		case inTool:
			if f[0] == ")" {
				inTool = false
			} else if f[0] == loomTool {
				fmt.Fprintf(out, "go.mod: tool %s already pinned\n", loomTool)
				return nil
			}
		case f[0] == "tool" && len(f) == 2 && f[1] == "(":
			inTool = true
		case f[0] == "tool" && len(f) == 2 && f[1] == loomTool:
			fmt.Fprintf(out, "go.mod: tool %s already pinned\n", loomTool)
			return nil
		case f[0] == "go" && len(f) == 2:
			goVer = f[1]
		}
	}
	var major, minor int
	if _, err := fmt.Sscanf(goVer, "%d.%d", &major, &minor); err != nil || major < 1 || (major == 1 && minor < 24) {
		fmt.Fprintf(out, "go.mod: not pinning %s: tool directives need go 1.24 or newer (go.mod says %q); raise the go line, then add: tool %s\n", loomTool, goVer, loomTool)
		return nil
	}
	text := string(raw)
	if !strings.HasSuffix(text, "\n") {
		text += "\n"
	}
	text += "\ntool " + loomTool + "\n"
	if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
		return err
	}
	fmt.Fprintf(out, "go.mod: added tool %s\n", loomTool)
	fmt.Fprintf(out, "run `go get -tool %s@<version>` or `go mod tidy` so go.mod has a require line for it\n", loomTool)
	return nil
}

func runGenerate(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("generate", flag.ExitOnError)
	dir := fs.String("dir", ".", "service directory (where loom.yml lives)")
	check := fs.Bool("check", false, "write nothing; name generated files that are stale or missing and fail if any")
	if err := fs.Parse(args); err != nil {
		return err
	}

	raw, err := os.ReadFile(filepath.Join(*dir, "loom.yml"))
	if err != nil {
		return fmt.Errorf("no loom.yml in %s (run loom init first): %w", *dir, err)
	}
	var cfg config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return fmt.Errorf("loom.yml: %w", err)
	}
	if cfg.Schema == "" {
		return fmt.Errorf("loom.yml: schema is required")
	}
	if cfg.Module == "" {
		cfg.Module, err = modulePath(*dir)
		if err != nil {
			return err
		}
	}

	s, err := loadSchemas(*dir, cfg.Schema)
	if err != nil {
		return err
	}

	if cfg.Layout != "" && cfg.Layout != "flat" && cfg.Layout != "folders" {
		return fmt.Errorf("loom.yml: layout must be flat or folders, got %q", cfg.Layout)
	}
	gcfg := gen.Config{
		Dir:     *dir,
		Package: cfg.Package,
		GenDir:  cfg.Generated,
		Module:  cfg.Module,
		Layout:  cfg.Layout,
	}
	if *check {
		stale, missing, err := gen.Check(s, gcfg)
		if err != nil {
			return err
		}
		for _, f := range stale {
			fmt.Fprintln(out, "stale  ", rel(*dir, f))
		}
		for _, f := range missing {
			fmt.Fprintln(out, "missing", rel(*dir, f))
		}
		if n := len(stale) + len(missing); n > 0 {
			return fmt.Errorf("%d generated file(s) out of date; run loom generate", n)
		}
		return nil
	}
	res, err := gen.Generate(s, gcfg)
	if err != nil {
		return err
	}
	for _, f := range res.Written {
		fmt.Fprintln(out, "wrote", rel(*dir, f))
	}
	for _, f := range res.Skipped {
		fmt.Fprintln(out, "kept ", rel(*dir, f), "(stub exists)")
	}
	return nil
}

// loadSchemas reads loom.yml's schema: a directory (every *.loom under it,
// at any depth), a single file, or a glob. However many files it names,
// they are parsed as one schema, in path order.
func loadSchemas(dir, spec string) (*schema.Schema, error) {
	p := filepath.Join(dir, spec)
	if info, err := os.Stat(p); err == nil {
		if info.IsDir() {
			return sdl.ParseDir(p)
		}
		return sdl.ParsePaths([]string{p})
	}
	paths, err := filepath.Glob(p)
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("no schema files match %s", spec)
	}
	sort.Slice(paths, func(i, j int) bool { return filepath.ToSlash(paths[i]) < filepath.ToSlash(paths[j]) })
	return sdl.ParsePaths(paths)
}

func modulePath(dir string) (string, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "go.mod"))
	if err != nil {
		return "", fmt.Errorf("cannot determine module (no loom.yml module and no go.mod): %w", err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(line), "module "); ok {
			return strings.TrimSpace(rest), nil
		}
	}
	return "", fmt.Errorf("no module line in %s/go.mod", dir)
}

func rel(dir, path string) string {
	if r, err := filepath.Rel(dir, path); err == nil {
		return r
	}
	return path
}
