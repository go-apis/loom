// Command loom is the schema-first toolchain:
//
//	loom init <service>   scaffold loom.yml + schema/<service>.loom
//	loom generate         regenerate models/registry + missing stubs
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/go-apis/loom/gen"
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
		err = runGenerate(os.Args[2:])
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
	fmt.Fprintln(os.Stderr, "       loom generate [--dir <service dir>]")
	os.Exit(2)
}

// config is loom.yml. Everything except the schema glob has a default.
type config struct {
	Schema    string `yaml:"schema"`
	Module    string `yaml:"module,omitempty"`
	Package   string `yaml:"package,omitempty"`
	Generated string `yaml:"generated,omitempty"`
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
	cfgYaml := fmt.Sprintf("schema: schema/%s.loom\n", service)
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
	return nil
}

func runGenerate(args []string) error {
	fs := flag.NewFlagSet("generate", flag.ExitOnError)
	dir := fs.String("dir", ".", "service directory (where loom.yml lives)")
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

	res, err := gen.Generate(s, gen.Config{
		Dir:     *dir,
		Package: cfg.Package,
		GenDir:  cfg.Generated,
		Module:  cfg.Module,
	})
	if err != nil {
		return err
	}
	for _, f := range res.Written {
		fmt.Println("wrote", rel(*dir, f))
	}
	for _, f := range res.Skipped {
		fmt.Println("kept ", rel(*dir, f), "(stub exists)")
	}
	return nil
}

func loadSchemas(dir, glob string) (*schema.Schema, error) {
	paths, err := filepath.Glob(filepath.Join(dir, glob))
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("no schema files match %s", glob)
	}
	var merged *schema.Schema
	for _, p := range paths {
		src, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		s, err := sdl.Parse(string(src))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", p, err)
		}
		if merged == nil {
			merged = s
			continue
		}
		if s.Service != merged.Service {
			return nil, fmt.Errorf("%s declares service %s; expected %s", p, s.Service, merged.Service)
		}
		merged.Aggregates = append(merged.Aggregates, s.Aggregates...)
		merged.Entities = append(merged.Entities, s.Entities...)
		merged.Events = append(merged.Events, s.Events...)
		merged.Policies = append(merged.Policies, s.Policies...)
		merged.Processes = append(merged.Processes, s.Processes...)
		merged.Projections = append(merged.Projections, s.Projections...)
		merged.Types = append(merged.Types, s.Types...)
	}
	merged.Sort()
	if err := merged.Validate(); err != nil {
		return nil, err
	}
	return merged, nil
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
