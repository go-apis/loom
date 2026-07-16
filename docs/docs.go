// Package docs generates the system map from a set of Loom schemas:
// per-service pages, the cross-service topology, the event catalog, and the
// foreign-event drift report.
package docs

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/go-apis/loom/schema"
	"github.com/go-apis/loom/sdl"
)

func Generate(schemaDir, outDir string) error {
	sdlPaths, err := filepath.Glob(filepath.Join(schemaDir, "*.loom"))
	if err != nil {
		return err
	}
	yamlPaths, err := filepath.Glob(filepath.Join(schemaDir, "*.loom.yaml"))
	if err != nil {
		return err
	}
	paths := append(sdlPaths, yamlPaths...)
	if len(paths) == 0 {
		return fmt.Errorf("no *.loom or *.loom.yaml files in %s", schemaDir)
	}
	var schemas []*schema.Schema
	for _, p := range paths {
		var s *schema.Schema
		if strings.HasSuffix(p, ".loom") {
			src, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			s, err = sdl.Parse(string(src))
			if err != nil {
				return fmt.Errorf("%s: %w", p, err)
			}
		} else {
			var err error
			s, err = schema.Load(p)
			if err != nil {
				return err
			}
		}
		schemas = append(schemas, s)
	}
	sort.Slice(schemas, func(i, j int) bool { return schemas[i].Service < schemas[j].Service })

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}

	m := link(schemas)
	files := map[string]string{
		"README.md":   renderIndex(m),
		"topology.md": renderTopology(m),
		"catalog.md":  renderCatalog(m),
		"drift.md":    renderDrift(m),
	}
	for _, s := range schemas {
		files[s.Service+".md"] = renderService(m, s)
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(outDir, name), []byte(content), 0o644); err != nil {
			return err
		}
	}
	fmt.Printf("docs: %d services -> %s (%d files)\n", len(schemas), outDir, len(files))
	return nil
}
