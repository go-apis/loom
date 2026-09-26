package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadSchemasDirFileGlob(t *testing.T) {
	dir := t.TempDir()
	write := func(rel, src string) {
		p := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// the aggregate in orders/order.loom emits the event from events.loom
	write("schema/events.loom", "service orders\n\nevent OrderPlaced {\n  status: string!\n}\n")
	write("schema/orders/order.loom", `aggregate Order {
  state {
    status: string
  }
  command PlaceOrder {
    status: string!
  } -> OrderPlaced
}
`)
	for _, spec := range []string{"schema", "schema/", "schema/*.loom"} {
		s, err := loadSchemas(dir, spec)
		if spec == "schema/*.loom" {
			// the glob misses orders/, so the event has no producer — but it parses
			if err != nil || len(s.Aggregates) != 0 || len(s.Events) != 1 {
				t.Fatalf("%s: %v", spec, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: %v", spec, err)
		}
		if s.Service != "orders" || len(s.Aggregates) != 1 || len(s.Events) != 1 {
			t.Fatalf("%s: service %q, %d aggregates, %d events", spec, s.Service, len(s.Aggregates), len(s.Events))
		}
	}
	if _, err := loadSchemas(dir, "schema/events.loom"); err != nil {
		t.Fatalf("single file: %v", err)
	}
	if err := runCheck([]string{filepath.Join(dir, "schema")}); err != nil {
		t.Fatalf("check dir: %v", err)
	}
}
