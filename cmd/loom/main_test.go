package main

import (
	"os"
	"path/filepath"
	"strings"
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

// The README's example layout, exactly: billing/invoice.loom sorts before
// orders.loom, so it is the first file and carries the header like the rest.
func TestReadmeLayoutChecks(t *testing.T) {
	files := map[string]string{
		"billing/invoice.loom": `service orders

aggregate Invoice {
  state {
    status: InvoiceStatus
  }
  command IssueInvoice {
    order_id: string!
  } -> InvoiceIssued
}
`,
		"orders.loom": `service orders

enum InvoiceStatus { draft issued }

event InvoiceIssued {
  order_id: string!
}

event ShipmentSent {
  order_id: string!
}
`,
		"shipping/shipment.loom": `service orders

aggregate Shipment {
  state {
    order_id: string
  }
  command SendShipment {
    order_id: string!
  } -> ShipmentSent
}
`,
	}
	dir := t.TempDir()
	write := func(rel, src string) {
		p := filepath.Join(dir, "schema", filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for rel, src := range files {
		write(rel, src)
	}
	s, err := loadSchemas(dir, "schema/")
	if err != nil {
		t.Fatal(err)
	}
	if s.Service != "orders" || len(s.Aggregates) != 2 || len(s.Events) != 2 || len(s.Enums) != 1 {
		t.Fatalf("service %q, %d aggregates, %d events, %d enums", s.Service, len(s.Aggregates), len(s.Events), len(s.Enums))
	}
	if err := runCheck([]string{filepath.Join(dir, "schema")}); err != nil {
		t.Fatalf("check dir: %v", err)
	}

	// Why the README says to repeat the header: without it the first file
	// by path is headerless and the schema is refused.
	write("billing/invoice.loom", strings.TrimPrefix(files["billing/invoice.loom"], "service orders\n"))
	_, err = loadSchemas(dir, "schema/")
	if err == nil || !strings.Contains(err.Error(), filepath.Join("schema", "billing", "invoice.loom")+":") {
		t.Fatalf("want a refusal naming billing/invoice.loom, got %v", err)
	}
}
