package e2e_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/orders"
	ordersgen "github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

// TestConsole exercises the console page and its data endpoints: the
// registry document (including reaction dispatch contracts), runner lag
// after catch-up, timers, and the batches list.
func TestConsole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	old := orders.AutoCancelAfter
	orders.AutoCancelAfter = time.Hour
	t.Cleanup(func() { orders.AutoCancelAfter = old })

	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Registry: orders.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cli.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(cli.HTTPHandler())
	defer srv.Close()

	// the page itself is embedded and self-contained
	resp, err := http.Get(srv.URL + "/console")
	if err != nil {
		t.Fatal(err)
	}
	page, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(resp.Header.Get("Content-Type"), "text/html") {
		t.Fatalf("console page: %d %s", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	if !strings.Contains(string(page), "loom console") || strings.Contains(string(page), "src=\"http") {
		t.Fatalf("console page wrong or not self-contained")
	}

	// the registry document drives the Design tab — dispatch contracts included
	var reg struct {
		Service    string `json:"service"`
		Aggregates []struct {
			Name     string `json:"name"`
			Commands []struct {
				Name  string   `json:"name"`
				Emits []string `json:"emits"`
			} `json:"commands"`
		} `json:"aggregates"`
		Processes []struct {
			Name string `json:"name"`
			Subs []struct {
				Event      string   `json:"event"`
				Dispatches []string `json:"dispatches"`
			} `json:"subs"`
		} `json:"processes"`
	}
	getJSON(t, ctx, srv.URL+"/registry", &reg)
	if reg.Service != "orders" || len(reg.Aggregates) != 1 || reg.Aggregates[0].Name != "Order" {
		t.Fatalf("registry doc: %+v", reg)
	}
	if len(reg.Processes) != 1 || reg.Processes[0].Subs[0].Event != "InvoicePaid" ||
		reg.Processes[0].Subs[0].Dispatches[0] != "ShipOrder" {
		t.Fatalf("process topology: %+v", reg.Processes)
	}

	// produce some log, then runners catch up to the head
	err = cli.Dispatch(ctx, &ordersgen.PlaceOrder{
		CommandBase: loom.CommandBase{AggregateID: uuid.New(), Namespace: "default"},
		CustomerId:  uuid.New(), Currency: "USD",
		Items: []ordersgen.OrderItem{{Sku: "widget", Quantity: 1, PriceCents: 100}},
	})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "runners caught up", func() bool {
		var rs struct {
			Latest  int64 `json:"latest"`
			Runners []struct {
				Runner string `json:"runner"`
				Kind   string `json:"kind"`
				Lag    int64  `json:"lag"`
			} `json:"runners"`
		}
		getJSON(t, ctx, srv.URL+"/runners", &rs)
		if rs.Latest == 0 || len(rs.Runners) == 0 {
			return false
		}
		for _, r := range rs.Runners {
			if r.Kind != "process(bus)" && r.Lag != 0 {
				return false
			}
		}
		return true
	})

	// the auto-cancel timer shows on the schedule
	var timers struct {
		Items []struct {
			CommandType string `json:"command_type"`
			Overdue     bool   `json:"overdue"`
		} `json:"items"`
	}
	getJSON(t, ctx, srv.URL+"/timers", &timers)
	if len(timers.Items) != 1 || timers.Items[0].CommandType != "CancelOrder" || timers.Items[0].Overdue {
		t.Fatalf("timers: %+v", timers.Items)
	}

	// batches list endpoint (empty is fine, must be well-formed)
	var batches struct {
		Items []loom.Batch `json:"items"`
	}
	getJSON(t, ctx, srv.URL+"/batches", &batches)
	if batches.Items == nil {
		t.Fatalf("batches list missing items array")
	}
}
