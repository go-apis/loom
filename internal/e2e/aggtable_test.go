package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	loomgql "github.com/go-apis/loom/graphql"
	"github.com/go-apis/loom/internal/e2e/billing"
	billinggen "github.com/go-apis/loom/internal/e2e/billing/loomgen"
)

// TestAggregateTable proves @table on an aggregate end to end: the state
// mirrors into loom_t_billing_payee in the same transaction as the
// command (no projection, no lag), @pii state fields never become
// columns, list queries and the gateway serve the mirror, and a freshly
// created table backfills from the event log on Migrate.
func TestAggregateTable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Registry: billing.NewRegistry(), Keys: testKeys(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cli.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	payee := uuid.New()
	if err := cli.Dispatch(ctx, &billinggen.RegisterPayee{
		CommandBase: loom.CommandBase{AggregateID: payee, Namespace: "default"},
		Name:        "Ada Lovelace", Tin: "123456789", TinLast4: "6789",
	}); err != nil {
		t.Fatal(err)
	}

	// the mirror row is visible the moment Dispatch returns — written in
	// the command transaction, not by a projection catching up
	rows, err := cli.QueryEntities(ctx, "Payee", loom.Query{
		Namespace: "default",
		Filters:   []loom.Filter{{Field: "tin_last4", Value: "6789"}},
	})
	if err != nil || len(rows) != 1 {
		t.Fatalf("payee mirror immediately after dispatch: %d rows (%v)", len(rows), err)
	}
	var doc map[string]any
	if err := json.Unmarshal(rows[0].Data, &doc); err != nil {
		t.Fatal(err)
	}
	if doc["name"] != "Ada Lovelace" {
		t.Fatalf("mirror row: %v", doc)
	}
	if _, present := doc["tin"]; present {
		t.Fatalf("@pii tin leaked into the mirror row: %v", doc)
	}

	// @pii never becomes a column
	var cols int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM information_schema.columns
		WHERE table_name = 'loom_t_billing_payee' AND column_name = 'tin'`).Scan(&cols); err != nil {
		t.Fatal(err)
	}
	if cols != 0 {
		t.Fatal("tin column exists on the mirror table")
	}

	// re-registering updates the same row, still synchronously
	if err := cli.Dispatch(ctx, &billinggen.RegisterPayee{
		CommandBase: loom.CommandBase{AggregateID: payee, Namespace: "default"},
		Name:        "Ada King", Tin: "123456789", TinLast4: "6789",
	}); err != nil {
		t.Fatal(err)
	}
	var name string
	if err := pool.QueryRow(ctx, `
		SELECT "name" FROM loom_t_billing_payee
		WHERE service='billing' AND namespace='default' AND id=$1`, payee).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "Ada King" {
		t.Fatalf("mirror after update: %q", name)
	}

	// the gateway serves the mirror as an entity-style list; the @pii
	// field has no column and resolves null
	gateway, err := loomgql.New(loomgql.Config{Services: []*loom.Client{cli}})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(gateway)
	defer srv.Close()
	body, _ := json.Marshal(map[string]any{
		"query": `query($ns: Namespace!) { payees(namespace: $ns) { id name tinLast4 tin } }`,
		"variables": map[string]any{"ns": "default"},
	})
	resp, err := http.Post(srv.URL, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Data struct {
			Payees []map[string]any `json:"payees"`
		} `json:"data"`
		Errors []struct{ Message string } `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if len(out.Errors) > 0 {
		t.Fatalf("gateway errors: %+v", out.Errors)
	}
	if len(out.Data.Payees) != 1 || out.Data.Payees[0]["name"] != "Ada King" || out.Data.Payees[0]["tinLast4"] != "6789" {
		t.Fatalf("gateway payees: %+v", out.Data.Payees)
	}
	if out.Data.Payees[0]["tin"] != nil {
		t.Fatalf("gateway served tin from the mirror: %+v", out.Data.Payees[0])
	}

	// a mirror created after the aggregate has history backfills from the
	// log: drop the table, Migrate recreates and replays every stream
	if _, err := pool.Exec(ctx, `DROP TABLE loom_t_billing_payee`); err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT "name" FROM loom_t_billing_payee
		WHERE service='billing' AND namespace='default' AND id=$1`, payee).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "Ada King" {
		t.Fatalf("mirror after backfill: %q", name)
	}

	// unknown entities still error (the tables map must not swallow the check)
	if _, err := cli.QueryEntities(ctx, "Nope", loom.Query{Namespace: "default"}); err == nil || !strings.Contains(err.Error(), "unknown entity") {
		t.Fatalf("unknown entity: %v", err)
	}
}
