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

	"github.com/go-apis/loom"
	loomgql "github.com/go-apis/loom/graphql"
	"github.com/go-apis/loom/internal/e2e/orders"
	ordersgen "github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

// TestSeries proves the series storage path end to end on plain
// Postgres: Migrate creates the typed table with a BRIN time index,
// AppendSeries dedups on identity so re-appending converges, range and
// bucket queries compile to real columns, the HTTP surface serves
// append/list/buckets, the gateway serves the generated queries and
// mutation, and the column diff is additive-only with drift reported
// for hand migration (series data has no rebuild source). The
// hypertable path runs in TestSeriesHypertable.
func TestSeries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	// force the plain-Postgres path even when the server ships timescale
	if _, err := pool.Exec(ctx, `DROP EXTENSION IF EXISTS timescaledb CASCADE`); err != nil {
		t.Fatal(err)
	}
	cli, err := loom.New(loom.Config{DB: pool, Registry: orders.NewRegistry(), Blobs: loom.NewDirBlobStore(t.TempDir(), "http://blobs.local")})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	var brin int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_indexes
		WHERE tablename='loom_s_orders_sku_price' AND indexdef ILIKE '%USING brin%'`).Scan(&brin); err != nil {
		t.Fatal(err)
	}
	if brin != 1 {
		t.Fatalf("want 1 BRIN time index on the series table, got %d", brin)
	}

	base := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	rows := []loom.SeriesRow{
		&ordersgen.SkuPrice{Sku: "widget", Source: "ebay", ObservedAt: base, PriceCents: 500},
		&ordersgen.SkuPrice{Sku: "widget", Source: "shop", ObservedAt: base.Add(2 * time.Hour), PriceCents: 650, Note: "listed"},
		&ordersgen.SkuPrice{Sku: "widget", Source: "ebay", ObservedAt: base.AddDate(0, 0, 1), PriceCents: 700},
		&ordersgen.SkuPrice{Sku: "gadget", Source: "ebay", ObservedAt: base, PriceCents: 900},
	}
	inserted, err := cli.AppendSeries(ctx, "default", rows...)
	if err != nil {
		t.Fatal(err)
	}
	if inserted != 4 {
		t.Fatalf("want 4 inserted, got %d", inserted)
	}
	// idempotency: the same batch again inserts nothing
	inserted, err = cli.AppendSeries(ctx, "default", rows...)
	if err != nil {
		t.Fatal(err)
	}
	if inserted != 0 {
		t.Fatalf("re-append inserted %d rows — identity dedup broken", inserted)
	}
	// another namespace is a different world
	if _, err := cli.AppendSeries(ctx, "other", &ordersgen.SkuPrice{Sku: "widget", Source: "ebay", ObservedAt: base, PriceCents: 111}); err != nil {
		t.Fatal(err)
	}

	// range query: dim filter, namespace scoping, order, since
	points, err := cli.QuerySeries(ctx, "SkuPrice", loom.SeriesQuery{
		Namespace: "default",
		Filters:   []loom.Filter{{Field: "sku", Value: "widget"}},
		Ascending: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 3 {
		t.Fatalf("want 3 widget points, got %d", len(points))
	}
	var first struct {
		PriceCents int64  `json:"price_cents"`
		Note       string `json:"note"`
	}
	if err := json.Unmarshal(points[0].Data, &first); err != nil {
		t.Fatal(err)
	}
	if first.PriceCents != 500 {
		t.Fatalf("ascending order broken: first widget point is %+v", first)
	}
	points, err = cli.QuerySeries(ctx, "SkuPrice", loom.SeriesQuery{
		Namespace: "default",
		Since:     base.AddDate(0, 0, 1),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 1 {
		t.Fatalf("since filter: want 1 point, got %d", len(points))
	}
	if _, err := cli.QuerySeries(ctx, "SkuPrice", loom.SeriesQuery{Namespace: "default", Filters: []loom.Filter{{Field: "nope", Value: "x"}}}); err == nil {
		t.Fatal("unknown filter field must error")
	}

	// buckets: per-day, grouped by sku; day one holds widget 500 and 650
	buckets, err := cli.QuerySeriesBuckets(ctx, "SkuPrice", loom.SeriesBucketQuery{
		Namespace: "default",
		Value:     "price_cents",
		Bucket:    "day",
		By:        []string{"sku"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(buckets) != 3 { // day1 gadget, day1 widget, day2 widget
		t.Fatalf("want 3 buckets, got %d: %+v", len(buckets), buckets)
	}
	var d1widget *loom.SeriesBucket
	for i := range buckets {
		if buckets[i].Group["sku"] == "widget" && buckets[i].T.Day() == 1 {
			d1widget = &buckets[i]
		}
	}
	if d1widget == nil || d1widget.Count != 2 ||
		*d1widget.Avg != 575 || *d1widget.Min != 500 || *d1widget.Max != 650 || *d1widget.Last != 650 {
		t.Fatalf("day-1 widget bucket wrong: %+v", d1widget)
	}
	if _, err := cli.QuerySeriesBuckets(ctx, "SkuPrice", loom.SeriesBucketQuery{
		Namespace: "default", Value: "note", Bucket: "day",
	}); err == nil {
		t.Fatal("non-numeric value column must error")
	}

	// the HTTP surface: append, list, buckets
	srv := httptest.NewServer(cli.HTTPHandler())
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/series/SkuPrice", "application/json", strings.NewReader(`{
		"namespace": "default",
		"rows": [{"sku": "widget", "source": "shop", "observed_at": "2026-08-03T09:00:00Z", "price_cents": 640}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	var appendOut struct {
		Inserted int64 `json:"inserted"`
		Total    int64 `json:"total"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&appendOut); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || appendOut.Inserted != 1 || appendOut.Total != 1 {
		t.Fatalf("POST /series: %d %+v", resp.StatusCode, appendOut)
	}
	resp, err = http.Get(srv.URL + "/series/SkuPrice?namespace=default&sku=widget&price_cents.gte=600&order=asc")
	if err != nil {
		t.Fatal(err)
	}
	var listOut struct {
		Items []loom.SeriesPoint `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&listOut); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(listOut.Items) != 3 { // 650, 700, 640
		t.Fatalf("GET /series: want 3 items, got %+v", listOut)
	}
	resp, err = http.Get(srv.URL + "/series/SkuPrice/buckets?namespace=default&bucket=day&value=price_cents&by=sku,source")
	if err != nil {
		t.Fatal(err)
	}
	var bucketOut struct {
		Items []loom.SeriesBucket `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&bucketOut); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(bucketOut.Items) == 0 || bucketOut.Items[0].Group["source"] == "" {
		t.Fatalf("GET /series/buckets: %+v", bucketOut)
	}

	// the gateway: generated list + bucket queries and the append mutation
	gateway, err := loomgql.New(loomgql.Config{Services: []*loom.Client{cli}})
	if err != nil {
		t.Fatal(err)
	}
	gwsrv := httptest.NewServer(gateway)
	defer gwsrv.Close()
	gql := func(query string, vars map[string]any) map[string]any {
		t.Helper()
		body, _ := json.Marshal(map[string]any{"query": query, "variables": vars})
		resp, err := http.Post(gwsrv.URL, "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out struct {
			Data   map[string]any `json:"data"`
			Errors []struct {
				Message string `json:"message"`
			} `json:"errors"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		if len(out.Errors) > 0 {
			t.Fatalf("graphql errors: %+v", out.Errors)
		}
		return out.Data
	}

	data := gql(`query { skuPrices(namespace: "default", where: [{field: "sku", op: EQ, value: "widget"}], order: "asc") { sku source priceCents observedAt namespace } }`, nil)
	prices := data["skuPrices"].([]any)
	if len(prices) != 4 {
		t.Fatalf("skuPrices: want 4 rows, got %+v", prices)
	}
	if row := prices[0].(map[string]any); row["priceCents"] != float64(500) || row["namespace"] != "default" {
		t.Fatalf("skuPrices first row: %+v", row)
	}
	data = gql(`query { skuPriceBuckets(namespace: "default", value: "price_cents", bucket: DAY, by: ["sku"]) { t group count avg min max last } }`, nil)
	gwBuckets := data["skuPriceBuckets"].([]any)
	if len(gwBuckets) != 4 { // day1 gadget, day1 widget, day2 widget, day3 widget
		t.Fatalf("skuPriceBuckets: want 4, got %+v", gwBuckets)
	}
	data = gql(`mutation($rows: [SkuPriceInput!]!) { appendSkuPrices(namespace: "default", rows: $rows) { inserted total } }`, map[string]any{
		"rows": []any{
			map[string]any{"sku": "gadget", "source": "shop", "observedAt": "2026-08-02T10:00:00Z", "priceCents": 950},
			// a duplicate of an existing row: dedup counts it out
			map[string]any{"sku": "gadget", "source": "ebay", "observedAt": base.Format(time.RFC3339), "priceCents": 900},
		},
	})
	appendRes := data["appendSkuPrices"].(map[string]any)
	if appendRes["inserted"] != float64(1) || appendRes["total"] != float64(2) {
		t.Fatalf("appendSkuPrices: %+v", appendRes)
	}

	// additive migration: a dropped value column comes back; type drift
	// is a loud error naming the hand-migration remediation
	if _, err := pool.Exec(ctx, `ALTER TABLE loom_s_orders_sku_price DROP COLUMN "note"`); err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	var noteType string
	if err := pool.QueryRow(ctx, `SELECT data_type FROM information_schema.columns
		WHERE table_name='loom_s_orders_sku_price' AND column_name='note'`).Scan(&noteType); err != nil {
		t.Fatalf("note column not re-added: %v", err)
	}
	if _, err := pool.Exec(ctx, `ALTER TABLE loom_s_orders_sku_price DROP COLUMN "note"`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `ALTER TABLE loom_s_orders_sku_price ADD COLUMN "note" bigint`); err != nil {
		t.Fatal(err)
	}
	err = cli.Migrate(ctx)
	if err == nil || !strings.Contains(err.Error(), "NOT rebuildable") {
		t.Fatalf("series drift must error with the hand-migration message, got %v", err)
	}
}

// TestSeriesHypertable proves the engine adaptation: with the
// timescaledb extension installed, Migrate promotes the series table to
// a hypertable and everything else behaves identically. Skips when the
// server doesn't ship the extension.
func TestSeriesHypertable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	if _, err := pool.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS timescaledb`); err != nil {
		t.Skipf("timescaledb unavailable: %v", err)
	}
	cli, err := loom.New(loom.Config{DB: pool, Registry: orders.NewRegistry(), Blobs: loom.NewDirBlobStore(t.TempDir(), "http://blobs.local")})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	var hypertables int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM timescaledb_information.hypertables
		WHERE hypertable_name = 'loom_s_orders_sku_price'`).Scan(&hypertables); err != nil {
		t.Fatal(err)
	}
	if hypertables != 1 {
		t.Fatalf("series table is not a hypertable")
	}
	// Migrate is idempotent on an existing hypertable
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	base := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	inserted, err := cli.AppendSeries(ctx, "default",
		&ordersgen.SkuPrice{Sku: "widget", Source: "ebay", ObservedAt: base, PriceCents: 500},
		&ordersgen.SkuPrice{Sku: "widget", Source: "ebay", ObservedAt: base.AddDate(0, 1, 0), PriceCents: 800})
	if err != nil {
		t.Fatal(err)
	}
	if inserted != 2 {
		t.Fatalf("want 2 inserted, got %d", inserted)
	}
	if inserted, err = cli.AppendSeries(ctx, "default",
		&ordersgen.SkuPrice{Sku: "widget", Source: "ebay", ObservedAt: base, PriceCents: 500}); err != nil || inserted != 0 {
		t.Fatalf("hypertable dedup: inserted %d, err %v", inserted, err)
	}
	buckets, err := cli.QuerySeriesBuckets(ctx, "SkuPrice", loom.SeriesBucketQuery{
		Namespace: "default", Value: "price_cents", Bucket: "month",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(buckets) != 2 || buckets[0].Count != 1 {
		t.Fatalf("monthly buckets on hypertable: %+v", buckets)
	}
}
