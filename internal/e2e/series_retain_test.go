package e2e_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/orders"
	ordersgen "github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

// TestSeriesRetain proves @retain on plain Postgres: Migrate creates the
// series as a range-partitioned parent with day partitions ahead, an
// append into a day whose partition is missing repairs itself,
// maintenance drops partitions past retention and leaves the rest, and
// bucket queries return percentiles — per time bucket and as one "all"
// bucket per dim, the summary-table shape.
func TestSeriesRetain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
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

	const table = "loom_s_orders_req_sample"
	var kind string
	if err := pool.QueryRow(ctx, `SELECT relkind FROM pg_class WHERE relname = $1`, table).Scan(&kind); err != nil {
		t.Fatal(err)
	}
	if kind != "p" {
		t.Fatalf("relkind = %q, want p (partitioned parent)", kind)
	}
	partitions := func() []string {
		rows, err := pool.Query(ctx, `SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = $1::regclass ORDER BY 1`, table)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var n string
			_ = rows.Scan(&n)
			out = append(out, n)
		}
		return out
	}
	if got := partitions(); len(got) != 9 { // yesterday .. +7 days
		t.Fatalf("partitions after migrate = %d %v, want 9", len(got), got)
	}

	// Appends within the window, across two routes, with a known spread.
	now := time.Now().UTC()
	var rows []loom.SeriesRow
	for i := 1; i <= 100; i++ {
		rows = append(rows, &ordersgen.ReqSample{SampleId: uuid.New(), Route: "/jobs", At: now.Add(-time.Duration(i) * time.Minute), DurationMs: float64(i), Status: 200})
	}
	rows = append(rows, &ordersgen.ReqSample{SampleId: uuid.New(), Route: "/home", At: now.Add(-time.Minute), DurationMs: 5000, Status: 500})
	if n, err := cli.AppendSeries(ctx, "perf", rows...); err != nil || n != 101 {
		t.Fatalf("append = %d, %v", n, err)
	}

	// Days with no partition yet (a backfill 20 days back, a clock 21
	// days ahead): the append creates exactly those days and retries.
	// A row older than retention has nowhere to go and fails.
	backfill := now.AddDate(0, 0, -20)
	future := now.AddDate(0, 0, 21)
	if n, err := cli.AppendSeries(ctx, "perf",
		&ordersgen.ReqSample{SampleId: uuid.New(), Route: "/x", At: backfill, DurationMs: 1},
		&ordersgen.ReqSample{SampleId: uuid.New(), Route: "/x", At: future, DurationMs: 1}); err != nil || n != 2 {
		t.Fatalf("append into missing partitions did not self-repair: n=%d %v", n, err)
	}
	if got := partitions(); len(got) != 11 {
		t.Fatalf("partitions after repair = %d %v, want 11", len(got), got)
	}
	if _, err := cli.AppendSeries(ctx, "perf", &ordersgen.ReqSample{SampleId: uuid.New(), Route: "/x", At: now.AddDate(0, 0, -40), DurationMs: 1}); err == nil {
		t.Fatal("a row older than retention was accepted")
	}
	old := now.AddDate(0, 0, -45).Truncate(24 * time.Hour)
	oldName := fmt.Sprintf("%s_p%s", table, old.Format("20060102"))
	if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')`,
		oldName, table, old.Format("2006-01-02"), old.AddDate(0, 0, 1).Format("2006-01-02"))); err != nil {
		t.Fatal(err)
	}
	if err := cli.MaintainSeries(ctx); err != nil {
		t.Fatal(err)
	}
	for _, p := range partitions() {
		if p == oldName {
			t.Fatalf("expired partition %s survived maintenance", oldName)
		}
	}
	if got := partitions(); len(got) < 9 {
		t.Fatalf("maintenance dropped live partitions: %v", got)
	}

	// Percentiles per hour bucket and per route over the whole window.
	buckets, err := cli.QuerySeriesBuckets(ctx, "ReqSample", loom.SeriesBucketQuery{
		Namespace: "perf", Value: "duration_ms", Bucket: "all", By: []string{"route"},
		Since: now.Add(-2 * time.Hour), Percentiles: []float64{50, 95},
	})
	if err != nil {
		t.Fatal(err)
	}
	byRoute := map[string]loom.SeriesBucket{}
	for _, b := range buckets {
		byRoute[b.Group["route"]] = b
	}
	jobs := byRoute["/jobs"]
	if jobs.Count != 100 || jobs.Percentiles["p50"] != 50.5 || jobs.Percentiles["p95"] < 95 || jobs.Percentiles["p95"] > 96 {
		t.Fatalf("/jobs bucket = %+v", jobs)
	}
	if home := byRoute["/home"]; home.Count != 1 || home.Percentiles["p50"] != 5000 {
		t.Fatalf("/home bucket = %+v", home)
	}
	hourly, err := cli.QuerySeriesBuckets(ctx, "ReqSample", loom.SeriesBucketQuery{
		Namespace: "perf", Value: "duration_ms", Bucket: "hour", Since: now.Add(-2 * time.Hour), Percentiles: []float64{99.9},
	})
	if err != nil || len(hourly) == 0 || hourly[len(hourly)-1].Percentiles["p99.9"] == 0 {
		t.Fatalf("hourly = %+v, %v", hourly, err)
	}
	if _, err := cli.QuerySeriesBuckets(ctx, "ReqSample", loom.SeriesBucketQuery{Namespace: "perf", Value: "duration_ms", Bucket: "hour", Percentiles: []float64{100}}); err == nil {
		t.Fatal("percentile 100 accepted")
	}
	// A non-retained series is untouched by maintenance and stays plain.
	if err := pool.QueryRow(ctx, `SELECT relkind FROM pg_class WHERE relname = 'loom_s_orders_sku_price'`).Scan(&kind); err != nil || kind != "r" {
		t.Fatalf("SkuPrice relkind = %q, %v", kind, err)
	}
}
