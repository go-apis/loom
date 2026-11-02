package e2e_test

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/orders"
	ordersgen "github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

// TestSeriesCompressAndRollup proves ADR 0011 on TimescaleDB: Migrate
// enables compression with its policy and keeps one real-time continuous
// aggregate per @rollup unit; every bucket query a rollup can answer
// gives exactly what the raw table gives, before and after the rollup is
// materialized, and a rollup is what answers (a raw delete the rollup has
// not re-materialized shows through it); appends into a compressed chunk
// still converge; Migrate is idempotent, replaces a rollup whose
// definition changed, and moves a changed compression age. Skips when
// the server doesn't ship timescaledb.
func TestSeriesCompressAndRollup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	if _, err := pool.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS timescaledb`); err != nil {
		t.Skipf("timescaledb unavailable: %v", err)
	}
	var tz string
	if err := pool.QueryRow(ctx, `SELECT current_setting('TimeZone')`).Scan(&tz); err != nil {
		t.Fatal(err)
	}
	if tz != "UTC" && tz != "Etc/UTC" {
		t.Skipf("session time zone is %s; rollups answer only on UTC", tz)
	}
	newClient := func() *loom.Client {
		cli, err := loom.New(loom.Config{DB: pool, Registry: orders.NewRegistry(), Blobs: loom.NewDirBlobStore(t.TempDir(), "http://blobs.local")})
		if err != nil {
			t.Fatal(err)
		}
		return cli
	}
	cli := newClient()
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	const table = "loom_s_orders_sku_quote"
	hourView, dayView := "loom_r_orders_sku_quote_hour", "loom_r_orders_sku_quote_day"

	// compression on, with its policy
	var compressed bool
	if err := pool.QueryRow(ctx, `SELECT compression_enabled FROM timescaledb_information.hypertables
		WHERE hypertable_name = $1`, table).Scan(&compressed); err != nil || !compressed {
		t.Fatalf("compression enabled = %v, %v", compressed, err)
	}
	compressAfter := func() string {
		var after string
		if err := pool.QueryRow(ctx, `SELECT config->>'compress_after' FROM timescaledb_information.jobs
			WHERE proc_name = 'policy_compression' AND hypertable_name = $1`, table).Scan(&after); err != nil {
			t.Fatal(err)
		}
		return after
	}
	if got := compressAfter(); got != "7 days" {
		t.Fatalf("compress_after = %q, want 7 days", got)
	}
	// the series without @compress or @rollup gets neither
	var plainCompressed bool
	if err := pool.QueryRow(ctx, `SELECT compression_enabled FROM timescaledb_information.hypertables
		WHERE hypertable_name = 'loom_s_orders_sku_price'`).Scan(&plainCompressed); err != nil || plainCompressed {
		t.Fatalf("SkuPrice compression = %v, %v; want off", plainCompressed, err)
	}

	viewOID := func(view string) uint32 {
		var oid *uint32
		if err := pool.QueryRow(ctx, `SELECT to_regclass($1)::oid`, view).Scan(&oid); err != nil {
			t.Fatal(err)
		}
		if oid == nil {
			return 0
		}
		return *oid
	}
	for _, v := range []string{hourView, dayView} {
		if viewOID(v) == 0 {
			t.Fatalf("rollup %s missing", v)
		}
		var policies int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM timescaledb_information.jobs
			WHERE proc_name = 'policy_refresh_continuous_aggregate' AND hypertable_name IN ($1, (
			  SELECT materialization_hypertable_name FROM timescaledb_information.continuous_aggregates WHERE view_name = $1))`, v).Scan(&policies); err != nil || policies != 1 {
			t.Fatalf("rollup %s refresh policies = %d, %v", v, policies, err)
		}
	}

	// observations across days, hours, skus and sources
	base := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC) // a Monday
	var rows []loom.SeriesRow
	for d := 0; d < 40; d++ {
		for h := 0; h < 24; h += 5 {
			at := base.AddDate(0, 0, d).Add(time.Duration(h)*time.Hour + 7*time.Minute)
			for i, sku := range []string{"widget", "gadget"} {
				for j, src := range []string{"ebay", "shop"} {
					q := float64((d*7+h*3+i*11+j*5)%17) + 0.25
					rows = append(rows, &ordersgen.SkuQuote{
						Sku: sku, Source: src, ObservedAt: at,
						PriceCents: int64(100 + (d*13+h*7+i*29+j*3)%250),
						Qty:        q,
					})
				}
			}
		}
	}
	inserted, err := cli.AppendSeries(ctx, "default", rows...)
	if err != nil {
		t.Fatal(err)
	}
	if inserted != int64(len(rows)) {
		t.Fatalf("inserted %d of %d", inserted, len(rows))
	}

	until := base.AddDate(0, 0, 40)
	queries := []loom.SeriesBucketQuery{}
	for _, bucket := range []string{"hour", "day", "week", "month", "all"} {
		for _, by := range [][]string{nil, {"sku"}, {"sku", "source"}} {
			for _, value := range []string{"price_cents", "qty"} {
				queries = append(queries, loom.SeriesBucketQuery{Namespace: "default", Value: value, Bucket: bucket, By: by, Limit: 10000})
			}
		}
		queries = append(queries,
			loom.SeriesBucketQuery{Namespace: "default", Value: "price_cents", Bucket: bucket, By: []string{"source"},
				Filters: []loom.Filter{{Field: "sku", Value: "widget"}}, Limit: 10000},
			loom.SeriesBucketQuery{Namespace: "default", Value: "qty", Bucket: bucket,
				Since: base.AddDate(0, 0, 3), Until: until.AddDate(0, 0, -2), Limit: 10000},
		)
	}
	same := func(stage string) {
		t.Helper()
		for _, q := range queries {
			got, err := cli.QuerySeriesBuckets(ctx, "SkuQuote", q)
			if err != nil {
				t.Fatalf("%s %+v: %v", stage, q, err)
			}
			raw := q
			raw.Raw = true
			want, err := cli.QuerySeriesBuckets(ctx, "SkuQuote", raw)
			if err != nil {
				t.Fatalf("%s raw %+v: %v", stage, q, err)
			}
			if msg := bucketsDiffer(got, want); msg != "" {
				t.Fatalf("%s: %s/%s by %v filters %v: rollup differs from raw: %s", stage, q.Bucket, q.Value, q.By, q.Filters, msg)
			}
		}
	}
	// refresh materializes both rollups by hand, waiting out the refresh
	// policy's own background run when the two collide.
	refresh := func() {
		t.Helper()
		for _, v := range []string{hourView, dayView} {
			for try := 0; ; try++ {
				_, err := pool.Exec(ctx, fmt.Sprintf(`CALL refresh_continuous_aggregate('%s', NULL, NULL)`, v))
				if err == nil {
					break
				}
				if try < 50 && strings.Contains(err.Error(), "55P03") {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				t.Fatal(err)
			}
		}
	}
	same("real-time only")
	refresh()
	same("materialized")

	// A rollup is what answers: a raw delete the rollups have not
	// re-materialized still shows through them, and not through Raw.
	dayQ := loom.SeriesBucketQuery{Namespace: "default", Value: "price_cents", Bucket: "month", Limit: 100}
	if _, err := pool.Exec(ctx, `DELETE FROM `+table+` WHERE sku = 'widget' AND source = 'ebay' AND observed_at = $1`,
		base.Add(7*time.Minute)); err != nil {
		t.Fatal(err)
	}
	viaRollup, err := cli.QuerySeriesBuckets(ctx, "SkuQuote", dayQ)
	if err != nil {
		t.Fatal(err)
	}
	dayQ.Raw = true
	viaRaw, err := cli.QuerySeriesBuckets(ctx, "SkuQuote", dayQ)
	if err != nil {
		t.Fatal(err)
	}
	if viaRollup[0].Count != viaRaw[0].Count+1 {
		t.Fatalf("after an unmaterialized delete: rollup count %d, raw %d — the rollup did not answer", viaRollup[0].Count, viaRaw[0].Count)
	}
	// percentiles always read raw
	pq := loom.SeriesBucketQuery{Namespace: "default", Value: "price_cents", Bucket: "month", Percentiles: []float64{50}, Limit: 100}
	if got, err := cli.QuerySeriesBuckets(ctx, "SkuQuote", pq); err != nil || got[0].Count != viaRaw[0].Count {
		t.Fatalf("percentile query: %+v, %v; want the raw count %d", got, err, viaRaw[0].Count)
	}
	refresh()
	same("after re-materializing the delete")

	// compress every chunk; appends into compressed chunks still converge
	if _, err := pool.Exec(ctx, `SELECT compress_chunk(c, if_not_compressed => TRUE) FROM show_chunks($1::regclass) c`, table); err != nil {
		t.Fatal(err)
	}
	if n, err := cli.AppendSeries(ctx, "default", rows[1:50]...); err != nil || n != 0 {
		t.Fatalf("re-append into compressed chunks: inserted %d, %v; want 0", n, err)
	}
	if n, err := cli.AppendSeries(ctx, "default", rows[0]); err != nil || n != 1 {
		t.Fatalf("append the deleted row into a compressed chunk: inserted %d, %v; want 1", n, err)
	}
	refresh()
	same("compressed")
	pts, err := cli.QuerySeries(ctx, "SkuQuote", loom.SeriesQuery{Namespace: "default",
		Filters: []loom.Filter{{Field: "sku", Value: "gadget"}, {Field: "source", Value: "shop"}}, Limit: 10000})
	if err != nil || len(pts) != len(rows)/4 {
		t.Fatalf("raw read from compressed chunks: %d points, %v; want %d", len(pts), err, len(rows)/4)
	}

	// Migrate again: nothing replaced
	hourOID := viewOID(hourView)
	if err := newClient().Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if viewOID(hourView) != hourOID {
		t.Fatalf("an unchanged rollup was replaced")
	}
	// a rollup whose definition changed is replaced; a moved compression
	// age is moved back to the declared one
	if _, err := pool.Exec(ctx, `COMMENT ON VIEW `+hourView+` IS 'loom:stale'`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `SELECT alter_job(job_id, config => config || '{"compress_after": "30 days"}')
		FROM timescaledb_information.jobs WHERE proc_name = 'policy_compression' AND hypertable_name = $1`, table); err != nil {
		t.Fatal(err)
	}
	cli = newClient()
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if o := viewOID(hourView); o == 0 || o == hourOID {
		t.Fatalf("a changed rollup was not replaced (oid %d, was %d)", o, hourOID)
	}
	if got := compressAfter(); got != "7 days" {
		t.Fatalf("compress_after after Migrate = %q, want 7 days", got)
	}
	same("after replacing the hour rollup")
}

// bucketsDiffer compares two bucket answers field by field, floats to a
// relative 1e-9 (a rollup's avg is sum/count over parts).
func bucketsDiffer(got, want []loom.SeriesBucket) string {
	if len(got) != len(want) {
		return fmt.Sprintf("%d buckets, want %d", len(got), len(want))
	}
	near := func(a, b *float64) bool {
		if a == nil || b == nil {
			return a == nil && b == nil
		}
		return math.Abs(*a-*b) <= 1e-9*math.Max(1, math.Abs(*b))
	}
	for i := range got {
		g, w := got[i], want[i]
		if !g.T.Equal(w.T) || g.Count != w.Count || fmt.Sprint(g.Group) != fmt.Sprint(w.Group) ||
			!near(g.Avg, w.Avg) || !near(g.Min, w.Min) || !near(g.Max, w.Max) || !near(g.Last, w.Last) {
			return fmt.Sprintf("bucket %d: got %s, want %s", i, fmtBucket(g), fmtBucket(w))
		}
	}
	return ""
}

func fmtBucket(b loom.SeriesBucket) string {
	p := func(f *float64) string {
		if f == nil {
			return "nil"
		}
		return fmt.Sprint(*f)
	}
	return fmt.Sprintf("{t %s group %v count %d avg %s min %s max %s last %s}",
		b.T.UTC().Format(time.RFC3339), b.Group, b.Count, p(b.Avg), p(b.Min), p(b.Max), p(b.Last))
}
