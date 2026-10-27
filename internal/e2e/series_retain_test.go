package e2e_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

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

// TestSeriesRetainAsDMLRole proves @retain upkeep works when the serving
// process holds a role with DML rights only: Migrate runs as the schema
// owner and leaves the partition functions behind, and the DML role then
// keeps partitions ahead (by Config.SeriesPartitionAhead), repairs an
// append into a missing day and expires old days — none of which it could
// do with the DDL itself. The functions refuse a table that is not one of
// the service's @retain series and a day retention no longer keeps.
func TestSeriesRetainAsDMLRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	admin := testDB(t, ctx)
	if _, err := admin.Exec(ctx, `DROP EXTENSION IF EXISTS timescaledb CASCADE`); err != nil {
		t.Fatal(err)
	}
	suffix := uuid.NewString()[:8]
	owner, app := "loom_owner_"+suffix, "loom_app_"+suffix
	var dbName string
	if err := admin.QueryRow(ctx, `SELECT current_database()`).Scan(&dbName); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD 'pw'`, owner),
		fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD 'pw'`, app),
		fmt.Sprintf(`GRANT CONNECT ON DATABASE %s TO %s, %s`, dbName, owner, app),
		`REVOKE CREATE ON SCHEMA public FROM PUBLIC`,
		fmt.Sprintf(`GRANT USAGE, CREATE ON SCHEMA public TO %s`, owner),
		fmt.Sprintf(`GRANT USAGE ON SCHEMA public TO %s`, app),
		fmt.Sprintf(`ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %s`, owner, app),
		fmt.Sprintf(`ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO %s`, owner, app),
	} {
		if _, err := admin.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	t.Cleanup(func() {
		cleanCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		// The database drops after this (testDB's cleanup ran first-in,
		// so runs last); its objects go with it, then the roles.
		_, _ = admin.Exec(cleanCtx, fmt.Sprintf(`DROP OWNED BY %s, %s CASCADE`, owner, app))
	})
	poolAs := func(user string) *pgxpool.Pool {
		cfg := admin.Config().Copy()
		cfg.ConnConfig.User, cfg.ConnConfig.Password = user, "pw"
		p, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(p.Close)
		return p
	}
	ownerPool, appPool := poolAs(owner), poolAs(app)

	newCli := func(pool *pgxpool.Pool) *loom.Client {
		cli, err := loom.New(loom.Config{DB: pool, Registry: orders.NewRegistry(), SeriesPartitionAhead: 14,
			Blobs: loom.NewDirBlobStore(t.TempDir(), "http://blobs.local")})
		if err != nil {
			t.Fatal(err)
		}
		return cli
	}
	if err := newCli(ownerPool).Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	cli := newCli(appPool)

	const table = "loom_s_orders_req_sample"
	partitions := func() map[string]bool {
		rows, err := admin.Query(ctx, `SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = $1::regclass`, table)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		out := map[string]bool{}
		for rows.Next() {
			var n string
			_ = rows.Scan(&n)
			out[n] = true
		}
		return out
	}
	day := func(d int) string {
		return fmt.Sprintf("%s_p%s", table, time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, d).Format("20060102"))
	}

	// The DDL is out of the app role's reach — the reason the functions exist.
	if _, err := appPool.Exec(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM ('2001-01-01') TO ('2001-01-02')`, day(30), table)); err == nil {
		t.Fatal("the app role ran DDL; the test does not prove anything")
	}

	if err := cli.MaintainSeries(ctx); err != nil {
		t.Fatalf("maintenance as the DML role: %v", err)
	}
	got := partitions()
	for d := -1; d <= 14; d++ {
		if !got[day(d)] {
			t.Fatalf("missing %s after maintenance with ahead 14: %v", day(d), got)
		}
	}

	// An append into a day with no partition repairs itself as the app role.
	backfill := time.Now().UTC().AddDate(0, 0, -20)
	if n, err := cli.AppendSeries(ctx, "perf", &ordersgen.ReqSample{SampleId: uuid.New(), Route: "/x", At: backfill, DurationMs: 1}); err != nil || n != 1 {
		t.Fatalf("append into a missing day as the DML role: n=%d %v", n, err)
	}
	if !partitions()[day(-20)] {
		t.Fatal("the repaired partition is missing")
	}

	// Expiry: an old day (created by the owner) goes at the next sweep.
	old := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -45)
	oldName := fmt.Sprintf("%s_p%s", table, old.Format("20060102"))
	if _, err := ownerPool.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')`,
		oldName, table, old.Format("2006-01-02"), old.AddDate(0, 0, 1).Format("2006-01-02"))); err != nil {
		t.Fatal(err)
	}
	if err := cli.MaintainSeries(ctx); err != nil {
		t.Fatal(err)
	}
	if partitions()[oldName] {
		t.Fatalf("expired partition %s survived maintenance as the DML role", oldName)
	}

	// The functions take nothing the service did not declare.
	if _, err := appPool.Exec(ctx, `SELECT loom_series_partition_orders('loom_events', current_date)`); err == nil {
		t.Fatal("the partition function accepted a table that is not a @retain series")
	}
	if _, err := appPool.Exec(ctx, `SELECT loom_series_partition_orders($1, current_date - 400)`, table); err == nil {
		t.Fatal("the partition function accepted a day past retention")
	}
}
