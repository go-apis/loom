package e2e_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/orders"
)

// the index orders.loom's declared `query (status, updated_at desc nulls
// last)` line desugars into, under its deterministic default name
const (
	summaryQueryIndex     = "idx_order_summary_status_updated_at_id"
	summaryQueryIndexCols = `(status, updated_at DESC NULLS LAST, id DESC)`
)

// summaryQueryIndexState reads how many indexes carry the declared-query's
// default name, the definition of the one there is, and whether pg_index
// marks it valid — the same shape index_test.go's summaryIndexState reads
// for the hand-written index, queried directly against pg_index/pg_class.
func summaryQueryIndexState(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (n int, def string, valid bool) {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT p.indexdef, i.indisvalid FROM pg_indexes p
		JOIN pg_class c ON c.relname = p.indexname
		JOIN pg_namespace ns ON ns.oid = c.relnamespace AND ns.nspname = p.schemaname
		JOIN pg_index i ON i.indexrelid = c.oid
		WHERE p.schemaname = current_schema() AND p.tablename = 'loom_t_orders_order_summary'
		  AND p.indexname = $1`, summaryQueryIndex)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		n++
		if err := rows.Scan(&def, &valid); err != nil {
			t.Fatal(err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return n, def, valid
}

// TestDeclaredQueryMigratesItsIndex: a `query (...)` line on a @table
// entity desugars into an Index at parse time (ADR 0011), which rides the
// unchanged ADR-0009 generator/Migrate pipeline — generate emits it as a
// loom.TableIndex, and Migrate builds it the same way it builds any
// hand-written `index (...)` line, CONCURRENTLY and valid.
func TestDeclaredQueryMigratesItsIndex(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Registry: orders.NewRegistry(), Blobs: loom.NewDirBlobStore(t.TempDir(), "http://blobs.local")})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	n, def, valid := summaryQueryIndexState(t, ctx, pool)
	if n != 1 {
		t.Fatalf("Migrate: %d indexes named %s, want 1", n, summaryQueryIndex)
	}
	if !strings.HasSuffix(def, summaryQueryIndexCols) || strings.Contains(def, "UNIQUE") {
		t.Fatalf("Migrate: index is %q, want the declared query's %s", def, summaryQueryIndexCols)
	}
	if !valid {
		t.Fatalf("Migrate: index %s is not valid", summaryQueryIndex)
	}
}
