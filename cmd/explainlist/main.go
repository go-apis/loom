// Command explainlist measures what an asked-fields list read saves over
// a whole-row read on a @table entity with a TOASTed column. It creates a
// scratch table shaped like a loom @table mirror (service, namespace, id,
// updated_at plus state columns, one of them a wide `heavy` text column
// Postgres stores out-of-line), fills it with enough rows that heavy
// values are TOASTed, then runs EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
// over the exact SQL loom.ListSQL (table.go's tableListSQL) builds for a
// page read: once with Query.Fields nil (whole row) and once with
// Query.Fields naming the one small field (asked fields). It prints each
// plan's shared buffers (hit+read) and their ratio.
//
// DSN: LOOM_TEST_PG, else the default internal/e2e/e2e_test.go uses
// (postgres://postgres:mysecret@localhost:5432/postgres).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/go-apis/loom"
)

const (
	tableName = "loom_explainlist_heavy"
	service   = "explainlist"
	namespace = "default"
	rowCount  = 5000
	heavySize = 8192
	pageSize  = 50
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	dsn := os.Getenv("LOOM_TEST_PG")
	if dsn == "" {
		dsn = "postgres://postgres:mysecret@localhost:5432/postgres"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping: %w", err)
	}

	if err := setup(ctx, pool); err != nil {
		return fmt.Errorf("setup: %w", err)
	}
	defer pool.Exec(ctx, "DROP TABLE IF EXISTS "+tableName)

	def := &loom.TableDef{
		Name: tableName,
		Columns: []loom.TableColumn{
			{Name: "status", Type: "text"},
			{Name: "total_cents", Type: "bigint"},
			{Name: "heavy", Type: "text"},
		},
	}
	base := loom.Query{Namespace: namespace, Limit: pageSize}

	whole, wholeArgs, err := loom.ListSQL(def, service, base)
	if err != nil {
		return fmt.Errorf("build whole-row SQL: %w", err)
	}
	asked := base
	asked.Fields = []string{"status"}
	askedSQL, askedArgs, err := loom.ListSQL(def, service, asked)
	if err != nil {
		return fmt.Errorf("build asked-fields SQL: %w", err)
	}

	wholeBuf, err := explainBuffers(ctx, pool, whole, wholeArgs)
	if err != nil {
		return fmt.Errorf("explain whole-row: %w", err)
	}
	askedBuf, err := explainBuffers(ctx, pool, askedSQL, askedArgs)
	if err != nil {
		return fmt.Errorf("explain asked-fields: %w", err)
	}

	fmt.Printf("whole-row page   (Fields nil):       shared hit=%d read=%d total=%d\n",
		wholeBuf.hit, wholeBuf.read, wholeBuf.total())
	fmt.Printf("asked-fields page (Fields=[status]):  shared hit=%d read=%d total=%d\n",
		askedBuf.hit, askedBuf.read, askedBuf.total())
	fmt.Printf("ratio (asked/whole): %s\n", bufferRatio(askedBuf, wholeBuf))
	return nil
}

// setup creates the scratch table and fills it with rowCount rows: a few
// small columns plus `heavy`, a wide random text value that Postgres
// stores out-of-line (TOAST) rather than inline in the heap tuple.
func setup(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, "DROP TABLE IF EXISTS "+tableName); err != nil {
		return err
	}
	ddl := `CREATE TABLE ` + tableName + ` (
		service      text NOT NULL,
		namespace    text NOT NULL,
		id           uuid NOT NULL,
		"status"     text,
		"total_cents" bigint,
		"heavy"      text,
		updated_at   timestamptz NOT NULL DEFAULT now(),
		PRIMARY KEY (service, namespace, id)
	)`
	if _, err := pool.Exec(ctx, ddl); err != nil {
		return err
	}

	statuses := []string{"open", "shipped", "done", "cancelled"}
	rows := make([][]any, rowCount)
	for i := range rows {
		rows[i] = []any{
			service, namespace, uuid.New(),
			statuses[i%len(statuses)], rand.IntN(100000),
			randomHeavy(),
		}
	}
	_, err := pool.CopyFrom(ctx,
		pgx.Identifier{tableName},
		[]string{"service", "namespace", "id", "status", "total_cents", "heavy"},
		pgx.CopyFromRows(rows))
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, "ANALYZE "+tableName)
	return err
}

const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

func randomHeavy() string {
	b := make([]byte, heavySize)
	for i := range b {
		b[i] = charset[rand.IntN(len(charset))]
	}
	return string(b)
}

type buffers struct {
	hit, read int64
}

func (b buffers) total() int64 { return b.hit + b.read }

// explainBuffers runs EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) over query
// and parses the resulting plan for its shared buffer counts.
func explainBuffers(ctx context.Context, pool *pgxpool.Pool, query string, args []any) (buffers, error) {
	var raw []byte
	err := pool.QueryRow(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+query, args...).Scan(&raw)
	if err != nil {
		return buffers{}, err
	}
	return parseBuffers(raw)
}

// parseBuffers sums "Shared Hit Blocks" and "Shared Read Blocks" across
// every node of an EXPLAIN (FORMAT JSON) plan tree: Postgres reports each
// node's own buffers, not a running total, so the query's total is the
// sum over the whole tree.
func parseBuffers(raw []byte) (buffers, error) {
	var plans []struct {
		Plan json.RawMessage `json:"Plan"`
	}
	if err := json.Unmarshal(raw, &plans); err != nil {
		return buffers{}, err
	}
	if len(plans) == 0 {
		return buffers{}, fmt.Errorf("no plan in EXPLAIN output")
	}
	var total buffers
	sumPlanBuffers(plans[0].Plan, &total)
	return total, nil
}

func sumPlanBuffers(raw json.RawMessage, total *buffers) {
	var node struct {
		SharedHitBlocks  int64             `json:"Shared Hit Blocks"`
		SharedReadBlocks int64             `json:"Shared Read Blocks"`
		Plans            []json.RawMessage `json:"Plans"`
	}
	if err := json.Unmarshal(raw, &node); err != nil {
		return
	}
	total.hit += node.SharedHitBlocks
	total.read += node.SharedReadBlocks
	for _, child := range node.Plans {
		sumPlanBuffers(child, total)
	}
}

// bufferRatio formats asked's total shared buffers over whole's as a
// decimal ratio string; whole with zero buffers has no meaningful ratio.
func bufferRatio(asked, whole buffers) string {
	if whole.total() == 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.4f", float64(asked.total())/float64(whole.total()))
}
