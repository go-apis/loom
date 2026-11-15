package loomtest

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Postgres gives a test a database of its own on a real Postgres, for
// what the given/when/then harness cannot show because it needs the
// store: a client's Migrate and Start, dispatches that commit, runners.
// It connects to the server LOOM_TEST_PG names (default
// postgres://postgres:mysecret@localhost:5432/postgres), creates a fresh
// database there, and answers a pool on it; the database is dropped and
// the pool closed when the test ends. With no server to reach, the test
// is skipped.
//
//	pool := loomtest.Postgres(t)
//	cli, _ := loom.New(loom.Config{DB: pool, Registry: NewRegistry()})
//	_ = cli.Migrate(ctx)
func Postgres(t testing.TB) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	admin := os.Getenv("LOOM_TEST_PG")
	if admin == "" {
		admin = "postgres://postgres:mysecret@localhost:5432/postgres"
	}
	adminPool, err := pgxpool.New(ctx, admin)
	if err != nil {
		t.Skipf("no postgres available: %v", err)
	}
	if err := adminPool.Ping(ctx); err != nil {
		adminPool.Close()
		t.Skipf("no postgres available: %v", err)
	}
	name := "loom_test_" + uuid.NewString()[:8]
	if _, err := adminPool.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		adminPool.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = adminPool.Exec(cleanCtx, "DROP DATABASE "+name+" WITH (FORCE)")
		adminPool.Close()
	})

	cfg, err := pgxpool.ParseConfig(admin)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Database = name
	// Every started client parks two connections for its whole life: one
	// on LISTEN, one holding the process lease. A test that runs two
	// services off this pool has lost four before it issues a query, and
	// pgx sizes a pool at max(NumCPU, 4) — exactly four on a small CI
	// runner, where every Load and Dispatch would then wait on the pool
	// until the test's context expired. Size it for a test, not for the
	// host's cores.
	cfg.MaxConns = 20
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}
