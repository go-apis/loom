package e2e_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/orders"
)

// Several processes migrating one empty database at once — a rollout's
// instances, or a test suite's packages on one DSN — each finish
// without error. CREATE ... IF NOT EXISTS is not enough for that: two
// sessions creating the same table, or the same day's partition of a
// @retain series, collide in the catalog. Migrate takes a lock for the
// database, so one runs at a time.
func TestMigrateRunsOneAtATimeOnOneDatabase(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	pool := testDB(t, ctx)

	// four pools stand for four processes, and two clients share each,
	// as two services of one binary do
	pools := []*pgxpool.Pool{pool}
	for range 3 {
		other, err := pgxpool.New(ctx, pool.Config().ConnString())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(other.Close)
		pools = append(pools, other)
	}
	const migrators = 8
	start := make(chan struct{})
	errs := make([]error, migrators)
	var wg sync.WaitGroup
	for i := range migrators {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cli, err := loom.New(loom.Config{DB: pools[i%len(pools)], Registry: orders.NewRegistry(), Blobs: loom.NewDirBlobStore(t.TempDir(), "http://blobs.local")})
			if err != nil {
				errs[i] = err
				return
			}
			<-start
			errs[i] = cli.Migrate(ctx)
		}()
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("migrator %d: %v", i, err)
		}
	}
}
