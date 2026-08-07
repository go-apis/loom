package loom

import (
	"context"
	"fmt"
	"strings"
)

// Reset factory-resets the store: TRUNCATE every loom_* table in the
// current schema — events, snapshots, entities, records, checkpoints,
// dedup, outbox, timers, batches, effects, dead letters, and the
// per-stream data keys — in one transaction, and drops this instance's
// unwrapped-key cache. The returned list names what was truncated.
//
// Scope: the WHOLE schema, every service sharing it — the log is one
// table, so there is no per-service reset. Sequences are NOT restarted:
// a running fan-out reader holds its position in memory, and continuing
// seqs mean post-reset events are still seen without a restart.
//
// Other instances of a scaled service should still be restarted after a
// reset: their unwrapped-key caches can hold a truncated stream's key,
// and a re-created stream with the same id mints a fresh one.
func (c *Client) Reset(ctx context.Context) ([]string, error) {
	rows, err := c.db.Query(ctx,
		`SELECT tablename FROM pg_tables WHERE schemaname = current_schema() AND tablename LIKE 'loom\_%'`)
	if err != nil {
		return nil, err
	}
	var tables []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			rows.Close()
			return nil, err
		}
		tables = append(tables, t)
	}
	rows.Close()
	if len(tables) == 0 {
		return nil, fmt.Errorf("no loom tables in the current schema")
	}

	tx, err := c.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `TRUNCATE `+strings.Join(tables, ", ")+` CASCADE`); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	// the key rows are gone; a cached unwrapped key must not outlive them
	c.dekMu.Lock()
	c.deks = map[string][]byte{}
	c.dekMu.Unlock()

	return tables, nil
}
