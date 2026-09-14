package loom

import (
	"context"
	"fmt"
	"strings"
)

// Reset factory-resets the store: TRUNCATE every loom_* table in the
// current schema — events, snapshots, entities, records, checkpoints,
// dedup, outbox, timers, batches, effects, dead letters, and the
// per-stream data keys. Generated @table mirrors (loom_t_*) ride the
// same sweep; a hand-named TableDef gets a service-scoped DELETE. All
// one transaction, then this instance's unwrapped-key cache drops. The
// returned list names what was truncated.
// Tables are emptied, never dropped: the schema survives and no
// re-migration is needed.
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
	// a projection step in flight on this or another instance has read
	// its batch and is folding it: wait for every projection's lock so
	// no step lands pre-reset rows after the truncate (each step's folds
	// and checkpoint are its own transaction, gated by this lock)
	for _, p := range c.reg.Projections {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('loom_' || $1 || '_' || $2))`, c.reg.Service, "projection:"+p.Name); err != nil {
			return nil, err
		}
	}
	if _, err := tx.Exec(ctx, `TRUNCATE `+strings.Join(tables, ", ")+` CASCADE`); err != nil {
		return nil, err
	}
	// generated @table mirrors are loom_t_* and already truncated above;
	// a hand-named TableDef escapes the prefix, so sweep it here —
	// service-scoped, since only this registry knows it belongs to loom
	seen := map[string]bool{}
	for _, t := range tables {
		seen[t] = true
	}
	for _, ts := range sortedTables(c.tables) {
		if seen[ts.def.Name] {
			continue
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE service=$1`, ts.def.Name), c.reg.Service); err != nil {
			return nil, err
		}
		tables = append(tables, ts.def.Name)
	}
	// the fan-out buffer holds pre-reset events, pre-decrypted: drop it
	// while the projections are still held, so the first step after the
	// commit reads an empty log, not the buffer
	c.fan.flush()
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	// the key rows are gone; a cached unwrapped key must not outlive them
	c.dekMu.Lock()
	c.deks = map[string][]byte{}
	c.dekMu.Unlock()
	c.fan.wakeAll()

	return tables, nil
}
