package loom

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// StreamDeletion reports what DeleteStream removed and what it left for
// remediation: projections that fold this stream's event types keep any
// row NOT keyed by the stream id (row-per-event shapes, re-keyed rows)
// until they are rebuilt.
type StreamDeletion struct {
	Events      int64    `json:"events"`
	Snapshots   int64    `json:"snapshots"`
	Outbox      int64    `json:"outbox"`
	Rows        int64    `json:"rows"`
	Projections []string `json:"projections"`
}

// DeleteStream permanently removes one aggregate's history: its events,
// snapshots, outbox rows, read-model rows keyed by the stream id (the
// doc store and every @table), and — via Shred — its data key and blob
// files. This is the junk lever, for test streams and bad imports, NOT
// the erasure lever: for personal data use a tombstone event plus Shred,
// which keeps the history honest. Two things it cannot undo: events
// already published to the bus have been consumed by other services,
// and projections keying rows by anything but the stream id keep those
// rows until rebuilt — the returned Projections list names every
// projection folding this stream's event types, so the caller (or the
// console) knows what to Rebuild.
func (c *Client) DeleteStream(ctx context.Context, namespace string, id uuid.UUID) (*StreamDeletion, error) {
	rows, err := c.db.Query(ctx, `
		SELECT DISTINCT type FROM loom_events WHERE service=$1 AND namespace=$2 AND aggregate_id=$3`,
		c.reg.Service, namespace, id)
	if err != nil {
		return nil, err
	}
	types := map[string]bool{}
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			rows.Close()
			return nil, err
		}
		types[t] = true
	}
	rows.Close()
	if len(types) == 0 {
		return nil, fmt.Errorf("loom: no such stream %s/%s", namespace, id)
	}

	tx, err := c.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	res := &StreamDeletion{}
	ct, err := tx.Exec(ctx, `
		DELETE FROM loom_events WHERE service=$1 AND namespace=$2 AND aggregate_id=$3`,
		c.reg.Service, namespace, id)
	if err != nil {
		return nil, err
	}
	res.Events = ct.RowsAffected()
	ct, err = tx.Exec(ctx, `
		DELETE FROM loom_snapshots WHERE service=$1 AND namespace=$2 AND aggregate_id=$3`,
		c.reg.Service, namespace, id)
	if err != nil {
		return nil, err
	}
	res.Snapshots = ct.RowsAffected()
	// unpublished and published alike: every outbox row is this stream's
	ct, err = tx.Exec(ctx, `
		DELETE FROM loom_outbox WHERE service=$1
		AND envelope->>'namespace'=$2 AND envelope->>'aggregate_id'=$3`,
		c.reg.Service, namespace, id.String())
	if err != nil {
		return nil, err
	}
	res.Outbox = ct.RowsAffected()
	ct, err = tx.Exec(ctx, `
		DELETE FROM loom_entities WHERE service=$1 AND namespace=$2 AND id=$3`,
		c.reg.Service, namespace, id)
	if err != nil {
		return nil, err
	}
	res.Rows = ct.RowsAffected()
	for _, ts := range sortedTables(c.tables) {
		ct, err = tx.Exec(ctx, fmt.Sprintf(
			`DELETE FROM %s WHERE service=$1 AND namespace=$2 AND id=$3`, ts.def.Name),
			c.reg.Service, namespace, id)
		if err != nil {
			return nil, err
		}
		res.Rows += ct.RowsAffected()
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	// the stream's key, files, and every instance's cached DEK go the way
	// of its rows; buffered events must not fold post-delete
	if err := c.Shred(ctx, namespace, id); err != nil {
		return res, err
	}

	for _, p := range c.reg.Projections {
		for _, e := range p.Events {
			if types[e] {
				res.Projections = append(res.Projections, p.Name)
				break
			}
		}
	}
	return res, nil
}
