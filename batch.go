package loom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Batches are the bulk-dispatch primitive: N commands persisted durably up
// front, worked through in claimed chunks, each item succeeding or failing
// on its own. The failure shape this kills: a handler fanning 50k commands
// into one giant transaction (or holding them only in memory) and losing
// the tail on a crash. Items survive restarts; processing is at-least-once,
// so batch commands want deterministic aggregate ids, like every other
// redeliverable reaction in the system.

const (
	batchChunk        = 50
	batchStaleReclaim = 2 * time.Minute
)

type asBatch struct {
	Command
	id   uuid.UUID // zero = mint one
	cmds []Command
}

// AsBatch wraps commands so a reaction can fan out durably: the unit of
// work enqueues them as a batch (atomic with the triggering event) instead
// of executing them inline, under a fresh batch id.
func AsBatch(cmds ...Command) Command {
	if len(cmds) == 0 {
		return nil
	}
	return &asBatch{Command: cmds[0], cmds: cmds}
}

// AsBatchKeyed is AsBatch with a caller-chosen batch id — use the
// triggering event's aggregate id so a redelivered reaction converges on
// the batch it already enqueued (the duplicate enqueue no-ops) and the
// batch is findable from the aggregate.
func AsBatchKeyed(id uuid.UUID, cmds ...Command) Command {
	if len(cmds) == 0 {
		return nil
	}
	return &asBatch{Command: cmds[0], id: id, cmds: cmds}
}

// Batch is the progress row.
type Batch struct {
	ID        uuid.UUID `json:"id"`
	Namespace string    `json:"namespace"`
	Status    string    `json:"status"` // running | completed
	Total     int       `json:"total"`
	Done      int       `json:"done"`
	Failed    int       `json:"failed"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// BatchFailure is one failed item: which command, and why.
type BatchFailure struct {
	Seq         int             `json:"seq"`
	CommandType string          `json:"command_type"`
	Command     json.RawMessage `json:"command"`
	Error       string          `json:"error"`
}

// EnqueueBatch persists a batch outside any unit of work (API-layer use)
// and returns its id. The batch runner works it asynchronously.
func (c *Client) EnqueueBatch(ctx context.Context, namespace string, cmds []Command) (uuid.UUID, error) {
	if len(cmds) == 0 {
		return uuid.Nil, fmt.Errorf("loom: empty batch")
	}
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	defer tx.Rollback(ctx)
	id := uuid.New()
	if err := c.enqueueBatchTx(ctx, tx, id, namespace, cmds); err != nil {
		return uuid.Nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, err
	}
	c.nudgeBatches()
	return id, nil
}

func (c *Client) enqueueBatchTx(ctx context.Context, tx pgx.Tx, id uuid.UUID, namespace string, cmds []Command) error {
	// keyed batches converge: a redelivered reaction re-enqueueing the same
	// id is a no-op rather than a duplicate fan-out
	tag, err := tx.Exec(ctx, `
		INSERT INTO loom_batches (service, namespace, id, status, total)
		VALUES ($1,$2,$3,'running',$4)
		ON CONFLICT DO NOTHING`,
		c.reg.Service, namespace, id, len(cmds))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return nil
	}
	rows := make([][]any, 0, len(cmds))
	for i, cmd := range cmds {
		raw, err := json.Marshal(cmd)
		if err != nil {
			return fmt.Errorf("batch item %d (%s): %w", i, cmd.LoomCommand(), err)
		}
		rows = append(rows, []any{c.reg.Service, id, i, cmd.LoomCommand(), raw})
	}
	_, err = tx.CopyFrom(ctx,
		pgx.Identifier{"loom_batch_items"},
		[]string{"service", "batch_id", "seq", "command_type", "command"},
		pgx.CopyFromRows(rows))
	return err
}

func (c *Client) nudgeBatches() {
	select {
	case c.batchNudge <- struct{}{}:
	default:
	}
}

// GetBatch reads a batch's progress row (nil if absent).
func (c *Client) GetBatch(ctx context.Context, id uuid.UUID) (*Batch, error) {
	b := &Batch{ID: id}
	err := c.db.QueryRow(ctx, `
		SELECT namespace, status, total, done, failed, created_at, updated_at
		FROM loom_batches WHERE service=$1 AND id=$2`,
		c.reg.Service, id).Scan(&b.Namespace, &b.Status, &b.Total, &b.Done, &b.Failed, &b.CreatedAt, &b.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return b, nil
}

// BatchFailures lists a batch's failed items.
func (c *Client) BatchFailures(ctx context.Context, id uuid.UUID) ([]BatchFailure, error) {
	rows, err := c.db.Query(ctx, `
		SELECT seq, command_type, command, error FROM loom_batch_items
		WHERE service=$1 AND batch_id=$2 AND status='failed'
		ORDER BY seq`,
		c.reg.Service, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BatchFailure
	for rows.Next() {
		var f BatchFailure
		if err := rows.Scan(&f.Seq, &f.CommandType, &f.Command, &f.Error); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

func (c *Client) startBatchRunner(ctx context.Context, poll time.Duration) {
	go func() {
		t := time.NewTicker(poll)
		defer t.Stop()
		for {
			for {
				n, err := c.batchStep(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					c.log.ErrorContext(ctx, "batch runner failed", "error", err)
					break
				}
				if n == 0 {
					break
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-c.batchNudge:
			case <-t.C:
			}
		}
	}()
}

// batchStep claims one chunk of pending items and works it: dispatch each
// command in its own unit of work, record done/failed per item, advance the
// counters. Items claimed by a crashed instance reclaim after a timeout.
func (c *Client) batchStep(ctx context.Context) (int, error) {
	type item struct {
		batchID uuid.UUID
		seq     int
		cmdType string
		raw     []byte
	}

	tx, err := c.db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	// reclaim stale claims first (crashed workers)
	if _, err := tx.Exec(ctx, `
		UPDATE loom_batch_items SET status='pending', claimed_at=NULL
		WHERE service=$1 AND status='working' AND claimed_at < now() - $2::interval`,
		c.reg.Service, batchStaleReclaim.String()); err != nil {
		return 0, err
	}

	rows, err := tx.Query(ctx, `
		UPDATE loom_batch_items SET status='working', claimed_at=now()
		WHERE (service, batch_id, seq) IN (
			SELECT service, batch_id, seq FROM loom_batch_items
			WHERE service=$1 AND status='pending'
			ORDER BY batch_id, seq
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		RETURNING batch_id, seq, command_type, command`,
		c.reg.Service, batchChunk)
	if err != nil {
		return 0, err
	}
	var chunk []item
	for rows.Next() {
		var it item
		if err := rows.Scan(&it.batchID, &it.seq, &it.cmdType, &it.raw); err != nil {
			rows.Close()
			return 0, err
		}
		chunk = append(chunk, it)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	if len(chunk) == 0 {
		return 0, nil
	}

	touched := map[uuid.UUID]bool{}
	for _, it := range chunk {
		touched[it.batchID] = true
		var dispatchErr error
		cmd, err := c.decodeCommand(it.cmdType, it.raw)
		if err != nil {
			dispatchErr = err
		} else {
			dispatchErr = c.Dispatch(ctx, cmd)
		}
		status, msg := "done", ""
		counter := "done"
		if dispatchErr != nil {
			status, msg, counter = "failed", dispatchErr.Error(), "failed"
		}
		if _, err := c.db.Exec(ctx, fmt.Sprintf(`
			WITH marked AS (
				UPDATE loom_batch_items SET status=$4, error=$5, claimed_at=NULL
				WHERE service=$1 AND batch_id=$2 AND seq=$3 AND status='working'
				RETURNING 1
			)
			UPDATE loom_batches SET %s = %s + (SELECT count(*) FROM marked), updated_at=now()
			WHERE service=$1 AND id=$2`, counter, counter),
			c.reg.Service, it.batchID, it.seq, status, msg); err != nil {
			return 0, err
		}
	}

	// completion check for every batch this chunk touched
	for id := range touched {
		if _, err := c.db.Exec(ctx, `
			UPDATE loom_batches SET status='completed', updated_at=now()
			WHERE service=$1 AND id=$2 AND status='running' AND done + failed >= total`,
			c.reg.Service, id); err != nil {
			return 0, err
		}
	}
	c.broadcastLog() // progress watchers wake like everything else
	return len(chunk), nil
}
