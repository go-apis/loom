package loom

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
)

// The outbox relay is the one design carried over from the old runtime —
// it earned its keep in production: rows written in the event's own
// transaction, a single active relay per service via an advisory lock,
// claims with SKIP LOCKED, drained in insert order.

func (c *Client) enqueueOutbox(ctx context.Context, tx pgx.Tx, evt *Event) error {
	data, err := json.Marshal(evt.Data)
	if err != nil {
		return err
	}
	env := &Envelope{
		Service:       evt.Service,
		Namespace:     evt.Namespace,
		AggregateType: evt.AggregateType,
		AggregateID:   evt.AggregateID.String(),
		Version:       evt.Version,
		GlobalSeq:     evt.GlobalSeq,
		Type:          evt.Type,
		SchemaVersion: evt.SchemaVersion,
		At:            evt.At.Format(time.RFC3339Nano),
		Meta:          evt.Meta,
		Data:          data,
	}
	// the dispatch's trace context rides the stored envelope: the relay
	// publishes after this transaction is long gone
	injectTrace(ctx, env)
	raw, err := json.Marshal(env)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO loom_outbox (service, ordering_key, envelope)
		VALUES ($1, $2, $3)`,
		evt.Service, env.OrderingKey(), raw)
	return err
}

func (c *Client) nudgeRelay() {
	select {
	case c.relayNudge <- struct{}{}:
	default:
	}
}

// StartRelay runs the outbox drain loop until ctx ends. Safe to run on
// every instance: the advisory lock elects one active relay per service.
func (c *Client) StartRelay(ctx context.Context, poll time.Duration) {
	if poll <= 0 {
		poll = 5 * time.Second
	}
	go func() {
		t := time.NewTicker(poll)
		defer t.Stop()
		for {
			if err := c.drainOutbox(ctx); err != nil && ctx.Err() == nil {
				c.log.ErrorContext(ctx, "outbox drain failed", "error", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-c.relayNudge:
			case <-t.C:
			}
		}
	}()
}

// drainOutbox publishes unpublished rows in insert order, batch by batch,
// while holding the service's relay advisory lock. Another instance holding
// the lock makes this a no-op.
func (c *Client) drainOutbox(ctx context.Context) error {
	for {
		n, err := c.drainBatch(ctx, 100)
		if err != nil || n == 0 {
			return err
		}
	}
}

func (c *Client) drainBatch(ctx context.Context, limit int) (int, error) {
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	var locked bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtext('loom_relay_' || $1))`, c.reg.Service).Scan(&locked); err != nil {
		return 0, err
	}
	if !locked {
		return 0, nil
	}

	rows, err := tx.Query(ctx, `
		SELECT id, envelope FROM loom_outbox
		WHERE service = $1 AND published_at IS NULL
		ORDER BY id ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED`,
		c.reg.Service, limit)
	if err != nil {
		return 0, err
	}
	type row struct {
		id  int64
		env Envelope
	}
	var batch []row
	for rows.Next() {
		var r row
		var raw []byte
		if err := rows.Scan(&r.id, &raw); err != nil {
			rows.Close()
			return 0, err
		}
		if err := json.Unmarshal(raw, &r.env); err != nil {
			rows.Close()
			return 0, err
		}
		batch = append(batch, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	for _, r := range batch {
		// the publish span is a child of the dispatch that enqueued the
		// row; re-injecting lets the consumer's span join the same trace
		pubCtx, end := c.tel.span(extractTrace(ctx, &r.env), "loom.publish",
			attribute.String("loom.event", r.env.Type), attribute.Int64("loom.global_seq", r.env.GlobalSeq))
		injectTrace(pubCtx, &r.env)
		err := c.bus.Publish(pubCtx, &r.env)
		end(err)
		if err != nil {
			// stop at the first failure: rows behind it stay unpublished,
			// preserving insert order for the next drain.
			return 0, err
		}
		c.tel.count(ctx, c.tel.published, 1)
		if _, err := tx.Exec(ctx, `UPDATE loom_outbox SET published_at = now() WHERE id = $1`, r.id); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(batch), nil
}

// OutboxDepth reports unpublished rows and the age of the oldest — the
// health signals the console surfaces.
func (c *Client) OutboxDepth(ctx context.Context) (int64, time.Duration, error) {
	var depth int64
	var oldest *time.Time
	err := c.db.QueryRow(ctx, `
		SELECT count(*), min(created_at) FROM loom_outbox
		WHERE service = $1 AND published_at IS NULL`,
		c.reg.Service).Scan(&depth, &oldest)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, err
	}
	age := time.Duration(0)
	if oldest != nil {
		age = time.Since(*oldest)
	}
	return depth, age, nil
}
