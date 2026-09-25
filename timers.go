package loom

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
)

// Timers are durable scheduled commands: "dispatch this later", written in
// the same transaction as the unit of work that decided it. Reactions
// return them through the ordinary command channel:
//
//	return []loom.Command{
//		loom.After(&ExpireDocRequest{...}, 3*365*24*time.Hour),
//	}, nil
//
// Keys make scheduling idempotent (default: command type + target), so
// redelivered reactions overwrite rather than duplicate, and CancelTimer
// with the same key deletes the pending timer.

type scheduledCommand struct {
	Command
	at  time.Time
	key string
}

type cancelTimer struct {
	Command
	key string
}

// At schedules cmd to be dispatched at t.
func At(cmd Command, t time.Time) Command {
	return &scheduledCommand{Command: cmd, at: t.UTC()}
}

// After schedules cmd to be dispatched d from now.
func After(cmd Command, d time.Duration) Command {
	return At(cmd, time.Now().Add(d))
}

// WithKey overrides a timer's idempotency key. Apply to the result of
// At/After (or to CancelTimer's argument) when the default key — command
// type + target aggregate — is not unique enough.
func WithKey(cmd Command, key string) Command {
	switch c := cmd.(type) {
	case *scheduledCommand:
		c.key = key
		return c
	case *cancelTimer:
		c.key = key
		return c
	default:
		return &scheduledCommand{Command: cmd, at: time.Now().UTC(), key: key}
	}
}

// CancelTimer deletes the pending timer that would dispatch a command like
// cmd (same type, same target, or the same explicit key).
func CancelTimer(cmd Command) Command {
	return &cancelTimer{Command: cmd}
}

func timerKey(cmd Command, explicit string) string {
	if explicit != "" {
		return explicit
	}
	ns, id := cmd.CommandTarget()
	return cmd.LoomCommand() + "/" + ns + "/" + id.String()
}

func (c *Client) writeTimer(ctx context.Context, tx pgx.Tx, sc *scheduledCommand, meta Metadata) error {
	data, err := json.Marshal(sc.Command)
	if err != nil {
		return err
	}
	if data, err = c.sealCommand(ctx, sc.Command, data); err != nil {
		return err
	}
	metaRaw, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	ns, _ := sc.CommandTarget()
	_, err = tx.Exec(ctx, `
		INSERT INTO loom_timers (service, namespace, key, command_type, command, meta, fire_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (service, key)
		DO UPDATE SET command_type = EXCLUDED.command_type, command = EXCLUDED.command, meta = EXCLUDED.meta, fire_at = EXCLUDED.fire_at`,
		c.reg.Service, ns, timerKey(sc.Command, sc.key), sc.LoomCommand(), data, metaRaw, sc.at)
	return err
}

func (c *Client) deleteTimer(ctx context.Context, tx pgx.Tx, ct *cancelTimer) error {
	_, err := tx.Exec(ctx, `DELETE FROM loom_timers WHERE service=$1 AND key=$2`,
		c.reg.Service, timerKey(ct.Command, ct.key))
	return err
}

// startTimerRunner fires due timers until ctx ends. Runs on every instance;
// SKIP LOCKED leases make concurrent runners safe.
func (c *Client) startTimerRunner(ctx context.Context, poll time.Duration) {
	go func() {
		t := time.NewTicker(poll)
		defer t.Stop()
		for {
			for {
				n, err := c.fireDueTimers(ctx, 50)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					c.log.ErrorContext(ctx, "timer runner failed", "error", err)
					break
				}
				if n == 0 {
					break
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
}

// timerLease is how long a claimed timer is hidden from other pollers
// while it fires. A claim pushes fire_at this far out and commits; if the
// process dies before the post-fire delete, the row comes due again when
// the lease runs out and is re-fired (at-least-once, as before). A fire
// that outlasts the lease may be fired a second time by another poller —
// the same duplicate a crash could always cause.
const timerLease = 5 * time.Minute

// dueTimer is one claimed loom_timers row. leasedAt and cmd are the row's
// version as the claim left it: the post-fire delete (and a retry's
// re-arm) only touches the row if both still match, so a timer the fired
// command's reactions re-armed under the same key survives.
type dueTimer struct {
	key, cmdType string
	cmd, meta    []byte
	leasedAt     time.Time
	dueAt        time.Time
}

// No transaction across a dispatch (docs/adr/0002). The audit, per
// runner:
//
//   - timers (fireDueTimers, here): claim -> commit -> fire. The claim is
//     one autocommitted UPDATE that leases the batch; every fire, park and
//     conditional delete runs after it, with no claim transaction open.
//     Before this, the claim transaction held the rows' FOR UPDATE locks
//     across each fire: a fired command whose reactions re-armed the same
//     key waited on that row lock while holding the namespace append lock
//     (appendEvents' pg_advisory_xact_lock), and the poller waited on the
//     dispatch — a cycle half in the application, which Postgres cannot
//     see (runsheet on v0.50.0, 2026-09-25).
//   - durable @retry rows (retry.go fireRetry): claimed in the same lease;
//     c.react runs with no transaction open, and only its outcome (clear,
//     re-arm, park) is written, in a short transaction of its own.
//   - batches (batch.go batchStep): already claim -> commit -> work; each
//     item's dispatch runs after the claim committed.
//   - process runners (runner.go processStep, subscribeForeign): no
//     transaction at all around a reaction. The checkpoint read, the
//     dedup check, markProcessed and writeCheckpoint are single
//     statements before or after react; react and its Dispatch run with
//     nothing open.
//   - relay (relay.go drainBatch): the exception. It holds its claim
//     transaction across bus.Publish, which is not a command dispatch,
//     takes no append lock and writes nothing a dispatch could wait on, so
//     it cannot close this cycle. Documented, not changed.

// fireDueTimers leases a batch of due timers, then — the claim committed —
// dispatches each in its own unit of work and deletes the fired row if it
// is still the version it claimed. A timer whose dispatch keeps failing is
// parked to dead letters — fired loudly-broken, never lost.
func (c *Client) fireDueTimers(ctx context.Context, limit int) (int, error) {
	batch, err := c.claimDueTimers(ctx, limit)
	if err != nil || len(batch) == 0 {
		return 0, err
	}
	for i, d := range batch {
		if err := c.fireClaimed(ctx, d); err != nil {
			c.releaseTimers(batch[i:])
			return 0, err
		}
	}
	return len(batch), nil
}

// claimDueTimers leases up to limit due timers in one autocommitted
// statement: SKIP LOCKED keeps concurrent pollers apart for the instant of
// the claim, and the pushed-out fire_at keeps them apart afterwards, with
// no lock held.
func (c *Client) claimDueTimers(ctx context.Context, limit int) ([]dueTimer, error) {
	rows, err := c.db.Query(ctx, `
		UPDATE loom_timers t SET fire_at = now() + make_interval(secs => $3)
		FROM (
			SELECT service, key, fire_at FROM loom_timers
			WHERE service = $1 AND fire_at <= now()
			ORDER BY fire_at ASC
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		) due
		WHERE t.service = due.service AND t.key = due.key
		RETURNING t.key, t.command_type, t.command, t.meta, t.fire_at, due.fire_at`,
		c.reg.Service, limit, timerLease.Seconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var batch []dueTimer
	for rows.Next() {
		var d dueTimer
		if err := rows.Scan(&d.key, &d.cmdType, &d.cmd, &d.meta, &d.leasedAt, &d.dueAt); err != nil {
			return nil, err
		}
		batch = append(batch, d)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.SliceStable(batch, func(i, j int) bool { return batch[i].dueAt.Before(batch[j].dueAt) })
	return batch, nil
}

// fireClaimed fires one leased timer. Nothing here runs inside the claim.
func (c *Client) fireClaimed(ctx context.Context, d dueTimer) error {
	if d.cmdType == retryTimerType { // a @retry reaction (retry.go)
		return c.fireRetry(ctx, d)
	}
	fireErr := c.fireTimer(ctx, d.key, d.cmdType, d.cmd, d.meta)
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if fireErr != nil {
		c.log.ErrorContext(ctx, "timer dispatch failed; parking", "key", d.key, "error", fireErr)
		if err := c.parkTimer(ctx, tx, d.key, d.cmdType, d.cmd, fireErr); err != nil {
			return err
		}
	}
	if err := c.deleteClaimed(ctx, tx, d); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// deleteClaimed deletes a fired timer only if it is still the row the
// claim leased: a same-key re-arm (writeTimer's upsert) changed fire_at
// and command, and is the next firing — not this one's to clear.
func (c *Client) deleteClaimed(ctx context.Context, q executor, d dueTimer) error {
	_, err := q.Exec(ctx, `
		DELETE FROM loom_timers
		WHERE service=$1 AND key=$2 AND fire_at=$3 AND command=$4`,
		c.reg.Service, d.key, d.leasedAt, d.cmd)
	return err
}

// releaseTimers hands claims back unfired (the runner is stopping, or a
// fire's bookkeeping failed), restoring their due time so the next poll
// takes them instead of waiting out the lease. Best effort: a release that
// fails leaves the lease to expire.
func (c *Client) releaseTimers(batch []dueTimer) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, d := range batch {
		if _, err := c.db.Exec(ctx, `
			UPDATE loom_timers SET fire_at=$5
			WHERE service=$1 AND key=$2 AND fire_at=$3 AND command=$4`,
			c.reg.Service, d.key, d.leasedAt, d.cmd, d.dueAt); err != nil {
			c.log.WarnContext(ctx, "timer release failed; lease will expire", "key", d.key, "error", err)
			return
		}
	}
}

func (c *Client) fireTimer(ctx context.Context, key, cmdType string, cmdRaw, metaRaw []byte) (retErr error) {
	ctx, end := c.tel.span(ctx, "loom.timer.fire",
		attribute.String("loom.timer", key), attribute.String("loom.command", cmdType))
	defer func() { end(retErr) }()
	c.tel.count(ctx, c.tel.timersFired, 1)
	cmdRaw, err := c.openCommand(ctx, cmdType, cmdRaw)
	if err != nil {
		return err
	}
	cmd, err := c.decodeCommand(cmdType, cmdRaw)
	if err != nil {
		return err
	}
	var meta Metadata
	_ = json.Unmarshal(metaRaw, &meta)
	meta.CausationID = "timer:" + key
	var lastErr error
	for attempt := 0; attempt < processRetries; attempt++ {
		lastErr = c.Dispatch(WithMeta(ctx, meta), cmd)
		if lastErr == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return lastErr
		case <-time.After(time.Duration(attempt+1) * 100 * time.Millisecond):
		}
	}
	return lastErr
}

func (c *Client) parkTimer(ctx context.Context, q executor, key, cmdType string, cmdRaw []byte, cause error) error {
	envelope, _ := json.Marshal(map[string]any{"timer_key": key, "command_type": cmdType, "command": json.RawMessage(cmdRaw)})
	_, err := q.Exec(ctx, `
		INSERT INTO loom_dead_letters (service, runner, envelope, error, attempts)
		VALUES ($1,'timer',$2,$3,$4)`,
		c.reg.Service, envelope, cause.Error(), processRetries)
	return err
}

func (c *Client) decodeCommand(cmdType string, raw []byte) (Command, error) {
	for _, agg := range c.reg.Aggregates {
		for _, def := range agg.Commands {
			if def.Name == cmdType {
				cmd := def.New()
				return cmd, json.Unmarshal(raw, cmd)
			}
		}
	}
	for _, rec := range c.reg.Records {
		for _, def := range rec.Commands {
			if def.Name == cmdType {
				cmd := def.New()
				return cmd, json.Unmarshal(raw, cmd)
			}
		}
	}
	return nil, fmt.Errorf("loom: timer holds unknown command type %q", cmdType)
}

// Schedule writes a timer outside any unit of work — for API-layer code.
func (c *Client) Schedule(ctx context.Context, cmd Command) error {
	sc, ok := cmd.(*scheduledCommand)
	if !ok {
		return fmt.Errorf("loom: Schedule wants loom.At/loom.After")
	}
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := c.writeTimer(ctx, tx, sc, MetaFrom(ctx)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
