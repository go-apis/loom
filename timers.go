package loom

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
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
// SKIP LOCKED claims make concurrent runners safe.
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

// fireDueTimers claims a batch of due timers, dispatches each in its own
// unit of work, and deletes fired rows. A timer whose dispatch keeps
// failing is parked to dead letters — fired loudly-broken, never lost.
func (c *Client) fireDueTimers(ctx context.Context, limit int) (int, error) {
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT key, command_type, command, meta FROM loom_timers
		WHERE service = $1 AND fire_at <= now()
		ORDER BY fire_at ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED`,
		c.reg.Service, limit)
	if err != nil {
		return 0, err
	}
	type due struct {
		key, cmdType string
		cmd, meta    []byte
	}
	var batch []due
	for rows.Next() {
		var d due
		if err := rows.Scan(&d.key, &d.cmdType, &d.cmd, &d.meta); err != nil {
			rows.Close()
			return 0, err
		}
		batch = append(batch, d)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(batch) == 0 {
		return 0, nil
	}

	for _, d := range batch {
		if err := c.fireTimer(ctx, d.key, d.cmdType, d.cmd, d.meta); err != nil {
			c.log.ErrorContext(ctx, "timer dispatch failed; parking", "key", d.key, "error", err)
			if perr := c.parkTimer(ctx, d.key, d.cmdType, d.cmd, err); perr != nil {
				return 0, perr
			}
		}
		if _, err := tx.Exec(ctx, `DELETE FROM loom_timers WHERE service=$1 AND key=$2`, c.reg.Service, d.key); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(batch), nil
}

func (c *Client) fireTimer(ctx context.Context, key, cmdType string, cmdRaw, metaRaw []byte) error {
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

func (c *Client) parkTimer(ctx context.Context, key, cmdType string, cmdRaw []byte, cause error) error {
	envelope, _ := json.Marshal(map[string]any{"timer_key": key, "command_type": cmdType, "command": json.RawMessage(cmdRaw)})
	_, err := c.db.Exec(ctx, `
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
