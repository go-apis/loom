package loom

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Background-work hooks tell a service when its processes, runners or
// effects go wrong, so it need not poll for it. The instance holding the
// process lease (election.go) runs one watchdog loop, every poll, that:
//
//   - redrives the dead letters whose turn on Config.RedriveLadder has
//     come, so a passing failure heals itself;
//   - calls Config.OnDeadLetter once for each letter that is past its
//     ladder (or was parked with a Persistent error, or has no redrive);
//   - calls Config.OnRunnerLag once when a projection or local-event
//     process runner falls behind by more than Config.RunnerLagThreshold,
//     and again only after it has caught up;
//   - calls Config.OnEffectInDoubt once for each effect a reaction found
//     claimed and never settled, that neither @idempotent nor a
//     Config.Reconcile hook could settle.
//
// "Once" is kept in the store, not in memory (loom_dead_letters.notified_at,
// loom_checkpoints.lag_notified_at, loom_effects.doubt_notified_at): each
// is claimed before its hook is called, so a hook runs at most once per
// occurrence across a change of leader. A hook runs on the watchdog's
// goroutine; one that blocks holds the next sweep, and a panic is
// recovered and logged.

// RunnerLag is what OnRunnerLag is told: Runner ("projection:<name>" or
// "process:<name>") has not handled the event at Seq, one of the types it
// subscribes to, appended Behind ago; its checkpoint is at Checkpoint.
type RunnerLag struct {
	Runner     string        `json:"runner"`
	Checkpoint int64         `json:"checkpoint"`
	Seq        int64         `json:"seq"`
	Behind     time.Duration `json:"behind"`
}

// defaultRunnerLagThreshold is Config.RunnerLagThreshold's default.
const defaultRunnerLagThreshold = time.Minute

// persistentError marks an error that will not pass on its own.
type persistentError struct{ err error }

func (e *persistentError) Error() string { return e.err.Error() }
func (e *persistentError) Unwrap() error { return e.err }

// Persistent marks err as one that will not pass by trying again — a
// malformed request, a refusal, a missing record. A process reaction
// failing with it is dead-lettered at once: no immediate retries, no
// @retry backoff, no turn on the redrive ladder, and OnDeadLetter is
// called. Any other error is retried as before. Persistent(nil) is nil.
func Persistent(err error) error {
	if err == nil {
		return nil
	}
	return &persistentError{err: err}
}

// IsPersistent reports whether err, or any error it wraps, was marked
// with Persistent.
func IsPersistent(err error) bool {
	var p *persistentError
	return errors.As(err, &p)
}

// watching reports whether any hook or the ladder needs the watchdog.
func (c *Client) watching() bool {
	return c.onDeadLetter != nil || c.onRunnerLag != nil || c.onEffectInDoubt != nil || len(c.ladder) > 0
}

// redrivable reports whether RedriveDeadLetter can re-run a runner's
// letters (a parked projection event has none).
func redrivable(runner string) bool {
	return strings.HasPrefix(runner, "process:") || runner == "timer"
}

// firstRedrive is when a letter parked now by runner with cause takes
// its first turn on the ladder; nil means none: it is reported at once.
func (c *Client) firstRedrive(runner string, cause error) *time.Time {
	if len(c.ladder) == 0 || !redrivable(runner) || IsPersistent(cause) {
		return nil
	}
	at := time.Now().Add(c.ladder[0])
	return &at
}

// runWatchdog sweeps every poll while this instance holds the lease.
func (c *Client) runWatchdog(ctx context.Context, poll time.Duration) {
	t := time.NewTicker(poll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if !c.leading() {
			continue
		}
		for _, sweep := range []func(context.Context) error{
			c.redriveDue, c.notifyDeadLetters, c.checkLag, c.notifyInDoubt,
		} {
			if err := sweep(ctx); err != nil && ctx.Err() == nil {
				c.log.ErrorContext(ctx, "loom watchdog sweep failed", "error", err)
			}
		}
	}
}

// callHook runs a service's hook, recovering a panic.
func (c *Client) callHook(ctx context.Context, name string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			c.log.ErrorContext(ctx, "loom hook panicked", "hook", name, "panic", fmt.Sprint(r))
		}
	}()
	fn()
}

// redriveDue redrives the letters whose ladder step is due. A failure
// takes the next step, or — the ladder spent, or the error Persistent —
// leaves the letter for notifyDeadLetters.
func (c *Client) redriveDue(ctx context.Context) error {
	if len(c.ladder) == 0 {
		return nil
	}
	rows, err := c.db.Query(ctx, `
		SELECT id, redrives FROM loom_dead_letters
		WHERE service=$1 AND redrive_at <= now()
		ORDER BY id LIMIT 50`, c.reg.Service)
	if err != nil {
		return err
	}
	type due struct {
		id       int64
		redrives int
	}
	letters, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (due, error) {
		var d due
		return d, r.Scan(&d.id, &d.redrives)
	})
	if err != nil {
		return err
	}
	for _, d := range letters {
		cause := c.RedriveDeadLetter(ctx, d.id)
		if cause == nil {
			c.log.InfoContext(ctx, "dead letter healed on redrive", "id", d.id, "redrives", d.redrives+1)
			continue
		}
		var next *time.Time
		if n := d.redrives + 1; n < len(c.ladder) && !IsPersistent(cause) {
			at := time.Now().Add(c.ladder[n])
			next = &at
		}
		c.log.WarnContext(ctx, "dead letter redrive failed", "id", d.id, "redrives", d.redrives+1, "error", cause)
		if _, err := c.db.Exec(ctx, `
			UPDATE loom_dead_letters SET redrives=redrives+1, error=$3, redrive_at=$4
			WHERE service=$1 AND id=$2`,
			c.reg.Service, d.id, cause.Error(), next); err != nil {
			return err
		}
	}
	return nil
}

// notifyDeadLetters calls OnDeadLetter once for each letter off the ladder.
func (c *Client) notifyDeadLetters(ctx context.Context) error {
	if c.onDeadLetter == nil {
		return nil
	}
	rows, err := c.db.Query(ctx, `
		UPDATE loom_dead_letters SET notified_at=now()
		WHERE id IN (
			SELECT id FROM loom_dead_letters
			WHERE service=$1 AND redrive_at IS NULL AND notified_at IS NULL
			ORDER BY id LIMIT 100
			FOR UPDATE SKIP LOCKED)
		RETURNING id, runner, envelope, error, attempts, redrives, parked_at`, c.reg.Service)
	if err != nil {
		return err
	}
	letters, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (DeadLetter, error) {
		var d DeadLetter
		return d, r.Scan(&d.ID, &d.Runner, &d.Envelope, &d.Error, &d.Attempts, &d.Redrives, &d.ParkedAt)
	})
	if err != nil {
		return err
	}
	for _, d := range letters {
		c.callHook(ctx, "OnDeadLetter", func() { c.onDeadLetter(ctx, d) })
	}
	return nil
}

// checkLag measures each checkpointed runner: its lag is how long ago the
// oldest event it subscribes to and has not handled was appended (zero
// when it has handled all of them). Past the threshold OnRunnerLag is
// called once; under it again, the runner has recovered and may be
// reported again.
func (c *Client) checkLag(ctx context.Context) error {
	if c.onRunnerLag == nil {
		return nil
	}
	type watched struct {
		runner string
		events []string
	}
	var runners []watched
	for _, p := range c.reg.Projections {
		runners = append(runners, watched{"projection:" + p.Name, p.Events})
	}
	for _, p := range c.reg.Processes {
		// foreign events come off the bus and have no checkpoint
		if local, _ := c.splitSubscriptions(p); len(local) > 0 {
			runners = append(runners, watched{"process:" + p.Name, local})
		}
	}
	for _, w := range runners {
		var lag RunnerLag
		var notified bool
		err := c.db.QueryRow(ctx, `
			SELECT global_seq, lag_notified_at IS NOT NULL FROM loom_checkpoints
			WHERE service=$1 AND runner=$2`, c.reg.Service, w.runner).Scan(&lag.Checkpoint, &notified)
		if errors.Is(err, pgx.ErrNoRows) {
			continue // never stepped: nothing to be behind on yet
		}
		if err != nil {
			return err
		}
		var at time.Time
		err = c.db.QueryRow(ctx, `
			SELECT global_seq, at FROM loom_events
			WHERE service=$1 AND global_seq > $2 AND type = ANY($3)
			ORDER BY global_seq LIMIT 1`, c.reg.Service, lag.Checkpoint, w.events).Scan(&lag.Seq, &at)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if err == nil {
			lag.Behind = time.Since(at)
		}
		lag.Runner = w.runner
		if lag.Behind <= c.lagThreshold {
			if notified {
				c.log.InfoContext(ctx, "runner caught up", "runner", w.runner)
				if _, err := c.db.Exec(ctx, `
					UPDATE loom_checkpoints SET lag_notified_at=NULL WHERE service=$1 AND runner=$2`,
					c.reg.Service, w.runner); err != nil {
					return err
				}
			}
			continue
		}
		if notified {
			continue
		}
		// claimed in the store: a hand-over between leaders reports once
		tag, err := c.db.Exec(ctx, `
			UPDATE loom_checkpoints SET lag_notified_at=now()
			WHERE service=$1 AND runner=$2 AND lag_notified_at IS NULL`, c.reg.Service, w.runner)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 1 {
			c.log.WarnContext(ctx, "runner behind", "runner", w.runner, "behind", lag.Behind.String(), "seq", lag.Seq)
			c.callHook(ctx, "OnRunnerLag", func() { c.onRunnerLag(ctx, lag) })
		}
	}
	return nil
}

// markInDoubt records that a reaction found the effect in doubt, for
// notifyInDoubt. Once the row settles or is re-claimed the mark clears.
func (c *Client) markInDoubt(ctx context.Context, scope, key string) {
	if c.onEffectInDoubt == nil {
		return
	}
	if _, err := c.db.Exec(ctx, `
		UPDATE loom_effects SET in_doubt_at=now()
		WHERE service=$1 AND scope=$2 AND key=$3 AND status='running' AND in_doubt_at IS NULL`,
		c.reg.Service, scope, key); err != nil {
		c.log.ErrorContext(ctx, "marking effect in doubt", "scope", scope, "key", key, "error", err)
	}
}

// notifyInDoubt calls OnEffectInDoubt once for each effect still in doubt.
func (c *Client) notifyInDoubt(ctx context.Context) error {
	if c.onEffectInDoubt == nil {
		return nil
	}
	rows, err := c.db.Query(ctx, `
		UPDATE loom_effects e SET doubt_notified_at=now()
		FROM (
			SELECT scope, key FROM loom_effects
			WHERE service=$1 AND status='running' AND in_doubt_at IS NOT NULL AND doubt_notified_at IS NULL
			LIMIT 100
			FOR UPDATE SKIP LOCKED) d
		WHERE e.service=$1 AND e.scope=d.scope AND e.key=d.key
		RETURNING e.scope, e.key, e.status, e.result, e.error, e.attempts, e.started_at, e.settled_at`, c.reg.Service)
	if err != nil {
		return err
	}
	effects, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (EffectRecord, error) {
		var e EffectRecord
		return e, r.Scan(&e.Scope, &e.Key, &e.Status, &e.Result, &e.Error, &e.Attempts, &e.StartedAt, &e.SettledAt)
	})
	if err != nil {
		return err
	}
	for _, e := range effects {
		c.callHook(ctx, "OnEffectInDoubt", func() { c.onEffectInDoubt(ctx, e) })
	}
	return nil
}
