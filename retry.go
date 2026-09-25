package loom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
)

// Durable retries are a process's `@retry(max, min..max)`. A reaction
// that exhausts its immediate in-process attempts (reactWithRetry) does
// not park: it is written to loom_timers as a retry record, marked by
// command_type retryTimerType and keyed by (process, event), and the timer
// runner re-fires the same reaction after a backoff. Each durable attempt
// is one reaction; a failure re-arms the row with the next backoff until
// Max durable attempts have failed, and only then does the event park to
// dead letters exactly as an undeclared process's would.
//
// Reusing loom_timers costs no new table or loop: the retry is claimed
// with the same SKIP LOCKED batch as a scheduled command, survives a
// restart, and shows on /timers (and in /stats' timers_pending) while it
// waits, so a reaction between its immediate attempts and its eventual
// park is never invisible.

// retryTimerType marks a loom_timers row as a durable retry rather than a
// scheduled command. No command type can carry a ':'.
const retryTimerType = "loom:retry"

// retryRecord is a durable retry's command column: which process, the
// event as it would rest in a dead letter, the durable attempts made so
// far, and the last failure.
type retryRecord struct {
	Process   string          `json:"process"`
	Attempts  int             `json:"attempts"`
	LastError string          `json:"last_error"`
	Event     json.RawMessage `json:"event"`
}

// retryKey keys a durable retry by (process, event): a redelivery or a
// duplicate firing lands on the same row instead of arming a second retry.
func retryKey(process string, evt *Event) string {
	return fmt.Sprintf("%s:process:%s/%s:%d", retryTimerType, process, evt.Service, evt.GlobalSeq)
}

// backoff is the wait before durable attempt n (1-based): full jitter over
// [Min, min(Min·2^(n-1), MaxBackoff)].
func (r *RetryPolicy) backoff(n int) time.Duration {
	ceil := r.Min
	for i := 1; i < n && ceil < r.MaxBackoff; i++ {
		ceil *= 2
	}
	ceil = min(ceil, r.MaxBackoff)
	return r.Min + rand.N(ceil-r.Min+1)
}

// exhausted handles a reaction whose immediate attempts all failed: park
// it, or under @retry arm its durable retry.
func (c *Client) exhausted(ctx context.Context, runner string, p *ReactorDef, evt *Event, cause error) error {
	if p.Retry == nil {
		return c.park(ctx, runner, evt, cause)
	}
	rec, err := json.Marshal(retryRecord{Process: p.Name, LastError: cause.Error(), Event: c.sealedEnvelope(ctx, evt)})
	if err != nil {
		return err
	}
	c.log.WarnContext(ctx, "reaction failed; retrying durably", "runner", runner, "type", evt.Type, "error", cause)
	// DO NOTHING: an existing row is the same retry, further along
	_, err = c.db.Exec(ctx, `
		INSERT INTO loom_timers (service, namespace, key, command_type, command, meta, fire_at)
		VALUES ($1,$2,$3,$4,$5,'{}',$6)
		ON CONFLICT (service, key) DO NOTHING`,
		c.reg.Service, evt.Namespace, retryKey(p.Name, evt), retryTimerType, rec,
		time.Now().Add(p.Retry.backoff(1)))
	return err
}

// retryPending reports whether a durable retry already owns evt for p.
func (c *Client) retryPending(ctx context.Context, p *ReactorDef, evt *Event) (bool, error) {
	if p.Retry == nil {
		return false, nil
	}
	var one int
	err := c.db.QueryRow(ctx, `SELECT 1 FROM loom_timers WHERE service=$1 AND key=$2`,
		c.reg.Service, retryKey(p.Name, evt)).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// fireRetry runs one durable attempt of a retry row claimed (FOR UPDATE)
// in tx: react once; on success clear the row (and mark a foreign event
// processed), on failure re-arm it with the next backoff, or — the Max-th
// durable attempt failed — park the event and clear the row, atomically.
func (c *Client) fireRetry(ctx context.Context, tx pgx.Tx, key string, raw []byte) (retErr error) {
	var rec retryRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return c.parkRetryRaw(ctx, tx, key, raw, "unreadable retry record: "+err.Error())
	}
	runner := "process:" + rec.Process
	p := c.process(rec.Process)
	if p == nil {
		return c.parkRetryRaw(ctx, tx, key, rec.Event, "retry for unknown process "+rec.Process)
	}
	evt, err := c.openEnvelope(ctx, rec.Event)
	if err != nil {
		return c.parkRetryRaw(ctx, tx, key, rec.Event, err.Error())
	}
	ctx, end := c.tel.span(ctx, "loom.retry.fire",
		attribute.String("loom.runner", runner), attribute.String("loom.event", evt.Type),
		attribute.Int64("loom.global_seq", evt.GlobalSeq), attribute.Int("loom.attempt", rec.Attempts+1))
	defer func() { end(retErr) }()

	cause := c.react(ctx, p, evt)
	if cause == nil {
		if evt.Service != c.reg.Service {
			if err := c.markProcessed(ctx, p.Name, fmt.Sprintf("%s:%d", evt.Service, evt.GlobalSeq)); err != nil {
				return err
			}
		}
		return deleteRetry(ctx, tx, c.reg.Service, key)
	}
	rec.Attempts++
	rec.LastError = cause.Error()
	// a policy removed or tightened since the row was armed still ends
	if p.Retry == nil || rec.Attempts >= p.Retry.Max {
		if err := c.parkOn(ctx, tx, runner, evt, cause, processRetries+rec.Attempts); err != nil {
			return err
		}
		return deleteRetry(ctx, tx, c.reg.Service, key)
	}
	c.log.WarnContext(ctx, "durable retry failed; re-arming", "runner", runner, "type", evt.Type,
		"attempt", rec.Attempts, "max", p.Retry.Max, "error", cause)
	next, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE loom_timers SET command=$3, fire_at=$4 WHERE service=$1 AND key=$2`,
		c.reg.Service, key, next, time.Now().Add(p.Retry.backoff(rec.Attempts+1)))
	return err
}

// parkRetryRaw parks a retry row that cannot be re-fired at all, so it is
// loud in dead letters rather than failing the timer batch forever.
func (c *Client) parkRetryRaw(ctx context.Context, tx pgx.Tx, key string, envelope []byte, cause string) error {
	if !json.Valid(envelope) {
		envelope, _ = json.Marshal(map[string]string{"retry_key": key})
	}
	c.log.ErrorContext(ctx, "parking unfireable durable retry", "key", key, "error", cause)
	if _, err := tx.Exec(ctx, `
		INSERT INTO loom_dead_letters (service, runner, envelope, error, attempts)
		VALUES ($1,'retry',$2,$3,0)`,
		c.reg.Service, envelope, cause); err != nil {
		return err
	}
	return deleteRetry(ctx, tx, c.reg.Service, key)
}

func deleteRetry(ctx context.Context, tx pgx.Tx, service, key string) error {
	_, err := tx.Exec(ctx, `DELETE FROM loom_timers WHERE service=$1 AND key=$2`, service, key)
	return err
}
