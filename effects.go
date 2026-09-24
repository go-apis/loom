package loom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/attribute"
)

// Effects are journaled external calls — the outbox's cousin for the
// request/response direction. Everything else in the runtime is safe to
// retry; a call to the outside world (an IRS transmission, a payment
// capture) is not. A process reaction wraps such a call in Once:
//
//	receipt, err := loom.Once(ctx, "iris_transmit", func(ctx context.Context) (string, error) {
//		return irs.Transmit(ctx, payload)
//	})
//
// The claim row commits before fn runs; success commits the result. A
// reaction retry (or a bus redelivery) replays the journaled result instead
// of calling again. If fn returns an error the effect is marked failed and
// the next attempt re-runs it — returning an error is the handler asserting
// the call did not happen.
//
// The settle write is decoupled from the reaction's own cancellation
// (context.WithoutCancel): once fn has returned, the outcome is a settled
// fact, and a per-step deadline that fires while the call was in flight
// must not turn it back into doubt. Only the call itself sees the
// reaction's ctx; recording what it did always completes.
//
// A crash between claim and settle leaves the effect in doubt: the call may
// or may not have executed. Two declarations let the runtime settle that
// doubt itself, before anyone is paged:
//
//   - `effect notify_customer @idempotent` says the call is safe to repeat,
//     so a claim left running simply re-runs.
//   - a Config.Reconcile hook for the effect asks the other side: "it
//     executed, here is the result" settles the row done and Once replays
//     that result; "it did not" re-runs the call.
//
// Otherwise (or when the hook errs) Once refuses to re-run it — the
// reaction parks to dead letters — until an operator settles the question
// (check with the other side, then ResolveEffect / POST /effects/resolve,
// with redrive: true to re-run the parked reaction in the same call).
// At-most-once, with loud ambiguity instead of silent repeats.
//
// The schema declares which effects a process performs (`effect
// iris_transmit` in its process block); an undeclared key is an error, not
// a fresh journal identity.

// EffectInDoubtError means a previous attempt claimed the effect and never
// settled it — the external call may or may not have happened. It is not
// retryable by the runtime; an operator resolves it.
type EffectInDoubtError struct {
	Scope string
	Key   string
	// Reconcile is the error the effect's Reconcile hook returned, when it
	// had one and could not tell what happened.
	Reconcile error
}

func (e *EffectInDoubtError) Error() string {
	msg := fmt.Sprintf("loom: effect %s %s is in doubt (an earlier attempt may have executed the call) — resolve it, then redrive", e.Scope, e.Key)
	if e.Reconcile != nil {
		msg += fmt.Sprintf("; reconcile failed: %v", e.Reconcile)
	}
	return msg
}

// ReconcileFunc answers, for an effect left in doubt, whether the external
// call actually executed — typically by asking the other side (a lookup by
// the idempotency key the call carried). scope and key identify the journal
// row (key is the full Once key, "/suffix" and all). executed=true with the
// call's result settles the effect done and Once returns that result, as if
// it had been journaled; executed=false re-runs the call. An error leaves
// the effect in doubt for an operator, exactly as without a hook.
//
// The result must be the JSON encoding of Once's T; nil records null.
type ReconcileFunc func(ctx context.Context, scope, key string) (result json.RawMessage, executed bool, err error)

type effectScope struct {
	c     *Client
	p     *ReactorDef
	scope string
}

type effectScopeKey struct{}

func withEffectScope(ctx context.Context, c *Client, p *ReactorDef, evt *Event) context.Context {
	scope := fmt.Sprintf("process:%s/%s:%d", p.Name, evt.Service, evt.GlobalSeq)
	return context.WithValue(ctx, effectScopeKey{}, &effectScope{c: c, p: p, scope: scope})
}

// Once executes fn at most once for the current reaction step, keyed by the
// event being reacted to plus key. The result must be JSON-serializable —
// replays decode it back. Use a "name/suffix" key (declared name, dynamic
// suffix) when one reaction makes several calls of the same kind.
func Once[T any](ctx context.Context, key string, fn func(context.Context) (T, error)) (T, error) {
	var zero T
	es, ok := ctx.Value(effectScopeKey{}).(*effectScope)
	if !ok {
		return zero, fmt.Errorf("loom: Once(%q) outside a process reaction — effects belong to processes", key)
	}
	if !es.p.declaresEffect(key) {
		return zero, fmt.Errorf("loom: process %s does not declare effect %q — add `effect %s` to its schema block", es.p.Name, key, effectName(key))
	}

	tel := es.c.tel
	ctx, end := tel.span(ctx, "loom.effect", attribute.String("loom.effect", effectName(key)))
	outcome := func(o string, err error) {
		tel.count(ctx, tel.effects, 1, attribute.String("loom.effect", effectName(key)), attribute.String("loom.outcome", o))
		end(err)
	}

	replay, run, err := es.c.claimEffect(ctx, es.p, es.scope, key)
	if err != nil {
		var doubt *EffectInDoubtError
		if errors.As(err, &doubt) {
			outcome("in_doubt", err)
		} else {
			end(err)
		}
		return zero, err
	}
	if !run {
		outcome("replayed", nil)
		if err := json.Unmarshal(replay, &zero); err != nil {
			return zero, fmt.Errorf("loom: effect %s %s: journaled result does not decode: %w", es.scope, key, err)
		}
		return zero, nil
	}

	out, err := fn(ctx)
	// fn has returned a definite answer, so the settle write must land even
	// if the reaction's own ctx died while the call was in flight — a
	// per-step deadline that fires as the call returns would otherwise
	// leave the row 'running', in doubt over a settled question
	settleCtx := context.WithoutCancel(ctx)
	if err != nil {
		defer outcome("failed", err)
		if serr := es.c.settleEffect(settleCtx, es.scope, key, nil, err.Error()); serr != nil {
			return zero, fmt.Errorf("loom: effect %s %s failed (%w) and the journal write also failed: %v", es.scope, key, err, serr)
		}
		return zero, err
	}
	raw, err := json.Marshal(out)
	if err != nil {
		outcome("executed", err)
		return zero, fmt.Errorf("loom: effect %s %s: result does not serialize: %w", es.scope, key, err)
	}
	// if this write fails the effect stays 'running': in doubt, never
	// silently re-executed
	if err := es.c.settleEffect(settleCtx, es.scope, key, raw, ""); err != nil {
		outcome("executed", err)
		return zero, fmt.Errorf("loom: effect %s %s executed but recording the result failed: %w", es.scope, key, err)
	}
	outcome("executed", nil)
	return out, nil
}

// Do is Once for calls with no result worth journaling.
func Do(ctx context.Context, key string, fn func(context.Context) error) error {
	_, err := Once(ctx, key, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
	return err
}

func (p *ReactorDef) declaresEffect(key string) bool {
	return contains(p.Effects, effectName(key))
}

func (p *ReactorDef) idempotentEffect(key string) bool {
	return contains(p.IdempotentEffects, effectName(key))
}

func (r *Registry) declaresEffect(name string) bool {
	for _, p := range r.Processes {
		if p.declaresEffect(name) {
			return true
		}
	}
	return false
}

func effectName(key string) string {
	if i := strings.IndexByte(key, '/'); i > 0 {
		return key[:i]
	}
	return key
}

// claimEffect wins, replays, or refuses one execution slot. The claim is
// its own committed write so a crash mid-call leaves evidence.
func (c *Client) claimEffect(ctx context.Context, p *ReactorDef, scope, key string) (replay json.RawMessage, run bool, err error) {
	tag, err := c.db.Exec(ctx, `
		INSERT INTO loom_effects (service, scope, key, status)
		VALUES ($1,$2,$3,'running')
		ON CONFLICT DO NOTHING`,
		c.reg.Service, scope, key)
	if err != nil {
		return nil, false, err
	}
	if tag.RowsAffected() == 1 {
		return nil, true, nil
	}

	var status string
	var result []byte
	err = c.db.QueryRow(ctx, `
		SELECT status, result FROM loom_effects WHERE service=$1 AND scope=$2 AND key=$3`,
		c.reg.Service, scope, key).Scan(&status, &result)
	if err != nil {
		return nil, false, err
	}
	switch status {
	case "done":
		return result, false, nil
	case "failed":
		// the previous attempt asserted the call did not happen: re-claim
		return c.reclaimEffect(ctx, scope, key, "failed")
	case "running":
		// an earlier attempt claimed and never settled: in doubt, unless
		// the schema or a hook can say what happened
		if p.idempotentEffect(key) {
			c.log.InfoContext(ctx, "re-running unsettled @idempotent effect", "scope", scope, "key", key)
			return c.reclaimEffect(ctx, scope, key, "running")
		}
		hook := c.reconcile[effectName(key)]
		if hook == nil {
			return nil, false, &EffectInDoubtError{Scope: scope, Key: key}
		}
		result, executed, herr := hook(ctx, scope, key)
		if herr != nil {
			return nil, false, &EffectInDoubtError{Scope: scope, Key: key, Reconcile: herr}
		}
		if !executed {
			c.log.InfoContext(ctx, "reconcile: unsettled effect did not execute, re-running", "scope", scope, "key", key)
			return c.reclaimEffect(ctx, scope, key, "running")
		}
		if result == nil {
			result = json.RawMessage("null")
		}
		c.log.InfoContext(ctx, "reconcile: unsettled effect executed, settling done", "scope", scope, "key", key)
		if err := c.settleEffect(context.WithoutCancel(ctx), scope, key, result, ""); err != nil {
			return nil, false, fmt.Errorf("loom: effect %s %s reconciled as executed but recording the result failed: %w", scope, key, err)
		}
		return result, false, nil
	default:
		return nil, false, &EffectInDoubtError{Scope: scope, Key: key}
	}
}

// reclaimEffect takes the execution slot back from a row in status from:
// one more attempt, running again.
func (c *Client) reclaimEffect(ctx context.Context, scope, key, from string) (replay json.RawMessage, run bool, err error) {
	tag, err := c.db.Exec(ctx, `
		UPDATE loom_effects SET status='running', attempts=attempts+1, error='', started_at=now(), settled_at=NULL
		WHERE service=$1 AND scope=$2 AND key=$3 AND status=$4`,
		c.reg.Service, scope, key, from)
	if err != nil {
		return nil, false, err
	}
	if tag.RowsAffected() == 1 {
		return nil, true, nil
	}
	// lost the re-claim race to a concurrent runner
	return nil, false, &EffectInDoubtError{Scope: scope, Key: key}
}

func (c *Client) settleEffect(ctx context.Context, scope, key string, result []byte, errMsg string) error {
	status := "done"
	if errMsg != "" {
		status = "failed"
	}
	_, err := c.db.Exec(ctx, `
		UPDATE loom_effects SET status=$4, result=$5, error=$6, settled_at=now()
		WHERE service=$1 AND scope=$2 AND key=$3`,
		c.reg.Service, scope, key, status, result, errMsg)
	return err
}

// EffectRecord is one journal row, as listed by Effects and the /effects
// endpoint.
type EffectRecord struct {
	Scope     string          `json:"scope"`
	Key       string          `json:"key"`
	Status    string          `json:"status"`
	Result    json.RawMessage `json:"result,omitempty"`
	Error     string          `json:"error,omitempty"`
	Attempts  int             `json:"attempts"`
	StartedAt time.Time       `json:"started_at"`
	SettledAt *time.Time      `json:"settled_at,omitempty"`
}

// Effects lists journal rows, optionally filtered by status
// (running/done/failed), newest first.
func (c *Client) Effects(ctx context.Context, status string, limit int) ([]EffectRecord, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := c.db.Query(ctx, `
		SELECT scope, key, status, result, error, attempts, started_at, settled_at
		FROM loom_effects
		WHERE service=$1 AND ($2 = '' OR status=$2)
		ORDER BY started_at DESC
		LIMIT $3`,
		c.reg.Service, status, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EffectRecord
	for rows.Next() {
		var r EffectRecord
		if err := rows.Scan(&r.Scope, &r.Key, &r.Status, &r.Result, &r.Error, &r.Attempts, &r.StartedAt, &r.SettledAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ResolveOptions tunes ResolveEffect.
type ResolveOptions struct {
	// Redrive re-runs the dead letter the in-doubt effect parked, in the
	// same call, once the resolve has landed — the operator's two steps
	// in the one order that works.
	Redrive bool
}

// ResolveEffect settles an in-doubt effect after the operator has checked
// what actually happened on the other side. A non-nil result records the
// call as done with that result; nil records it as failed (the call did not
// happen), letting a redrive re-run it. With ResolveOptions{Redrive: true}
// the parked reaction is redriven too (see ResolveEffectRedrive).
func (c *Client) ResolveEffect(ctx context.Context, scope, key string, result json.RawMessage, opts ...ResolveOptions) error {
	var o ResolveOptions
	for _, opt := range opts {
		o.Redrive = o.Redrive || opt.Redrive
	}
	if !o.Redrive {
		return c.resolveEffect(ctx, scope, key, result)
	}
	_, err := c.ResolveEffectRedrive(ctx, scope, key, result)
	return err
}

// ResolveEffectRedrive resolves an in-doubt effect, then redrives the dead
// letter its reaction parked: the letter whose runner is the effect's
// process and whose envelope is the event the effect's scope names. It
// returns the redriven letter's id, or 0 when there was none to redrive (the
// reaction had not parked yet; its next retry sees the resolution). The
// resolve stands even if the redrive fails — the dead letter stays, and a
// plain redrive retries it.
func (c *Client) ResolveEffectRedrive(ctx context.Context, scope, key string, result json.RawMessage) (letterID int64, err error) {
	if err := c.resolveEffect(ctx, scope, key, result); err != nil {
		return 0, err
	}
	return c.redriveEffectLetter(ctx, scope, key)
}

func (c *Client) redriveEffectLetter(ctx context.Context, scope, key string) (letterID int64, err error) {
	process, service, seq, ok := parseEffectScope(scope)
	if !ok {
		return 0, fmt.Errorf("loom: effect %s %s resolved, but its scope names no process reaction to redrive", scope, key)
	}
	err = c.db.QueryRow(ctx, `
		SELECT id FROM loom_dead_letters
		WHERE service=$1 AND runner=$2 AND envelope->>'service'=$3 AND (envelope->>'global_seq')::bigint=$4
		ORDER BY id ASC
		LIMIT 1`,
		c.reg.Service, "process:"+process, service, seq).Scan(&letterID)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("loom: effect %s %s resolved, but finding its dead letter failed: %w", scope, key, err)
	}
	if err := c.RedriveDeadLetter(ctx, letterID); err != nil {
		return letterID, fmt.Errorf("loom: effect %s %s resolved, but redriving dead letter %d failed: %w", scope, key, letterID, err)
	}
	return letterID, nil
}

// parseEffectScope splits the scope withEffectScope builds,
// "process:<name>/<service>:<global_seq>".
func parseEffectScope(scope string) (process, service string, seq int64, ok bool) {
	rest, ok := strings.CutPrefix(scope, "process:")
	if !ok {
		return "", "", 0, false
	}
	process, rest, ok = strings.Cut(rest, "/")
	if !ok {
		return "", "", 0, false
	}
	i := strings.LastIndexByte(rest, ':')
	if i < 0 {
		return "", "", 0, false
	}
	seq, err := strconv.ParseInt(rest[i+1:], 10, 64)
	if err != nil {
		return "", "", 0, false
	}
	return process, rest[:i], seq, true
}

func (c *Client) resolveEffect(ctx context.Context, scope, key string, result json.RawMessage) error {
	var tag pgconn.CommandTag
	var err error
	if result != nil {
		tag, err = c.db.Exec(ctx, `
			UPDATE loom_effects SET status='done', result=$4, error='', settled_at=now()
			WHERE service=$1 AND scope=$2 AND key=$3 AND status='running'`,
			c.reg.Service, scope, key, []byte(result))
	} else {
		tag, err = c.db.Exec(ctx, `
			UPDATE loom_effects SET status='failed', error='resolved by operator: call did not execute', settled_at=now()
			WHERE service=$1 AND scope=$2 AND key=$3 AND status='running'`,
			c.reg.Service, scope, key)
	}
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var status string
	err = c.db.QueryRow(ctx, `
		SELECT status FROM loom_effects WHERE service=$1 AND scope=$2 AND key=$3`,
		c.reg.Service, scope, key).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("loom: no effect %s %s", scope, key)
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("loom: effect %s %s is %s, not in doubt", scope, key, status)
}
