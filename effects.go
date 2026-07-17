package loom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
// A crash between claim and settle leaves the effect in doubt: the call may
// or may not have executed. Once refuses to re-run it — the reaction parks
// to dead letters — until an operator settles the question (check with the
// other side, then ResolveEffect / POST /effects/resolve) and redrives the
// dead letter. At-most-once, with loud ambiguity instead of silent repeats.
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
}

func (e *EffectInDoubtError) Error() string {
	return fmt.Sprintf("loom: effect %s %s is in doubt (an earlier attempt may have executed the call) — resolve it, then redrive", e.Scope, e.Key)
}

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

	replay, run, err := es.c.claimEffect(ctx, es.scope, key)
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
	if err != nil {
		defer outcome("failed", err)
		if serr := es.c.settleEffect(ctx, es.scope, key, nil, err.Error()); serr != nil {
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
	if err := es.c.settleEffect(ctx, es.scope, key, raw, ""); err != nil {
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

func effectName(key string) string {
	if i := strings.IndexByte(key, '/'); i > 0 {
		return key[:i]
	}
	return key
}

// claimEffect wins, replays, or refuses one execution slot. The claim is
// its own committed write so a crash mid-call leaves evidence.
func (c *Client) claimEffect(ctx context.Context, scope, key string) (replay json.RawMessage, run bool, err error) {
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
		tag, err := c.db.Exec(ctx, `
			UPDATE loom_effects SET status='running', attempts=attempts+1, error='', started_at=now(), settled_at=NULL
			WHERE service=$1 AND scope=$2 AND key=$3 AND status='failed'`,
			c.reg.Service, scope, key)
		if err != nil {
			return nil, false, err
		}
		if tag.RowsAffected() == 1 {
			return nil, true, nil
		}
		// lost the re-claim race to a concurrent runner
		return nil, false, &EffectInDoubtError{Scope: scope, Key: key}
	default:
		return nil, false, &EffectInDoubtError{Scope: scope, Key: key}
	}
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

// ResolveEffect settles an in-doubt effect after the operator has checked
// what actually happened on the other side. A non-nil result records the
// call as done with that result; nil records it as failed (the call did not
// happen), letting a redrive re-run it.
func (c *Client) ResolveEffect(ctx context.Context, scope, key string, result json.RawMessage) error {
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
