package loom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Forking a stream re-runs an instance from a step. The log already holds
// how an instance got where it is; a fork copies its events up to a
// version into another namespace, as if they had happened there, and lets
// the service carry on from that point on the code deployed now: its
// loop is looked at, its processes react, its projections fold. The
// source is not touched.
//
// What the fork's processes would have called the outside world for, the
// source already did: every settled effect of a reaction to a copied
// event is copied too, keyed to the copied event, so loom.Once replays
// the recorded result instead of calling again. Only a step past the
// fork point reaches the outside world. That is what makes a fork safe
// to take of a run that charged a card or sent a message, and what makes
// one a regression test of new code against a real recorded run.
//
// @pii fields are copied sealed, as they rest, and the fork reads and
// writes them under its source's data key rather than a key of its own
// (a loom_key_aliases row; pii.go keyHome). One person's data has one
// key wherever it was copied: shredding the source makes the fork's copy
// unreadable too, and shredding the fork redacts the fork alone.
//
// A fork is refused for an @table aggregate (its typed row is written by
// command handling, which a fork does not run).

// ForkOptions names the stream to fork and where to.
type ForkOptions struct {
	Aggregate string
	Namespace string
	ID        uuid.UUID
	// UpTo is the last version copied; 0 copies the whole stream.
	UpTo int
	// ToNamespace is where the fork lives. It must differ from
	// Namespace, and the stream must not exist there yet.
	ToNamespace string
}

// ForkResult is what a fork copied.
type ForkResult struct {
	Namespace string    `json:"namespace"`
	ID        uuid.UUID `json:"id"`
	// Version is the fork's version: the last copied.
	Version int `json:"version"`
	// Events and Effects count what was copied.
	Events  int `json:"events"`
	Effects int `json:"effects"`
	// Seqs maps each copied event's global_seq to its copy's.
	Seqs map[int64]int64 `json:"seqs"`
}

// ForkStream copies an aggregate instance's events up to opts.UpTo into
// opts.ToNamespace, with the settled effects of reactions to them, in one
// unit of work. Each copy keeps its type, schema version, time, actor and
// correlation; its cause is fork:<namespace>/<id>@<seq> of the event it
// copies.
func (c *Client) ForkStream(ctx context.Context, opts ForkOptions) (*ForkResult, error) {
	agg := c.reg.aggregateDef(opts.Aggregate)
	switch {
	case agg == nil:
		return nil, fmt.Errorf("loom: fork: %s is not an aggregate of %s", opts.Aggregate, c.reg.Service)
	case opts.ToNamespace == "" || opts.ToNamespace == opts.Namespace:
		return nil, errors.New("loom: fork: name a namespace other than the source's to fork into")
	case c.tables[agg.Name] != nil:
		return nil, fmt.Errorf("loom: fork: %s is an @table aggregate; a fork is refused", agg.Name)
	case opts.UpTo < 0:
		return nil, errors.New("loom: fork: UpTo is a version, 0 for the whole stream")
	}

	tx, err := c.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var exists bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM loom_events WHERE service=$1 AND namespace=$2 AND aggregate_type=$3 AND aggregate_id=$4)`,
		c.reg.Service, opts.ToNamespace, agg.Name, opts.ID).Scan(&exists); err != nil {
		return nil, err
	}
	if exists {
		return nil, fmt.Errorf("loom: fork: %s %s already exists in %s", agg.Name, opts.ID, opts.ToNamespace)
	}

	upTo := opts.UpTo
	if upTo == 0 {
		upTo = math.MaxInt32 // versions are int4
	}
	rows, err := tx.Query(ctx, `
		SELECT global_seq, version, type, schema_version, at, correlation_id, causation_id, actor, data, command
		FROM loom_events
		WHERE service=$1 AND namespace=$2 AND aggregate_type=$3 AND aggregate_id=$4 AND version <= $5
		ORDER BY version`,
		c.reg.Service, opts.Namespace, agg.Name, opts.ID, upTo)
	if err != nil {
		return nil, err
	}
	var (
		evts []*Event
		from []int64
	)
	for rows.Next() {
		var (
			seq   int64
			e     Event
			at    time.Time
			cause string
			data  []byte
		)
		if err := rows.Scan(&seq, &e.Version, &e.Type, &e.SchemaVersion, &at, &e.Meta.CorrelationID, &cause, &e.Meta.Actor, &data, &e.Command); err != nil {
			rows.Close()
			return nil, err
		}
		e.Service, e.Namespace, e.AggregateType, e.AggregateID = c.reg.Service, opts.ToNamespace, agg.Name, opts.ID
		e.At = at.UTC()
		e.Meta.CausationID = "fork:" + opts.Namespace + "/" + opts.ID.String() + "@" + strconv.FormatInt(seq, 10)
		e.Data = json.RawMessage(data)
		evts = append(evts, &e)
		from = append(from, seq)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(evts) == 0 {
		return nil, fmt.Errorf("loom: fork: %s %s has no events in %s", agg.Name, opts.ID, opts.Namespace)
	}

	// the fork seals under its source's key, and a fork of a fork under
	// the first source's: written before the copies, on the same unit
	if c.reg.hasPII() {
		if _, err := tx.Exec(ctx, `
			INSERT INTO loom_key_aliases (service, namespace, stream_id, of_namespace)
			SELECT $1, $2, $3, COALESCE(
				(SELECT of_namespace FROM loom_key_aliases WHERE service=$1 AND namespace=$4 AND stream_id=$3), $4)
			ON CONFLICT (service, namespace, stream_id) DO UPDATE SET of_namespace = EXCLUDED.of_namespace`,
			c.reg.Service, opts.ToNamespace, opts.ID, opts.Namespace); err != nil {
			return nil, err
		}
	}

	if err := c.appendEvents(ctx, tx, evts); err != nil {
		return nil, err
	}
	res := &ForkResult{Namespace: opts.ToNamespace, ID: opts.ID, Version: evts[len(evts)-1].Version, Events: len(evts), Seqs: map[int64]int64{}}
	for i, e := range evts {
		res.Seqs[from[i]] = e.GlobalSeq
	}

	// a reaction to a copied event replays what the source's reaction
	// recorded: process:<name>/<service>:<seq> becomes :<copy's seq>
	for old, seq := range res.Seqs {
		suffix := "/" + c.reg.Service + ":" + strconv.FormatInt(old, 10)
		tag, err := tx.Exec(ctx, `
			INSERT INTO loom_effects (service, scope, key, status, result, error, attempts, started_at, settled_at)
			SELECT service, left(scope, length(scope) - length($2)) || $3, key, status, result, error, attempts, started_at, settled_at
			FROM loom_effects
			WHERE service=$1 AND status='done' AND scope LIKE 'process:%' AND right(scope, length($2)) = $2
			ON CONFLICT DO NOTHING`,
			c.reg.Service, suffix, "/"+c.reg.Service+":"+strconv.FormatInt(seq, 10))
		if err != nil {
			return nil, err
		}
		res.Effects += int(tag.RowsAffected())
	}

	// told after commit, never inside it (stream.go notifyLog): the log's
	// readers here and on other instances, and every instance's key cache,
	// which may hold a "no alias" for the fork's stream
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	c.dropDEK(opts.ToNamespace, opts.ID.String())
	c.broadcastLog()
	c.notifyLog()
	_, _ = c.db.Exec(ctx, `SELECT pg_notify($1, $2)`, "loom_"+c.reg.Service, "keys:"+opts.ToNamespace+":"+opts.ID.String())
	return res, nil
}

// ForkedFrom reads a fork's cause back: the namespace, id and global_seq
// of the event a copy was made from. ok is false for an event that is
// not a fork's copy.
func ForkedFrom(e *Event) (namespace string, id uuid.UUID, seq int64, ok bool) {
	rest, found := strings.CutPrefix(e.Meta.CausationID, "fork:")
	if !found {
		return "", uuid.Nil, 0, false
	}
	at := strings.LastIndexByte(rest, '@')
	slash := strings.LastIndexByte(rest[:max(at, 0)], '/')
	if at < 0 || slash < 0 {
		return "", uuid.Nil, 0, false
	}
	id, err := uuid.Parse(rest[slash+1 : at])
	if err != nil {
		return "", uuid.Nil, 0, false
	}
	seq, err = strconv.ParseInt(rest[at+1:], 10, 64)
	if err != nil {
		return "", uuid.Nil, 0, false
	}
	return rest[:slash], id, seq, true
}
