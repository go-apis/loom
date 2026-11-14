package loom

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
)

// A process reacts to an event once, as it passes: it starts at the
// log's head, or at its origin on first deploy (@from). What it cannot
// do by itself is react again to what is already behind it — and that
// is what is wanted when a process is added after the things it is
// about exist, when what it calls was down or misconfigured while they
// went by, or when its reaction changed. ReplayProcess hands a process
// events it has been past, on purpose.

// ReplayOptions chooses what a process is handed again.
type ReplayOptions struct {
	// Types narrows the replay to these of the process's own event
	// types. Empty means every type it subscribes to locally.
	Types []string
	// Namespace narrows it to one namespace; empty means all.
	Namespace string
	// AggregateID narrows it to one stream; uuid.Nil means all.
	AggregateID uuid.UUID
	// LatestPerAggregate hands the process only the newest matching
	// event of each stream: once for every thing that exists, rather
	// than once for everything that ever happened to it.
	LatestPerAggregate bool
	// DryRun counts what would be handed over and reacts to none of it.
	DryRun bool
}

// ReplayResult is what a replay did.
type ReplayResult struct {
	// ID names this replay. Its effects are journaled under it, apart
	// from the process's own passes and from every other replay.
	ID string `json:"id"`
	// Events is how many events matched, Reacted how many the process
	// reacted to, and Parked how many it failed on after its retries —
	// those are in dead letters, under the runner "process:<name>
	// (replay)".
	Events  int `json:"events"`
	Reacted int `json:"reacted"`
	Parked  int `json:"parked"`
}

// replayBatch is how many events a replay reads at a time.
const replayBatch = 500

// ReplayProcess hands the process named events it has already been
// past, oldest first, and reacts to each as it would have: the same
// reaction, the same retries, the commands it returns dispatched, a
// failure parked to dead letters.
//
// What is different from the process's own pass:
//
//   - Its checkpoint does not move, and the lease is not needed: the
//     replay runs here, in the caller, beside whatever the process is
//     doing.
//   - Its effects run again. They are journaled under the replay's id,
//     so loom.Once does not answer them from the first pass — calling
//     the outside world again is the point — while a retry inside this
//     replay still finds its own.
//
// A reaction is at-least-once in any case, so one that converges —
// commands that no-op on a state already reached, effects that are
// idempotent at the far end — replays safely. One that does not (it
// sends an email) will do it again: narrow the replay to what needs it.
func (c *Client) ReplayProcess(ctx context.Context, process string, opts ReplayOptions) (*ReplayResult, error) {
	var p *ReactorDef
	for _, def := range c.reg.Processes {
		if def.Name == process {
			p = def
		}
	}
	if p == nil {
		return nil, fmt.Errorf("loom: no process is named %q", process)
	}
	local, _ := c.splitSubscriptions(p)
	types := local
	if len(opts.Types) > 0 {
		types = nil
		for _, t := range opts.Types {
			if !contains(local, t) {
				return nil, fmt.Errorf("loom: process %s does not react to %q of its own service; it reacts to: %s", process, t, strings.Join(local, ", "))
			}
			types = append(types, t)
		}
	}
	if len(types) == 0 {
		return nil, fmt.Errorf("loom: process %s reacts to no event of its own service: there is nothing in the log to replay", process)
	}
	sort.Strings(types)

	res := &ReplayResult{ID: uuid.NewString()}
	runner := "process:" + p.Name + " (replay)"
	after := int64(0)
	for {
		events, err := c.readReplay(ctx, types, opts, after, replayBatch)
		if err != nil {
			return res, err
		}
		if len(events) == 0 {
			return res, nil
		}
		for _, evt := range events {
			after = evt.GlobalSeq
			res.Events++
			if opts.DryRun {
				continue
			}
			decoded, err := c.decodeEvent(evt)
			if err != nil {
				return res, err
			}
			if err := c.reactWithRetry(withReplay(ctx, res.ID), p, decoded); err != nil {
				if ctx.Err() != nil {
					return res, ctx.Err()
				}
				if err := c.park(ctx, runner, decoded, err); err != nil {
					return res, err
				}
				res.Parked++
				continue
			}
			res.Reacted++
		}
	}
}

// readReplay is readLog narrowed to what a replay asks for.
func (c *Client) readReplay(ctx context.Context, types []string, opts ReplayOptions, afterSeq int64, limit int) ([]*Event, error) {
	where := []string{"service=$1", "type = ANY($2)"}
	args := []any{c.reg.Service, types}
	if opts.Namespace != "" {
		args = append(args, opts.Namespace)
		where = append(where, fmt.Sprintf("namespace=$%d", len(args)))
	}
	if opts.AggregateID != uuid.Nil {
		args = append(args, opts.AggregateID)
		where = append(where, fmt.Sprintf("aggregate_id=$%d", len(args)))
	}
	match := strings.Join(where, " AND ")
	keep := "TRUE"
	if opts.LatestPerAggregate {
		keep = `global_seq IN (
			SELECT max(global_seq) FROM loom_events WHERE ` + match + `
			GROUP BY namespace, aggregate_type, aggregate_id)`
	}
	args = append(args, afterSeq, limit)
	return c.queryEvents(ctx, c.db, `
		SELECT global_seq, namespace, aggregate_type, aggregate_id, version, type, schema_version, at, correlation_id, causation_id, actor, data
		FROM loom_events
		WHERE `+match+` AND `+keep+fmt.Sprintf(` AND global_seq > $%d
		ORDER BY global_seq ASC
		LIMIT $%d`, len(args)-1, len(args)), args...)
}

// replayKey carries a replay's id to the effect scope of the reactions
// it runs.
type replayKey struct{}

func withReplay(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, replayKey{}, id)
}
