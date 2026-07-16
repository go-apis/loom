package loom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Runners are the async half of the runtime.
//
// Projections and processes subscribed to LOCAL events are checkpointed
// catch-up readers over the service's slice of the global sequence — no bus
// involved, rebuildable by resetting the checkpoint. This kills the old
// world's publish-to-your-own-bus hack for async self-handling.
//
// Processes subscribed to FOREIGN events consume the bus, dedup on the
// (service, process, envelope) key, and park to dead letters after
// exhausting retries — nothing is ever silently dropped.

const (
	logBatch       = 200
	processRetries = 3
)

// Start launches the relay and every runner, returning immediately. Safe on
// every instance: advisory locks elect active workers per service.
func (c *Client) Start(ctx context.Context, poll time.Duration) error {
	if poll <= 0 {
		poll = 5 * time.Second
	}
	c.StartRelay(ctx, poll)
	c.startTimerRunner(ctx, poll)
	c.startBatchRunner(ctx, poll)
	go c.listenLoop(ctx)

	for _, p := range c.reg.Projections {
		go c.runLogLoop(ctx, "projection:"+p.Name, poll, c.projectionStep(p))
	}
	for _, p := range c.reg.Processes {
		local, foreign := c.splitSubscriptions(p)
		if len(local) > 0 {
			go c.runLogLoop(ctx, "process:"+p.Name, poll, c.processStep(p, local))
		}
		if len(foreign) > 0 {
			if err := c.subscribeForeign(ctx, p, foreign); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Client) splitSubscriptions(p *ReactorDef) (local, foreign []string) {
	for _, name := range p.Events {
		def := c.reg.eventDef(name)
		if def != nil && def.Service != "" {
			foreign = append(foreign, name)
		} else {
			local = append(local, name)
		}
	}
	return local, foreign
}

// runLogLoop drives one checkpointed reader: catch up, then sleep until
// nudged (same instance), notified (other instances), or the poll tick.
func (c *Client) runLogLoop(ctx context.Context, runner string, poll time.Duration, step func(ctx context.Context) (int, error)) {
	t := time.NewTicker(poll)
	defer t.Stop()
	for {
		for {
			n, err := step(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				c.log.ErrorContext(ctx, "runner step failed", "runner", runner, "error", err)
				break
			}
			if n == 0 {
				break
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-c.nudge:
		case <-t.C:
		}
	}
}

// projectionStep folds one batch into the read model. Entity writes and the
// checkpoint advance share a transaction guarded by the runner's advisory
// lock — exactly-once effects on read models, and rebuild is just a
// checkpoint reset.
func (c *Client) projectionStep(p *ProjectionDef) func(ctx context.Context) (int, error) {
	runner := "projection:" + p.Name
	return func(ctx context.Context) (int, error) {
		tx, err := c.db.Begin(ctx)
		if err != nil {
			return 0, err
		}
		defer tx.Rollback(ctx)

		var locked bool
		if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtext('loom_' || $1 || '_' || $2))`, c.reg.Service, runner).Scan(&locked); err != nil {
			return 0, err
		}
		if !locked {
			return 0, nil
		}
		seq, err := checkpoint(ctx, tx, c.reg.Service, runner)
		if err != nil {
			return 0, err
		}
		events, err := c.readLog(ctx, seq, logBatch)
		if err != nil {
			return 0, err
		}
		if len(events) == 0 {
			return 0, nil
		}

		for _, evt := range events {
			if !contains(p.Events, evt.Type) {
				continue
			}
			id := p.EntityID(evt)
			state := p.NewState()
			var data []byte
			err := tx.QueryRow(ctx, `
				SELECT data FROM loom_entities
				WHERE service=$1 AND namespace=$2 AND entity_type=$3 AND id=$4
				FOR UPDATE`,
				c.reg.Service, evt.Namespace, p.Entity, id).Scan(&data)
			if err == nil {
				if err := json.Unmarshal(data, state); err != nil {
					return 0, fmt.Errorf("entity %s/%s: %w", p.Entity, id, err)
				}
			}
			if err := state.Fold(evt.Type, evt.Data); err != nil {
				return 0, fmt.Errorf("projection %s fold %s: %w", p.Name, evt.Type, err)
			}
			out, err := json.Marshal(state)
			if err != nil {
				return 0, err
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO loom_entities (service, namespace, entity_type, id, data, updated_at)
				VALUES ($1,$2,$3,$4,$5, now())
				ON CONFLICT (service, namespace, entity_type, id)
				DO UPDATE SET data = EXCLUDED.data, updated_at = now()`,
				c.reg.Service, evt.Namespace, p.Entity, id, out); err != nil {
				return 0, err
			}
		}

		last := events[len(events)-1].GlobalSeq
		if err := saveCheckpoint(ctx, tx, c.reg.Service, runner, last); err != nil {
			return 0, err
		}
		if err := tx.Commit(ctx); err != nil {
			return 0, err
		}
		return len(events), nil
	}
}

// Rebuild resets a projection: checkpoint to zero and its read model
// truncated. The runner refolds the full log on its next pass.
func (c *Client) Rebuild(ctx context.Context, projection string) error {
	var def *ProjectionDef
	for _, p := range c.reg.Projections {
		if p.Name == projection {
			def = p
		}
	}
	if def == nil {
		return fmt.Errorf("loom: unknown projection %s", projection)
	}
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `DELETE FROM loom_entities WHERE service=$1 AND entity_type=$2`, c.reg.Service, def.Entity); err != nil {
		return err
	}
	if err := saveCheckpoint(ctx, tx, c.reg.Service, "projection:"+projection, 0); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	select {
	case c.nudge <- struct{}{}:
	default:
	}
	return nil
}

// processStep runs a local-event process from the log: react, dispatch (its
// own unit of work), advance the checkpoint. Failures retry, then park the
// event to dead letters and move on — at-least-once, no head-of-line block.
func (c *Client) processStep(p *ReactorDef, local []string) func(ctx context.Context) (int, error) {
	runner := "process:" + p.Name
	return func(ctx context.Context) (int, error) {
		seq, err := c.readCheckpoint(ctx, runner)
		if err != nil {
			return 0, err
		}
		events, err := c.readLog(ctx, seq, logBatch)
		if err != nil {
			return 0, err
		}
		if len(events) == 0 {
			return 0, nil
		}
		for _, evt := range events {
			if contains(local, evt.Type) {
				if err := c.reactWithRetry(ctx, p, evt); err != nil {
					if err := c.park(ctx, runner, evt, err); err != nil {
						return 0, err
					}
				}
			}
			if err := c.writeCheckpoint(ctx, runner, evt.GlobalSeq); err != nil {
				return 0, err
			}
		}
		return len(events), nil
	}
}

func (c *Client) reactWithRetry(ctx context.Context, p *ReactorDef, evt *Event) error {
	var err error
	for attempt := 0; attempt < processRetries; attempt++ {
		err = c.react(ctx, p, evt)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(time.Duration(attempt+1) * 100 * time.Millisecond):
		}
	}
	return err
}

func (c *Client) react(ctx context.Context, p *ReactorDef, evt *Event) error {
	ctx = withEffectScope(ctx, c, p, evt)
	cmds, err := p.React(ctx, evt)
	if err != nil {
		return err
	}
	if len(cmds) == 0 {
		return nil
	}
	ctx = WithMeta(ctx, Metadata{
		CorrelationID: evt.Meta.CorrelationID,
		CausationID:   fmt.Sprintf("evt:%s:%d", evt.Service, evt.GlobalSeq),
		Actor:         evt.Meta.Actor,
	})
	return c.Dispatch(ctx, cmds...)
}

// subscribeForeign consumes foreign events off the bus for one process:
// dedup, react with retries, park on exhaustion.
func (c *Client) subscribeForeign(ctx context.Context, p *ReactorDef, foreign []string) error {
	group := c.reg.Service + "." + p.Name
	return c.bus.Subscribe(ctx, group, func(ctx context.Context, env *Envelope) error {
		if !contains(foreign, env.Type) {
			return nil // not ours; other services' events share the bus
		}
		evt, err := c.eventFromEnvelope(env)
		if err != nil {
			c.log.ErrorContext(ctx, "undecodable foreign event", "process", p.Name, "type", env.Type, "error", err)
			return c.park(ctx, "process:"+p.Name, &Event{Type: env.Type, Service: env.Service, GlobalSeq: env.GlobalSeq}, err)
		}

		key := fmt.Sprintf("%s:%d", env.Service, env.GlobalSeq)
		done, err := c.alreadyProcessed(ctx, p.Name, key)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		if err := c.reactWithRetry(ctx, p, evt); err != nil {
			return c.park(ctx, "process:"+p.Name, evt, err)
		}
		return c.markProcessed(ctx, p.Name, key)
	})
}

func (c *Client) eventFromEnvelope(env *Envelope) (*Event, error) {
	payload, err := c.decode(env.Type, env.Data)
	if err != nil {
		return nil, err
	}
	at, _ := time.Parse(time.RFC3339Nano, env.At)
	evt := &Event{
		Service:       env.Service,
		Namespace:     env.Namespace,
		AggregateType: env.AggregateType,
		Version:       env.Version,
		GlobalSeq:     env.GlobalSeq,
		Type:          env.Type,
		SchemaVersion: env.SchemaVersion,
		At:            at,
		Meta:          env.Meta,
		Data:          payload,
	}
	if id, err := uuid.Parse(env.AggregateID); err == nil {
		evt.AggregateID = id
	}
	return evt, nil
}

func (c *Client) alreadyProcessed(ctx context.Context, process, key string) (bool, error) {
	var one int
	err := c.db.QueryRow(ctx, `
		SELECT 1 FROM loom_dedup WHERE service=$1 AND process=$2 AND event_key=$3`,
		c.reg.Service, process, key).Scan(&one)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return false, err
}

func (c *Client) markProcessed(ctx context.Context, process, key string) error {
	_, err := c.db.Exec(ctx, `
		INSERT INTO loom_dedup (service, process, event_key) VALUES ($1,$2,$3)
		ON CONFLICT DO NOTHING`,
		c.reg.Service, process, key)
	return err
}

// park writes a dead letter. Parked events are queryable (and re-drivable)
// via the console; parking is loud, dropping is impossible.
func (c *Client) park(ctx context.Context, runner string, evt *Event, cause error) error {
	raw, err := json.Marshal(evt)
	if err != nil {
		raw = []byte(fmt.Sprintf(`{"type":%q}`, evt.Type))
	}
	c.log.ErrorContext(ctx, "parking event to dead letters", "runner", runner, "type", evt.Type, "error", cause)
	_, err = c.db.Exec(ctx, `
		INSERT INTO loom_dead_letters (service, runner, envelope, error, attempts)
		VALUES ($1,$2,$3,$4,$5)`,
		c.reg.Service, runner, raw, cause.Error(), processRetries)
	return err
}

// DeadLetter is one parked delivery, as listed by DeadLetters and the
// /dead_letters endpoint.
type DeadLetter struct {
	ID       int64           `json:"id"`
	Runner   string          `json:"runner"`
	Envelope json.RawMessage `json:"envelope"`
	Error    string          `json:"error"`
	Attempts int             `json:"attempts"`
	ParkedAt time.Time       `json:"parked_at"`
}

// DeadLetters lists parked deliveries, oldest first.
func (c *Client) DeadLetters(ctx context.Context, limit int) ([]DeadLetter, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := c.db.Query(ctx, `
		SELECT id, runner, envelope, error, attempts, parked_at
		FROM loom_dead_letters
		WHERE service=$1
		ORDER BY id ASC
		LIMIT $2`,
		c.reg.Service, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeadLetter
	for rows.Next() {
		var d DeadLetter
		if err := rows.Scan(&d.ID, &d.Runner, &d.Envelope, &d.Error, &d.Attempts, &d.ParkedAt); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// RedriveDeadLetter re-runs one parked delivery: the process reacts to the
// parked event again (journaled effects replay, resolved-as-failed ones
// re-run), or a parked timer command re-dispatches. The row is deleted on
// success and kept on failure.
func (c *Client) RedriveDeadLetter(ctx context.Context, id int64) error {
	var runner string
	var raw []byte
	err := c.db.QueryRow(ctx, `
		SELECT runner, envelope FROM loom_dead_letters WHERE service=$1 AND id=$2`,
		c.reg.Service, id).Scan(&runner, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("loom: no dead letter %d", id)
	}
	if err != nil {
		return err
	}

	switch {
	case strings.HasPrefix(runner, "process:"):
		if err := c.redriveProcess(ctx, strings.TrimPrefix(runner, "process:"), raw); err != nil {
			return err
		}
	case runner == "timer":
		var parked struct {
			Key         string          `json:"timer_key"`
			CommandType string          `json:"command_type"`
			Command     json.RawMessage `json:"command"`
		}
		if err := json.Unmarshal(raw, &parked); err != nil {
			return err
		}
		if err := c.fireTimer(ctx, parked.Key, parked.CommandType, parked.Command, nil); err != nil {
			return err
		}
	default:
		return fmt.Errorf("loom: dead letter %d belongs to %s, which has no redrive", id, runner)
	}

	_, err = c.db.Exec(ctx, `DELETE FROM loom_dead_letters WHERE service=$1 AND id=$2`, c.reg.Service, id)
	return err
}

func (c *Client) redriveProcess(ctx context.Context, process string, raw []byte) error {
	var p *ReactorDef
	for _, cand := range c.reg.Processes {
		if cand.Name == process {
			p = cand
		}
	}
	if p == nil {
		return fmt.Errorf("loom: dead letter belongs to unknown process %s", process)
	}
	var parked struct {
		Event
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &parked); err != nil {
		return err
	}
	evt := parked.Event
	payload, err := c.decode(evt.Type, parked.Data)
	if err != nil {
		return err
	}
	evt.Data = payload
	if err := c.react(ctx, p, &evt); err != nil {
		return err
	}
	// foreign events parked instead of being marked processed; mark now so a
	// later redelivery no-ops
	if evt.Service != c.reg.Service {
		return c.markProcessed(ctx, p.Name, fmt.Sprintf("%s:%d", evt.Service, evt.GlobalSeq))
	}
	return nil
}

func (c *Client) readCheckpoint(ctx context.Context, runner string) (int64, error) {
	return checkpoint(ctx, c.db, c.reg.Service, runner)
}

func (c *Client) writeCheckpoint(ctx context.Context, runner string, seq int64) error {
	return saveCheckpoint(ctx, c.db, c.reg.Service, runner, seq)
}

func checkpoint(ctx context.Context, q querier, service, runner string) (int64, error) {
	var seq int64
	err := q.QueryRow(ctx, `
		SELECT global_seq FROM loom_checkpoints WHERE service=$1 AND runner=$2`,
		service, runner).Scan(&seq)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return seq, nil
}

func saveCheckpoint(ctx context.Context, q executor, service, runner string, seq int64) error {
	_, err := q.Exec(ctx, `
		INSERT INTO loom_checkpoints (service, runner, global_seq, updated_at)
		VALUES ($1,$2,$3, now())
		ON CONFLICT (service, runner) DO UPDATE SET global_seq = EXCLUDED.global_seq, updated_at = now()`,
		service, runner, seq)
	return err
}
