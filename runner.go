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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Runners are the async half of the runtime.
//
// Projections and processes subscribed to LOCAL events are checkpointed
// catch-up readers over the service's slice of the global sequence — no bus
// involved, rebuildable by resetting the checkpoint. This kills the old
// world's publish-to-your-own-bus hack for async self-handling. Both are
// elected, so a scaled-out deployment runs one of each at a time: a
// projection step by a transaction-scoped advisory lock (its folds and
// checkpoint are one transaction anyway); the local-event processes by a
// per-service leader holding a session-scoped one (election.go), since a
// reaction is a long external call that must not pin a connection.
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

	// The fan-out reader starts at the current head; anything older is
	// served to lagging runners by their direct-read fallback.
	head, err := c.maxLogSeq(ctx)
	if err != nil {
		return err
	}
	c.fan.setHead(head)
	go c.runReader(ctx, poll)
	if c.hasRetainedSeries() {
		go c.runSeriesMaintenance(ctx, time.Hour)
	}

	for _, p := range c.reg.Projections {
		rw := c.fan.register("projection:"+p.Name, p.Events)
		go c.runLogLoop(ctx, "projection:"+p.Name, poll, rw, c.projectionStep(p))
	}
	var localProcesses bool
	for _, p := range c.reg.Processes {
		local, foreign := c.splitSubscriptions(p)
		if len(local) > 0 {
			localProcesses = true
			if err := c.seedProcessCheckpoint(ctx, p, head); err != nil {
				return err
			}
			rw := c.fan.register("process:"+p.Name, local)
			go c.runLogLoop(ctx, "process:"+p.Name, poll, rw, c.processStep(p, local))
		}
		if len(foreign) > 0 {
			if err := c.subscribeForeign(ctx, p, foreign); err != nil {
				return err
			}
		}
	}
	if localProcesses {
		go c.runElection(ctx, poll)
	}
	return nil
}

// seedProcessCheckpoint fixes a brand-new process's start position
// before its runner can take a step. Without a checkpoint row a process
// reads from sequence 0 — it would react to the service's entire
// history on its first run, which is how a newly deployed process ends
// up re-notifying every member it ever had. The default (@from(head),
// and what an unset From means) seeds the row at the current head, so
// the process reacts to what happens after it exists. @from(origin) is
// the explicit opt-in to replay everything, and a process that has
// checkpointed before — a redeploy, another instance that started
// first — keeps the row it has: the insert never overwrites.
func (c *Client) seedProcessCheckpoint(ctx context.Context, p *ReactorDef, head int64) error {
	if p.From == FromOrigin || head == 0 {
		return nil
	}
	_, err := c.db.Exec(ctx, `
		INSERT INTO loom_checkpoints (service, runner, global_seq, updated_at)
		VALUES ($1,$2,$3, now())
		ON CONFLICT (service, runner) DO NOTHING`,
		c.reg.Service, "process:"+p.Name, head)
	return err
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
// the fan-out reader wakes it (typed: only when events it subscribes to
// were ingested) or its poll tick fires (the LISTEN-failure safety net).
// Steps pass through the client-wide semaphore so a burst of runners
// can't saturate small pools and starve Dispatch (see
// Config.StepConcurrency).
//
// A failing runner is loud but not noisy: `runner step failed` logs at
// most once a minute per runner, carrying the failures since the last
// line, and a stalled projection (stallError) sleeps a backoff derived
// from its attempts instead of retrying on every wake — only an
// operator's skip or rebuild (kick) cuts that short. A runner whose
// checkpoint row still doesn't exist one poll after it started is
// logged as `runner never checkpointed`, under the same ceiling.
func (c *Client) runLogLoop(ctx context.Context, runner string, poll time.Duration, rw *runnerWake, step func(ctx context.Context) (int, error)) {
	t := time.NewTicker(poll)
	defer t.Stop()
	boundedStep := func(ctx context.Context) (int, error) {
		select {
		case c.stepSem <- struct{}{}:
			defer func() { <-c.stepSem }()
			return step(ctx)
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	started := time.Now()
	checkpointed := false
	failed := logLimiter{every: time.Minute}
	silent := logLimiter{every: time.Minute}
	for {
		var backoff time.Duration
		for {
			n, err := boundedStep(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				attrs := []any{"runner", runner, "error", err}
				var stall *stallError
				if errors.As(err, &stall) {
					backoff = stallBackoff(poll, stall.attempts)
					attrs = append(attrs, "failing_seq", stall.seq, "attempts", stall.attempts, "backoff", backoff.String())
				}
				if count, ok := failed.allow(time.Now()); ok {
					c.log.ErrorContext(ctx, "runner step failed", append(attrs, "failures", count)...)
				}
				break
			}
			if n == 0 {
				break
			}
		}
		if !checkpointed && time.Since(started) >= poll {
			if _, exists, err := checkpointRow(ctx, c.db, c.reg.Service, runner); err == nil {
				if exists {
					checkpointed = true
				} else if count, ok := silent.allow(time.Now()); ok {
					c.log.ErrorContext(ctx, "runner never checkpointed", "runner", runner,
						"started", started, "checks", count)
				}
			}
		}
		if backoff > 0 {
			select {
			case <-ctx.Done():
				return
			case <-rw.kick:
			case <-time.After(backoff):
			}
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-rw.ch:
		case <-rw.kick:
		case <-t.C:
		}
	}
}

// logLimiter lets a repeating log line through at most once per every,
// counting the occurrences in between (the one let through included).
// Each runner loop owns its own, so the ceiling is per runner.
type logLimiter struct {
	every time.Duration
	last  time.Time
	count int
}

func (l *logLimiter) allow(now time.Time) (int, bool) {
	l.count++
	if !l.last.IsZero() && now.Sub(l.last) < l.every {
		return 0, false
	}
	n := l.count
	l.last, l.count = now, 0
	return n, true
}

// maxStallBackoff caps how long a stalled projection waits between
// attempts at its failing event.
const maxStallBackoff = 5 * time.Minute

// stallBackoff doubles from twice the poll interval per failed attempt,
// capped at maxStallBackoff.
func stallBackoff(poll time.Duration, attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	d := poll
	for i := 0; i < attempts && d < maxStallBackoff; i++ {
		d *= 2
	}
	return min(d, maxStallBackoff)
}

// stallError is a projection step that halted on one event: the
// checkpoint sits before seq, and the stall is recorded on its row.
type stallError struct {
	runner   string
	seq      int64
	attempts int
	err      error
}

func (e *stallError) Error() string {
	return fmt.Sprintf("%s stalled at seq %d (attempt %d): %v", e.runner, e.seq, e.attempts, e.err)
}

func (e *stallError) Unwrap() error { return e.err }

// runReader is the single per-instance log reader behind logFanout: it
// ingests each new event exactly once (previously every runner re-read
// and re-decrypted the same slice) and issues typed wake-ups. Its poll
// tick is the LISTEN-failure safety net, like every other runner's.
func (c *Client) runReader(ctx context.Context, poll time.Duration) {
	t := time.NewTicker(poll)
	defer t.Stop()
	for {
		for {
			events, err := c.readLog(ctx, c.fan.headSeq(), logBatch)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				c.log.ErrorContext(ctx, "log reader failed", "error", err)
				break
			}
			if len(events) == 0 {
				break
			}
			c.fan.wake(c.fan.ingest(events))
			if len(events) < logBatch {
				break
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-c.readerNudge:
		case <-t.C:
		}
	}
}

// readEvents serves a runner's next batch: from the shared buffer when
// it covers the runner's checkpoint, straight from the log otherwise
// (catch-up after skipped wakes, rebuilds). Buffered events are shared
// pointers, pre-decrypted but undecoded — callers treat them as read-only
// and decode their own subscribed types with decodeEvent.
func (c *Client) readEvents(ctx context.Context, afterSeq int64, limit int) ([]*Event, error) {
	if evts, ok := c.fan.tail(afterSeq, limit); ok {
		return evts, nil
	}
	return c.readLog(ctx, afterSeq, limit)
}

// projectionStep folds one batch into the read model. Entity writes and the
// checkpoint advance share a transaction guarded by the runner's advisory
// lock — exactly-once effects on read models, and rebuild is just a
// checkpoint reset.
//
// An event the projection cannot decode, fold, or write halts it rather
// than being skipped (folding past it would leave the read model wrong
// in ways no later event repairs): the events before it commit, the
// checkpoint stops just short of it, and the row records the stall —
// see haltProjection. The step's stallError backs the runner off, and
// the next step that gets past the event clears the stall.
func (c *Client) projectionStep(p *ProjectionDef) func(ctx context.Context) (int, error) {
	runner := "projection:" + p.Name
	return func(ctx context.Context) (n int, retErr error) {
		tx, err := c.db.Begin(ctx)
		if err != nil {
			return 0, err
		}
		defer tx.Rollback(ctx)

		locked, err := c.tryRunnerLock(ctx, tx, runner)
		if err != nil || !locked {
			return 0, err
		}
		seq, exists, err := checkpointRow(ctx, tx, c.reg.Service, runner)
		if err != nil {
			return 0, err
		}
		events, err := c.readEvents(ctx, seq, logBatch)
		if err != nil {
			return 0, err
		}
		if len(events) == 0 {
			if !exists {
				// idle, but alive: the row is how runLogLoop tells a
				// runner with nothing to do from one that never ran
				if err := seedCheckpoint(ctx, tx, c.reg.Service, runner, seq); err != nil {
					return 0, err
				}
				return 0, tx.Commit(ctx)
			}
			return 0, nil
		}
		// span only when there is work: an idle poll is not a trace
		ctx, end := c.tel.span(ctx, "loom.projection.step",
			attribute.String("loom.runner", runner), attribute.Int("loom.events", len(events)))
		start := nowUTC()
		defer func() {
			end(retErr)
			c.tel.stepDur.Record(ctx, nowUTC().Sub(start).Seconds(),
				metric.WithAttributes(c.tel.service, attribute.String("loom.runner", runner)))
		}()

		if i, err := c.foldEvents(ctx, tx, p, events); err != nil {
			// the failed statement may have aborted the transaction:
			// start clean and commit only the events before the failure
			tx.Rollback(ctx)
			return 0, c.haltProjection(ctx, p, seq, events[:i], events[i], err)
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

func (c *Client) tryRunnerLock(ctx context.Context, tx pgx.Tx, runner string) (bool, error) {
	var locked bool
	err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtext('loom_' || $1 || '_' || $2))`, c.reg.Service, runner).Scan(&locked)
	return locked, err
}

// foldEvents folds the projection's subscribed events of a batch into
// the read model inside tx. On failure it returns the index of the event
// that failed alongside the error.
func (c *Client) foldEvents(ctx context.Context, tx pgx.Tx, p *ProjectionDef, events []*Event) (int, error) {
	table := c.tables[p.Entity] // @table: typed columns, not the doc store
	for i, evt := range events {
		if !contains(p.Events, evt.Type) {
			continue
		}
		if err := c.foldEvent(ctx, tx, p, table, evt); err != nil {
			return i, fmt.Errorf("projection %s at seq %d (%s): %w", p.Name, evt.GlobalSeq, evt.Type, err)
		}
	}
	return 0, nil
}

func (c *Client) foldEvent(ctx context.Context, tx pgx.Tx, p *ProjectionDef, table *tableSQL, evt *Event) error {
	// decode only what this projection subscribes to, into a copy:
	// the batch is shared with every other runner on this instance
	evt, err := c.decodeEvent(evt)
	if err != nil {
		return err
	}
	id := p.EntityID(evt)
	state := p.NewState()
	var data []byte
	if table != nil {
		err = tx.QueryRow(ctx, table.selectForUpdate, c.reg.Service, evt.Namespace, id).Scan(&data)
	} else {
		err = tx.QueryRow(ctx, `
			SELECT data FROM loom_entities
			WHERE service=$1 AND namespace=$2 AND entity_type=$3 AND id=$4
			FOR UPDATE`,
			c.reg.Service, evt.Namespace, p.Entity, id).Scan(&data)
	}
	if err == nil {
		if table == nil { // @table excludes @pii by validation
			if data, err = c.decryptFields(ctx, evt.Namespace, id, data, p.PII); err != nil {
				return fmt.Errorf("entity %s/%s: %w", p.Entity, id, err)
			}
		}
		if err := json.Unmarshal(data, state); err != nil {
			return fmt.Errorf("entity %s/%s: %w", p.Entity, id, err)
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	fold := func() error { return state.Fold(evt.Type, evt.Data) }
	if p.Fold != nil { // @fold: the hand-written fold
		fold = func() error { return p.Fold(state, evt) }
	}
	if err := fold(); err != nil {
		return fmt.Errorf("fold: %w", err)
	}
	if table != nil {
		args := append([]any{c.reg.Service, evt.Namespace, id}, table.def.Values(state)...)
		_, err := tx.Exec(ctx, table.upsert, args...)
		return err
	}
	out, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if out, err = c.encryptFields(ctx, evt.Namespace, id, out, p.PII); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO loom_entities (service, namespace, entity_type, id, data, updated_at)
		VALUES ($1,$2,$3,$4,$5, now())
		ON CONFLICT (service, namespace, entity_type, id)
		DO UPDATE SET data = EXCLUDED.data, updated_at = now()`,
		c.reg.Service, evt.Namespace, p.Entity, id, out)
	return err
}

// haltProjection stalls a projection on the event it cannot get past. In
// a fresh transaction (the step's own may be aborted) it refolds the
// events before the failing one, checkpoints just short of it, and
// records the stall on the checkpoint row: failing_seq, attempts (reset
// when a different event fails), last_error, and stalled_since, set on
// the first failure only. The refold is the cost of a failure, not of
// every step: the happy path pays no savepoint per event.
func (c *Client) haltProjection(ctx context.Context, p *ProjectionDef, from int64, before []*Event, failing *Event, cause error) error {
	runner := "projection:" + p.Name
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return errors.Join(cause, err)
	}
	defer tx.Rollback(ctx)
	locked, err := c.tryRunnerLock(ctx, tx, runner)
	if err != nil {
		return errors.Join(cause, err)
	}
	if !locked {
		return cause // another instance took the step; it records its own
	}
	if seq, _, err := checkpointRow(ctx, tx, c.reg.Service, runner); err != nil {
		return errors.Join(cause, err)
	} else if seq != from {
		return cause // moved under us (rebuild, skip): the next step decides
	}
	seq := from
	if len(before) > 0 {
		if _, err := c.foldEvents(ctx, tx, p, before); err != nil {
			return err // a prefix that folded a moment ago doesn't now: retry
		}
		seq = before[len(before)-1].GlobalSeq
	}
	var attempts int
	err = tx.QueryRow(ctx, `
		INSERT INTO loom_checkpoints AS c (service, runner, global_seq, failing_seq, attempts, last_error, stalled_since, updated_at)
		VALUES ($1,$2,$3,$4,1,$5, now(), now())
		ON CONFLICT (service, runner) DO UPDATE SET
			global_seq    = EXCLUDED.global_seq,
			attempts      = CASE WHEN c.failing_seq = EXCLUDED.failing_seq THEN c.attempts + 1 ELSE 1 END,
			failing_seq   = EXCLUDED.failing_seq,
			last_error    = EXCLUDED.last_error,
			stalled_since = coalesce(c.stalled_since, now()),
			updated_at    = now()
		RETURNING attempts`,
		c.reg.Service, runner, seq, failing.GlobalSeq, cause.Error()).Scan(&attempts)
	if err != nil {
		return errors.Join(cause, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return errors.Join(cause, err)
	}
	return &stallError{runner: runner, seq: failing.GlobalSeq, attempts: attempts, err: cause}
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
	if table := c.tables[def.Entity]; table != nil {
		if _, err := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE service=$1`, table.def.Name), c.reg.Service); err != nil {
			return err
		}
	} else if _, err := tx.Exec(ctx, `DELETE FROM loom_entities WHERE service=$1 AND entity_type=$2`, c.reg.Service, def.Entity); err != nil {
		return err
	}
	if err := saveCheckpoint(ctx, tx, c.reg.Service, "projection:"+projection, 0); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	// the rebuilt projection re-reads from seq 0 (post-shred rebuilds
	// must not fold pre-shred buffered plaintext); wake everyone — only
	// the runner whose checkpoint moved has work
	c.fan.flush()
	c.fan.wakeAll()
	c.fan.kickRunner("projection:" + projection) // out of any stall backoff
	return nil
}

// ErrNotStalled is returned by SkipStalled for a projection that is not
// halted on an event.
var ErrNotStalled = errors.New("loom: projection is not stalled")

// SkipStalled moves a stalled projection past the event it halted on:
// the event is parked to loom_dead_letters (with the stall's error and
// attempts), the checkpoint advances to its seq, and the stall clears —
// one transaction, under the runner's advisory lock, so it never races a
// step. The read model never sees the skipped event; a parked
// projection event has no redrive — fix the fold and Rebuild instead.
// runner is the projection's name, with or without the "projection:"
// prefix. Returns the skipped event's global_seq.
func (c *Client) SkipStalled(ctx context.Context, runner string) (int64, error) {
	name := strings.TrimPrefix(runner, "projection:")
	var def *ProjectionDef
	for _, p := range c.reg.Projections {
		if p.Name == name {
			def = p
		}
	}
	if def == nil {
		return 0, fmt.Errorf("loom: unknown projection %s", runner)
	}
	runner = "projection:" + name

	tx, err := c.db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	// wait out an in-flight step rather than failing beside it
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('loom_' || $1 || '_' || $2))`, c.reg.Service, runner); err != nil {
		return 0, err
	}
	var failingSeq int64
	var attempts int
	var lastError string
	err = tx.QueryRow(ctx, `
		SELECT failing_seq, attempts, last_error FROM loom_checkpoints
		WHERE service=$1 AND runner=$2`,
		c.reg.Service, runner).Scan(&failingSeq, &attempts, &lastError)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && failingSeq == 0) {
		return 0, fmt.Errorf("%w: %s", ErrNotStalled, runner)
	}
	if err != nil {
		return 0, err
	}

	evts, err := c.readLog(ctx, failingSeq-1, 1)
	if err != nil {
		return 0, err
	}
	if len(evts) == 0 || evts[0].GlobalSeq != failingSeq {
		return 0, fmt.Errorf("loom: %s stalled at seq %d, which is not in the log", runner, failingSeq)
	}
	evt, err := c.decodeEvent(evts[0])
	if err != nil { // undecodable is one way to stall: park the raw payload
		raw := *evts[0]
		raw.Data = json.RawMessage(raw.raw)
		evt = &raw
	}
	if err := c.parkOn(ctx, tx, runner, evt, errors.New(lastError), attempts); err != nil {
		return 0, err
	}
	if err := saveCheckpoint(ctx, tx, c.reg.Service, runner, failingSeq); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	c.fan.kickRunner(runner)
	return failingSeq, nil
}

// processStep runs a local-event process from the log: react, dispatch (its
// own unit of work), advance the checkpoint. Failures retry, then park the
// event to dead letters (or, under @retry, re-arm it as a durable timer —
// see retry.go) and move on — at-least-once, no head-of-line block.
// Steps only while this instance leads the service's processes (see
// election.go): one instance reacts at a time, and the reaction runs on
// no pinned connection — a process may be a long external call.
func (c *Client) processStep(p *ReactorDef, local []string) func(ctx context.Context) (int, error) {
	runner := "process:" + p.Name
	return func(ctx context.Context) (n int, retErr error) {
		if !c.leading() {
			return 0, nil // a follower: the leader advances the checkpoint
		}
		seq, exists, err := checkpointRow(ctx, c.db, c.reg.Service, runner)
		if err != nil {
			return 0, err
		}
		events, err := c.readEvents(ctx, seq, logBatch)
		if err != nil {
			return 0, err
		}
		if len(events) == 0 {
			if !exists { // idle, but alive (see projectionStep)
				return 0, seedCheckpoint(ctx, c.db, c.reg.Service, runner, seq)
			}
			return 0, nil
		}
		ctx, end := c.tel.span(ctx, "loom.process.step",
			attribute.String("loom.runner", runner), attribute.Int("loom.events", len(events)))
		start := nowUTC()
		defer func() {
			end(retErr)
			c.tel.stepDur.Record(ctx, nowUTC().Sub(start).Seconds(),
				metric.WithAttributes(c.tel.service, attribute.String("loom.runner", runner)))
		}()
		for _, evt := range events {
			if contains(local, evt.Type) {
				// same as the projection: decode this process's own types
				// only, and into a copy of the shared event
				decoded, err := c.decodeEvent(evt)
				if err != nil {
					return 0, err
				}
				// a durable retry already owns this event (@retry): the
				// redelivery converges on it rather than reacting twice
				pending, err := c.retryPending(ctx, p, decoded)
				if err != nil {
					return 0, err
				}
				if !pending {
					if err := c.reactWithRetry(ctx, p, decoded); err != nil {
						if err := c.exhausted(ctx, runner, p, decoded, err); err != nil {
							return 0, err
						}
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
	ctx = withReader(ctx, c)
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
// dedup, react with retries, park (or durably retry, under @retry) on
// exhaustion.
func (c *Client) subscribeForeign(ctx context.Context, p *ReactorDef, foreign []string) error {
	group := c.reg.Service + "." + p.Name
	return c.bus.Subscribe(ctx, group, func(ctx context.Context, env *Envelope) (retErr error) {
		if !contains(foreign, env.Type) {
			return nil // not ours; other services' events share the bus
		}
		// join the trace of the dispatch that published this event
		ctx, end := c.tel.span(extractTrace(ctx, env), "loom.consume",
			append(metaAttrs(env.Meta),
				attribute.String("loom.process", p.Name), attribute.String("loom.event", env.Type),
				attribute.String("loom.source", env.Service), attribute.Int64("loom.global_seq", env.GlobalSeq))...)
		defer func() { end(retErr) }()

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
			c.tel.count(ctx, c.tel.dedupHits, 1, attribute.String("loom.process", p.Name))
			return nil
		}
		if pending, err := c.retryPending(ctx, p, evt); err != nil || pending {
			return err // a durable retry owns it, and marks it processed
		}
		if err := c.reactWithRetry(ctx, p, evt); err != nil {
			return c.exhausted(ctx, "process:"+p.Name, p, evt, err)
		}
		return c.markProcessed(ctx, p.Name, key)
	})
}

func (c *Client) eventFromEnvelope(env *Envelope) (*Event, error) {
	payload, err := c.decode(env.Type, env.SchemaVersion, env.Data)
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
	return c.markProcessedOn(ctx, c.db, process, key)
}

// markProcessedOn is markProcessed on a given connection or transaction.
func (c *Client) markProcessedOn(ctx context.Context, q executor, process, key string) error {
	_, err := q.Exec(ctx, `
		INSERT INTO loom_dedup (service, process, event_key) VALUES ($1,$2,$3)
		ON CONFLICT DO NOTHING`,
		c.reg.Service, process, key)
	return err
}

// park writes a dead letter. Parked events are queryable (and re-drivable)
// via the console; parking is loud, dropping is impossible. @pii payload
// fields are re-sealed — dead letters are at rest too.
func (c *Client) park(ctx context.Context, runner string, evt *Event, cause error) error {
	return c.parkOn(ctx, c.db, runner, evt, cause, processRetries)
}

// parkOn is park on a given connection or transaction, with the attempts
// the runner actually made.
func (c *Client) parkOn(ctx context.Context, q executor, runner string, evt *Event, cause error, attempts int) error {
	raw := c.sealedEnvelope(ctx, evt)
	c.log.ErrorContext(ctx, "parking event to dead letters", "runner", runner, "type", evt.Type, "error", cause)
	c.tel.count(ctx, c.tel.parked, 1, attribute.String("loom.runner", runner))
	_, err := q.Exec(ctx, `
		INSERT INTO loom_dead_letters (service, runner, envelope, error, attempts)
		VALUES ($1,$2,$3,$4,$5)`,
		c.reg.Service, runner, raw, cause.Error(), attempts)
	return err
}

// sealedEnvelope is an event as it rests outside the log — a dead letter
// or a durable retry — with its @pii payload fields re-sealed. openEnvelope
// reads it back.
func (c *Client) sealedEnvelope(ctx context.Context, evt *Event) []byte {
	parked := evt
	if def := c.reg.eventDef(evt.Type); def != nil && len(def.PII) > 0 && evt.Data != nil {
		if dataRaw, err := json.Marshal(evt.Data); err == nil {
			if dataRaw, err = c.encryptFields(ctx, evt.Namespace, evt.AggregateID, dataRaw, def.PII); err == nil {
				copied := *evt
				copied.Data = json.RawMessage(dataRaw)
				parked = &copied
			}
		}
	}
	raw, err := json.Marshal(parked)
	if err != nil {
		raw = []byte(fmt.Sprintf(`{"type":%q}`, evt.Type))
	}
	return raw
}

// openEnvelope decodes a sealedEnvelope back into a typed event.
func (c *Client) openEnvelope(ctx context.Context, raw []byte) (*Event, error) {
	var parked struct {
		Event
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &parked); err != nil {
		return nil, err
	}
	evt := parked.Event
	data, err := c.decryptEventData(ctx, evt.Namespace, evt.AggregateID, evt.Type, parked.Data)
	if err != nil {
		return nil, err
	}
	if evt.Data, err = c.decode(evt.Type, evt.SchemaVersion, data); err != nil {
		return nil, err
	}
	return &evt, nil
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
	p := c.process(process)
	if p == nil {
		return fmt.Errorf("loom: dead letter belongs to unknown process %s", process)
	}
	evt, err := c.openEnvelope(ctx, raw)
	if err != nil {
		return err
	}
	if err := c.react(ctx, p, evt); err != nil {
		return err
	}
	// foreign events parked instead of being marked processed; mark now so a
	// later redelivery no-ops
	if evt.Service != c.reg.Service {
		return c.markProcessed(ctx, p.Name, fmt.Sprintf("%s:%d", evt.Service, evt.GlobalSeq))
	}
	return nil
}

// process finds a registered process by name; nil when none.
func (c *Client) process(name string) *ReactorDef {
	for _, p := range c.reg.Processes {
		if p.Name == name {
			return p
		}
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
	seq, _, err := checkpointRow(ctx, q, service, runner)
	return seq, err
}

// checkpointRow reads a runner's checkpoint and whether its row exists.
func checkpointRow(ctx context.Context, q querier, service, runner string) (int64, bool, error) {
	var seq int64
	err := q.QueryRow(ctx, `
		SELECT global_seq FROM loom_checkpoints WHERE service=$1 AND runner=$2`,
		service, runner).Scan(&seq)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return seq, true, nil
}

// saveCheckpoint advances (or resets) a runner's checkpoint. Any write
// through here means the runner got past where it was, so it clears a
// projection's stall.
func saveCheckpoint(ctx context.Context, q executor, service, runner string, seq int64) error {
	_, err := q.Exec(ctx, `
		INSERT INTO loom_checkpoints (service, runner, global_seq, updated_at)
		VALUES ($1,$2,$3, now())
		ON CONFLICT (service, runner) DO UPDATE SET global_seq = EXCLUDED.global_seq, updated_at = now(),
			failing_seq = 0, attempts = 0, last_error = '', stalled_since = NULL`,
		service, runner, seq)
	return err
}

// seedCheckpoint writes a runner's first row without moving an existing
// one.
func seedCheckpoint(ctx context.Context, q executor, service, runner string, seq int64) error {
	_, err := q.Exec(ctx, `
		INSERT INTO loom_checkpoints (service, runner, global_seq, updated_at)
		VALUES ($1,$2,$3, now())
		ON CONFLICT (service, runner) DO NOTHING`,
		service, runner, seq)
	return err
}

// hasRetainedSeries reports whether any series declares @retain; only
// then does the runner carry a maintenance loop.
func (c *Client) hasRetainedSeries() bool {
	for _, ss := range c.series {
		if ss.def.RetainDays > 0 {
			return true
		}
	}
	return false
}

// runSeriesMaintenance keeps @retain series' day partitions created
// ahead and drops the expired ones (plain Postgres; a no-op under a
// TimescaleDB retention policy). Every instance runs it — the work is
// idempotent (CREATE/DROP IF EXISTS), so two runners crossing is
// harmless — and a failure just logs and waits for the next tick.
func (c *Client) runSeriesMaintenance(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		if err := c.MaintainSeries(ctx); err != nil && ctx.Err() == nil {
			c.log.ErrorContext(ctx, "series maintenance failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
