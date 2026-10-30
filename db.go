package loom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/attribute"
)

// Storage schema v2. Fixed shape, hand-written SQL, no ORM. The global
// sequence is the backbone: projections and local processes are checkpointed
// catch-up readers over it, which is what makes them rebuildable.
const ddl = `
CREATE TABLE IF NOT EXISTS loom_events (
	global_seq     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	service        text NOT NULL,
	namespace      text NOT NULL,
	aggregate_type text NOT NULL,
	aggregate_id   uuid NOT NULL,
	version        int NOT NULL,
	type           text NOT NULL,
	schema_version int NOT NULL DEFAULT 1,
	at             timestamptz NOT NULL DEFAULT now(),
	correlation_id text NOT NULL DEFAULT '',
	causation_id   text NOT NULL DEFAULT '',
	actor          text NOT NULL DEFAULT '',
	data           jsonb NOT NULL,
	CONSTRAINT loom_events_stream UNIQUE (service, namespace, aggregate_type, aggregate_id, version)
);
CREATE INDEX IF NOT EXISTS loom_events_by_service_seq ON loom_events (service, global_seq);
-- the log is append-only and time-ordered: BRIN makes time-range scans
-- cheap at negligible write/storage cost
CREATE INDEX IF NOT EXISTS loom_events_at_brin ON loom_events USING brin (at);
CREATE INDEX IF NOT EXISTS loom_events_correlation ON loom_events (service, correlation_id);

CREATE TABLE IF NOT EXISTS loom_snapshots (
	service        text NOT NULL,
	namespace      text NOT NULL,
	aggregate_type text NOT NULL,
	aggregate_id   uuid NOT NULL,
	version        int NOT NULL,
	state          jsonb NOT NULL,
	PRIMARY KEY (service, namespace, aggregate_type, aggregate_id)
);

CREATE TABLE IF NOT EXISTS loom_outbox (
	id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	service      text NOT NULL,
	ordering_key text NOT NULL,
	envelope     jsonb NOT NULL,
	created_at   timestamptz NOT NULL DEFAULT now(),
	published_at timestamptz
);
CREATE INDEX IF NOT EXISTS loom_outbox_unpublished ON loom_outbox (service, id) WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS loom_entities (
	service     text NOT NULL,
	namespace   text NOT NULL,
	entity_type text NOT NULL,
	id          uuid NOT NULL,
	data        jsonb NOT NULL,
	updated_at  timestamptz NOT NULL DEFAULT now(),
	PRIMARY KEY (service, namespace, entity_type, id)
);

CREATE INDEX IF NOT EXISTS loom_entities_data ON loom_entities USING gin (data jsonb_path_ops);

CREATE TABLE IF NOT EXISTS loom_checkpoints (
	service    text NOT NULL,
	runner     text NOT NULL,
	global_seq bigint NOT NULL,
	updated_at timestamptz NOT NULL DEFAULT now(),
	PRIMARY KEY (service, runner)
);
-- a projection halted on an event it cannot fold: the event's seq, how
-- many steps have failed on it, the last error, and when it first did
ALTER TABLE loom_checkpoints ADD COLUMN IF NOT EXISTS failing_seq bigint NOT NULL DEFAULT 0;
ALTER TABLE loom_checkpoints ADD COLUMN IF NOT EXISTS attempts int NOT NULL DEFAULT 0;
ALTER TABLE loom_checkpoints ADD COLUMN IF NOT EXISTS last_error text NOT NULL DEFAULT '';
ALTER TABLE loom_checkpoints ADD COLUMN IF NOT EXISTS stalled_since timestamptz;

CREATE TABLE IF NOT EXISTS loom_dedup (
	service   text NOT NULL,
	process   text NOT NULL,
	event_key text NOT NULL,
	at        timestamptz NOT NULL DEFAULT now(),
	PRIMARY KEY (service, process, event_key)
);

CREATE TABLE IF NOT EXISTS loom_records (
	service     text NOT NULL,
	namespace   text NOT NULL,
	record_type text NOT NULL,
	id          uuid NOT NULL,
	version     int NOT NULL,
	data        jsonb NOT NULL,
	updated_at  timestamptz NOT NULL DEFAULT now(),
	PRIMARY KEY (service, namespace, record_type, id)
);

CREATE TABLE IF NOT EXISTS loom_timers (
	service      text NOT NULL,
	namespace    text NOT NULL,
	key          text NOT NULL,
	command_type text NOT NULL,
	command      jsonb NOT NULL,
	meta         jsonb NOT NULL,
	fire_at      timestamptz NOT NULL,
	created_at   timestamptz NOT NULL DEFAULT now(),
	PRIMARY KEY (service, key)
);
CREATE INDEX IF NOT EXISTS loom_timers_due ON loom_timers (service, fire_at);

CREATE TABLE IF NOT EXISTS loom_batches (
	service    text NOT NULL,
	namespace  text NOT NULL,
	id         uuid NOT NULL,
	status     text NOT NULL,
	total      int NOT NULL,
	done       int NOT NULL DEFAULT 0,
	failed     int NOT NULL DEFAULT 0,
	created_at timestamptz NOT NULL DEFAULT now(),
	updated_at timestamptz NOT NULL DEFAULT now(),
	PRIMARY KEY (service, id)
);

CREATE TABLE IF NOT EXISTS loom_batch_items (
	service      text NOT NULL,
	batch_id     uuid NOT NULL,
	seq          int NOT NULL,
	command_type text NOT NULL,
	command      jsonb NOT NULL,
	status       text NOT NULL DEFAULT 'pending',
	error        text NOT NULL DEFAULT '',
	claimed_at   timestamptz,
	PRIMARY KEY (service, batch_id, seq)
);
CREATE INDEX IF NOT EXISTS loom_batch_items_pending ON loom_batch_items (service, batch_id, seq) WHERE status IN ('pending','working');

CREATE TABLE IF NOT EXISTS loom_keys (
	service     text NOT NULL,
	namespace   text NOT NULL,
	stream_id   uuid NOT NULL,
	wrapped_dek bytea NOT NULL,
	created_at  timestamptz NOT NULL DEFAULT now(),
	PRIMARY KEY (service, namespace, stream_id)
);

CREATE TABLE IF NOT EXISTS loom_effects (
	service    text NOT NULL,
	scope      text NOT NULL,
	key        text NOT NULL,
	status     text NOT NULL,
	result     jsonb,
	error      text NOT NULL DEFAULT '',
	attempts   int NOT NULL DEFAULT 1,
	started_at timestamptz NOT NULL DEFAULT now(),
	settled_at timestamptz,
	PRIMARY KEY (service, scope, key)
);
CREATE INDEX IF NOT EXISTS loom_effects_unsettled ON loom_effects (service, status) WHERE status <> 'done';

CREATE TABLE IF NOT EXISTS loom_dead_letters (
	id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	service   text NOT NULL,
	runner    text NOT NULL,
	envelope  jsonb NOT NULL,
	error     text NOT NULL,
	attempts  int NOT NULL,
	parked_at timestamptz NOT NULL DEFAULT now()
);
`

// Migrate applies the storage schema, including any @table entity tables
// with their additive column diff and their declared indexes. Idempotent; meant for a deploy-time
// migration endpoint or command, never for application boot.
func (c *Client) Migrate(ctx context.Context) error {
	if _, err := c.db.Exec(ctx, ddl); err != nil {
		return err
	}
	if err := c.migrateTables(ctx); err != nil {
		return err
	}
	// CREATE INDEX CONCURRENTLY refuses a transaction: declared @table
	// indexes go after the table DDL, each on its own connection
	if err := c.migrateIndexes(ctx); err != nil {
		return err
	}
	if err := c.migrateSeries(ctx); err != nil {
		return err
	}
	return c.checkLogTypes(ctx)
}

// checkLogTypes refuses a migration whose registry cannot name every event
// type already in this service's log. Runners decode lazily now, so an
// undeclared type is invisible until some subscriber trips on it — this
// is the one place it can be said loudly and once, at deploy time, rather
// than forever in a runner's error log. The remedy is to declare the type:
// `event X @retired` keeps old rows decodable without letting anything
// produce or subscribe to X again.
func (c *Client) checkLogTypes(ctx context.Context) error {
	rows, err := c.db.Query(ctx,
		`SELECT DISTINCT type FROM loom_events WHERE service=$1`, c.reg.Service)
	if err != nil {
		return err
	}
	defer rows.Close()
	var unknown []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return err
		}
		if c.reg.eventDef(t) == nil {
			unknown = append(unknown, t)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(unknown) == 0 {
		return nil
	}
	sort.Strings(unknown)
	return fmt.Errorf("loom: %s log holds event types this build does not declare: %s — declare each one (`event %s @retired` if nothing should produce it again) and migrate again",
		c.reg.Service, strings.Join(unknown, ", "), unknown[0])
}

type storedEvent struct {
	Event
	dataRaw []byte
}

// appendEvents writes a batch for one aggregate inside tx. The stream's
// UNIQUE constraint is the optimistic-concurrency guard: a losing racer
// gets a typed ConflictError.
//
// The service-wide xact lock serializes appends so global_seq order ==
// commit order. Without it, a later-seq transaction committing before an
// earlier-seq one lets runners read past the still-invisible earlier
// event and advance their checkpoints over it — skipped forever. The
// lock holds only to commit; re-acquiring in the same tx (multi-command
// dispatches) is a no-op. Should one-writer-at-a-time per service ever
// bind, the escape hatch is a commit-visibility barrier on the read
// side, not dropping this lock.
func (c *Client) appendEvents(ctx context.Context, tx pgx.Tx, evts []*Event) error {
	if err := c.lockLog(ctx, tx); err != nil {
		return err
	}
	for _, e := range evts {
		data, err := json.Marshal(e.Data)
		if err != nil {
			return fmt.Errorf("marshal %s: %w", e.Type, err)
		}
		if def := c.reg.eventDef(e.Type); def != nil && len(def.PII) > 0 {
			if data, err = c.encryptFields(ctx, tx, e.Namespace, e.AggregateID, data, def.PII); err != nil {
				return err
			}
		}
		err = tx.QueryRow(ctx, `
			INSERT INTO loom_events (service, namespace, aggregate_type, aggregate_id, version, type, schema_version, at, correlation_id, causation_id, actor, data)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
			RETURNING global_seq`,
			e.Service, e.Namespace, e.AggregateType, e.AggregateID, e.Version, e.Type, e.SchemaVersion, e.At, e.Meta.CorrelationID, e.Meta.CausationID, e.Meta.Actor, data,
		).Scan(&e.GlobalSeq)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "loom_events_stream" {
				return &ConflictError{AggregateType: e.AggregateType, AggregateID: e.AggregateID, Version: e.Version}
			}
			return err
		}
		c.tel.count(ctx, c.tel.appended, 1, attribute.String("loom.event", e.Type))
	}
	return nil
}

// defaultLogLockWait is Config.LogLockWait's default.
const defaultLogLockWait = 15 * time.Second

// lockLog takes the service's log lock for tx, waiting at most
// c.logLockWait. Its own deadline is the guard: a unit of work that holds
// the lock while it waits on something outside its transaction (a pooled
// connection, a mutex, an HTTP call) would otherwise hang every dispatch
// behind it, each holding a pooled connection, until nothing in the
// service that touches the database answers (mesh-api, 2026-10-30). Past
// the wait the statement is cancelled, the dispatch fails with a
// *LogLockError, and its connection goes back to the pool, which is
// also what lets a holder waiting on the pool go on.
func (c *Client) lockLog(ctx context.Context, tx pgx.Tx) error {
	lctx, cancel := context.WithTimeout(ctx, c.logLockWait)
	defer cancel()
	_, err := tx.Exec(lctx, `SELECT pg_advisory_xact_lock(hashtext('loom_log_' || $1))`, c.reg.Service)
	if err == nil {
		return nil
	}
	if ctx.Err() == nil && errors.Is(lctx.Err(), context.DeadlineExceeded) {
		// who holds it is read once this unit's connection is back in
		// the pool (Dispatch), never while it holds one
		return &LogLockError{Service: c.reg.Service, Waited: c.logLockWait}
	}
	return err
}

// LogLockError is a dispatch that waited Config.LogLockWait for the
// service's log lock and gave up. Holder is the session that held it, as
// far as it could be read.
type LogLockError struct {
	Service string
	Waited  time.Duration
	Holder  LockHolder
}

// LockHolder is the Postgres session holding a lock: its pid, state
// ("idle in transaction" is a unit of work waiting on something outside
// its transaction), when its transaction began and its last statement.
type LockHolder struct {
	PID   int
	State string
	Since time.Time
	Query string
}

func (e *LogLockError) Error() string {
	h := "an unknown session"
	if e.Holder.PID != 0 {
		h = fmt.Sprintf("pid %d (%s since %s)", e.Holder.PID, e.Holder.State, e.Holder.Since.UTC().Format(time.RFC3339))
	}
	return fmt.Sprintf("loom: waited %s for %s's log lock, held by %s: a unit of work is holding it while it waits on something outside its transaction", e.Waited, e.Service, h)
}

// logLockHolder reads who holds the service's log lock, best effort and
// briefly, holding no other connection: a starved pool may have none to
// give it, and then it names no one.
func (c *Client) logLockHolder() LockHolder {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var h LockHolder
	var since *time.Time
	var state, query *string
	err := c.db.QueryRow(ctx, `
		SELECT a.pid, a.state, a.xact_start, left(a.query, 200)
		FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
		WHERE l.locktype = 'advisory' AND l.granted
		  AND l.objid = (hashtext('loom_log_' || $1)::bigint & 4294967295)::oid
		  AND l.classid = ((hashtext('loom_log_' || $1)::bigint >> 32) & 4294967295)::oid
		  AND l.objsubid = 1
		LIMIT 1`, c.reg.Service).Scan(&h.PID, &state, &since, &query)
	if err != nil {
		return LockHolder{}
	}
	if state != nil {
		h.State = *state
	}
	if since != nil {
		h.Since = *since
	}
	if query != nil {
		h.Query = *query
	}
	return h
}

// loadState folds an aggregate from its snapshot (if any) plus subsequent
// events, in explicit version order. Returns the current version.
func (c *Client) loadState(ctx context.Context, q querier, agg *AggregateDef, namespace string, id uuid.UUID) (AggregateState, int, error) {
	state := agg.NewState()
	version := 0

	var snap []byte
	var snapVersion int
	err := q.QueryRow(ctx, `
		SELECT version, state FROM loom_snapshots
		WHERE service=$1 AND namespace=$2 AND aggregate_type=$3 AND aggregate_id=$4`,
		c.reg.Service, namespace, agg.Name, id,
	).Scan(&snapVersion, &snap)
	switch {
	case err == nil:
		if snap, err = c.decryptFields(ctx, q, namespace, id, snap, agg.StatePII); err != nil {
			return nil, 0, fmt.Errorf("snapshot %s/%s: %w", agg.Name, id, err)
		}
		if err := json.Unmarshal(snap, state); err != nil {
			return nil, 0, fmt.Errorf("snapshot %s/%s: %w", agg.Name, id, err)
		}
		version = snapVersion
	case errors.Is(err, pgx.ErrNoRows):
	default:
		return nil, 0, err
	}

	rows, err := q.Query(ctx, `
		SELECT version, type, schema_version, data FROM loom_events
		WHERE service=$1 AND namespace=$2 AND aggregate_type=$3 AND aggregate_id=$4 AND version > $5
		ORDER BY version ASC`,
		c.reg.Service, namespace, agg.Name, id, version,
	)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	type stored struct {
		v, sv int
		typ   string
		data  []byte
	}
	var evts []stored
	for rows.Next() {
		var e stored
		if err := rows.Scan(&e.v, &e.typ, &e.sv, &e.data); err != nil {
			return nil, 0, err
		}
		evts = append(evts, e)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	// Decrypt after the rows are closed: on a transaction a key read
	// cannot run while rows are open, and on the pool it would take a
	// second connection while holding the first (see readLog).
	rows.Close()
	for _, e := range evts {
		data, err := c.decryptEventData(ctx, q, namespace, id, e.typ, e.data)
		if err != nil {
			return nil, 0, err
		}
		payload, err := c.decode(e.typ, e.sv, data)
		if err != nil {
			return nil, 0, err
		}
		if err := state.Fold(e.typ, payload); err != nil {
			return nil, 0, fmt.Errorf("fold %s at version %d: %w", e.typ, e.v, err)
		}
		version = e.v
	}
	return state, version, nil
}

// decode turns a stored payload into the typed current-version struct,
// chaining upcast hops when the stored schema_version is behind. Events
// with no declared upcasts keep the permissive behavior: any version
// unmarshals as-is (additive changes stay free).
func (c *Client) decode(eventType string, storedVersion int, data []byte) (any, error) {
	def := c.reg.eventDef(eventType)
	if def == nil {
		return nil, &UnknownEventError{Type: eventType}
	}
	if len(def.Upcasts) > 0 {
		v := storedVersion
		if v == 0 {
			v = 1 // rows from before @v existed
		}
		if v > def.SchemaVersion {
			return nil, &UpcastError{Type: def.Name, Stored: v, To: def.SchemaVersion}
		}
		for ; v < def.SchemaVersion; v++ {
			fn := def.Upcasts[v]
			if fn == nil {
				return nil, &UpcastError{Type: def.Name, Stored: storedVersion, At: v, To: def.SchemaVersion}
			}
			out, err := fn(data)
			if err != nil {
				return nil, &UpcastError{Type: def.Name, Stored: storedVersion, At: v, To: def.SchemaVersion, Err: err}
			}
			data = out
		}
	}
	payload := def.New()
	if err := json.Unmarshal(data, payload); err != nil {
		return nil, fmt.Errorf("decode %s: %w", eventType, err)
	}
	return payload, nil
}

// decodeEvent decodes a log-read event's payload, returning a COPY with
// Data set. Buffered events are shared by every runner on this instance —
// decoding must stay local to the caller, never a write to the shared
// pointer. Events that already carry a payload (bus deliveries, redrives)
// pass through untouched.
func (c *Client) decodeEvent(evt *Event) (*Event, error) {
	if evt.Data != nil {
		return evt, nil
	}
	payload, err := c.decode(evt.Type, evt.SchemaVersion, evt.raw)
	if err != nil {
		return nil, err
	}
	local := *evt
	local.Data = payload
	return &local, nil
}

func (c *Client) saveSnapshot(ctx context.Context, tx pgx.Tx, agg *AggregateDef, namespace string, id uuid.UUID, state AggregateState, version int) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if data, err = c.encryptFields(ctx, tx, namespace, id, data, agg.StatePII); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO loom_snapshots (service, namespace, aggregate_type, aggregate_id, version, state)
		VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (service, namespace, aggregate_type, aggregate_id)
		DO UPDATE SET version = EXCLUDED.version, state = EXCLUDED.state`,
		c.reg.Service, namespace, agg.Name, id, version, data)
	return err
}

// readLog returns up to limit events after seq for this service — the
// catch-up read that feeds projection and local process runners. Rows are
// decrypted but NOT decoded: the payload rides Event.raw and each runner
// decodes only the types it subscribes to (see decodeEvent). Eager decode
// here meant one row of an undeclared or @retired type failed the whole
// batch — for every runner sharing it through the fan-out buffer, even
// the ones that never look at that type.
func (c *Client) readLog(ctx context.Context, q querier, afterSeq int64, limit int) ([]*Event, error) {
	rows, err := q.Query(ctx, `
		SELECT global_seq, namespace, aggregate_type, aggregate_id, version, type, schema_version, at, correlation_id, causation_id, actor, data
		FROM loom_events
		WHERE service=$1 AND global_seq > $2
		ORDER BY global_seq ASC
		LIMIT $3`,
		c.reg.Service, afterSeq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*Event
	for rows.Next() {
		e := &Event{Service: c.reg.Service}
		if err := rows.Scan(&e.GlobalSeq, &e.Namespace, &e.AggregateType, &e.AggregateID, &e.Version, &e.Type, &e.SchemaVersion, &e.At, &e.Meta.CorrelationID, &e.Meta.CausationID, &e.Meta.Actor, &e.raw); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Decrypt only once the rows are closed, on the same q: a key read
	// with the rows still open would wait for a second pooled connection
	// while holding the first, and with every connection held that way
	// the pool deadlocks.
	rows.Close()
	for _, e := range out {
		if e.raw, err = c.decryptEventData(ctx, q, e.Namespace, e.AggregateID, e.Type, e.raw); err != nil {
			return nil, err
		}
	}
	return out, nil
}

type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type executor interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// dbConn is the pool or a transaction on it, where a write may also seal.
type dbConn interface {
	querier
	executor
}

// maxLogSeq is the reader's starting high-water mark: everything at or
// below it is served to lagging runners by direct log reads.
func (c *Client) maxLogSeq(ctx context.Context) (int64, error) {
	var seq int64
	err := c.db.QueryRow(ctx,
		`SELECT COALESCE(MAX(global_seq), 0) FROM loom_events WHERE service=$1`,
		c.reg.Service).Scan(&seq)
	return seq, err
}

func nowUTC() time.Time { return time.Now().UTC() }
