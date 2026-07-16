package loom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

// Migrate applies the storage schema. Idempotent; meant for a deploy-time
// migration endpoint or command, never for application boot.
func (c *Client) Migrate(ctx context.Context) error {
	_, err := c.db.Exec(ctx, ddl)
	return err
}

type storedEvent struct {
	Event
	dataRaw []byte
}

// appendEvents writes a batch for one aggregate inside tx. The stream's
// UNIQUE constraint is the optimistic-concurrency guard: a losing racer
// gets a typed ConflictError.
func (c *Client) appendEvents(ctx context.Context, tx pgx.Tx, evts []*Event) error {
	for _, e := range evts {
		data, err := json.Marshal(e.Data)
		if err != nil {
			return fmt.Errorf("marshal %s: %w", e.Type, err)
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
	}
	return nil
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
	for rows.Next() {
		var v, sv int
		var typ string
		var data []byte
		if err := rows.Scan(&v, &typ, &sv, &data); err != nil {
			return nil, 0, err
		}
		payload, err := c.decode(typ, data)
		if err != nil {
			return nil, 0, err
		}
		if err := state.Fold(typ, payload); err != nil {
			return nil, 0, fmt.Errorf("fold %s at version %d: %w", typ, v, err)
		}
		version = v
	}
	return state, version, rows.Err()
}

func (c *Client) decode(eventType string, data []byte) (any, error) {
	def := c.reg.eventDef(eventType)
	if def == nil {
		return nil, &UnknownEventError{Type: eventType}
	}
	payload := def.New()
	if err := json.Unmarshal(data, payload); err != nil {
		return nil, fmt.Errorf("decode %s: %w", eventType, err)
	}
	return payload, nil
}

func (c *Client) saveSnapshot(ctx context.Context, tx pgx.Tx, agg *AggregateDef, namespace string, id uuid.UUID, state AggregateState, version int) error {
	data, err := json.Marshal(state)
	if err != nil {
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
// catch-up read that feeds projection and local process runners.
func (c *Client) readLog(ctx context.Context, afterSeq int64, limit int) ([]*Event, error) {
	rows, err := c.db.Query(ctx, `
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
		var data []byte
		if err := rows.Scan(&e.GlobalSeq, &e.Namespace, &e.AggregateType, &e.AggregateID, &e.Version, &e.Type, &e.SchemaVersion, &e.At, &e.Meta.CorrelationID, &e.Meta.CausationID, &e.Meta.Actor, &data); err != nil {
			return nil, err
		}
		payload, err := c.decode(e.Type, data)
		if err != nil {
			return nil, err
		}
		e.Data = payload
		out = append(out, e)
	}
	return out, rows.Err()
}

type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type executor interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// notify nudges runners and SSE watchers — this instance directly, other
// instances via pg_notify (their listenLoop rebroadcasts locally).
func (c *Client) notifyLog(ctx context.Context) {
	_, _ = c.db.Exec(ctx, `SELECT pg_notify($1, '')`, "loom_"+c.reg.Service)
	c.broadcastLog()
}

func nowUTC() time.Time { return time.Now().UTC() }
