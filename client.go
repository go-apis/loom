package loom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	DB       *pgxpool.Pool
	Bus      Bus
	Registry *Registry
	Logger   *slog.Logger

	// Keys wraps per-stream data keys for @pii fields. Required when the
	// schema declares any @pii field; see LocalKeys.
	Keys KeyWrapper

	// Blobs stores uploaded files. Required when the schema declares any
	// upload; see gblob.New and NewDirBlobStore.
	Blobs BlobStore

	// ConflictRetries re-runs a Dispatch that lost an optimistic-concurrency
	// race, against fresh state. Default 3.
	ConflictRetries int
}

type Client struct {
	db         *pgxpool.Pool
	bus        Bus
	reg        *Registry
	log        *slog.Logger
	nudge      chan struct{} // log advanced: wake projection/process runners
	relayNudge chan struct{} // outbox rows written: wake the relay
	batchNudge chan struct{} // batch enqueued: wake the batch runner

	watchMu  sync.Mutex
	watchers map[chan struct{}]bool // SSE streams awaiting log advances

	keys  KeyWrapper
	blobs BlobStore
	dekMu sync.Mutex
	deks  map[string][]byte // unwrapped per-stream data keys

	tables map[string]*tableSQL // @table entities: precomputed SQL by entity

	retries int
}

func New(cfg Config) (*Client, error) {
	if cfg.DB == nil || cfg.Registry == nil {
		return nil, fmt.Errorf("loom: Config.DB and Config.Registry are required")
	}
	if cfg.Bus == nil {
		cfg.Bus = NewMemoryBus()
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.ConflictRetries == 0 {
		cfg.ConflictRetries = 3
	}
	if cfg.Registry.hasPII() && cfg.Keys == nil {
		return nil, fmt.Errorf("loom: the schema declares @pii fields — Config.Keys is required (see loom.LocalKeys)")
	}
	if len(cfg.Registry.Uploads) > 0 && cfg.Blobs == nil {
		return nil, fmt.Errorf("loom: the schema declares uploads — Config.Blobs is required (see gblob.New, loom.NewDirBlobStore)")
	}
	c := &Client{
		db:         cfg.DB,
		bus:        cfg.Bus,
		reg:        cfg.Registry,
		log:        cfg.Logger.With("service", cfg.Registry.Service),
		nudge:      make(chan struct{}, 1),
		relayNudge: make(chan struct{}, 1),
		batchNudge: make(chan struct{}, 1),
		watchers:   map[chan struct{}]bool{},
		keys:       cfg.Keys,
		blobs:      cfg.Blobs,
		deks:       map[string][]byte{},
		tables:     buildTables(cfg.Registry),
		retries:    cfg.ConflictRetries,
	}
	// the dev store signals finalized uploads in-process; production
	// stores are wired to FinalizeUpload in the deployment (gblob.Watch)
	if n, ok := cfg.Blobs.(UploadNotifier); ok {
		n.NotifyUploads(c.FinalizeUpload)
	}
	return c, nil
}

func (c *Client) Registry() *Registry { return c.reg }

// maxPolicyDepth bounds policy → command → event → policy chains inside one
// unit of work; hitting it is always a schema design error (a cycle).
const maxPolicyDepth = 10

// Dispatch runs commands in one unit of work: load state, handle, append
// events (checked against each command's emit contract), run subscribed
// policies in the same transaction, write outbox rows for published events,
// snapshot, commit. Conflicts retry the whole unit against fresh state.
func (c *Client) Dispatch(ctx context.Context, cmds ...Command) error {
	var err error
	for attempt := 0; attempt <= c.retries; attempt++ {
		err = c.dispatchOnce(ctx, cmds)
		var conflict *ConflictError
		if !errors.As(err, &conflict) {
			return err
		}
		c.log.WarnContext(ctx, "dispatch conflict, retrying", "attempt", attempt+1, "error", err)
	}
	return err
}

func (c *Client) dispatchOnce(ctx context.Context, cmds []Command) error {
	ctx = withReader(ctx, c)
	meta := MetaFrom(ctx)
	if meta.CorrelationID == "" {
		meta.CorrelationID = uuid.NewString()
	}

	tx, err := c.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	published := false
	batched := false
	type queued struct {
		cmd   Command
		cause string
	}
	queue := make([]queued, 0, len(cmds))
	for _, cmd := range cmds {
		queue = append(queue, queued{cmd: cmd, cause: meta.CausationID})
	}

	for depth := 0; len(queue) > 0; depth++ {
		if depth > maxPolicyDepth {
			return fmt.Errorf("loom: policy chain exceeded depth %d — schema cycle?", maxPolicyDepth)
		}
		batch := queue
		queue = nil

		for _, q := range batch {
			cmdMeta := Metadata{
				CorrelationID: meta.CorrelationID,
				CausationID:   q.cause,
				Actor:         meta.Actor,
			}
			// timer and batch wrappers join the transaction instead of
			// executing
			switch w := q.cmd.(type) {
			case *scheduledCommand:
				if err := c.writeTimer(ctx, tx, w, cmdMeta); err != nil {
					return err
				}
				continue
			case *cancelTimer:
				if err := c.deleteTimer(ctx, tx, w); err != nil {
					return err
				}
				continue
			case *asBatch:
				ns, _ := w.CommandTarget()
				id := w.id
				if id == uuid.Nil {
					id = uuid.New()
				}
				if err := c.enqueueBatchTx(ctx, tx, id, ns, w.cmds); err != nil {
					return err
				}
				batched = true
				continue
			}
			newEvents, err := c.handleCommand(ctx, tx, q.cmd, cmdMeta)
			if err != nil {
				return err
			}
			for _, evt := range newEvents {
				def := c.reg.eventDef(evt.Type)
				if def != nil && def.Publish {
					if err := c.enqueueOutbox(ctx, tx, evt); err != nil {
						return err
					}
					published = true
				}
				followups, err := c.runPolicies(ctx, evt)
				if err != nil {
					return err
				}
				for _, f := range followups {
					queue = append(queue, queued{cmd: f, cause: fmt.Sprintf("evt:%d", evt.GlobalSeq)})
				}
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	c.notifyLog(ctx)
	if published {
		c.nudgeRelay()
	}
	if batched {
		c.nudgeBatches()
	}
	return nil
}

func (c *Client) handleCommand(ctx context.Context, tx pgx.Tx, cmd Command, meta Metadata) ([]*Event, error) {
	cmdName := cmd.LoomCommand()
	agg, def := c.reg.aggregateForCommand(cmdName)
	if def == nil {
		if rec, rdef := c.reg.recordForCommand(cmdName); rdef != nil {
			return c.handleRecordCommand(ctx, tx, rec, rdef, cmd, meta)
		}
		return nil, fmt.Errorf("loom: nothing handles command %s", cmdName)
	}
	namespace, id := cmd.CommandTarget()
	if namespace == "" {
		return nil, fmt.Errorf("loom: command %s has no namespace", cmdName)
	}

	state, version, err := c.loadState(ctx, tx, agg, namespace, id)
	if err != nil {
		return nil, err
	}

	payloads, err := def.Handle(ctx, state, cmd)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", cmdName, err)
	}

	events := make([]*Event, 0, len(payloads))
	for _, payload := range payloads {
		name, sv, err := c.eventNameOf(payload)
		if err != nil {
			return nil, err
		}
		if !contains(def.Emits, name) {
			return nil, &ContractError{Command: cmdName, Emitted: name}
		}
		version++
		evt := &Event{
			Service:       c.reg.Service,
			Namespace:     namespace,
			AggregateType: agg.Name,
			AggregateID:   id,
			Version:       version,
			Type:          name,
			SchemaVersion: sv,
			At:            nowUTC(),
			Meta:          meta,
			Data:          payload,
		}
		if err := state.Fold(name, payload); err != nil {
			return nil, err
		}
		events = append(events, evt)
	}
	if len(events) == 0 {
		return nil, nil
	}

	if err := c.appendEvents(ctx, tx, events); err != nil {
		return nil, err
	}

	if agg.SnapshotEvery > 0 {
		first := events[0].Version - 1
		if version/agg.SnapshotEvery > first/agg.SnapshotEvery {
			if err := c.saveSnapshot(ctx, tx, agg, namespace, id, state, version); err != nil {
				return nil, err
			}
		}
	}
	return events, nil
}

// handleRecordCommand runs a command against a state-of-record object: the
// row (not the log) is the source of truth. The row lock serializes
// writers; emitted events land in the log as announcements with the
// record's stream identity.
func (c *Client) handleRecordCommand(ctx context.Context, tx pgx.Tx, rec *RecordDef, def *RecordCommandDef, cmd Command, meta Metadata) ([]*Event, error) {
	namespace, id := cmd.CommandTarget()
	if namespace == "" {
		return nil, fmt.Errorf("loom: command %s has no namespace", def.Name)
	}

	state := rec.NewState()
	version := 0
	var data []byte
	err := tx.QueryRow(ctx, `
		SELECT version, data FROM loom_records
		WHERE service=$1 AND namespace=$2 AND record_type=$3 AND id=$4
		FOR UPDATE`,
		c.reg.Service, namespace, rec.Name, id).Scan(&version, &data)
	existed := true
	if errors.Is(err, pgx.ErrNoRows) {
		existed = false
	} else if err != nil {
		return nil, err
	} else {
		if data, err = c.decryptFields(ctx, namespace, id, data, rec.StatePII); err != nil {
			return nil, fmt.Errorf("record %s/%s: %w", rec.Name, id, err)
		}
		if err := json.Unmarshal(data, state); err != nil {
			return nil, fmt.Errorf("record %s/%s: %w", rec.Name, id, err)
		}
	}

	payloads, err := def.Handle(ctx, state, cmd)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", def.Name, err)
	}

	events := make([]*Event, 0, len(payloads))
	v := version
	for _, payload := range payloads {
		name, sv, err := c.eventNameOf(payload)
		if err != nil {
			return nil, err
		}
		if !contains(def.Emits, name) {
			return nil, &ContractError{Command: def.Name, Emitted: name}
		}
		v++
		events = append(events, &Event{
			Service:       c.reg.Service,
			Namespace:     namespace,
			AggregateType: rec.Name,
			AggregateID:   id,
			Version:       v,
			Type:          name,
			SchemaVersion: sv,
			At:            nowUTC(),
			Meta:          meta,
			Data:          payload,
		})
	}
	if len(events) > 0 {
		if err := c.appendEvents(ctx, tx, events); err != nil {
			return nil, err
		}
	} else {
		v = version + 1 // state-only write still advances the row version
	}

	out, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}
	if out, err = c.encryptFields(ctx, namespace, id, out, rec.StatePII); err != nil {
		return nil, err
	}
	if existed {
		_, err = tx.Exec(ctx, `
			UPDATE loom_records SET version=$5, data=$6, updated_at=now()
			WHERE service=$1 AND namespace=$2 AND record_type=$3 AND id=$4`,
			c.reg.Service, namespace, rec.Name, id, v, out)
	} else {
		_, err = tx.Exec(ctx, `
			INSERT INTO loom_records (service, namespace, record_type, id, version, data)
			VALUES ($1,$2,$3,$4,$5,$6)`,
			c.reg.Service, namespace, rec.Name, id, v, out)
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return nil, &ConflictError{AggregateType: rec.Name, AggregateID: id, Version: v}
		}
	}
	if err != nil {
		return nil, err
	}
	return events, nil
}

// Record reads a state-of-record object (nil if absent), decoded into the
// generated state type.
func (c *Client) Record(ctx context.Context, record, namespace string, id uuid.UUID) (any, error) {
	for _, rec := range c.reg.Records {
		if rec.Name != record {
			continue
		}
		var data []byte
		err := c.db.QueryRow(ctx, `
			SELECT data FROM loom_records
			WHERE service=$1 AND namespace=$2 AND record_type=$3 AND id=$4`,
			c.reg.Service, namespace, record, id).Scan(&data)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		if data, err = c.decryptFields(ctx, namespace, id, data, rec.StatePII); err != nil {
			return nil, err
		}
		state := rec.NewState()
		if err := json.Unmarshal(data, state); err != nil {
			return nil, err
		}
		return state, nil
	}
	return nil, fmt.Errorf("loom: unknown record %s", record)
}

// runPolicies reacts to one new event with every subscribed policy, inside
// the same transaction. Policies see committed-with-us state and their
// commands join the unit atomically.
func (c *Client) runPolicies(ctx context.Context, evt *Event) ([]Command, error) {
	var out []Command
	for _, p := range c.reg.Policies {
		if !contains(p.Events, evt.Type) {
			continue
		}
		cmds, err := p.React(ctx, evt)
		if err != nil {
			return nil, fmt.Errorf("policy %s on %s: %w", p.Name, evt.Type, err)
		}
		out = append(out, cmds...)
	}
	return out, nil
}

// Load folds and returns an aggregate's current state and version, outside
// any unit of work (for queries and the console).
func (c *Client) Load(ctx context.Context, aggregate, namespace string, id uuid.UUID) (AggregateState, int, error) {
	for _, agg := range c.reg.Aggregates {
		if agg.Name == aggregate {
			return c.loadState(ctx, c.db, agg, namespace, id)
		}
	}
	return nil, 0, fmt.Errorf("loom: unknown aggregate %s", aggregate)
}

// Entity returns a read-model row (nil if absent), decoded into the
// projection's generated state type.
func (c *Client) Entity(ctx context.Context, entity, namespace string, id uuid.UUID) (EntityState, error) {
	var proj *ProjectionDef
	for _, p := range c.reg.Projections {
		if p.Entity == entity {
			proj = p
			break
		}
	}
	if proj == nil {
		return nil, fmt.Errorf("loom: no projection maintains entity %s", entity)
	}
	var data []byte
	var err error
	if table := c.tables[entity]; table != nil {
		err = c.db.QueryRow(ctx, table.selectDoc, c.reg.Service, namespace, id).Scan(&data)
	} else {
		err = c.db.QueryRow(ctx, `
			SELECT data FROM loom_entities
			WHERE service=$1 AND namespace=$2 AND entity_type=$3 AND id=$4`,
			c.reg.Service, namespace, entity, id).Scan(&data)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if c.tables[entity] == nil { // @table excludes @pii by validation
		if data, err = c.decryptFields(ctx, namespace, id, data, proj.PII); err != nil {
			return nil, err
		}
	}
	state := proj.NewState()
	if err := json.Unmarshal(data, state); err != nil {
		return nil, err
	}
	return state, nil
}

func (c *Client) eventNameOf(payload any) (string, int, error) {
	de, ok := payload.(DomainEvent)
	if !ok {
		return "", 0, fmt.Errorf("loom: handler returned %T, which is not a generated event", payload)
	}
	name := de.LoomEvent()
	def := c.reg.eventDef(name)
	if def == nil {
		return "", 0, &UnknownEventError{Type: name}
	}
	return def.Name, def.SchemaVersion, nil
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
