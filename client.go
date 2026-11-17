package loom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
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

	// LoopFailureLimit is how many looks in a row of a @loop instance
	// may fail before it is parked with a wait on its operators
	// (LoopState.ParkPast). Zero takes LoopFailureLimit (10); a
	// negative value never parks.
	LoopFailureLimit int

	// StepConcurrency caps how many projection/process runner steps may
	// run at once. A NOTIFY wakes every runner simultaneously (one per
	// read model + process — dozens on a wide schema), and unbounded
	// they saturate small connection pools and starve Dispatch of a
	// connection. Default: half the pool's MaxConns, floor 2, so
	// dispatches always find headroom regardless of pool size.
	StepConcurrency int

	// RunnerIdlePoll is how long a projection or process runner with
	// nothing to do may sleep before it steps unwoken. New events wake a
	// runner at once through the log fan-out (NOTIFY), so the unwoken
	// step is only a safety net for a wake it missed. A runner that had
	// work or an error steps again after Start's poll; each idle round
	// doubles that, up to RunnerIdlePoll. Zero (the default) keeps every
	// runner on Start's poll, as before.
	RunnerIdlePoll time.Duration

	// Reconcile maps a declared effect name (the part of a Once key before
	// any "/suffix") to a hook Once consults before declaring that effect
	// in doubt. Optional; an effect with no hook (and not @idempotent)
	// still parks for an operator. See ReconcileFunc.
	Reconcile map[string]ReconcileFunc

	// OnDeadLetter is called once for each dead letter, on the instance
	// holding the process lease, when no redrive is left to heal it: its
	// RedriveLadder is spent, it was parked with a Persistent error, or
	// it has no redrive (a skipped projection event). Optional. See
	// hooks.go.
	OnDeadLetter func(ctx context.Context, letter DeadLetter)

	// RedriveLadder is the delays, from the park and then from each failed
	// redrive, at which a process or timer dead letter is redriven before
	// OnDeadLetter is called: []time.Duration{time.Minute, 10 *
	// time.Minute} redrives a letter a minute after it parks and ten
	// minutes after that, then reports it. Empty (the default) redrives
	// nothing on its own.
	RedriveLadder []time.Duration

	// OnRunnerLag is called once, on the instance holding the process
	// lease, when a projection or local-event process runner falls behind
	// by more than RunnerLagThreshold: the oldest event of a type it
	// subscribes to that it has not handled was appended longer ago than
	// that. It is called again only after the runner has caught up to
	// within the threshold. Optional.
	OnRunnerLag func(ctx context.Context, lag RunnerLag)

	// RunnerLagThreshold is OnRunnerLag's threshold. Default 1m.
	RunnerLagThreshold time.Duration

	// OnEffectInDoubt is called once, on the instance holding the process
	// lease, for each Once effect a reaction found claimed and never
	// settled (the call may or may not have happened) that neither
	// @idempotent nor its Reconcile hook could settle. The effect stays in
	// doubt for ResolveEffect. Optional.
	OnEffectInDoubt func(ctx context.Context, effect EffectRecord)

	// SeriesPartitionAhead is how many days of @retain series partitions
	// maintenance keeps created in advance on plain Postgres. Default 7.
	SeriesPartitionAhead int

	// LogLockWait bounds how long an append waits for the service's log
	// lock, which one unit of work holds from its first append to its
	// commit. A unit holds it for milliseconds; one that holds it for
	// longer is stuck on something outside its transaction, and every
	// dispatch behind it would otherwise wait with it, a pooled
	// connection apiece, until the pool and the service hang. Past the
	// wait the dispatch fails with a *LogLockError naming the holder, and
	// gives its connection back. Default 15s.
	LogLockWait time.Duration

	// Generation orders this instance's build against the service's
	// other instances for the process lease: a follower of a strictly
	// newer generation takes the lease from an older leader, so the
	// processes run the newest deploy's code soon after it starts rather
	// than whenever the previous revision's last instance goes away
	// (election.go, generation.go, ADR 0010). It must grow with every
	// deploy and be the same on every instance of one revision; by
	// convention, the deploy's Unix time in seconds. Zero (the default)
	// takes LOOM_GENERATION, which a deploy sets (gcloud run deploy
	// --update-env-vars LOOM_GENERATION=$(date +%s)), and failing that,
	// on Cloud Run, the revision's createTime from the Cloud Run Admin API
	// (needs run.revisions.get). A negative Generation turns generations
	// off. With none, the election is as it always was: whoever takes the
	// lease keeps it until its session ends. Not the sequence in a Cloud
	// Run revision's name: Cloud Run does not number revisions in deploy
	// order.
	Generation int64

	// Revision names this instance's build on its lease session and in
	// the election's logs. Default: K_REVISION, which Cloud Run sets.
	Revision string

	// Release numbers this build among the service's releases, for the
	// event floor (floor.go): the store remembers the release that first
	// wrote each event type, and a Client of an older Release refuses to
	// Start against it, since it could not decode what is stored. It
	// belongs to the code, not the deploy: the same number every time
	// one build is deployed, a rollback included, and a larger one for
	// every later build — a build number, a commit count. That is what
	// Generation is not: a rollback deploys an old Release under a new
	// Generation. Zero (the default) takes LOOM_RELEASE; with neither,
	// the build writes floors of zero and is never refused.
	Release int64

	// Mesh offers the jobs a @loop aggregate's declared agents are asked
	// as (agent.go): mesh.HTTPClient against Mesh, mesh.Fake in tests.
	// Required to ask an agent. MeshTenant is the tenant a job runs for
	// when the ask names none, and MeshHookSecret the secret Mesh signs
	// its job hooks with, which Client.MeshHook verifies.
	Mesh           Mesh
	MeshTenant     string
	MeshHookSecret []byte
}

type Client struct {
	db *pgxpool.Pool
	// seriesTimescale records what migrateSeries found: with the extension,
	// retention is a hypertable policy and partition upkeep is a no-op.
	seriesTimescale bool
	bus             Bus
	reg             *Registry
	log             *slog.Logger
	readerNudge     chan struct{}  // log advanced: wake the fan-out reader (runReader)
	fan             *logFanout     // shared log tail + typed runner wake-ups
	relayNudge      chan struct{}  // outbox rows written: wake the relay
	batchNudge      chan struct{}  // batch enqueued: wake the batch runner
	stepSem         chan struct{}  // bounds concurrent runner steps (see Config.StepConcurrency)
	runnerIdlePoll  time.Duration  // Config.RunnerIdlePoll
	notifyNudge     chan struct{}  // an append committed: tell other instances (notifyLog)
	notifyOnce      sync.Once      // starts the notifier on the first append
	loopFails       int            // Config.LoopFailureLimit
	leader          leaderFlag     // this instance holds the service's process lease (election.go)
	gen             generation     // Config.Generation, Config.Revision as the election orders them
	release         int64          // Config.Release, or LOOM_RELEASE
	floors          floors         // what is known of loom_event_floors (floor.go)
	genOnce         sync.Once      // the generation is resolved once, by whichever runner needs it first
	superseded      atomic.Bool    // a newer generation is alive: this instance leaves the service's work to it (instances.go)
	beaten          chan struct{}  // closed after the instance's first beat; nil until Start
	beatNudge       chan struct{}  // another instance announced itself: beat now (instances.go)
	every           []*ScheduleDef // recurring functions added with Every (schedules.go)
	procSteps       atomic.Int64   // process runner steps in flight (a yielding leader drains them)

	watchMu  sync.Mutex
	watchers map[chan struct{}]bool // SSE streams awaiting log advances

	keys  KeyWrapper
	blobs BlobStore
	dekMu sync.Mutex
	deks  map[string][]byte // unwrapped per-stream data keys
	// keyHomes maps a stream to the namespace its data key lives in:
	// its own, or a fork's source's (keyHome)
	keyHomes map[string]string

	tables map[string]*tableSQL  // @table entities: precomputed SQL by entity
	series map[string]*seriesSQL // series: precomputed SQL by name
	tel    *telemetry

	retries int

	reconcile map[string]ReconcileFunc // Config.Reconcile, by effect name

	// background-work hooks (hooks.go)
	onDeadLetter    func(context.Context, DeadLetter)
	onRunnerLag     func(context.Context, RunnerLag)
	onEffectInDoubt func(context.Context, EffectRecord)
	ladder          []time.Duration
	lagThreshold    time.Duration

	seriesAhead int           // Config.SeriesPartitionAhead
	logLockWait time.Duration // Config.LogLockWait
	seriesFuncs atomic.Bool   // the series partition functions exist (seriesfuncs.go)

	mesh       Mesh   // Config.Mesh
	meshTenant string // Config.MeshTenant
	meshSecret []byte // Config.MeshHookSecret
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
	if cfg.LoopFailureLimit == 0 {
		cfg.LoopFailureLimit = LoopFailureLimit
	}
	if cfg.ConflictRetries == 0 {
		cfg.ConflictRetries = 3
	}
	if cfg.StepConcurrency <= 0 {
		cfg.StepConcurrency = int(cfg.DB.Config().MaxConns) / 2
		if cfg.StepConcurrency < 2 {
			cfg.StepConcurrency = 2
		}
	}
	if cfg.Registry.hasPII() && cfg.Keys == nil {
		return nil, fmt.Errorf("loom: the schema declares @pii fields — Config.Keys is required (see loom.LocalKeys)")
	}
	if len(cfg.Registry.Uploads) > 0 && cfg.Blobs == nil {
		return nil, fmt.Errorf("loom: the schema declares uploads — Config.Blobs is required (see gblob.New, loom.NewDirBlobStore)")
	}
	for name := range cfg.Reconcile {
		if !cfg.Registry.declaresEffect(name) {
			return nil, fmt.Errorf("loom: Config.Reconcile names effect %q, which no process declares", name)
		}
	}
	c := &Client{
		db:              cfg.DB,
		bus:             cfg.Bus,
		reg:             cfg.Registry,
		log:             cfg.Logger.With("service", cfg.Registry.Service),
		readerNudge:     make(chan struct{}, 1),
		fan:             &logFanout{},
		relayNudge:      make(chan struct{}, 1),
		batchNudge:      make(chan struct{}, 1),
		stepSem:         make(chan struct{}, cfg.StepConcurrency),
		mesh:            cfg.Mesh,
		meshTenant:      cfg.MeshTenant,
		meshSecret:      cfg.MeshHookSecret,
		runnerIdlePoll:  cfg.RunnerIdlePoll,
		notifyNudge:     make(chan struct{}, 1),
		watchers:        map[chan struct{}]bool{},
		keys:            cfg.Keys,
		blobs:           cfg.Blobs,
		deks:            map[string][]byte{},
		keyHomes:        map[string]string{},
		tables:          buildTables(cfg.Registry),
		series:          buildSeries(cfg.Registry),
		tel:             newTelemetry(cfg.Registry.Service),
		retries:         cfg.ConflictRetries,
		loopFails:       cfg.LoopFailureLimit,
		reconcile:       cfg.Reconcile,
		onDeadLetter:    cfg.OnDeadLetter,
		onRunnerLag:     cfg.OnRunnerLag,
		onEffectInDoubt: cfg.OnEffectInDoubt,
		ladder:          cfg.RedriveLadder,
		lagThreshold:    cfg.RunnerLagThreshold,
		seriesAhead:     cfg.SeriesPartitionAhead,
		gen:             generationFrom(cfg.Generation, cfg.Revision),
		release:         releaseFrom(cfg.Release),
	}
	if c.lagThreshold <= 0 {
		c.lagThreshold = defaultRunnerLagThreshold
	}
	if c.seriesAhead <= 0 {
		c.seriesAhead = defaultSeriesPartitionAhead
	}
	c.logLockWait = cfg.LogLockWait
	if c.logLockWait <= 0 {
		c.logLockWait = defaultLogLockWait
	}
	c.registerGauges()
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
// Validator lets generated commands reject bad payloads (enum values
// outside their set) before any handler runs. loom generate emits it on
// commands that need it; hand-written commands may implement it too.
type Validator interface {
	Validate() error
}

func (c *Client) Dispatch(ctx context.Context, cmds ...Command) (err error) {
	// a handler or reaction a plan is running returns its commands; one
	// that dispatched them itself would do what the plan only works out
	if Planning(ctx) {
		return errors.New("loom: Dispatch while planning: a plan runs nothing — return the commands from the reaction")
	}
	names := make([]string, len(cmds))
	for i, cmd := range cmds {
		names[i] = cmd.LoomCommand()
		if v, ok := cmd.(Validator); ok {
			if err := v.Validate(); err != nil {
				return fmt.Errorf("loom: %s: %w", cmd.LoomCommand(), err)
			}
		}
	}
	attrs := append(metaAttrs(MetaFrom(ctx)), attribute.StringSlice("loom.commands", names))
	ctx, end := c.tel.span(ctx, "loom.dispatch", attrs...)
	start := time.Now()
	defer func() {
		end(err)
		c.tel.dispatchDur.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(c.tel.service))
		c.tel.count(ctx, c.tel.dispatches, 1)
	}()

	for attempt := 0; attempt <= c.retries; attempt++ {
		err = c.dispatchOnce(ctx, cmds)
		var stuck *LogLockError
		if errors.As(err, &stuck) {
			// the unit's transaction is rolled back and its connection
			// returned: name the holder now, loudly
			stuck.Holder = c.logLockHolder()
			c.log.ErrorContext(ctx, "loom: log lock wait exceeded", "commands", names, "waited", stuck.Waited.String(),
				"holder_pid", stuck.Holder.PID, "holder_state", stuck.Holder.State,
				"holder_since", stuck.Holder.Since, "holder_query", stuck.Holder.Query)
			return err
		}
		var conflict *ConflictError
		if !errors.As(err, &conflict) {
			return err
		}
		c.tel.count(ctx, c.tel.conflicts, 1)
		c.log.WarnContext(ctx, "dispatch conflict, retrying", "attempt", attempt+1, "error", err)
	}
	return err
}

func (c *Client) dispatchOnce(ctx context.Context, cmds []Command) error {
	meta := MetaFrom(ctx)
	if meta.CorrelationID == "" {
		meta.CorrelationID = uuid.NewString()
	}

	tx, err := c.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	// handlers and policies read on this transaction: it holds the log
	// lock from its first append, so a read that waited for a pooled
	// connection would wait on the dispatches queued behind that lock
	// (reader.go)
	ctx = withTxReader(ctx, c, tx)

	// a repeat of a dispatch that already committed under the caller's
	// key does nothing (idempotency.go)
	if run, err := c.claimIdempotency(ctx, tx, cmds); err != nil || !run {
		return err
	}

	published, batched, err := c.runUnit(ctx, tx, cmds, meta, nil)
	if err != nil {
		return err
	}

	// No NOTIFY inside the transaction: a transaction that has queued one
	// takes a cluster-wide lock at commit and holds it until the
	// notification is queued, so every append serialised on it as well as
	// on the log lock. Other instances are told after commit, coalesced
	// (notifyLog); a crash between commit and that notify leaves them to
	// their poll, as for any lost wake.
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	c.broadcastLog() // local fast path
	c.notifyLog()    // other instances, via LISTEN
	if published {
		c.nudgeRelay()
	}
	if batched {
		c.nudgeBatches()
	}
	return nil
}

// runUnit is a unit of work's body on tx: each command handled, its
// events appended and queued for the bus, every subscribed policy run on
// each event, and the commands the policies answer with handled in turn,
// to maxPolicyDepth. Dispatch commits what it wrote; a plan (plans.go)
// passes the scope it records under and rolls it back.
func (c *Client) runUnit(ctx context.Context, tx pgx.Tx, cmds []Command, meta Metadata, plan *planScope) (published, batched bool, err error) {
	type queued struct {
		cmd   Command
		cause string
		// by is the policy whose answer this command is; empty for the
		// unit's own commands
		by string
	}
	queue := make([]queued, 0, len(cmds))
	for _, cmd := range cmds {
		queue = append(queue, queued{cmd: cmd, cause: meta.CausationID})
	}

	for depth := 0; len(queue) > 0; depth++ {
		if depth > maxPolicyDepth {
			return false, false, fmt.Errorf("loom: policy chain exceeded depth %d — schema cycle?", maxPolicyDepth)
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
					return false, false, err
				}
				continue
			case *cancelTimer:
				if err := c.deleteTimer(ctx, tx, w); err != nil {
					return false, false, err
				}
				continue
			case *asBatch:
				ns, _ := w.CommandTarget()
				id := w.id
				if id == uuid.Nil {
					id = uuid.New()
				}
				if err := c.enqueueBatchTx(ctx, tx, id, ns, w.cmds); err != nil {
					return false, false, err
				}
				batched = true
				continue
			}
			newEvents, err := c.handleCommand(ctx, tx, q.cmd, cmdMeta)
			if err != nil {
				return false, false, err
			}
			for _, evt := range newEvents {
				def := c.reg.eventDef(evt.Type)
				if def != nil && def.Publish {
					if err := c.enqueueOutbox(ctx, tx, evt); err != nil {
						return false, false, err
					}
					published = true
				}
				planned := plan.emitted(evt, q.cmd, q.by)
				for _, p := range c.reg.Policies {
					if !contains(p.Events, evt.Type) {
						continue
					}
					followups, err := p.React(ctx, evt)
					if err != nil {
						return false, false, fmt.Errorf("policy %s on %s: %w", p.Name, evt.Type, err)
					}
					planned.decided("policy", p.Name, followups)
					for _, f := range followups {
						queue = append(queue, queued{cmd: f, cause: fmt.Sprintf("evt:%d", evt.GlobalSeq), by: "policy " + p.Name})
					}
				}
			}
		}
	}
	return published, batched, nil
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
	if err := stalePlan(ctx, cmd, version); err != nil {
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
			Command:       cmdName,
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

	// @table aggregate: mirror the folded state into the typed row in the
	// same transaction — the read model can never lag the command
	if ts := c.tables[agg.Name]; ts != nil {
		args := append([]any{c.reg.Service, namespace, id}, ts.def.Values(state)...)
		if _, err := tx.Exec(ctx, ts.upsert, args...); err != nil {
			return nil, fmt.Errorf("loom: %s table mirror: %w", agg.Name, err)
		}
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
		if data, err = c.decryptFields(ctx, tx, namespace, id, data, rec.StatePII); err != nil {
			return nil, fmt.Errorf("record %s/%s: %w", rec.Name, id, err)
		}
		if err := json.Unmarshal(data, state); err != nil {
			return nil, fmt.Errorf("record %s/%s: %w", rec.Name, id, err)
		}
	}
	if err := stalePlan(ctx, cmd, version); err != nil {
		return nil, err
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
			Command:       def.Name,
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
	if out, err = c.encryptFields(ctx, tx, namespace, id, out, rec.StatePII); err != nil {
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
	return c.record(ctx, c.db, record, namespace, id)
}

// record is Record on q: the pool, or a unit of work's transaction.
func (c *Client) record(ctx context.Context, q querier, record, namespace string, id uuid.UUID) (any, error) {
	for _, rec := range c.reg.Records {
		if rec.Name != record {
			continue
		}
		var data []byte
		err := q.QueryRow(ctx, `
			SELECT data FROM loom_records
			WHERE service=$1 AND namespace=$2 AND record_type=$3 AND id=$4`,
			c.reg.Service, namespace, record, id).Scan(&data)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		if data, err = c.decryptFields(ctx, q, namespace, id, data, rec.StatePII); err != nil {
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

// Load folds and returns an aggregate's current state and version, outside
// any unit of work (for queries and the console).
func (c *Client) Load(ctx context.Context, aggregate, namespace string, id uuid.UUID) (AggregateState, int, error) {
	return c.load(ctx, c.db, aggregate, namespace, id)
}

// load is Load on q: the pool, or a unit of work's transaction.
func (c *Client) load(ctx context.Context, q querier, aggregate, namespace string, id uuid.UUID) (AggregateState, int, error) {
	for _, agg := range c.reg.Aggregates {
		if agg.Name == aggregate {
			return c.loadState(ctx, q, agg, namespace, id)
		}
	}
	return nil, 0, fmt.Errorf("loom: unknown aggregate %s", aggregate)
}

// Entity returns a read-model row (nil if absent), decoded into the
// projection's generated state type.
func (c *Client) Entity(ctx context.Context, entity, namespace string, id uuid.UUID) (EntityState, error) {
	return c.entity(ctx, c.db, entity, namespace, id)
}

// entity is Entity on q: the pool, or a unit of work's transaction.
func (c *Client) entity(ctx context.Context, q querier, entity, namespace string, id uuid.UUID) (EntityState, error) {
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
		err = q.QueryRow(ctx, table.selectDoc, c.reg.Service, namespace, id).Scan(&data)
	} else {
		err = q.QueryRow(ctx, `
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
		if data, err = c.decryptFields(ctx, q, namespace, id, data, proj.PII); err != nil {
			return nil, err
		}
	}
	state := proj.NewState()
	if err := json.Unmarshal(data, state); err != nil {
		return nil, err
	}
	return state, nil
}

// Entities is the batched Entity: one query for many read-model rows.
// Ids that have no row are simply absent from the result — callers
// treat a missing key as the same null edge Entity's (nil, nil) means.
func (c *Client) Entities(ctx context.Context, entity, namespace string, ids []uuid.UUID) (map[uuid.UUID]EntityState, error) {
	if len(ids) == 0 {
		return map[uuid.UUID]EntityState{}, nil
	}
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
	var rows pgx.Rows
	var err error
	if table := c.tables[entity]; table != nil {
		rows, err = c.db.Query(ctx, fmt.Sprintf(
			`SELECT id, to_jsonb(t) - 'service' - 'namespace' - 'id' - 'updated_at' FROM %s t
			 WHERE service=$1 AND namespace=$2 AND id = ANY($3)`, table.def.Name),
			c.reg.Service, namespace, ids)
	} else {
		rows, err = c.db.Query(ctx, `
			SELECT id, data FROM loom_entities
			WHERE service=$1 AND namespace=$2 AND entity_type=$3 AND id = ANY($4)`,
			c.reg.Service, namespace, entity, ids)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type row struct {
		id   uuid.UUID
		data []byte
	}
	var found []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.data); err != nil {
			return nil, err
		}
		found = append(found, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// decrypt with the rows closed: a key read with them open would take
	// a second pooled connection while holding the first (see readLog)
	rows.Close()
	out := make(map[uuid.UUID]EntityState, len(found))
	for _, r := range found {
		data := r.data
		if c.tables[entity] == nil { // @table excludes @pii by validation
			if data, err = c.decryptFields(ctx, c.db, namespace, r.id, data, proj.PII); err != nil {
				return nil, err
			}
		}
		state := proj.NewState()
		if err := json.Unmarshal(data, state); err != nil {
			return nil, err
		}
		out[r.id] = state
	}
	return out, nil
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
