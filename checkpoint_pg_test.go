package loom

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// These tests need a Postgres (LOOM_TEST_PG, default
// postgres://postgres:mysecret@localhost:5432/postgres) and skip without
// one. They live in package loom, not internal/e2e, to drive a runner's
// step directly and count the statements it sends.

// statementCounter is a pgx tracer counting every statement a pool
// sends, begin and commit included.
type statementCounter struct {
	mu   sync.Mutex
	sqls []string
}

func (s *statementCounter) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	s.mu.Lock()
	s.sqls = append(s.sqls, strings.Join(strings.Fields(data.SQL), " "))
	s.mu.Unlock()
	return ctx
}

func (s *statementCounter) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (s *statementCounter) take() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.sqls
	s.sqls = nil
	return out
}

// checkpointPG is a database of the test's own on LOOM_TEST_PG's server,
// dropped when the test ends; tracer, when set, sees every statement.
func checkpointPG(t *testing.T, tracer pgx.QueryTracer) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin := os.Getenv("LOOM_TEST_PG")
	if admin == "" {
		admin = "postgres://postgres:mysecret@localhost:5432/postgres"
	}
	adminPool, err := pgxpool.New(ctx, admin)
	if err != nil {
		t.Skipf("no postgres available: %v", err)
	}
	if err := adminPool.Ping(ctx); err != nil {
		adminPool.Close()
		t.Skipf("no postgres available: %v", err)
	}
	name := "loom_ckpt_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	if _, err := adminPool.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		adminPool.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = adminPool.Exec(ctx, "DROP DATABASE "+name+" WITH (FORCE)")
		adminPool.Close()
	})
	cfg, err := pgxpool.ParseConfig(admin)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Database = name
	cfg.ConnConfig.Tracer = tracer
	cfg.MaxConns = 20
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// spreadTypes is the event types of the spread service, T00 to T19: a
// runner subscribed to one of them subscribes to 5% of a log that
// cycles through them.
var spreadTypes = func() []string {
	out := make([]string, 20)
	for i := range out {
		out[i] = fmt.Sprintf("T%02d", i)
	}
	return out
}()

type spreadPayload struct {
	N int `json:"n"`
}

type spreadState struct {
	Folds int `json:"folds"`
}

func (s *spreadState) Fold(string, any) error { s.Folds++; return nil }

// spreadHandled is what the spread processes reacted to, by process.
type spreadHandled struct {
	mu   sync.Mutex
	seqs map[string][]int64
}

func (h *spreadHandled) add(process string, seq int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.seqs == nil {
		h.seqs = map[string][]int64{}
	}
	h.seqs[process] = append(h.seqs[process], seq)
}

func (h *spreadHandled) all() map[string][]int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := map[string][]int64{}
	for k, v := range h.seqs {
		out[k] = slices.Clone(v)
	}
	return out
}

// spreadRegistry is twenty runners over twenty event types, one type
// each: projections P00..P09 fold T00..T09 into a row per event, and
// processes R10..R19 react to T10..T19.
func spreadRegistry(handled *spreadHandled) *Registry {
	reg := &Registry{Service: "spread"}
	for _, typ := range spreadTypes {
		reg.Events = append(reg.Events, &EventDef{Name: typ, SchemaVersion: 1, New: func() any { return &spreadPayload{} }})
	}
	for i, typ := range spreadTypes {
		if i < 10 {
			reg.Projections = append(reg.Projections, &ProjectionDef{
				Name:     fmt.Sprintf("P%02d", i),
				Events:   []string{typ},
				Entity:   "Spread",
				NewState: func() EntityState { return &spreadState{} },
				EntityID: func(evt *Event) uuid.UUID { return evt.AggregateID },
			})
			continue
		}
		name := fmt.Sprintf("R%02d", i)
		reg.Processes = append(reg.Processes, &ReactorDef{
			Name:   name,
			Events: []string{typ},
			Subs:   []SubscriptionDef{{Event: typ}},
			From:   FromOrigin,
			React: func(_ context.Context, evt *Event) ([]Command, error) {
				handled.add(name, evt.GlobalSeq)
				return nil, nil
			},
		})
	}
	return reg
}

// appendSpread appends events n of the spread log, from..to-1, one
// statement each: the i-th is of type T(i mod 20).
func appendSpread(t *testing.T, ctx context.Context, pool *pgxpool.Pool, from, to int, gap time.Duration) {
	t.Helper()
	for i := from; i < to; i++ {
		data, _ := json.Marshal(spreadPayload{N: i})
		if _, err := pool.Exec(ctx, `
			INSERT INTO loom_events (service, namespace, aggregate_type, aggregate_id, version, type, schema_version, at, data)
			VALUES ('spread', 'default', 'Spread', $1, 1, $2, 1, now(), $3)`,
			uuid.New(), spreadTypes[i%len(spreadTypes)], data); err != nil {
			t.Fatal(err)
		}
		if gap > 0 {
			time.Sleep(gap)
		}
	}
}

// countCheckpointWritesPG counts every write that sets a checkpoint's
// position, by a trigger: what Postgres did, not what a runner says.
func countCheckpointWritesPG(t *testing.T, ctx context.Context, pool *pgxpool.Pool) func() int {
	t.Helper()
	for _, sql := range []string{
		`CREATE TABLE checkpoint_writes (runner text NOT NULL, global_seq bigint NOT NULL)`,
		`CREATE FUNCTION note_checkpoint_write() RETURNS trigger AS $$
		 BEGIN
			INSERT INTO checkpoint_writes VALUES (NEW.runner, NEW.global_seq);
			RETURN NEW;
		 END $$ LANGUAGE plpgsql`,
		`CREATE TRIGGER checkpoint_written AFTER INSERT OR UPDATE OF global_seq ON loom_checkpoints
		 FOR EACH ROW EXECUTE FUNCTION note_checkpoint_write()`,
	} {
		if _, err := pool.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	return func() int {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM checkpoint_writes`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
}

func spreadCheckpoints(t *testing.T, ctx context.Context, pool *pgxpool.Pool) map[string]int64 {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT runner, global_seq FROM loom_checkpoints WHERE service='spread'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var runner string
		var seq int64
		if err := rows.Scan(&runner, &seq); err != nil {
			t.Fatal(err)
		}
		out[runner] = seq
	}
	return out
}

func waitUntil(t *testing.T, what string, within time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestTwentyRunnersWriteTheirCheckpointsOnlyWhenItMatters is the goal's
// first acceptance check: twenty runners over a thousand events, each
// subscribed to 5% of them, arriving a few at a time. Each runner
// applies its fifty and passes the other 950; the checkpoint writes are
// at most two an event (they were ~46 an event on runsheet's e2e, a
// write per runner per step). Then a crash and a restart: each runner
// resumes from its row, re-passes at most the tail it had not written,
// applies nothing twice, and its row reaches the head.
func TestTwentyRunnersWriteTheirCheckpointsOnlyWhenItMatters(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	pool := checkpointPG(t, nil)
	handled := &spreadHandled{}
	reg := spreadRegistry(handled)
	newClient := func() *Client {
		cli, err := New(Config{DB: pool, Registry: reg, StepConcurrency: 8})
		if err != nil {
			t.Fatal(err)
		}
		return cli
	}
	first := newClient()
	if err := first.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	writes := countCheckpointWritesPG(t, ctx, pool)

	// a short poll, so every runner steps many times while the events
	// trickle in: the case that cost a write a step
	const events = 1000
	runCtx, crash := context.WithCancel(ctx)
	if err := first.Start(runCtx, 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	appendSpread(t, ctx, pool, 0, events, 2*time.Millisecond)
	var head int64
	if err := pool.QueryRow(ctx, `SELECT max(global_seq) FROM loom_events WHERE service='spread'`).Scan(&head); err != nil {
		t.Fatal(err)
	}
	// every runner applies its share; the rows reach the head within the
	// liveness bound (Config.CheckpointEvery) of the log going quiet
	waitUntil(t, "every runner's row at the head", 30*time.Second, func() bool {
		cps := spreadCheckpoints(t, ctx, pool)
		if len(cps) != len(spreadTypes) {
			return false
		}
		for _, seq := range cps {
			if seq != head {
				return false
			}
		}
		return true
	})
	took := time.Since(began)
	n := writes()
	perEvent := float64(n) / events
	t.Logf("%d checkpoint writes for %d events across %d runners in %s: %.2f an event", n, events, len(spreadTypes), took.Round(time.Millisecond), perEvent)
	if perEvent > 2 {
		t.Fatalf("%d checkpoint writes for %d events (%.2f an event), want at most 2 an event", n, events, perEvent)
	}
	checkApplied := func(when string) {
		t.Helper()
		got := handled.all()
		for i := 10; i < 20; i++ {
			name := fmt.Sprintf("R%02d", i)
			if len(got[name]) != events/len(spreadTypes) {
				t.Fatalf("%s: %s handled %d events, want %d once each", when, name, len(got[name]), events/len(spreadTypes))
			}
			if !slices.IsSorted(got[name]) || len(slices.Compact(slices.Clone(got[name]))) != len(got[name]) {
				t.Fatalf("%s: %s handled %v, want each event once and in order", when, name, got[name])
			}
		}
		var rows, twice int
		if err := pool.QueryRow(ctx, `
			SELECT count(*), count(*) FILTER (WHERE (data->>'folds')::int <> 1)
			FROM loom_entities WHERE service='spread' AND entity_type='Spread'`).Scan(&rows, &twice); err != nil {
			t.Fatal(err)
		}
		if rows != events/2 || twice != 0 {
			t.Fatalf("%s: the projections hold %d rows, %d folded other than once; want %d rows, each folded once", when, rows, twice, events/2)
		}
	}
	checkApplied("before the crash")
	if behind, err := first.RunnersBehind(ctx, head); err != nil || len(behind) != 0 {
		t.Fatalf("runners behind the head: %v, %v; want none", behind, err)
	}
	// a row of a runner this registry does not declare counts behind
	// whenever it is short: what it subscribes to is not known here
	if _, err := pool.Exec(ctx, `INSERT INTO loom_checkpoints (service, runner, global_seq) VALUES ('spread', 'projection:elsewhere', $1)`, head-1); err != nil {
		t.Fatal(err)
	}
	if behind, err := first.RunnersBehind(ctx, head); err != nil || !slices.Equal(behind, []string{"projection:elsewhere"}) {
		t.Fatalf("runners behind the head: %v, %v; want the undeclared one", behind, err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM loom_checkpoints WHERE runner = 'projection:elsewhere'`); err != nil {
		t.Fatal(err)
	}

	// a crash mid-trickle: no runner gets to write what it has passed
	appendSpread(t, ctx, pool, events, events+40, 0)
	time.Sleep(50 * time.Millisecond)
	crash()
	waitUntil(t, "the first instance stopping", 10*time.Second, func() bool { return !first.Leading() })
	before := handled.all()
	var head2 int64
	if err := pool.QueryRow(ctx, `SELECT max(global_seq) FROM loom_events WHERE service='spread'`).Scan(&head2); err != nil {
		t.Fatal(err)
	}
	crashed := spreadCheckpoints(t, ctx, pool)
	for runner, seq := range crashed {
		if seq < head || seq > head2 {
			t.Fatalf("after the crash %s's row is at %d, want between %d and %d", runner, seq, head, head2)
		}
	}

	second := newClient()
	if err := second.Start(ctx, 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "every runner's row at the new head after the restart", 30*time.Second, func() bool {
		for _, seq := range spreadCheckpoints(t, ctx, pool) {
			if seq != head2 {
				return false
			}
		}
		return true
	})
	after := handled.all()
	for name, seqs := range after {
		// what was handled before the crash is not handled again: the
		// restart picked up after it
		if !slices.Equal(seqs[:len(before[name])], before[name]) {
			t.Fatalf("%s handled %v before the crash and %v after", name, before[name], seqs)
		}
		if len(slices.Compact(slices.Clone(seqs))) != len(seqs) {
			t.Fatalf("%s handled an event twice across the restart: %v", name, seqs)
		}
		if len(seqs) != (events+40)/len(spreadTypes) {
			t.Fatalf("%s handled %d events across the restart, want %d", name, len(seqs), (events+40)/len(spreadTypes))
		}
	}
	var twice int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE (data->>'folds')::int <> 1)
		FROM loom_entities WHERE service='spread' AND entity_type='Spread'`).Scan(&twice); err != nil {
		t.Fatal(err)
	}
	if twice != 0 {
		t.Fatalf("%d rows folded more than once across the restart", twice)
	}
}

// TestAnIdleRunnerStepIsOneStatement is the goal's second acceptance
// check: a projection or process with nothing new costs Postgres one
// statement when it looks at its row — lock, checkpoint and any new
// event at once — with no transaction around it, and nothing at all
// once it knows where it is and the fan-out says there is nothing new.
// What it passes is written by the client's flusher, one statement for
// every runner.
func TestAnIdleRunnerStepIsOneStatement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	counter := &statementCounter{}
	pool := checkpointPG(t, counter)
	handled := &spreadHandled{}
	cli, err := New(Config{DB: pool, Registry: spreadRegistry(handled)})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	appendSpread(t, ctx, pool, 0, 60, 0)
	head, err := cli.maxLogSeq(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// no Start: the steps are driven by hand. The fan-out's head is the
	// log's, so the steps read the log straight.
	cli.fan.setHead(head)
	cli.leader.Store(true)

	for _, tc := range []struct {
		runner string
		step   func(pos *runnerPos) func(context.Context) (int, error)
	}{
		{"projection:P03", func(pos *runnerPos) func(context.Context) (int, error) {
			return cli.projectionStep(cli.reg.Projections[3], pos)
		}},
		{"process:R13", func(pos *runnerPos) func(context.Context) (int, error) {
			return cli.processStep(cli.reg.Processes[3], []string{"T13"}, pos)
		}},
	} {
		t.Run(tc.runner, func(t *testing.T) {
			pos := cli.track(newRunnerPos(tc.runner, nil))
			step := tc.step(pos)
			for i := 0; ; i++ {
				n, err := step(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if n == 0 {
					break
				}
				if i > 100 {
					t.Fatal("never went idle")
				}
			}
			// what it passed is written by the flusher
			if err := cli.flushCheckpoints(ctx); err != nil {
				t.Fatal(err)
			}
			if cps := spreadCheckpoints(t, ctx, pool); cps[tc.runner] != head {
				t.Fatalf("caught up at %d, want the head %d", cps[tc.runner], head)
			}

			pos.forget() // the runner does not know where it is, as after a resync
			counter.take()
			if n, err := step(ctx); err != nil || n != 0 {
				t.Fatalf("idle step: %d, %v", n, err)
			}
			sqls := counter.take()
			if len(sqls) != 1 {
				t.Fatalf("an idle step that looks at its row sent %d statements, want one: %q", len(sqls), sqls)
			}
			for _, sql := range sqls {
				if s := strings.ToLower(sql); s == "begin" || s == "commit" || s == "rollback" {
					t.Fatalf("an idle step opened a transaction: %q", sqls)
				}
			}
			t.Logf("idle look: %s", sqls[0])

			// and nothing at all once it knows where it is
			if n, err := step(ctx); err != nil || n != 0 {
				t.Fatalf("idle step: %d, %v", n, err)
			}
			if sqls := counter.take(); len(sqls) != 0 {
				t.Fatalf("an idle step that knows where it is sent %q, want nothing", sqls)
			}

			// a new event of somebody else's is passed in memory, and the
			// step after is idle again without a statement
			appendSpread(t, ctx, pool, 0, 1, 0) // T00: nobody here but P00 folds it
			newHead, _ := cli.maxLogSeq(ctx)
			cli.fan.ingest(mustReadLog(t, ctx, cli, head))
			counter.take()
			if n, err := step(ctx); err != nil || n != 1 {
				t.Fatalf("passing step: %d, %v", n, err)
			}
			if n, err := step(ctx); err != nil || n != 0 {
				t.Fatalf("idle step: %d, %v", n, err)
			}
			if sqls := counter.take(); len(sqls) != 0 {
				t.Fatalf("passing one ignored event sent %q, want nothing", sqls)
			}
			if pos.seq != newHead || !pos.dirty() {
				t.Fatalf("position %d (dirty %v), want %d held in memory", pos.seq, pos.dirty(), newHead)
			}
			// the flush writes it, and reads every row back: one statement
			if err := cli.flushCheckpoints(ctx); err != nil {
				t.Fatal(err)
			}
			if sqls := counter.take(); len(sqls) != 1 {
				t.Fatalf("the flush sent %q, want one statement", sqls)
			}
			if cps := spreadCheckpoints(t, ctx, pool); cps[tc.runner] != newHead || pos.dirty() {
				t.Fatalf("the flush left the row at %d (dirty %v), want %d", cps[tc.runner], pos.dirty(), newHead)
			}

			// a row moved under the runner (a rewind, another instance)
			// is noticed by the next flush, and the runner reads it again
			if _, err := pool.Exec(ctx, `UPDATE loom_checkpoints SET global_seq = $2 WHERE service='spread' AND runner=$1`, tc.runner, head); err != nil {
				t.Fatal(err)
			}
			if err := cli.flushCheckpoints(ctx); err != nil {
				t.Fatal(err)
			}
			if pos.known {
				t.Fatal("the flush did not notice the row move under the runner")
			}
			counter.take()
			if n, err := step(ctx); err != nil || n == 0 {
				t.Fatalf("the step after the move: %d, %v; want it to read from the moved row", n, err)
			}
			for i := 0; i < 10; i++ {
				if n, err := step(ctx); err != nil || n == 0 {
					break
				}
			}
			if err := cli.flushCheckpoints(ctx); err != nil {
				t.Fatal(err)
			}
			if cps := spreadCheckpoints(t, ctx, pool); cps[tc.runner] != newHead {
				t.Fatalf("after the move the runner's row is at %d, want %d", cps[tc.runner], newHead)
			}
			head = newHead
		})
	}
}

func mustReadLog(t *testing.T, ctx context.Context, cli *Client, after int64) []*Event {
	t.Helper()
	evts, err := cli.readLog(ctx, cli.db, after, logBatch)
	if err != nil {
		t.Fatal(err)
	}
	return evts
}
