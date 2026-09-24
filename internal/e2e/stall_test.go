package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/billing"
	billinggen "github.com/go-apis/loom/internal/e2e/billing/loomgen"
)

// brokenRunner is a projection beside billing's healthy invoiceSummary,
// reading the same events, whose fold always fails on InvoicePaid.
const brokenRunner = "projection:brokenSummary"

// stallFixture is billing plus brokenSummary, started, with an invoice
// raised and paid (the paid event is the one brokenSummary cannot fold)
// and a second invoice raised after it. It returns the client, the error
// log, and the InvoicePaid event's global_seq.
func stallFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (*loom.Client, *errorLog, int64) {
	t.Helper()
	reg := billing.NewRegistry()
	reg.Projections = append(reg.Projections, &loom.ProjectionDef{
		Name:     "brokenSummary",
		Entity:   "BrokenSummary",
		Events:   []string{"InvoicePaid", "InvoiceRaised"},
		NewState: func() loom.EntityState { return &billinggen.InvoiceSummary{} },
		EntityID: func(evt *loom.Event) uuid.UUID { return evt.AggregateID },
		Fold: func(state loom.EntityState, evt *loom.Event) error {
			if evt.Type == "InvoicePaid" {
				return errors.New("cannot fold InvoicePaid")
			}
			return state.Fold(evt.Type, evt.Data)
		},
	})
	errs := &errorLog{}
	cli, err := loom.New(loom.Config{
		DB: pool, Bus: loom.NewMemoryBus(), Registry: reg, Keys: testKeys(t),
		Logger: slog.New(errs),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cli.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	paid := uuid.New()
	raiseInvoice(t, ctx, cli, paid)
	if err := cli.Dispatch(ctx, &billinggen.MarkInvoicePaid{
		CommandBase: loom.CommandBase{AggregateID: paid, Namespace: "default"},
	}); err != nil {
		t.Fatal(err)
	}
	raiseInvoice(t, ctx, cli, uuid.New())

	var paidSeq int64
	if err := pool.QueryRow(ctx, `
		SELECT global_seq FROM loom_events WHERE service='billing' AND type='InvoicePaid' AND aggregate_id=$1`,
		paid).Scan(&paidSeq); err != nil {
		t.Fatal(err)
	}
	return cli, errs, paidSeq
}

type stallRow struct {
	seq, failingSeq int64
	attempts        int
	lastError       string
	stalledSince    *time.Time
}

func stallOf(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runner string) stallRow {
	t.Helper()
	var r stallRow
	err := pool.QueryRow(ctx, `
		SELECT global_seq, failing_seq, attempts, last_error, stalled_since
		FROM loom_checkpoints WHERE service='billing' AND runner=$1`, runner).
		Scan(&r.seq, &r.failingSeq, &r.attempts, &r.lastError, &r.stalledSince)
	if err != nil {
		return stallRow{seq: -1}
	}
	return r
}

// TestFoldFailureStallsRunner: a fold that always fails leaves the runner
// stalled with the seq and error while other runners advance.
func TestFoldFailureStallsRunner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := testDB(t, ctx)
	cli, _, paidSeq := stallFixture(t, ctx, pool)

	waitFor(t, ctx, "healthy projection past the failing event", func() bool {
		return checkpointOf(t, ctx, pool, "billing", "projection:invoiceSummary") > paidSeq
	})
	waitFor(t, ctx, "broken projection stalled", func() bool {
		return stallOf(t, ctx, pool, brokenRunner).stalledSince != nil
	})
	first := stallOf(t, ctx, pool, brokenRunner)
	if first.failingSeq != paidSeq {
		t.Fatalf("failing_seq = %d, want the InvoicePaid event's %d", first.failingSeq, paidSeq)
	}
	if !strings.Contains(first.lastError, "cannot fold InvoicePaid") {
		t.Fatalf("last_error = %q", first.lastError)
	}
	if first.seq >= paidSeq {
		t.Fatalf("checkpoint %d moved to or past the failing event %d", first.seq, paidSeq)
	}

	// halted, not skipped: later attempts fail on the same event, the
	// checkpoint stays put, and stalled_since keeps the first failure
	waitFor(t, ctx, "a second attempt", func() bool {
		return stallOf(t, ctx, pool, brokenRunner).attempts >= 2
	})
	later := stallOf(t, ctx, pool, brokenRunner)
	if later.seq != first.seq || later.failingSeq != paidSeq {
		t.Fatalf("stalled runner moved: %+v then %+v", first, later)
	}
	if !later.stalledSince.Equal(*first.stalledSince) {
		t.Fatalf("stalled_since reset on a retry: %v then %v", first.stalledSince, later.stalledSince)
	}

	// the read surface shows what skip acts on
	srv := httptest.NewServer(cli.HTTPHandler())
	defer srv.Close()
	var runners struct {
		Runners []struct {
			Runner       string     `json:"runner"`
			Stalled      bool       `json:"stalled"`
			FailingSeq   int64      `json:"failing_seq"`
			LastError    string     `json:"last_error"`
			StalledSince *time.Time `json:"stalled_since"`
		} `json:"runners"`
	}
	getJSON(t, ctx, srv.URL+"/runners", &runners)
	seen := map[string]bool{}
	for _, r := range runners.Runners {
		seen[r.Runner] = r.Stalled
		if r.Runner == brokenRunner && (r.FailingSeq != paidSeq || r.LastError == "" || r.StalledSince == nil) {
			t.Fatalf("/runners shows %+v", r)
		}
	}
	if !seen[brokenRunner] || seen["projection:invoiceSummary"] {
		t.Fatalf("/runners stalled flags: %v", seen)
	}
}

// TestSkipStalledProjection: the skip endpoint parks the failing event,
// clears the stall, and the runner moves on past it.
func TestSkipStalledProjection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := testDB(t, ctx)
	cli, _, paidSeq := stallFixture(t, ctx, pool)
	waitFor(t, ctx, "broken projection stalled", func() bool {
		return stallOf(t, ctx, pool, brokenRunner).stalledSince != nil
	})

	srv := httptest.NewServer(cli.HTTPHandler())
	defer srv.Close()
	skip := func() *http.Response {
		t.Helper()
		resp, err := http.Post(srv.URL+"/runners/"+brokenRunner+"/skip", "application/json", nil)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	resp := skip()
	var out struct {
		Seq int64 `json:"seq"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || out.Seq != paidSeq {
		t.Fatalf("skip: %d, seq %d (want %d)", resp.StatusCode, out.Seq, paidSeq)
	}

	cleared := stallOf(t, ctx, pool, brokenRunner)
	if cleared.failingSeq != 0 || cleared.attempts != 0 || cleared.lastError != "" || cleared.stalledSince != nil {
		t.Fatalf("stall not cleared: %+v", cleared)
	}
	if cleared.seq < paidSeq {
		t.Fatalf("checkpoint %d not past the skipped event %d", cleared.seq, paidSeq)
	}
	waitFor(t, ctx, "broken projection past the skipped event", func() bool {
		r := stallOf(t, ctx, pool, brokenRunner)
		return r.seq > paidSeq && r.stalledSince == nil
	})

	letters, err := cli.DeadLetters(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	var parked bool
	for _, d := range letters {
		var env struct {
			GlobalSeq int64 `json:"global_seq"`
		}
		_ = json.Unmarshal(d.Envelope, &env)
		if d.Runner == brokenRunner && strings.Contains(d.Error, "cannot fold InvoicePaid") && env.GlobalSeq == paidSeq {
			parked = true
		}
	}
	if !parked {
		t.Fatalf("skipped event %d not in dead letters (%d letters)", paidSeq, len(letters))
	}

	// nothing left to skip
	resp = skip()
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("skip on a healthy projection: %d, want 409", resp.StatusCode)
	}
}

// TestRunnerFailureLogCeiling: a stalled runner's `runner step failed`
// does not repeat within a minute however many polls fail, and a runner
// that never gets to write its checkpoint row says so, once.
func TestRunnerFailureLogCeiling(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := testDB(t, ctx)

	// payeeDirectory can never step: another "instance" holds its lock
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(hashtext('loom_billing_projection:payeeDirectory'))`); err != nil {
		t.Fatal(err)
	}

	_, errs, _ := stallFixture(t, ctx, pool)
	waitFor(t, ctx, "several failed attempts", func() bool {
		return stallOf(t, ctx, pool, brokenRunner).attempts >= 4
	})

	var failed []string
	for _, m := range errs.matching("runner step failed") {
		if strings.Contains(m, brokenRunner) {
			failed = append(failed, m)
		}
	}
	if len(failed) != 1 {
		t.Fatalf("runner step failed logged %d times across %d attempts, want once: %v",
			len(failed), stallOf(t, ctx, pool, brokenRunner).attempts, failed)
	}

	var silent []string
	for _, m := range errs.matching("runner never checkpointed") {
		if strings.Contains(m, "projection:payeeDirectory") {
			silent = append(silent, m)
		} else if strings.Contains(m, "projection:") {
			t.Fatalf("a runner that checkpoints was reported silent: %s", m)
		}
	}
	if len(silent) != 1 {
		t.Fatalf("runner never checkpointed logged %d times, want once: %v", len(silent), silent)
	}
}
