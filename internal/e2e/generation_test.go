package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/billing"
)

// The process lease and a deploy (ADR 0010): an instance of the previous
// revision that is still alive must not keep the processes on the old
// code. A follower of a strictly newer generation takes the lease; an
// equal or older one never does; with no generation the election is as
// it always was.

const genPoll = 100 * time.Millisecond

// startGen starts a billing instance of the given generation (0: none)
// and answers it with the function that stops it.
func startGen(t *testing.T, ctx context.Context, pool *pgxpool.Pool, gen int64, revision string, migrate bool) (*loom.Client, context.CancelFunc) {
	t.Helper()
	cli, err := loom.New(loom.Config{DB: pool, Registry: billing.NewRegistry(), Keys: testKeys(t),
		Generation: gen, Revision: revision})
	if err != nil {
		t.Fatal(err)
	}
	if migrate {
		if err := cli.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
	}
	ictx, cancel := context.WithCancel(ctx)
	if err := cli.Start(ictx, genPoll); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cancel)
	return cli, cancel
}

// neverLeads fails if cli leads at any moment over d.
func neverLeads(t *testing.T, cli *loom.Client, d time.Duration, what string) {
	t.Helper()
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(5 * time.Millisecond) {
		if cli.Leading() {
			t.Fatalf("%s leads", what)
		}
	}
}

func resetBilling() {
	billing.Gateway.Reset()
	billing.Gateway.SetDelay(0)
	billing.SetLastReceipt("")
	billing.SetFailReactAfterCapture(0)
	billing.ResetReactions()
}

func assertNothingParked(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	var parked, doubt int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM loom_dead_letters`).Scan(&parked); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM loom_effects WHERE status <> 'done'`).Scan(&doubt); err != nil {
		t.Fatal(err)
	}
	if parked != 0 || doubt != 0 {
		t.Fatalf("%d parked, %d effects not done", parked, doubt)
	}
}

// TestANewerGenerationTakesTheProcessLease is a deploy: revision 20
// leads and is mid-reaction when revision 21 starts. 21 leads within a
// poll or two, 20 finishes the reaction it was on first (each invoice is
// captured exactly once, nothing parked), 20 never leads again while 21
// is alive, and takes the lease back only once 21 is gone.
func TestANewerGenerationTakesTheProcessLease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := testDB(t, ctx)
	resetBilling()

	old, _ := startGen(t, ctx, pool, 0, "billing-00020-abc", true)
	waitFor(t, ctx, "revision 20 leads", old.Leading)

	// reactions long enough that the hand-over lands in the middle of one
	billing.Gateway.SetDelay(300 * time.Millisecond)
	const invoices = 4
	for i := 0; i < invoices; i++ {
		payInvoice(t, ctx, old, uuid.New())
	}
	waitFor(t, ctx, "revision 20 reacting", func() bool { return billing.Gateway.CallsN() >= 1 })

	started := time.Now()
	newer, stopNewer := startGen(t, ctx, pool, 0, "billing-00021-xyz", false)
	waitFor(t, ctx, "revision 21 leads", newer.Leading)
	took := time.Since(started)
	if old.Leading() {
		t.Fatal("two leaders after the hand-over")
	}
	// 20 yields once its reaction checkpoints (300ms at most here), and
	// 21 tries every fastPoll meanwhile
	if took > 300*time.Millisecond+4*genPoll {
		t.Fatalf("the hand-over took %s", took)
	}
	t.Logf("hand-over in %s", took)

	waitFor(t, ctx, "every invoice captured", func() bool { return billing.Gateway.CallsN() == invoices })
	time.Sleep(5 * genPoll)
	if n := billing.Gateway.CallsN(); n != invoices {
		t.Fatalf("gateway called %d times for %d invoices: the hand-over reacted twice", n, invoices)
	}
	if n := billing.Reactions(); n != invoices {
		t.Fatalf("the process reacted %d times for %d invoices", n, invoices)
	}
	assertNothingParked(t, ctx, pool)

	// an older generation never takes the lease back from a newer one,
	// not even when the newer one's session is briefly away from it
	neverLeads(t, old, 10*genPoll, "revision 20, with 21 alive,")
	if !newer.Leading() {
		t.Fatal("revision 21 lost the lease")
	}
	var app string
	if err := pool.QueryRow(ctx, `
		SELECT a.application_name FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
		WHERE l.locktype = 'advisory' AND l.granted AND l.objid = (hashtext('loom_processes_billing')::bigint & 4294967295)::oid`).Scan(&app); err != nil {
		t.Fatal(err)
	}
	if app != "loom lease 21 billing-00021-xyz" {
		t.Fatalf("the lease session is named %q", app)
	}
	// 20's lease connection went back to its pool unnamed and deaf
	var named int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_stat_activity
		WHERE datname = current_database() AND application_name LIKE 'loom lease 20%'`).Scan(&named); err != nil {
		t.Fatal(err)
	}
	if named != 0 {
		t.Fatalf("%d sessions still named for revision 20's lease", named)
	}

	// 21 is the one that keeps reacting
	billing.Gateway.Reset()
	billing.Gateway.SetDelay(0)
	billing.ResetReactions()
	for i := 0; i < 3; i++ {
		payInvoice(t, ctx, old, uuid.New())
	}
	waitFor(t, ctx, "captures under 21", func() bool { return billing.Gateway.CallsN() == 3 })

	// with 21 gone, 20 leads again: better the old code than none
	stopNewer()
	waitFor(t, ctx, "revision 20 leads once 21 is gone", old.Leading)
}

// TestAnOlderGenerationNeverTakesTheLease: revision 21 leads; revision 20
// started after it (a tagged revision woken later) follows for good, and
// so does another instance of 21.
func TestAnOlderGenerationNeverTakesTheLease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := testDB(t, ctx)
	resetBilling()

	newer, _ := startGen(t, ctx, pool, 0, "billing-00021-xyz", true)
	waitFor(t, ctx, "revision 21 leads", newer.Leading)
	old, _ := startGen(t, ctx, pool, 0, "billing-00020-abc", false)
	twin, _ := startGen(t, ctx, pool, 0, "billing-00021-def", false)
	explicit, _ := startGen(t, ctx, pool, 21, "", false) // the same generation, given explicitly
	end := time.Now().Add(15 * genPoll)
	for time.Now().Before(end) {
		if old.Leading() || twin.Leading() || explicit.Leading() || !newer.Leading() {
			t.Fatalf("the lease moved: 21 %v, 20 %v, 21 twin %v, 21 explicit %v",
				newer.Leading(), old.Leading(), twin.Leading(), explicit.Leading())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestALeaderThatDoesNotYieldIsEnded: an older generation's leader that
// never answers — a CPU-throttled instance, a wedged one — holds the
// lease on a session named for its generation and listens to nothing.
// The newer follower ends that session after two polls and leads.
func TestALeaderThatDoesNotYieldIsEnded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := testDB(t, ctx)
	resetBilling()
	migrate(t, ctx, pool)

	wedged := holdLease(t, ctx, pool, "loom lease 20 billing-00020-abc")
	started := time.Now()
	newer, _ := startGen(t, ctx, pool, 0, "billing-00021-xyz", false)
	waitFor(t, ctx, "revision 21 leads", newer.Leading)
	took := time.Since(started)
	if took < 2*genPoll {
		t.Fatalf("led after %s: the wedged leader was not given two polls", took)
	}
	t.Logf("hand-over from a wedged leader in %s", took)
	if err := wedged.Ping(ctx); err == nil {
		t.Fatal("the wedged leader's session is still alive")
	}
}

// TestNoGenerationIsTheElectionAsItWas: with no generation (no
// K_REVISION, Generation unset) an instance never takes the lease from
// anyone, and a leader whose session names no generation (an older
// loom, an instance with none) is never taken from, even by a newer
// generation.
func TestNoGenerationIsTheElectionAsItWas(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := testDB(t, ctx)
	resetBilling()
	t.Setenv("K_REVISION", "")

	first, stopFirst := startGen(t, ctx, pool, 0, "", true)
	waitFor(t, ctx, "the first instance leads", first.Leading)
	second, _ := startGen(t, ctx, pool, 0, "", false)
	newer, _ := startGen(t, ctx, pool, 21, "billing-00021-xyz", false)
	end := time.Now().Add(15 * genPoll)
	for time.Now().Before(end) {
		if !first.Leading() || second.Leading() || newer.Leading() {
			t.Fatalf("the lease moved from a leader with no generation: first %v, second %v, 21 %v",
				first.Leading(), second.Leading(), newer.Leading())
		}
		time.Sleep(5 * time.Millisecond)
	}
	var app string
	if err := pool.QueryRow(ctx, `
		SELECT a.application_name FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
		WHERE l.locktype = 'advisory' AND l.granted AND l.objid = (hashtext('loom_processes_billing')::bigint & 4294967295)::oid`).Scan(&app); err != nil {
		t.Fatal(err)
	}
	if app != "" {
		t.Fatalf("a leader with no generation named its session %q", app)
	}
	// it goes, and someone else leads at the next poll, as ever
	stopFirst()
	waitFor(t, ctx, "a new leader", func() bool { return second.Leading() || newer.Leading() })

	// an older loom's leader: the lock on a session that is not named
	// for a generation
	pool2 := testDB(t, ctx)
	migrate(t, ctx, pool2)
	unnamed := holdLease(t, ctx, pool2, "billing-00019-old")
	newer2, _ := startGen(t, ctx, pool2, 0, "billing-00021-xyz", false)
	neverLeads(t, newer2, 15*genPoll, "a newer generation, over a leader that names none,")
	if err := unnamed.Ping(ctx); err != nil {
		t.Fatalf("the unnamed leader's session was ended: %v", err)
	}
}

func migrate(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	cli, err := loom.New(loom.Config{DB: pool, Registry: billing.NewRegistry(), Keys: testKeys(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
}

// holdLease takes billing's process lease on a session of its own named
// app, and does nothing else with it.
func holdLease(t *testing.T, ctx context.Context, pool *pgxpool.Pool, app string) *pgx.Conn {
	t.Helper()
	cfg := pool.Config().ConnConfig.Copy()
	cfg.RuntimeParams["application_name"] = app
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext('loom_processes_billing'))`).Scan(&got); err != nil || !got {
		t.Fatalf("taking the lease: %v %v", got, err)
	}
	return conn
}
