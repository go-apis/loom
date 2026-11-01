package e2e_test

import (
	"context"
	"strconv"
	"strings"
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
// it always was. Generations are deploy times, and the revision names
// are numbered as Cloud Run numbered latchkey's on 2026-11-01: the later
// deploy has the lower number.

const genPoll = 100 * time.Millisecond

// Deploy generations: the earlier deploy and the later one.
const (
	genEarlier = int64(1793440000)
	genLater   = int64(1793453000)
)

// noAmbientGeneration keeps the test's instances to the generations it
// gives them.
func noAmbientGeneration(t *testing.T) {
	t.Setenv("LOOM_GENERATION", "")
	t.Setenv("K_SERVICE", "")
	t.Setenv("K_REVISION", "")
}

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

// TestANewerGenerationTakesTheProcessLease is a deploy: revision 380
// leads and is mid-reaction when revision 355, deployed later, starts.
// 355 leads within a poll or two, 380 finishes the reaction it was on
// first (each invoice is captured exactly once, nothing parked), 380
// never leads again while 355 is alive, and takes the lease back only
// once 355 is gone. The revision numbers play no part.
func TestANewerGenerationTakesTheProcessLease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := testDB(t, ctx)
	resetBilling()
	noAmbientGeneration(t)

	old, _ := startGen(t, ctx, pool, genEarlier, "billing-00380-xih", true)
	waitFor(t, ctx, "revision 380 leads", old.Leading)

	// reactions long enough that the hand-over lands in the middle of one
	billing.Gateway.SetDelay(300 * time.Millisecond)
	const invoices = 4
	for i := 0; i < invoices; i++ {
		payInvoice(t, ctx, old, uuid.New())
	}
	waitFor(t, ctx, "revision 380 reacting", func() bool { return billing.Gateway.CallsN() >= 1 })

	started := time.Now()
	newer, stopNewer := startGen(t, ctx, pool, genLater, "billing-00355-7rg", false)
	waitFor(t, ctx, "revision 355 leads", newer.Leading)
	took := time.Since(started)
	if old.Leading() {
		t.Fatal("two leaders after the hand-over")
	}
	// 380 yields once its reaction checkpoints (300ms at most here), and
	// 355 tries every fastPoll meanwhile
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
	neverLeads(t, old, 10*genPoll, "revision 380, with 355 alive,")
	if !newer.Leading() {
		t.Fatal("revision 355 lost the lease")
	}
	var app string
	if err := pool.QueryRow(ctx, `
		SELECT a.application_name FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
		WHERE l.locktype = 'advisory' AND l.granted AND l.objid = (hashtext('loom_processes_billing')::bigint & 4294967295)::oid`).Scan(&app); err != nil {
		t.Fatal(err)
	}
	if app != "loom lease g1793453000 billing-00355-7rg" {
		t.Fatalf("the lease session is named %q", app)
	}
	// 380's lease connection went back to its pool unnamed and deaf
	var named int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_stat_activity
		WHERE datname = current_database() AND application_name LIKE 'loom lease g1793440000%'`).Scan(&named); err != nil {
		t.Fatal(err)
	}
	if named != 0 {
		t.Fatalf("%d sessions still named for revision 380's lease", named)
	}

	// 355 is the one that keeps reacting
	billing.Gateway.Reset()
	billing.Gateway.SetDelay(0)
	billing.ResetReactions()
	for i := 0; i < 3; i++ {
		payInvoice(t, ctx, old, uuid.New())
	}
	waitFor(t, ctx, "captures under 355", func() bool { return billing.Gateway.CallsN() == 3 })

	// with 355 gone, 380 leads again: better the old code than none
	stopNewer()
	waitFor(t, ctx, "revision 380 leads once 355 is gone", old.Leading)
}

// TestAnOlderGenerationNeverTakesTheLease: revision 355 (the later
// deploy) leads; revision 380 (the earlier deploy, a higher number)
// started after it, a tagged revision woken later, follows for good, and
// so do another instance of 355, an env-only redeploy that kept the
// generation (revision 356), and an instance whose revision name alone
// would once have been a newer generation.
func TestAnOlderGenerationNeverTakesTheLease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := testDB(t, ctx)
	resetBilling()
	noAmbientGeneration(t)

	newer, _ := startGen(t, ctx, pool, genLater, "billing-00355-7rg", true)
	waitFor(t, ctx, "revision 355 leads", newer.Leading)
	old, _ := startGen(t, ctx, pool, genEarlier, "billing-00380-xih", false)
	twin, _ := startGen(t, ctx, pool, genLater, "billing-00355-7rg", false)
	redeploy, _ := startGen(t, ctx, pool, genLater, "billing-00356-abc", false)
	byName, _ := startGen(t, ctx, pool, 0, "billing-99999-zzz", false)
	end := time.Now().Add(15 * genPoll)
	for time.Now().Before(end) {
		if old.Leading() || twin.Leading() || redeploy.Leading() || byName.Leading() || !newer.Leading() {
			t.Fatalf("the lease moved: 355 %v, 380 %v, 355 twin %v, 356 same generation %v, 99999 by name %v",
				newer.Leading(), old.Leading(), twin.Leading(), redeploy.Leading(), byName.Leading())
		}
		time.Sleep(5 * time.Millisecond)
	}
	// a v0.85.0 follower asks by revision sequence, however large: the
	// leader does not answer
	if _, err := pool.Exec(ctx, `SELECT pg_notify('loom.process_lease', 'billing 99999')`); err != nil {
		t.Fatal(err)
	}
	end = time.Now().Add(5 * genPoll)
	for time.Now().Before(end) {
		if !newer.Leading() {
			t.Fatal("a v0.85.0 follower's request moved the lease")
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
	noAmbientGeneration(t)
	migrate(t, ctx, pool)

	wedged := holdLease(t, ctx, pool, "loom lease g1793440000 billing-00380-xih")
	started := time.Now()
	newer, _ := startGen(t, ctx, pool, genLater, "billing-00355-7rg", false)
	waitFor(t, ctx, "revision 355 leads", newer.Leading)
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
	noAmbientGeneration(t)

	first, stopFirst := startGen(t, ctx, pool, 0, "", true)
	waitFor(t, ctx, "the first instance leads", first.Leading)
	second, _ := startGen(t, ctx, pool, 0, "", false)
	newer, _ := startGen(t, ctx, pool, genLater, "billing-00355-7rg", false)
	end := time.Now().Add(15 * genPoll)
	for time.Now().Before(end) {
		if !first.Leading() || second.Leading() || newer.Leading() {
			t.Fatalf("the lease moved from a leader with no generation: first %v, second %v, 355 %v",
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
	newer2, _ := startGen(t, ctx, pool2, genLater, "billing-00355-7rg", false)
	neverLeads(t, newer2, 15*genPoll, "a newer generation, over a leader that names none,")
	if err := unnamed.Ping(ctx); err != nil {
		t.Fatalf("the unnamed leader's session was ended: %v", err)
	}
}

// TestTheFirstDeployOntoThisLoom is the rollout from loom v0.85.0: its
// leader names a bare revision sequence (380) on its lease and yields to
// a bare number larger than its own. The first instance of this build
// with a deploy generation asks it in those terms, it yields, and the new
// build leads at once; a v0.85.0 leader that does not answer is ended
// after two polls, as a wedged one is.
func TestTheFirstDeployOntoThisLoom(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := testDB(t, ctx)
	resetBilling()
	noAmbientGeneration(t)
	migrate(t, ctx, pool)

	// a v0.85.0 leader of revision 380, which answers as v0.85.0 does
	v085 := holdLease(t, ctx, pool, "loom lease 380 billing-00380-xih")
	if _, err := v085.Exec(ctx, `LISTEN "loom.process_lease"`); err != nil {
		t.Fatal(err)
	}
	asked := make(chan string, 1)
	go func() {
		for {
			n, err := v085.WaitForNotification(ctx)
			if err != nil {
				return
			}
			svc, num, _ := strings.Cut(n.Payload, " ")
			gen, err := strconv.ParseInt(num, 10, 64)
			if svc != "billing" || err != nil || gen <= 380 {
				continue
			}
			_, _ = v085.Exec(ctx, `SELECT set_config('application_name', 'loom lease 380 yielding billing-00380-xih', false)`)
			_, _ = v085.Exec(ctx, `SELECT pg_advisory_unlock(hashtext('loom_processes_billing'))`)
			asked <- n.Payload
			return
		}
	}()
	started := time.Now()
	newer, _ := startGen(t, ctx, pool, genLater, "billing-00355-7rg", false)
	waitFor(t, ctx, "revision 355 leads", newer.Leading)
	took := time.Since(started)
	select {
	case p := <-asked:
		t.Logf("v0.85.0 leader asked %q, hand-over in %s", p, took)
	default:
		t.Fatal("the v0.85.0 leader was not asked in its own terms")
	}
	if took >= 2*genPoll {
		t.Fatalf("led after %s: the v0.85.0 leader did not hand over cooperatively", took)
	}
	if err := v085.Ping(ctx); err != nil {
		t.Fatalf("the v0.85.0 leader's session was ended though it yielded: %v", err)
	}

	// a v0.85.0 leader that does not answer
	pool2 := testDB(t, ctx)
	migrate(t, ctx, pool2)
	deaf := holdLease(t, ctx, pool2, "loom lease 380 billing-00380-xih")
	newer2, _ := startGen(t, ctx, pool2, genLater, "billing-00355-7rg", false)
	waitFor(t, ctx, "revision 355 leads over a deaf v0.85.0 leader", newer2.Leading)
	if err := deaf.Ping(ctx); err == nil {
		t.Fatal("the deaf v0.85.0 leader's session is still alive")
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
