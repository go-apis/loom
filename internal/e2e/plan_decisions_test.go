package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/billing"
	billinggen "github.com/go-apis/loom/internal/e2e/billing/loomgen"
	"github.com/go-apis/loom/internal/e2e/clock"
	clockgen "github.com/go-apis/loom/internal/e2e/clock/loomgen"
	"github.com/go-apis/loom/internal/e2e/orders"
	ordersgen "github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

// told renders a plan's events one per line: the event, the command
// that emits it and the reaction that dispatched that, then each
// reaction with what the plan found when it ran it.
func told(events []loom.PlannedEvent) string {
	var lines []string
	for _, e := range events {
		line := e.Type + " by " + e.Command
		if e.Cause != "" {
			line += " from " + e.Cause
		}
		for _, r := range e.Reactions {
			line += " | " + r.Kind + " " + r.Name
			if r.Outcome != "" {
				line += " " + r.Outcome
			}
			for _, d := range r.Commands {
				line += " " + d.How + ":" + d.Command
			}
			if len(r.Calls) > 0 {
				line += " " + strings.Join(r.Calls, ",")
			}
			if r.Detail != "" && r.Outcome != loom.OutcomeCalls {
				line += " (" + r.Detail + ")"
			}
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

// written counts the rows of everything a unit of work or a reaction
// writes.
func written(t *testing.T, ctx context.Context, pool *pgxpool.Pool) string {
	t.Helper()
	var out []string
	for _, table := range []string{"loom_events", "loom_outbox", "loom_timers", "loom_effects", "loom_records", "loom_batches", "loom_dead_letters"} {
		var n int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		out = append(out, fmt.Sprintf("%s=%d", table, n))
	}
	return strings.Join(out, " ")
}

// TestAPlanSaysWhatReactionsDecide: a plan runs the command, its
// policies and its processes and keeps nothing. A policy's commands and
// their events are in the plan, as they would be in the command's own
// transaction; a timer a policy sets is shown and not followed; a
// process is followed to the outside call it would make, which is not
// made.
func TestAPlanSaysWhatReactionsDecide(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	bus := loom.NewMemoryBus()
	ord, err := loom.New(loom.Config{DB: pool, Bus: bus, Registry: orders.NewRegistry(), Blobs: loom.NewDirBlobStore(t.TempDir(), "http://blobs.local")})
	if err != nil {
		t.Fatal(err)
	}
	bill, err := loom.New(loom.Config{DB: pool, Bus: bus, Registry: billing.NewRegistry(), Keys: testKeys(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := ord.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	billing.Gateway.Reset()
	t.Cleanup(billing.Gateway.Reset)

	invoice := uuid.New()
	if err := bill.Dispatch(ctx, &billinggen.RaiseInvoice{CommandBase: loom.CommandBase{AggregateID: invoice, Namespace: "acme"}, CustomerId: uuid.New(), AmountCents: 4200, Currency: "USD"}); err != nil {
		t.Fatal(err)
	}
	before := written(t, ctx, pool)

	// paying an invoice: the policy posts the ledger entry in the same
	// transaction, and the process would capture the payment
	paid, err := bill.Plan(ctx, &billinggen.MarkInvoicePaid{CommandBase: loom.CommandBase{AggregateID: invoice, Namespace: "acme"}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := "InvoicePaid by MarkInvoicePaid | policy postLedger decided run:PostLedgerEntry | process captureOnPaid calls gateway_capture | projection invoiceSummary\n" +
		"LedgerEntryPosted by PostLedgerEntry from policy postLedger"
	if got := told(paid.Events); got != want {
		t.Errorf("paying the invoice:\n got %s\nwant %s", got, want)
	}
	post := paid.Events[0].Reactions[0].Commands[0]
	if post.TargetID != invoice || post.Namespace != "acme" {
		t.Errorf("the ledger entry is posted to %s/%s, want acme/%s", post.Namespace, post.TargetID, invoice)
	}
	if raw, _ := json.Marshal(post.Data); !strings.Contains(string(raw), `"amount_cents":4200`) {
		t.Errorf("the ledger entry's command = %s, want the invoice's amount", raw)
	}
	if paid.Events[1].Aggregate != "LedgerEntry" || paid.Events[1].AggregateID != invoice {
		t.Errorf("the ledger event is written to %s %s", paid.Events[1].Aggregate, paid.Events[1].AggregateID)
	}
	if n := billing.Gateway.CallsN(); n != 0 {
		t.Errorf("planning called the payment gateway %d times", n)
	}

	// placing an order: the policy sets a timer, which is shown with
	// when it would fire and not followed
	place := &ordersgen.PlaceOrder{CustomerId: uuid.New(), Currency: "USD", Items: []ordersgen.OrderItem{{Sku: "widget", Quantity: 1, PriceCents: 500}}}
	place.Namespace, place.AggregateID = "acme", uuid.New()
	placed, err := ord.Plan(ctx, place, 0)
	if err != nil {
		t.Fatal(err)
	}
	want = "OrderPlaced by PlaceOrder | policy scheduleAutoCancel decided timer:CancelOrder | projection customerSpend | projection orderSummary"
	if got := told(placed.Events); got != want {
		t.Errorf("placing the order:\n got %s\nwant %s", got, want)
	}
	if at := placed.Events[0].Reactions[0].Commands[0].At; at == nil || !at.After(time.Now()) {
		t.Errorf("the cancel timer fires at %v, want a time ahead", at)
	}

	// neither plan wrote anything but itself
	if after := written(t, ctx, pool); after != before {
		t.Errorf("planning wrote:\n before %s\n after  %s", before, after)
	}
	if state, version, err := bill.Load(ctx, "Invoice", "acme", invoice); err != nil || version != 1 || state.(*billinggen.Invoice).Status != "raised" {
		t.Errorf("the invoice after planning: v%d %+v %v", version, state, err)
	}

	// a plan holds no dispatch up: it takes no log lock, so one made
	// while another transaction holds the lock still answers
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('loom_log_' || $1))`, "billing"); err != nil {
		t.Fatal(err)
	}
	locked, cancelLocked := context.WithTimeout(ctx, 5*time.Second)
	defer cancelLocked()
	if _, err := bill.Plan(locked, &billinggen.MarkInvoicePaid{CommandBase: loom.CommandBase{AggregateID: invoice, Namespace: "acme"}}, 0); err != nil {
		t.Errorf("planning while the log lock is held: %v", err)
	}
	_ = tx.Rollback(ctx)

	// the run does what the plan said: the invoice is paid and the
	// ledger entry posted, in one transaction
	if err := bill.RunPlan(ctx, "acme", paid.ID); err != nil {
		t.Fatal(err)
	}
	if entry, err := bill.Record(ctx, "LedgerEntry", "acme", invoice); err != nil || entry == nil {
		t.Errorf("the ledger entry after the run: %+v %v", entry, err)
	}
}

// TestAPlanFollowsProcesses: a process with no outside call is run, and
// what it dispatches is run in its turn, its events in the plan under
// the process's name. One that answers NotYet, one that fails, one
// whose command is refused and one the plan runs out of time for each
// say so, and none of them stops the plan.
func TestAPlanFollowsProcesses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	reset := func() {
		clock.HotFor.Store(0)
		clock.Looks.Store(0)
		clock.PlannedLooks.Store(0)
		clock.Dawdle.Store(0)
		clock.Impatience.Store(false)
		clock.Brittle.Store(false)
	}
	reset()
	t.Cleanup(reset)

	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Registry: clock.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	before := written(t, ctx, pool)
	plan := func() string {
		t.Helper()
		p, err := cli.Plan(ctx, &clockgen.StrikeAnvil{CommandBase: loom.CommandBase{Namespace: "forge", AggregateID: uuid.New()}}, 0)
		if err != nil {
			t.Fatal(err)
		}
		return told(p.Events)
	}

	// the anvil is cool at the first look: the process records it
	want := "AnvilStruck by StrikeAnvil | process coolAnvil decided run:RecordAnvilCool | process impatient decided\n" +
		"AnvilCooled by RecordAnvilCool from process coolAnvil"
	if got := plan(); got != want {
		t.Errorf("a cool anvil:\n got %s\nwant %s", got, want)
	}

	// still hot: both processes would look again
	clock.HotFor.Store(1000)
	clock.Impatience.Store(true)
	want = "AnvilStruck by StrikeAnvil | process coolAnvil not_yet (not yet: the anvil is still hot) | process impatient not_yet (not yet: the anvil is still hot)"
	if got := plan(); got != want {
		t.Errorf("a hot anvil:\n got %s\nwant %s", got, want)
	}
	clock.HotFor.Store(0)
	clock.Impatience.Store(false)

	// the command the process dispatches is refused: the process fails,
	// and nothing of its unit of work is in the plan
	clock.Brittle.Store(true)
	want = "AnvilStruck by StrikeAnvil | process coolAnvil fails run:RecordAnvilCool (what it dispatches is refused: RecordAnvilCool: the anvil cracked) | process impatient decided"
	if got := plan(); got != want {
		t.Errorf("a brittle anvil:\n got %s\nwant %s", got, want)
	}
	clock.Brittle.Store(false)

	// a process that takes longer than the plan gives processes is left
	// unfollowed, with the ones after it, and the plan still answers
	clock.Dawdle.Store(2200)
	started := time.Now()
	got := plan()
	if took := time.Since(started); took > 6*time.Second {
		t.Errorf("a plan with a slow process took %s", took)
	}
	if !strings.HasPrefix(got, "AnvilStruck by StrikeAnvil | process coolAnvil not_followed (the plan ran processes for 2s and stopped at coolAnvil) | process impatient not_followed (") || strings.Contains(got, "AnvilCooled") {
		t.Errorf("a slow process:\n got %s", got)
	}
	clock.Dawdle.Store(0)

	if after := written(t, ctx, pool); after != before {
		t.Errorf("planning wrote:\n before %s\n after  %s", before, after)
	}

	// each look above was made under a plan, and the process could tell
	if looks, planned := clock.Looks.Load(), clock.PlannedLooks.Load(); looks == 0 || planned != looks {
		t.Errorf("%d of %d looks saw loom.Planning", planned, looks)
	}
	if loom.Planning(ctx) {
		t.Error("a context of no plan says it is planning")
	}
}
