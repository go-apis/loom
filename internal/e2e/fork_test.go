package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/go-apis/loom"
	loomgql "github.com/go-apis/loom/graphql"
	"github.com/go-apis/loom/internal/e2e/billing"
	billinggen "github.com/go-apis/loom/internal/e2e/billing/loomgen"
	"github.com/go-apis/loom/internal/e2e/clock"
	clockgen "github.com/go-apis/loom/internal/e2e/clock/loomgen"
	loommcp "github.com/go-apis/loom/mcp"
)

// TestForkReplaysARecordedRun: forking a paid invoice re-runs it in
// another namespace on the code deployed now. The reaction to the copied
// payment replays the capture the source recorded, so the gateway is not
// called again. A fork taken before the payment has no capture to
// replay: paying it there is a step past the fork point, and reaches the
// gateway. The source is untouched, and a fork is refused into its own
// namespace, over a stream that exists, and for an @table aggregate.
func TestForkReplaysARecordedRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Registry: billing.NewRegistry(), Keys: testKeys(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cli.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	billing.Gateway.Reset()
	billing.SetLastReceipt("")
	billing.SetFailReactAfterCapture(0)

	invoice := uuid.New()
	payInvoice(t, ctx, cli, invoice)
	waitFor(t, ctx, "the source's capture", func() bool { return billing.LastReceipt() == "cap_"+invoice.String() })
	if n := billing.Gateway.CallsN(); n != 1 {
		t.Fatalf("the source captured %d times, want 1", n)
	}

	// the whole run, forked: its reaction replays the recorded capture
	billing.ResetReactions()
	billing.SetLastReceipt("")
	res, err := cli.ForkStream(ctx, loom.ForkOptions{Aggregate: "Invoice", Namespace: "default", ID: invoice, ToNamespace: "replay"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Events != 2 || res.Version != 2 || res.Effects != 1 || len(res.Seqs) != 2 {
		t.Fatalf("the fork copied %+v, want 2 events, version 2 and the capture", res)
	}
	waitFor(t, ctx, "the fork's reaction to the copied payment", func() bool { return billing.LastReceipt() == "cap_"+invoice.String() })
	if r := billing.Reactions(); r != 1 {
		t.Errorf("the fork's payment was reacted to %d times, want 1", r)
	}
	if n := billing.Gateway.CallsN(); n != 1 {
		t.Errorf("forking called the gateway again: %d calls, want the source's 1", n)
	}
	state, version, err := cli.Load(ctx, "Invoice", "replay", invoice)
	if err != nil {
		t.Fatal(err)
	}
	if inv := state.(*billinggen.Invoice); version != 2 || inv.PaidAt == nil || inv.AmountCents != 4200 {
		t.Errorf("the fork reads version %d, %+v; want the source's paid invoice", version, inv)
	}
	var cause string
	if err := pool.QueryRow(ctx, `SELECT causation_id FROM loom_events WHERE namespace='replay' AND aggregate_id=$1 AND version=2`, invoice).Scan(&cause); err != nil {
		t.Fatal(err)
	}
	var srcSeq int64
	if err := pool.QueryRow(ctx, `SELECT global_seq FROM loom_events WHERE namespace='default' AND aggregate_id=$1 AND version=2`, invoice).Scan(&srcSeq); err != nil {
		t.Fatal(err)
	}
	if ns, id, seq, ok := loom.ForkedFrom(&loom.Event{Meta: loom.Metadata{CausationID: cause}}); !ok || ns != "default" || id != invoice || seq != srcSeq {
		t.Errorf("the copy's cause %q reads back as %s %s @%d (%v), want default %s @%d", cause, ns, id, seq, ok, invoice, srcSeq)
	}

	// forked at step 1, before the payment: paying it there is new, and
	// reaches the gateway
	early, err := cli.ForkStream(ctx, loom.ForkOptions{Aggregate: "Invoice", Namespace: "default", ID: invoice, UpTo: 1, ToNamespace: "rewound"})
	if err != nil {
		t.Fatal(err)
	}
	if early.Events != 1 || early.Version != 1 || early.Effects != 0 {
		t.Fatalf("the rewound fork copied %+v, want the raise alone", early)
	}
	if err := cli.Dispatch(ctx, &billinggen.MarkInvoicePaid{CommandBase: loom.CommandBase{AggregateID: invoice, Namespace: "rewound"}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "the rewound fork's own capture", func() bool { return billing.Gateway.CallsN() == 2 })

	// the source is as it was
	if _, version, err := cli.Load(ctx, "Invoice", "default", invoice); err != nil || version != 2 {
		t.Errorf("the source reads version %d (%v) after forking, want 2", version, err)
	}

	for name, opts := range map[string]loom.ForkOptions{
		"into its own namespace":     {Aggregate: "Invoice", Namespace: "default", ID: invoice, ToNamespace: "default"},
		"over a stream that exists":  {Aggregate: "Invoice", Namespace: "default", ID: invoice, ToNamespace: "replay"},
		"of an @table aggregate":     {Aggregate: "Payee", Namespace: "default", ID: uuid.New(), ToNamespace: "replay"},
		"of a stream with no events": {Aggregate: "Invoice", Namespace: "default", ID: uuid.New(), ToNamespace: "elsewhere"},
	} {
		if _, err := cli.ForkStream(ctx, opts); err == nil {
			t.Errorf("a fork %s was not refused", name)
		}
	}
}

// TestForkALoopInstanceMidRun: a @loop instance forked part-way is looked
// at in its new namespace from where the fork left it, by the look
// deployed now; the source carries on as it was.
func TestForkALoopInstanceMidRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	cli, _ := loopClock(t, ctx, true)

	// today's look fires a kiln stoked twice
	var fireAt atomic.Int64
	fireAt.Store(2)
	clock.SetKilnLook(func(ctx context.Context) ([]loom.Command, error) {
		told := loom.LookingAt(ctx)
		state, _, err := loom.Load(ctx, "Kiln", told.Namespace, told.ID)
		if err != nil {
			return nil, err
		}
		if kiln := state.(*clockgen.Kiln); !kiln.Fired && kiln.Stokes >= fireAt.Load() {
			return []loom.Command{&clockgen.RecordKilnFired{CommandBase: loom.CommandBase{Namespace: told.Namespace, AggregateID: told.ID}}}, nil
		}
		return nil, nil
	})
	kiln := loom.CommandBase{Namespace: "pottery", AggregateID: uuid.New()}
	if err := cli.Dispatch(ctx, &clockgen.LightKiln{CommandBase: kiln}); err != nil {
		t.Fatal(err)
	}
	if err := cli.Dispatch(ctx, &clockgen.StokeKiln{CommandBase: kiln}); err != nil {
		t.Fatal(err)
	}

	// fork it after one stoke, with a new look that fires at one
	fireAt.Store(1)
	res, err := cli.ForkStream(ctx, loom.ForkOptions{Aggregate: "Kiln", Namespace: "pottery", ID: kiln.AggregateID, UpTo: 2, ToNamespace: "pottery-fork"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Version != 2 {
		t.Fatalf("the fork is at version %d, want 2", res.Version)
	}
	fired := func(ns string) bool {
		state, _, err := cli.Load(ctx, "Kiln", ns, kiln.AggregateID)
		if err != nil {
			t.Fatal(err)
		}
		return state.(*clockgen.Kiln).Fired
	}
	waitFor(t, ctx, "the forked kiln to be fired by today's look", func() bool { return fired("pottery-fork") })
	if _, ok, err := cli.LoopDue(ctx, "Kiln", "pottery-fork", kiln.AggregateID); err != nil || !ok {
		t.Errorf("the forked kiln has no looks due (%v)", err)
	}
	// the source was not touched by the fork; its own look now fires it
	// too, at one stoke, which is the source's own business
	if _, version, err := cli.Load(ctx, "Kiln", "pottery", kiln.AggregateID); err != nil || version < 2 {
		t.Errorf("the source reads version %d (%v)", version, err)
	}
}

// forkBilling is a started billing client for the fork tests.
func forkBilling(t *testing.T, ctx context.Context) (*loom.Client, *pgxpool.Pool) {
	t.Helper()
	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Registry: billing.NewRegistry(), Keys: testKeys(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cli.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	return cli, pool
}

// TestForkSharesTheSourcesDataKey: a stream with @pii fields forks, and
// the fork reads its source's personal data, the copies and what it
// writes itself, under the source's one data key. Shredding the source
// leaves both redacted; shredding a fork redacts the fork alone.
func TestForkSharesTheSourcesDataKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	cli, pool := forkBilling(t, ctx)
	email := func(ns string, id uuid.UUID) string {
		t.Helper()
		state, _, err := cli.Load(ctx, "Contact", ns, id)
		if err != nil {
			t.Fatal(err)
		}
		return state.(*billinggen.Contact).Email
	}
	record := func(ns string, id uuid.UUID, to string) {
		t.Helper()
		if err := cli.Dispatch(ctx, &billinggen.RecordContact{CommandBase: loom.CommandBase{Namespace: ns, AggregateID: id}, Email: to}); err != nil {
			t.Fatal(err)
		}
	}

	contact := uuid.New()
	record("default", contact, "jordan@example.com")
	if _, err := cli.ForkStream(ctx, loom.ForkOptions{Aggregate: "Contact", Namespace: "default", ID: contact, ToNamespace: "replay"}); err != nil {
		t.Fatal(err)
	}
	if got := email("replay", contact); got != "jordan@example.com" {
		t.Fatalf("the fork reads %q before the source is shredded, want the source's email", got)
	}
	// the fork's own writes seal under the same key, and a fork of the
	// fork reads them under it too
	record("replay", contact, "jordan@new.example.com")
	if _, err := cli.ForkStream(ctx, loom.ForkOptions{Aggregate: "Contact", Namespace: "replay", ID: contact, ToNamespace: "replay2"}); err != nil {
		t.Fatal(err)
	}
	if got := email("replay2", contact); got != "jordan@new.example.com" {
		t.Fatalf("a fork of the fork reads %q, want the fork's email", got)
	}
	var keys int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM loom_keys WHERE stream_id=$1`, contact).Scan(&keys); err != nil {
		t.Fatal(err)
	}
	if keys != 1 {
		t.Errorf("%d data keys for the stream and its forks, want the source's one", keys)
	}

	// shredding a fork redacts it alone
	other := uuid.New()
	record("default", other, "sam@example.com")
	if _, err := cli.ForkStream(ctx, loom.ForkOptions{Aggregate: "Contact", Namespace: "default", ID: other, ToNamespace: "replay"}); err != nil {
		t.Fatal(err)
	}
	if err := cli.Shred(ctx, "replay", other); err != nil {
		t.Fatal(err)
	}
	if got := email("replay", other); got != "" {
		t.Errorf("a shredded fork reads %q, want it redacted", got)
	}
	if got := email("default", other); got != "sam@example.com" {
		t.Errorf("shredding a fork left its source reading %q", got)
	}

	// shredding the source reaches every fork of it
	if err := cli.Shred(ctx, "default", contact); err != nil {
		t.Fatal(err)
	}
	for _, ns := range []string{"default", "replay", "replay2"} {
		if got := email(ns, contact); got != "" {
			t.Errorf("after the source's shred, %s reads %q, want it redacted", ns, got)
		}
	}
}

// TestForkCompareALoopRunWithAChangedLook: a kiln's run, recorded under
// the look that fires at two stokes, is forked after one stoke and
// re-run under a changed look that fires at one. Comparing lists the
// commands and events that part from the recorded run; a fork re-run on
// the unchanged look lists none.
func TestForkCompareALoopRunWithAChangedLook(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	cli, _ := loopClock(t, ctx, true)
	var fireAt atomic.Int64
	fireAt.Store(2)
	clock.SetKilnLook(func(ctx context.Context) ([]loom.Command, error) {
		told := loom.LookingAt(ctx)
		state, _, err := loom.Load(ctx, "Kiln", told.Namespace, told.ID)
		if err != nil {
			return nil, err
		}
		if kiln := state.(*clockgen.Kiln); kiln.Lit && !kiln.Fired && kiln.Stokes >= fireAt.Load() {
			return []loom.Command{&clockgen.RecordKilnFired{CommandBase: loom.CommandBase{Namespace: told.Namespace, AggregateID: told.ID}}}, nil
		}
		return nil, nil
	})
	fired := func(ns string, id uuid.UUID) bool {
		state, _, err := cli.Load(ctx, "Kiln", ns, id)
		if err != nil {
			t.Fatal(err)
		}
		return state.(*clockgen.Kiln).Fired
	}
	kiln := loom.CommandBase{Namespace: "pottery", AggregateID: uuid.New()}
	for _, cmd := range []loom.Command{&clockgen.LightKiln{CommandBase: kiln}, &clockgen.StokeKiln{CommandBase: kiln}, &clockgen.StokeKiln{CommandBase: kiln}} {
		if err := cli.Dispatch(ctx, cmd); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, ctx, "the recorded run to fire", func() bool { return fired("pottery", kiln.AggregateID) })

	// the unchanged look, re-run from after the second stoke: the same
	if _, err := cli.ForkStream(ctx, loom.ForkOptions{Aggregate: "Kiln", Namespace: "pottery", ID: kiln.AggregateID, UpTo: 3, ToNamespace: "pottery-same"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "the unchanged re-run to fire", func() bool { return fired("pottery-same", kiln.AggregateID) })
	same, err := cli.CompareFork(ctx, loom.CompareOptions{Aggregate: "Kiln", Namespace: "pottery", ID: kiln.AggregateID, ToNamespace: "pottery-same"})
	if err != nil {
		t.Fatal(err)
	}
	if !same.Same() || same.ForkedAt != 3 || len(same.Fork) != 1 || same.Fork[0].Command != "RecordKilnFired" || same.Fork[0].Cause != "loop" {
		t.Errorf("the unchanged re-run compares %+v, want the source's one looked-for firing and no differences", same)
	}

	// the changed look, re-run from after the first stoke
	fireAt.Store(1)
	if _, err := cli.ForkStream(ctx, loom.ForkOptions{Aggregate: "Kiln", Namespace: "pottery", ID: kiln.AggregateID, UpTo: 2, ToNamespace: "pottery-new"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "the changed re-run to fire", func() bool { return fired("pottery-new", kiln.AggregateID) })
	cmp, err := cli.CompareFork(ctx, loom.CompareOptions{Aggregate: "Kiln", Namespace: "pottery", ID: kiln.AggregateID, ToNamespace: "pottery-new"})
	if err != nil {
		t.Fatal(err)
	}
	if cmp.Same() || cmp.ForkedAt != 2 || len(cmp.Differences) != 2 {
		t.Fatalf("the changed re-run compares %+v, want two differences after version 2", cmp)
	}
	first, second := cmp.Differences[0], cmp.Differences[1]
	if first.Step != 1 || strings.Join(first.What, ",") != "command,event,data" ||
		first.Source.Command != "StokeKiln" || first.Source.Event != "KilnStoked" ||
		first.Fork.Command != "RecordKilnFired" || first.Fork.Event != "KilnFired" || first.Fork.Cause != "loop" {
		t.Errorf("step 1 differs as %+v (source %+v, fork %+v), want the source's second stoke against the fork's firing", first, first.Source, first.Fork)
	}
	if second.Step != 2 || strings.Join(second.What, ",") != "missing" || second.Fork != nil || second.Source.Command != "RecordKilnFired" {
		t.Errorf("step 2 differs as %+v, want the source's firing that the fork did not repeat", second)
	}
	if _, err := cli.CompareFork(ctx, loom.CompareOptions{Aggregate: "Kiln", Namespace: "pottery", ID: kiln.AggregateID, ToNamespace: "nowhere"}); err == nil {
		t.Error("comparing a stream that is no fork was not refused")
	}
}

// TestForkServedToOperatorsOnly: the gateway, and MCP over it, offer
// forkStream and compareFork to All access only. An operator re-runs a
// paid invoice from before its payment and compares the re-run with
// the source; a caller with both namespaces but not All access is
// neither shown them nor let run them.
func TestForkServedToOperatorsOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	cli, pool := forkBilling(t, ctx)
	billing.Gateway.Reset()
	billing.SetFailReactAfterCapture(0)
	invoice := uuid.New()
	payInvoice(t, ctx, cli, invoice)
	waitFor(t, ctx, "the source's capture", func() bool { return billing.Gateway.CallsN() == 1 })

	tokens := map[string]loomgql.Access{
		"operator": {All: true, Mutate: true},
		"tenant":   {Namespaces: []string{"default", "rerun"}, Mutate: true},
	}
	gateway, err := loomgql.New(loomgql.Config{
		Services: []*loom.Client{cli},
		Fork:     true,
		Auth: func(r *http.Request) (loomgql.Access, error) {
			a, ok := tokens[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
			if !ok {
				return loomgql.Access{}, fmt.Errorf("bad token")
			}
			return a, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	agents, err := loommcp.New(loommcp.Config{Gateway: gateway})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(agents)
	defer srv.Close()
	call := func(token, tool string, args map[string]any) (string, bool) {
		t.Helper()
		c := sdk.NewClient(&sdk.Implementation{Name: "test"}, nil)
		cs, err := c.Connect(ctx, &sdk.StreamableClientTransport{Endpoint: srv.URL, HTTPClient: &http.Client{Transport: bearer{token}}, DisableStandaloneSSE: true}, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer cs.Close()
		res, err := cs.CallTool(ctx, &sdk.CallToolParams{Name: tool, Arguments: args})
		if err != nil {
			t.Fatal(err)
		}
		return res.Content[0].(*sdk.TextContent).Text, res.IsError
	}

	if text, _ := call("operator", "search", map[string]any{"query": "fork"}); !strings.Contains(text, `"name":"forkStream"`) || !strings.Contains(text, `"name":"compareFork"`) {
		t.Errorf("search as the operator = %s", text)
	}
	if text, _ := call("tenant", "search", map[string]any{"query": "fork"}); strings.Contains(text, "forkStream") || strings.Contains(text, "compareFork") {
		t.Errorf("a tenant is shown the fork operations: %s", text)
	}
	fork := map[string]any{"name": "forkStream", "args": map[string]any{"aggregate": "Invoice", "namespace": "default", "id": invoice.String(), "upTo": 1, "toNamespace": "rerun"}}
	if text, refused := call("tenant", "dispatch", fork); !refused || !strings.Contains(text, "access denied") {
		t.Errorf("a tenant forking = %s", text)
	}
	var written int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM loom_events WHERE namespace='rerun' AND aggregate_id=$1`, invoice).Scan(&written); err != nil || written != 0 {
		t.Fatalf("a refused fork wrote %d events (%v)", written, err)
	}
	text, refused := call("operator", "dispatch", fork)
	if refused {
		t.Fatalf("the operator forking: %s", text)
	}
	var res struct{ Version, Events, Effects int }
	if err := json.Unmarshal([]byte(text), &res); err != nil || res.Version != 1 || res.Events != 1 || res.Effects != 0 {
		t.Fatalf("the operator's fork answered %s (%v), want the raise alone", text, err)
	}

	// re-run from the payment: paid again in the fork, a step past the
	// fork point, so the gateway is called for it
	if err := cli.Dispatch(ctx, &billinggen.MarkInvoicePaid{CommandBase: loom.CommandBase{AggregateID: invoice, Namespace: "rerun"}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "the re-run's capture", func() bool { return billing.Gateway.CallsN() == 2 })
	compare := map[string]any{"name": "compareFork", "args": map[string]any{"aggregate": "Invoice", "namespace": "default", "id": invoice.String(), "toNamespace": "rerun", "ignore": []any{"paid_at"}}, "fields": []any{"forkedAt", "same", "fork.command", "fork.event", "differences.what"}}
	if text, refused := call("tenant", "get", compare); !refused || !strings.Contains(text, "access denied") {
		t.Errorf("a tenant comparing = %s", text)
	}
	text, refused = call("operator", "get", compare)
	if refused {
		t.Fatalf("the operator comparing: %s", text)
	}
	var cmp struct {
		ForkedAt int
		Same     bool
		Fork     []struct{ Command, Event string }
	}
	if err := json.Unmarshal([]byte(text), &cmp); err != nil || cmp.ForkedAt != 1 || !cmp.Same || len(cmp.Fork) != 1 || cmp.Fork[0].Command != "MarkInvoicePaid" || cmp.Fork[0].Event != "InvoicePaid" {
		t.Errorf("the operator's comparison = %s (%v), want the re-run paying as the source did", text, err)
	}
}
