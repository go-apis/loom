package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/go-apis/loom"
	loomgql "github.com/go-apis/loom/graphql"
	"github.com/go-apis/loom/internal/e2e/billing"
	billinggen "github.com/go-apis/loom/internal/e2e/billing/loomgen"
	"github.com/go-apis/loom/internal/e2e/orders"
	loommcp "github.com/go-apis/loom/mcp"
)

// TestAnIdempotencyKeyRunsACommandOnce: a command sent twice under one
// key runs once — in core, over GraphQL by header, and through the MCP
// dispatch tool — and the repeat says it was one. A dispatch that
// failed left no key, so sending it again runs it. Two sent at once
// resolve to one. The key is scoped to the command and its stream.
func TestAnIdempotencyKeyRunsACommandOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	bill, err := loom.New(loom.Config{DB: pool, Registry: billing.NewRegistry(), Keys: testKeys(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := bill.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	events := func(cli *loom.Client, id uuid.UUID) int {
		t.Helper()
		log, err := cli.QueryLog(ctx, loom.LogQuery{AggregateID: id.String()})
		if err != nil {
			t.Fatal(err)
		}
		return len(log)
	}
	// PostLedgerEntry appends: twice is two entries, the case a key is for
	raise := func(id uuid.UUID) *billinggen.RaiseInvoice {
		return &billinggen.RaiseInvoice{CommandBase: loom.CommandBase{AggregateID: id, Namespace: "acme"}, CustomerId: uuid.New(), AmountCents: 100, Currency: "USD"}
	}

	// in core
	invoice := uuid.New()
	keyed, replayed := loom.WithIdempotencyKey(ctx, "k1")
	if err := bill.Dispatch(keyed, raise(invoice)); err != nil || replayed() {
		t.Fatalf("the first dispatch: %v, replayed %v", err, replayed())
	}
	keyed, replayed = loom.WithIdempotencyKey(ctx, "k1")
	if err := bill.Dispatch(keyed, raise(invoice)); err != nil || !replayed() {
		t.Fatalf("the repeat: %v, replayed %v; want nil and replayed", err, replayed())
	}
	if got := events(bill, invoice); got != 1 {
		t.Fatalf("the invoice has %d events after a repeat under its key", got)
	}
	// the same key on another stream is another thing
	other := uuid.New()
	keyed, replayed = loom.WithIdempotencyKey(ctx, "k1")
	if err := bill.Dispatch(keyed, raise(other)); err != nil || replayed() || events(bill, other) != 1 {
		t.Errorf("the same key on another stream: %v, replayed %v", err, replayed())
	}
	// a dispatch that failed left no key: sent again, it runs
	unpaid := uuid.New()
	pay := &billinggen.MarkInvoicePaid{CommandBase: loom.CommandBase{AggregateID: unpaid, Namespace: "acme"}}
	keyed, _ = loom.WithIdempotencyKey(ctx, "k2")
	if err := bill.Dispatch(keyed, pay); err == nil {
		t.Fatal("paying an invoice that was never raised succeeded")
	}
	if err := bill.Dispatch(ctx, raise(unpaid)); err != nil {
		t.Fatal(err)
	}
	keyed, replayed = loom.WithIdempotencyKey(ctx, "k2")
	if err := bill.Dispatch(keyed, pay); err != nil || replayed() {
		t.Errorf("the retry of a failed dispatch: %v, replayed %v; want it to run", err, replayed())
	}
	// two at once: one runs
	racing := uuid.New()
	var wg sync.WaitGroup
	ran := make(chan bool, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			keyed, replayed := loom.WithIdempotencyKey(ctx, "k3")
			if err := bill.Dispatch(keyed, raise(racing)); err != nil {
				t.Errorf("a racing dispatch: %v", err)
			}
			ran <- !replayed()
		}()
	}
	wg.Wait()
	close(ran)
	winners := 0
	for r := range ran {
		if r {
			winners++
		}
	}
	if winners != 1 || events(bill, racing) != 1 {
		t.Errorf("eight dispatches at once under one key: %d ran, %d events", winners, events(bill, racing))
	}

	// over GraphQL, by header, and through the MCP dispatch tool
	ord, err := loom.New(loom.Config{DB: pool, Bus: loom.NewMemoryBus(), Registry: orders.NewRegistry(), Blobs: loom.NewDirBlobStore(t.TempDir(), "http://blobs.local")})
	if err != nil {
		t.Fatal(err)
	}
	if err := ord.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	gateway, err := loomgql.New(loomgql.Config{Services: []*loom.Client{ord}})
	if err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(gateway)
	defer api.Close()
	parcel := uuid.New()
	post := func(key string) string {
		t.Helper()
		body, _ := json.Marshal(map[string]any{
			"query":     `mutation($in: RegisterParcelInput!) { registerParcel(input: $in) { status } }`,
			"variables": map[string]any{"in": map[string]any{"aggregateId": parcel.String(), "namespace": "acme", "depotId": uuid.NewString(), "label": "box"}},
		})
		req, _ := http.NewRequest(http.MethodPost, api.URL, bytes.NewReader(body))
		if key != "" {
			req.Header.Set(loomgql.IdempotencyHeader, key)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out struct {
			Data struct {
				RegisterParcel struct{ Status string } `json:"registerParcel"`
			}
			Errors []struct{ Message string }
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || len(out.Errors) > 0 {
			t.Fatalf("registerParcel: %v %+v", err, out.Errors)
		}
		return out.Data.RegisterParcel.Status
	}
	if first, again := post("g1"), post("g1"); first != "ok" || again != "duplicate" || events(ord, parcel) != 1 {
		t.Errorf("over GraphQL under one key: %q then %q, %d events; want ok, duplicate, 1", first, again, events(ord, parcel))
	}
	if got := post(""); got != "ok" || events(ord, parcel) != 2 {
		t.Errorf("with no key the command runs again: %q, %d events", got, events(ord, parcel))
	}

	agents, err := loommcp.New(loommcp.Config{Gateway: gateway})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(agents)
	defer srv.Close()
	c := sdk.NewClient(&sdk.Implementation{Name: "test"}, nil)
	cs, err := c.Connect(ctx, &sdk.StreamableClientTransport{Endpoint: srv.URL, DisableStandaloneSSE: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	boxed := uuid.New()
	dispatch := func() string {
		t.Helper()
		res, err := cs.CallTool(ctx, &sdk.CallToolParams{Name: "dispatch", Arguments: map[string]any{"name": "registerParcel", "key": "m1",
			"args": map[string]any{"input": map[string]any{"aggregateId": boxed.String(), "namespace": "acme", "depotId": uuid.NewString(), "label": "box"}}}})
		if err != nil || res.IsError {
			t.Fatalf("dispatch: %v %+v", err, res)
		}
		return res.Content[0].(*sdk.TextContent).Text
	}
	if first, again := dispatch(), dispatch(); first != `{"status":"ok"}` || again != `{"status":"duplicate"}` || events(ord, boxed) != 1 {
		t.Errorf("through MCP under one key: %s then %s, %d events", first, again, events(ord, boxed))
	}
}
