package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/billing"
	billinggen "github.com/go-apis/loom/internal/e2e/billing/loomgen"
	"github.com/go-apis/loom/internal/e2e/orders"
	ordersgen "github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

// TestOTel proves the instrumentation with a real SDK installed: the
// cross-service flow produces dispatch → publish → consume spans that all
// share ONE trace id (context rides the envelope across the bus), runner
// steps are spanned, and the metrics — counters and the DB-observed
// gauges — report. Without an SDK all of this is no-op; loom itself has
// no exporters and no config.
func TestOTel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	oldTP, oldMP := otel.GetTracerProvider(), otel.GetMeterProvider()
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	t.Cleanup(func() { otel.SetTracerProvider(oldTP); otel.SetMeterProvider(oldMP) })

	old := orders.AutoCancelAfter
	orders.AutoCancelAfter = time.Hour
	t.Cleanup(func() { orders.AutoCancelAfter = old })

	pool := testDB(t, ctx)
	bus := loom.NewMemoryBus()
	ordersCli, err := loom.New(loom.Config{DB: pool, Bus: bus, Registry: orders.NewRegistry(), Blobs: loom.NewDirBlobStore(t.TempDir(), "http://blobs.local")})
	if err != nil {
		t.Fatal(err)
	}
	billingCli, err := loom.New(loom.Config{DB: pool, Bus: bus, Registry: billing.NewRegistry(), Keys: testKeys(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := ordersCli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := ordersCli.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := billingCli.Start(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	orderID := uuid.New()
	placeOrder(t, ctx, ordersCli, orderID, uuid.New(), 1200)
	waitFor(t, ctx, "invoice raised off the bus", func() bool {
		state, _, err := billingCli.Load(ctx, "Invoice", "default", orderID)
		return err == nil && state.(*billinggen.Invoice).Status == "raised"
	})
	waitFor(t, ctx, "order summary projected", func() bool {
		e, err := ordersCli.Entity(ctx, "OrderSummary", "default", orderID)
		return err == nil && e != nil && e.(*ordersgen.OrderSummary).Status == "placed"
	})

	// --- spans: one trace crosses dispatch → relay publish → bus consume ---
	find := func(name string, want ...attribute.KeyValue) sdktrace.ReadOnlySpan {
		for _, sp := range recorder.Ended() {
			if sp.Name() != name {
				continue
			}
			ok := true
			for _, w := range want {
				hit := false
				for _, a := range sp.Attributes() {
					hit = hit || a.Key == w.Key && a.Value == w.Value
				}
				ok = ok && hit
			}
			if ok {
				return sp
			}
		}
		return nil
	}
	svc := func(s string) attribute.KeyValue { return attribute.String("loom.service", s) }

	dispatch := find("loom.dispatch", svc("orders"), attribute.StringSlice("loom.commands", []string{"PlaceOrder"}))
	publish := find("loom.publish", svc("orders"), attribute.String("loom.event", "OrderPlaced"))
	consume := find("loom.consume", svc("billing"), attribute.String("loom.event", "OrderPlaced"))
	if dispatch == nil || publish == nil || consume == nil {
		t.Fatalf("missing spans: dispatch=%v publish=%v consume=%v", dispatch != nil, publish != nil, consume != nil)
	}
	traceID := dispatch.SpanContext().TraceID()
	if publish.SpanContext().TraceID() != traceID {
		t.Fatalf("publish span left the trace: %s vs %s", publish.SpanContext().TraceID(), traceID)
	}
	if consume.SpanContext().TraceID() != traceID {
		t.Fatalf("consume span did not join the producer's trace: %s vs %s", consume.SpanContext().TraceID(), traceID)
	}
	// the reaction's own dispatch (RaiseInvoice) is a child inside the same trace
	raise := find("loom.dispatch", svc("billing"), attribute.StringSlice("loom.commands", []string{"RaiseInvoice"}))
	if raise == nil || raise.SpanContext().TraceID() != traceID {
		t.Fatalf("reaction dispatch did not stay in the trace: %v", raise)
	}
	if find("loom.projection.step", svc("orders")) == nil {
		t.Fatal("no projection step span")
	}

	// --- metrics: counters counted and the gauge callback observed the DB ---
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	sums := map[string]int64{}
	gauges := map[string]bool{}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					sums[m.Name] += dp.Value
				}
			case metricdata.Gauge[int64]:
				gauges[m.Name] = true
			}
		}
	}
	if sums["loom.dispatch.count"] < 2 { // PlaceOrder + RaiseInvoice at least
		t.Fatalf("dispatch count: %d", sums["loom.dispatch.count"])
	}
	if sums["loom.events.appended"] < 2 || sums["loom.outbox.published"] < 1 {
		t.Fatalf("event counters wrong: %+v", sums)
	}
	for _, g := range []string{"loom.outbox.depth", "loom.dead_letters.depth", "loom.runner.lag", "loom.timers.pending"} {
		if !gauges[g] {
			t.Fatalf("gauge %s not observed (have %v)", g, gauges)
		}
	}
}
