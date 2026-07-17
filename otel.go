package loom

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Observability rides the OpenTelemetry API only: every span and metric
// here is a no-op until the deployment installs SDK providers
// (otel.SetTracerProvider / otel.SetMeterProvider). Loom adds no config
// knobs and no exporters — instrumentation is the library's job, wiring
// is the deployment's. Trace context crosses the bus on the envelope, so
// a consumer's reaction joins the trace of the dispatch that published
// the event; correlation/causation ids ride every span as attributes,
// joining the domain's own causality to trace ids.

const otelScope = "github.com/go-apis/loom"

type telemetry struct {
	tracer  trace.Tracer
	service attribute.KeyValue

	dispatches  metric.Int64Counter   // loom.dispatch.count
	conflicts   metric.Int64Counter   // loom.dispatch.conflicts
	appended    metric.Int64Counter   // loom.events.appended
	published   metric.Int64Counter   // loom.outbox.published
	dedupHits   metric.Int64Counter   // loom.consume.dedup_hits
	parked      metric.Int64Counter   // loom.dead_letters.parked
	timersFired metric.Int64Counter   // loom.timers.fired
	batchItems  metric.Int64Counter   // loom.batch.items
	effects     metric.Int64Counter   // loom.effects.calls
	dispatchDur metric.Float64Histogram
	stepDur     metric.Float64Histogram
}

func newTelemetry(service string) *telemetry {
	m := otel.Meter(otelScope)
	t := &telemetry{
		tracer:  otel.Tracer(otelScope),
		service: attribute.String("loom.service", service),
	}
	// instrument constructors only fail on invalid names; the returned
	// instruments are usable no-ops either way
	t.dispatches, _ = m.Int64Counter("loom.dispatch.count", metric.WithDescription("units of work dispatched"))
	t.conflicts, _ = m.Int64Counter("loom.dispatch.conflicts", metric.WithDescription("optimistic-concurrency retries"))
	t.appended, _ = m.Int64Counter("loom.events.appended", metric.WithDescription("events written to the log"))
	t.published, _ = m.Int64Counter("loom.outbox.published", metric.WithDescription("envelopes relayed to the bus"))
	t.dedupHits, _ = m.Int64Counter("loom.consume.dedup_hits", metric.WithDescription("foreign deliveries skipped as already processed"))
	t.parked, _ = m.Int64Counter("loom.dead_letters.parked", metric.WithDescription("deliveries parked to dead letters"))
	t.timersFired, _ = m.Int64Counter("loom.timers.fired", metric.WithDescription("durable timers fired"))
	t.batchItems, _ = m.Int64Counter("loom.batch.items", metric.WithDescription("batch items settled, by status"))
	t.effects, _ = m.Int64Counter("loom.effects.calls", metric.WithDescription("journaled effect slots, by outcome"))
	t.dispatchDur, _ = m.Float64Histogram("loom.dispatch.duration", metric.WithUnit("s"), metric.WithDescription("unit-of-work duration"))
	t.stepDur, _ = m.Float64Histogram("loom.runner.step.duration", metric.WithUnit("s"), metric.WithDescription("runner step duration, by runner"))
	return t
}

func (t *telemetry) count(ctx context.Context, c metric.Int64Counter, n int64, extra ...attribute.KeyValue) {
	c.Add(ctx, n, metric.WithAttributes(append(extra, t.service)...))
}

// span starts a child span; end(err) records the error and closes it.
func (t *telemetry) span(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, func(error)) {
	ctx, sp := t.tracer.Start(ctx, name, trace.WithAttributes(append(attrs, t.service)...))
	return ctx, func(err error) { endSpan(sp, err) }
}

func endSpan(sp trace.Span, err error) {
	if err != nil {
		sp.RecordError(err)
		sp.SetStatus(codes.Error, err.Error())
	}
	sp.End()
}

func metaAttrs(meta Metadata) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, 2)
	if meta.CorrelationID != "" {
		out = append(out, attribute.String("loom.correlation_id", meta.CorrelationID))
	}
	if meta.CausationID != "" {
		out = append(out, attribute.String("loom.causation_id", meta.CausationID))
	}
	return out
}

// --- trace context across the bus ---

// envelopeCarrier adapts Envelope.Trace to the propagator interface; the
// map rides the envelope JSON so any bus implementation carries it.
type envelopeCarrier struct{ env *Envelope }

func (c envelopeCarrier) Get(key string) string { return c.env.Trace[key] }
func (c envelopeCarrier) Set(key, val string) {
	if c.env.Trace == nil {
		c.env.Trace = map[string]string{}
	}
	c.env.Trace[key] = val
}
func (c envelopeCarrier) Keys() []string {
	out := make([]string, 0, len(c.env.Trace))
	for k := range c.env.Trace {
		out = append(out, k)
	}
	return out
}

var busPropagator = propagation.TraceContext{}

func injectTrace(ctx context.Context, env *Envelope) {
	busPropagator.Inject(ctx, envelopeCarrier{env})
}

func extractTrace(ctx context.Context, env *Envelope) context.Context {
	return busPropagator.Extract(ctx, envelopeCarrier{env})
}

// --- gauges: the /stats numbers, observed on the SDK's collection cycle ---

func (c *Client) registerGauges() {
	m := otel.Meter(otelScope)
	depth, _ := m.Int64ObservableGauge("loom.outbox.depth", metric.WithDescription("unpublished outbox rows"))
	oldest, _ := m.Int64ObservableGauge("loom.outbox.oldest_age", metric.WithUnit("s"), metric.WithDescription("age of the oldest unpublished row"))
	dead, _ := m.Int64ObservableGauge("loom.dead_letters.depth", metric.WithDescription("parked deliveries awaiting redrive"))
	timers, _ := m.Int64ObservableGauge("loom.timers.pending", metric.WithDescription("scheduled timers"))
	fxRunning, _ := m.Int64ObservableGauge("loom.effects.running", metric.WithDescription("effect slots claimed but unsettled (in doubt if old)"))
	fxFailed, _ := m.Int64ObservableGauge("loom.effects.failed", metric.WithDescription("effect slots recorded as not executed"))
	lag, _ := m.Int64ObservableGauge("loom.runner.lag", metric.WithDescription("events between a checkpointed runner and the log head, by runner"))

	svc := metric.WithAttributes(c.tel.service)
	_, _ = m.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		if d, age, err := c.OutboxDepth(ctx); err == nil {
			o.ObserveInt64(depth, d, svc)
			o.ObserveInt64(oldest, int64(age.Seconds()), svc)
		}
		var deadN, timersN, fxR, fxF int64
		if err := c.db.QueryRow(ctx, `SELECT count(*) FROM loom_dead_letters WHERE service=$1`, c.reg.Service).Scan(&deadN); err == nil {
			o.ObserveInt64(dead, deadN, svc)
		}
		if err := c.db.QueryRow(ctx, `SELECT count(*) FROM loom_timers WHERE service=$1`, c.reg.Service).Scan(&timersN); err == nil {
			o.ObserveInt64(timers, timersN, svc)
		}
		if err := c.db.QueryRow(ctx, `
			SELECT count(*) FILTER (WHERE status='running'), count(*) FILTER (WHERE status='failed')
			FROM loom_effects WHERE service=$1`, c.reg.Service).Scan(&fxR, &fxF); err == nil {
			o.ObserveInt64(fxRunning, fxR, svc)
			o.ObserveInt64(fxFailed, fxF, svc)
		}

		var head int64
		if err := c.db.QueryRow(ctx, `SELECT coalesce(max(global_seq),0) FROM loom_events WHERE service=$1`, c.reg.Service).Scan(&head); err != nil {
			return nil // partial observation beats a failed cycle
		}
		seqs := map[string]int64{}
		rows, err := c.db.Query(ctx, `SELECT runner, global_seq FROM loom_checkpoints WHERE service=$1`, c.reg.Service)
		if err != nil {
			return nil
		}
		for rows.Next() {
			var name string
			var seq int64
			if rows.Scan(&name, &seq) == nil {
				seqs[name] = seq
			}
		}
		rows.Close()
		observe := func(runner string) {
			o.ObserveInt64(lag, head-seqs[runner], metric.WithAttributes(c.tel.service, attribute.String("loom.runner", runner)))
		}
		for _, p := range c.reg.Projections {
			observe("projection:" + p.Name)
		}
		for _, p := range c.reg.Processes {
			if local, _ := c.splitSubscriptions(p); len(local) > 0 {
				observe("process:" + p.Name)
			}
		}
		return nil
	}, depth, oldest, dead, timers, fxRunning, fxFailed, lag)
}
