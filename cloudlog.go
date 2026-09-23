package loom

import (
	"context"
	"io"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel/trace"
)

// Cloud Logging reads a structured payload's severity and trace from
// these keys and nothing else. A plain slog.NewJSONHandler writes
// `level` and `msg`, which Cloud Logging keeps as two more string fields
// — so every line lands at DEFAULT severity and `severity>=ERROR` finds
// nothing, however loudly the code logged it.
const (
	cloudSeverityKey = "severity"
	cloudMessageKey  = "message"
	cloudTraceKey    = "logging.googleapis.com/trace"
	cloudSpanKey     = "logging.googleapis.com/spanId"
	cloudSampledKey  = "logging.googleapis.com/trace_sampled"
)

// CloudLogHandler returns a JSON slog.Handler that Google Cloud Logging
// understands: `level` becomes `severity` (DEBUG/INFO/WARNING/ERROR —
// slog's WARN is Cloud Logging's WARNING), `msg` becomes `message`, and
// the OTel trace on the context, if there is one, rides the line as
// Cloud Logging's trace fields so a log entry links to the span that
// wrote it.
//
// It is a handler for a consumer's main to wire, not loom's default:
//
//	slog.SetDefault(slog.New(loom.CloudLogHandler(os.Stdout)))
//
// Loom's own calls — "runner step failed" and the rest — then come out
// at a severity the console and log-based alerts can filter on. Nothing
// here depends on the Cloud Logging client; it is log/slog plus the OTel
// API loom already uses, and it stays a no-op difference anywhere else
// (the JSON is still JSON).
//
// Wire it at the root of the logger tree. A logger that has opened a
// group with WithGroup nests the trace keys inside that group, where
// Cloud Logging does not look for them.
func CloudLogHandler(w io.Writer) slog.Handler {
	return CloudLogHandlerOptions(w, nil)
}

// CloudLogHandlerOptions is CloudLogHandler with the JSON handler's
// options — a minimum level, AddSource. A ReplaceAttr of your own runs
// after the Cloud Logging mapping, so it sees `severity` and `message`
// rather than `level` and `msg`.
func CloudLogHandlerOptions(w io.Writer, opts *slog.HandlerOptions) slog.Handler {
	var o slog.HandlerOptions
	if opts != nil {
		o = *opts
	}
	user := o.ReplaceAttr
	o.ReplaceAttr = func(groups []string, a slog.Attr) slog.Attr {
		// Only the record's own built-ins are renamed; inside a group
		// a `level` or `msg` attribute belongs to the caller.
		if len(groups) == 0 {
			switch a.Key {
			case slog.LevelKey:
				// The built-in carries a slog.Level; an application
				// attribute called "level" carries a string and is left
				// alone.
				if lvl, ok := a.Value.Any().(slog.Level); ok {
					a = slog.String(cloudSeverityKey, cloudSeverity(lvl))
				}
			case slog.MessageKey:
				a.Key = cloudMessageKey
			}
		}
		if user != nil {
			a = user(groups, a)
		}
		return a
	}
	return cloudLogHandler{
		Handler:     slog.NewJSONHandler(w, &o),
		traceprefix: traceResourcePrefix(),
	}
}

// cloudLogHandler adds the trace fields, which ReplaceAttr cannot: the
// trace lives on the context, and ReplaceAttr never sees one.
type cloudLogHandler struct {
	slog.Handler
	traceprefix string
}

func (h cloudLogHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r = r.Clone()
		r.AddAttrs(
			slog.String(cloudTraceKey, h.traceprefix+sc.TraceID().String()),
			slog.String(cloudSpanKey, sc.SpanID().String()),
			slog.Bool(cloudSampledKey, sc.IsSampled()),
		)
	}
	return h.Handler.Handle(ctx, r)
}

// WithAttrs and WithGroup keep the wrapper on: the embedded handler's
// own versions would return a bare JSON handler and the trace would stop
// halfway down a logger tree.

func (h cloudLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	h.Handler = h.Handler.WithAttrs(attrs)
	return h
}

func (h cloudLogHandler) WithGroup(name string) slog.Handler {
	h.Handler = h.Handler.WithGroup(name)
	return h
}

// cloudSeverity maps a slog level onto Cloud Logging's LogSeverity
// enum. The boundaries are ranges, not equalities, so a custom level
// between the named ones still sorts where the caller meant it.
func cloudSeverity(l slog.Level) string {
	switch {
	case l < slog.LevelInfo:
		return "DEBUG"
	case l < slog.LevelWarn:
		return "INFO"
	case l < slog.LevelError:
		return "WARNING"
	default:
		return "ERROR"
	}
}

// traceResourcePrefix is "projects/<id>/traces/", the only form Cloud
// Logging resolves to a trace. GOOGLE_CLOUD_PROJECT is set for you on
// Cloud Run, App Engine and GKE; without it the bare trace id still goes
// out — greppable, joinable by hand, just not linked in the console.
func traceResourcePrefix() string {
	if p := os.Getenv("GOOGLE_CLOUD_PROJECT"); p != "" {
		return "projects/" + p + "/traces/"
	}
	return ""
}
