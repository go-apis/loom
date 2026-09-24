package loom

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func decodeLine(t *testing.T, b *bytes.Buffer) map[string]any {
	t.Helper()
	var got map[string]any
	if err := json.Unmarshal(b.Bytes(), &got); err != nil {
		t.Fatalf("handler wrote non-JSON %q: %v", b.String(), err)
	}
	return got
}

// The incident: "runner step failed" logged at ErrorContext, and
// severity>=ERROR in Cloud Logging finding nothing, because the JSON
// said level=ERROR and Cloud Logging reads severity.
func TestCloudLogHandlerSeverityForFailedStep(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(CloudLogHandler(&buf))

	log.Error("runner step failed", "runner", "projection:activity", "error", "decode: unknown type")

	got := decodeLine(t, &buf)
	if got["severity"] != "ERROR" {
		t.Errorf("severity = %v, want ERROR (line: %s)", got["severity"], buf.String())
	}
	if got["message"] != "runner step failed" {
		t.Errorf("message = %v, want %q", got["message"], "runner step failed")
	}
	if _, ok := got["level"]; ok {
		t.Errorf("level key survived the rename: %s", buf.String())
	}
	if _, ok := got["msg"]; ok {
		t.Errorf("msg key survived the rename: %s", buf.String())
	}
	if got["runner"] != "projection:activity" || got["error"] != "decode: unknown type" {
		t.Errorf("attributes lost: %s", buf.String())
	}
	if got["time"] == nil {
		t.Errorf("no time on the record: %s", buf.String())
	}
}

func TestCloudLogHandlerSeverityLevels(t *testing.T) {
	for _, tc := range []struct {
		level slog.Level
		want  string
	}{
		{slog.LevelDebug, "DEBUG"},
		{slog.LevelInfo, "INFO"},
		{slog.LevelWarn, "WARNING"}, // slog says WARN, Cloud Logging says WARNING
		{slog.LevelError, "ERROR"},
		{slog.LevelError + 4, "ERROR"},
	} {
		var buf bytes.Buffer
		log := slog.New(CloudLogHandlerOptions(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

		log.Log(context.Background(), tc.level, "a step")

		got := decodeLine(t, &buf)
		if got["severity"] != tc.want {
			t.Errorf("%v: severity = %v, want %v", tc.level, got["severity"], tc.want)
		}
	}
}

// The trace already rides the envelope across the bus; a log line from a
// runner step should say which trace it belongs to.
func TestCloudLogHandlerPromotesTrace(t *testing.T) {
	traceID, err := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	if err != nil {
		t.Fatal(err)
	}
	spanID, err := trace.SpanIDFromHex("00f067aa0ba902b7")
	if err != nil {
		t.Fatal(err)
	}
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	}))

	var buf bytes.Buffer
	log := slog.New(CloudLogHandler(&buf))
	log.ErrorContext(ctx, "runner step failed")

	got := decodeLine(t, &buf)
	// No GOOGLE_CLOUD_PROJECT in a test process, so the bare id.
	if got["logging.googleapis.com/trace"] != traceID.String() {
		t.Errorf("trace = %v, want %v", got["logging.googleapis.com/trace"], traceID)
	}
	if got["logging.googleapis.com/spanId"] != spanID.String() {
		t.Errorf("spanId = %v, want %v", got["logging.googleapis.com/spanId"], spanID)
	}
	if got["logging.googleapis.com/trace_sampled"] != true {
		t.Errorf("trace_sampled = %v, want true", got["logging.googleapis.com/trace_sampled"])
	}

	// A context with no span leaves the line alone rather than writing
	// an all-zero trace id.
	buf.Reset()
	log.ErrorContext(context.Background(), "runner step failed")
	if _, ok := decodeLine(t, &buf)["logging.googleapis.com/trace"]; ok {
		t.Errorf("trace field written without a span: %s", buf.String())
	}
}

func TestCloudLogHandlerProjectPrefix(t *testing.T) {
	t.Setenv("GOOGLE_CLOUD_PROJECT", "mesh-prod")

	traceID, err := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	if err != nil {
		t.Fatal(err)
	}
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID,
		SpanID:  trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
	}))

	var buf bytes.Buffer
	slog.New(CloudLogHandler(&buf)).ErrorContext(ctx, "runner step failed")

	want := "projects/mesh-prod/traces/" + traceID.String()
	if got := decodeLine(t, &buf)["logging.googleapis.com/trace"]; got != want {
		t.Errorf("trace = %v, want %v", got, want)
	}
}

// WithAttrs/WithGroup must keep the wrapper: a service logger is almost
// always `slog.With(...)` over the root handler.
func TestCloudLogHandlerSurvivesWith(t *testing.T) {
	traceID, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID,
		SpanID:  trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
	}))

	var buf bytes.Buffer
	log := slog.New(CloudLogHandler(&buf)).With("service", "runsheet")
	log.ErrorContext(ctx, "runner step failed")

	got := decodeLine(t, &buf)
	if got["severity"] != "ERROR" || got["message"] != "runner step failed" {
		t.Errorf("mapping lost through With: %s", buf.String())
	}
	if got["service"] != "runsheet" {
		t.Errorf("attribute lost through With: %s", buf.String())
	}
	if got["logging.googleapis.com/trace"] != traceID.String() {
		t.Errorf("trace lost through With: %s", buf.String())
	}
}

// An application attribute called "level" is data, not the record's
// severity, and a nested one is never touched.
func TestCloudLogHandlerLeavesApplicationAttrsAlone(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(CloudLogHandler(&buf))

	log.Warn("threshold crossed", "level", "high", slog.Group("inner", "msg", "nested"))

	got := decodeLine(t, &buf)
	if got["severity"] != "WARNING" {
		t.Errorf("severity = %v, want WARNING", got["severity"])
	}
	if got["level"] != "high" {
		t.Errorf("application level attribute = %v, want high (line: %s)", got["level"], buf.String())
	}
	inner, _ := got["inner"].(map[string]any)
	if inner["msg"] != "nested" {
		t.Errorf("grouped msg was renamed: %s", buf.String())
	}
}

func TestCloudLogHandlerOptionsChainReplaceAttr(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(CloudLogHandlerOptions(&buf, &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) == 0 && a.Key == "token" {
				return slog.String("token", "redacted")
			}
			return a
		},
	}))

	log.Error("runner step failed", "token", "s3cret")

	got := decodeLine(t, &buf)
	if got["severity"] != "ERROR" || got["message"] != "runner step failed" {
		t.Errorf("mapping lost with a caller ReplaceAttr: %s", buf.String())
	}
	if got["token"] != "redacted" {
		t.Errorf("caller ReplaceAttr did not run: %s", buf.String())
	}
}
