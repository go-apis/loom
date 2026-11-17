// Package mesh is the part of Mesh's product API a loom service uses to
// ask an agent: offering a job of a kind the product publishes
// (POST /v1/jobs), cancelling one (POST /v1/jobs/{id}/cancel), and the
// signed hook Mesh posts each job change to (X-Mesh-Signature, an
// HMAC-SHA256 of the body under the product's hook secret). The shapes
// are Mesh's own, as its server and its conformance client declare them
// (mesh-api server/jobs.go, server/cancel.go, conformance/client.go,
// conformance/hooks.go; mesh-specs ADR 001, "Product API" and "Hook to
// the product"); this package invents none.
package mesh

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// The job hook's headers (ADR 001, "Hook to the product").
const (
	HeaderSignature = "X-Mesh-Signature"
	HeaderEvent     = "X-Mesh-Event"
	HeaderDelivery  = "X-Mesh-Delivery"
)

// MaxHookBody bounds one hook's body.
const MaxHookBody = 1 << 20

// MaxIdempotencyKey is the longest idempotency key Mesh takes, and
// MaxRefs the largest refs object an offer may carry.
const (
	MaxIdempotencyKey = 200
	MaxRefs           = 4096
)

// The hook events (ADR 001, "Hook to the product").
const (
	EventJobTaken      = "job.taken"
	EventJobHandedBack = "job.handed_back"
	EventJobHeld       = "job.held"
	EventJobParked     = "job.parked"
	EventJobEnded      = "job.ended"
	// EventJobConfirm is both the synchronous ask that precedes a lease
	// of a job that has waited (no seq, answered in the response body)
	// and the feed entry that follows it (mesh-specs ADR 018 §4).
	EventJobConfirm = "job.confirm"
)

// How a job ended (Outcome.End): Mesh's JobEnd.
const (
	EndFinished       = "finished"
	EndFailed         = "failed"
	EndCancelled      = "cancelled"
	EndHeldOut        = "held_out"
	EndNoLongerNeeded = "no_longer_needed"
)

// Subject is what a job is about, in the product's own terms.
type Subject struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

// Offer is POST /v1/jobs's body. Kind is the bare kind: the caller's
// client token names the product. A repeated IdempotencyKey converges
// on the job first offered at it.
type Offer struct {
	Kind           string          `json:"kind"`
	Tenant         string          `json:"tenant"`
	Subject        Subject         `json:"subject"`
	IdempotencyKey string          `json:"idempotency_key"`
	Key            string          `json:"key,omitempty"`
	Priority       string          `json:"priority,omitempty"`
	Group          string          `json:"group,omitempty"`
	Code           json.RawMessage `json:"code,omitempty"`
	Git            json.RawMessage `json:"git,omitempty"`
	Access         json.RawMessage `json:"access,omitempty"`
	// Refs is the product's own JSON object, handed back unchanged on
	// the job's hooks and feed.
	Refs      json.RawMessage `json:"refs,omitempty"`
	ExpiresAt *time.Time      `json:"expires_at,omitempty"`
}

// Offered is POST /v1/jobs's answer: 201 for a new job, 200 for the
// idempotency key's job, already offered.
type Offered struct {
	JobID       string    `json:"job_id"`
	Key         string    `json:"key,omitempty"`
	Status      string    `json:"status"`
	QueuedSince time.Time `json:"queued_since"`
	End         string    `json:"end,omitempty"`
	Cause       string    `json:"cause,omitempty"`
}

// JobChange is one feed entry, and the body of every job hook.
type JobChange struct {
	Seq        int64           `json:"seq"`
	Event      string          `json:"event"`
	Job        JobRef          `json:"job"`
	Lease      json.RawMessage `json:"lease,omitempty"`
	Outcome    *Outcome        `json:"outcome,omitempty"`
	Usage      json.RawMessage `json:"usage,omitempty"`
	SessionRef string          `json:"session_ref,omitempty"`
}

// JobRef names the job an entry is about.
type JobRef struct {
	ID      string          `json:"id"`
	Key     string          `json:"key,omitempty"`
	Kind    string          `json:"kind"`
	Subject Subject         `json:"subject"`
	Tenant  string          `json:"tenant"`
	Refs    json.RawMessage `json:"refs,omitempty"`
}

// Outcome is how a job ended; Result is the product's own, unread by
// Mesh.
type Outcome struct {
	Status    string          `json:"status,omitempty"`
	Handbacks int64           `json:"handbacks,omitempty"`
	End       string          `json:"end"`
	Cause     string          `json:"cause,omitempty"`
	Reason    string          `json:"reason,omitempty"`
	ExitCode  int64           `json:"exit_code,omitempty"`
	Result    json.RawMessage `json:"result,omitempty"`
}

// ConfirmAsk is a job.confirm ask's body: posted with no seq right
// before a take would lease a job that has waited, and answered in the
// response body (mesh-specs ADR 018 §4).
type ConfirmAsk struct {
	Event   string    `json:"event"`
	Job     JobRef    `json:"job"`
	WaitedS int64     `json:"waited_s"`
	At      time.Time `json:"at"`
}

// ConfirmReply answers a ConfirmAsk: run or cancel (with a reason).
type ConfirmReply struct {
	Answer string `json:"answer"`
	Reason string `json:"reason,omitempty"`
}

// Sign is a hook body's X-Mesh-Signature under secret:
// `sha256=<lowercase hex HMAC-SHA256 of the body>`, as Mesh sends it.
func Sign(secret, body []byte) string {
	m := hmac.New(sha256.New, secret)
	m.Write(body)
	return "sha256=" + hex.EncodeToString(m.Sum(nil))
}

// Verify reports whether header is body's signature under secret,
// compared in constant time. An empty secret verifies nothing.
func Verify(secret, body []byte, header string) bool {
	got, ok := strings.CutPrefix(strings.TrimSpace(header), "sha256=")
	if !ok || len(secret) == 0 {
		return false
	}
	sig, err := hex.DecodeString(got)
	if err != nil {
		return false
	}
	m := hmac.New(sha256.New, secret)
	m.Write(body)
	return hmac.Equal(sig, m.Sum(nil))
}

// TokenSource answers the product's client token for each call (a
// Latchkey client_credentials token).
type TokenSource interface {
	Token(ctx context.Context) (string, error)
}

// StaticToken is a TokenSource that always answers the same token.
type StaticToken string

// Token implements TokenSource.
func (s StaticToken) Token(context.Context) (string, error) { return string(s), nil }

// HTTPClient offers and cancels jobs on Mesh at BaseURL over HTTP.
type HTTPClient struct {
	// BaseURL is Mesh's root, e.g. https://api.projectmesh.io.
	BaseURL string
	Tokens  TokenSource
	// HTTP is the client to send with; nil is a 30-second one.
	HTTP *http.Client
}

// APIError is a non-2xx answer: its status and body.
type APIError struct {
	Method, Path string
	Status       int
	Body         string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("mesh %s %s: %d %s", e.Method, e.Path, e.Status, strings.TrimSpace(e.Body))
}

// OfferJob is POST /v1/jobs.
func (c *HTTPClient) OfferJob(ctx context.Context, o Offer) (Offered, error) {
	var out Offered
	err := c.do(ctx, http.MethodPost, "/v1/jobs", o, &out)
	return out, err
}

// CancelJob is POST /v1/jobs/{id}/cancel. A cancel asked again answers
// the same.
func (c *HTTPClient) CancelJob(ctx context.Context, jobID, reason string) error {
	var body any
	if reason != "" {
		body = struct {
			Reason string `json:"reason"`
		}{reason}
	}
	return c.do(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(jobID)+"/cancel", body, nil)
}

func (c *HTTPClient) do(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.BaseURL, "/")+path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if c.Tokens != nil {
		tok, err := c.Tokens.Token(ctx)
		if err != nil {
			return fmt.Errorf("mesh %s %s: product token: %w", method, path, err)
		}
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	hc := c.HTTP
	if hc == nil {
		hc = &http.Client{Timeout: 30 * time.Second}
	}
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return &APIError{Method: method, Path: path, Status: resp.StatusCode, Body: string(raw)}
	}
	if out == nil || len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("mesh %s %s: answer: %w", method, path, err)
	}
	return nil
}
