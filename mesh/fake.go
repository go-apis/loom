package mesh

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"time"
)

// Fake is Mesh in a test: it takes offers as Mesh does (a repeated
// idempotency key converges on the job first offered at it), runs no
// node, and ends a job when the test says so, posting the job.ended
// hook Mesh would — signed under Secret — to Hook.
type Fake struct {
	// Secret signs the hooks the fake posts.
	Secret []byte
	// Hook receives them; nil keeps them only for Ended.
	Hook http.Handler

	mu     sync.Mutex
	offers []Offer
	byKey  map[string]string
	jobs   map[string]*fakeJob
	seq    int64
	next   int
}

type fakeJob struct {
	offer     Offer
	ended     *Outcome
	cancelled string
}

// OfferJob takes an offer: a new job, or the one already at its
// idempotency key.
func (f *Fake) OfferJob(_ context.Context, o Offer) (Offered, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case o.Kind == "" || o.Tenant == "" || o.IdempotencyKey == "" || o.Subject.Type == "" || o.Subject.ID == "":
		return Offered{}, &APIError{Method: http.MethodPost, Path: "/v1/jobs", Status: http.StatusBadRequest, Body: "kind, tenant, subject and idempotency_key are required"}
	case len(o.IdempotencyKey) > MaxIdempotencyKey:
		return Offered{}, &APIError{Method: http.MethodPost, Path: "/v1/jobs", Status: http.StatusBadRequest, Body: "idempotency_key is too long"}
	case len(o.Refs) > MaxRefs:
		return Offered{}, &APIError{Method: http.MethodPost, Path: "/v1/jobs", Status: http.StatusBadRequest, Body: "refs is too large"}
	}
	if f.byKey == nil {
		f.byKey, f.jobs = map[string]string{}, map[string]*fakeJob{}
	}
	if id, ok := f.byKey[o.IdempotencyKey]; ok {
		return Offered{JobID: id, Status: "queued"}, nil
	}
	f.next++
	id := "job-" + strconv.Itoa(f.next)
	f.byKey[o.IdempotencyKey] = id
	f.jobs[id] = &fakeJob{offer: o}
	f.offers = append(f.offers, o)
	return Offered{JobID: id, Status: "queued", QueuedSince: time.Now().UTC()}, nil
}

// CancelJob notes the cancel. A queued job in Mesh would end cancelled;
// the fake leaves the ending to the test.
func (f *Fake) CancelJob(_ context.Context, jobID, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, ok := f.jobs[jobID]
	if !ok {
		return &APIError{Method: http.MethodPost, Path: "/v1/jobs/" + jobID + "/cancel", Status: http.StatusNotFound, Body: "no such job"}
	}
	j.cancelled = reason
	if j.cancelled == "" {
		j.cancelled = "cancelled"
	}
	return nil
}

// Offers is every job offered, oldest first: one per idempotency key.
func (f *Fake) Offers() []Offer {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Offer(nil), f.offers...)
}

// Offer is the offer behind a job.
func (f *Fake) Offer(jobID string) (Offer, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, ok := f.jobs[jobID]
	if !ok {
		return Offer{}, false
	}
	return j.offer, true
}

// Cancelled reports whether the job was cancelled, and why.
func (f *Fake) Cancelled(jobID string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, ok := f.jobs[jobID]
	if !ok || j.cancelled == "" {
		return "", false
	}
	return j.cancelled, true
}

// Ended is how the job ended, once the test has ended it.
func (f *Fake) Ended(jobID string) (Outcome, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, ok := f.jobs[jobID]
	if !ok || j.ended == nil {
		return Outcome{}, false
	}
	return *j.ended, true
}

// Finish ends the job finished with the product's result.
func (f *Fake) Finish(ctx context.Context, jobID string, result json.RawMessage) error {
	return f.End(ctx, jobID, Outcome{End: EndFinished, Result: result})
}

// Fail ends the job failed.
func (f *Fake) Fail(ctx context.Context, jobID, cause, reason string) error {
	return f.End(ctx, jobID, Outcome{End: EndFailed, Cause: cause, Reason: reason})
}

// End ends the job with o and, when Hook is set, posts the job.ended
// hook Mesh would. A hook not answered 2xx is an error, as Mesh would
// retry it.
func (f *Fake) End(ctx context.Context, jobID string, o Outcome) error {
	f.mu.Lock()
	j, ok := f.jobs[jobID]
	if !ok {
		f.mu.Unlock()
		return fmt.Errorf("mesh fake: no job %s", jobID)
	}
	if j.ended != nil {
		f.mu.Unlock()
		return fmt.Errorf("mesh fake: job %s has ended already", jobID)
	}
	j.ended = &o
	f.seq++
	change := JobChange{Seq: f.seq, Event: EventJobEnded, Outcome: &o, Job: JobRef{
		ID: jobID, Key: j.offer.Key, Kind: j.offer.Kind, Subject: j.offer.Subject, Tenant: j.offer.Tenant, Refs: j.offer.Refs,
	}}
	hook, secret := f.Hook, f.Secret
	f.mu.Unlock()
	if hook == nil {
		return nil
	}
	body, err := json.Marshal(change)
	if err != nil {
		return err
	}
	return Deliver(ctx, hook, secret, change.Event, strconv.FormatInt(change.Seq, 10), body)
}

// Deliver posts one hook body to h as Mesh does: signed under secret,
// with its event and delivery headers. Anything but 2xx is an error.
func Deliver(ctx context.Context, h http.Handler, secret []byte, event, delivery string, body []byte) error {
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/hooks/mesh", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(HeaderSignature, Sign(secret, body))
	req.Header.Set(HeaderEvent, event)
	req.Header.Set(HeaderDelivery, delivery)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code < 200 || rec.Code > 299 {
		return errors.New("mesh fake: hook answered " + strconv.Itoa(rec.Code) + ": " + rec.Body.String())
	}
	return nil
}
