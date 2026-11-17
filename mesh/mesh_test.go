package mesh_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-apis/loom/mesh"
)

// TestSignIsMeshsSignature: the signature is Mesh's — sha256= and the
// lowercase hex HMAC-SHA256 of the body — and verifies only under the
// same secret and body.
func TestSignIsMeshsSignature(t *testing.T) {
	body := []byte(`{"seq":1,"event":"job.ended"}`)
	// echo -n '{"seq":1,"event":"job.ended"}' | openssl dgst -sha256 -hmac secret
	const want = "sha256=d83022116741c7e6e8340f3bb2fb9dc85fd04314fd145639c0304a0f8252b07b"
	got := mesh.Sign([]byte("secret"), body)
	if got != want {
		t.Fatalf("Sign answered %q", got)
	}
	if !mesh.Verify([]byte("secret"), body, got) {
		t.Error("a body does not verify under its own signature")
	}
	if mesh.Verify([]byte("other"), body, got) || mesh.Verify([]byte("secret"), append(body, ' '), got) || mesh.Verify(nil, body, got) {
		t.Error("a signature verified under another secret, another body or no secret")
	}
}

// TestHTTPClientOffersAndCancelsAsMeshTakesThem: POST /v1/jobs with the
// offer as JSON and the product token as a bearer; POST
// /v1/jobs/{id}/cancel with the reason; a non-2xx is an *APIError.
func TestHTTPClientOffersAndCancelsAsMeshTakesThem(t *testing.T) {
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		seen = append(seen, r.Method+" "+r.URL.Path+" "+r.Header.Get("Authorization")+" "+string(body))
		switch r.URL.Path {
		case "/v1/jobs":
			w.WriteHeader(http.StatusCreated)
			w.Write([]byte(`{"job_id":"j1","key":"k","status":"queued","queued_since":"2026-11-17T00:00:00Z"}`))
		case "/v1/jobs/j1/cancel":
			w.Write([]byte(`{"job_id":"j1","status":"ended","end":"cancelled","cancelling":false}`))
		default:
			http.Error(w, `{"error":"not_found"}`, http.StatusNotFound)
		}
	}))
	defer srv.Close()
	c := &mesh.HTTPClient{BaseURL: srv.URL + "/", Tokens: mesh.StaticToken("tok")}
	ctx := context.Background()
	got, err := c.OfferJob(ctx, mesh.Offer{Kind: "triage", Tenant: "acme", Subject: mesh.Subject{Type: "Ticket", ID: "t1"}, IdempotencyKey: "loom/x/1"})
	if err != nil || got.JobID != "j1" || got.Status != "queued" {
		t.Fatalf("OfferJob answered %+v, %v", got, err)
	}
	var sent mesh.Offer
	if err := json.Unmarshal([]byte(seen[0][len("POST /v1/jobs Bearer tok "):]), &sent); err != nil || sent.IdempotencyKey != "loom/x/1" || sent.Subject.ID != "t1" {
		t.Errorf("the offer went as %q", seen[0])
	}
	if err := c.CancelJob(ctx, "j1", "late"); err != nil {
		t.Fatal(err)
	}
	if seen[1] != `POST /v1/jobs/j1/cancel Bearer tok {"reason":"late"}` {
		t.Errorf("the cancel went as %q", seen[1])
	}
	var apiErr *mesh.APIError
	if err := c.CancelJob(ctx, "nope", ""); !errors.As(err, &apiErr) || apiErr.Status != http.StatusNotFound {
		t.Errorf("a 404 answered %v", err)
	}
}

// TestFakeConvergesOnTheIdempotencyKey: an offer made again at its key
// is the same job, as Mesh answers it.
func TestFakeConvergesOnTheIdempotencyKey(t *testing.T) {
	f := &mesh.Fake{}
	o := mesh.Offer{Kind: "triage", Tenant: "acme", Subject: mesh.Subject{Type: "Ticket", ID: "t1"}, IdempotencyKey: "k"}
	a, _ := f.OfferJob(context.Background(), o)
	b, _ := f.OfferJob(context.Background(), o)
	if a.JobID != b.JobID || len(f.Offers()) != 1 {
		t.Fatalf("two offers at one key made %s and %s (%d offers)", a.JobID, b.JobID, len(f.Offers()))
	}
	if _, err := f.OfferJob(context.Background(), mesh.Offer{Kind: "triage"}); err == nil {
		t.Error("an offer with no tenant, subject or key was taken")
	}
}
