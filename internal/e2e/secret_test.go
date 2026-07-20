package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/billing"
	billinggen "github.com/go-apis/loom/internal/e2e/billing/loomgen"
)

// @secret = @pii sealing + write-only over HTTP: sealed at rest, plaintext
// for in-process reads, redacted to a stable fingerprint on API reads.
func TestSecretFields(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	payee := uuid.New()
	err = cli.Dispatch(ctx, &billinggen.RegisterPayee{
		CommandBase: loom.CommandBase{AggregateID: payee, Namespace: "default"},
		Name:        "Jordan Rivers",
		Tin:         "123-45-6789",
		TinLast4:    "6789",
		BankToken:   "tok-live-abcdef123456",
	})
	if err != nil {
		t.Fatal(err)
	}

	// at rest: sealed exactly like @pii — in the log and the snapshot
	var evtTok, snapTok string
	if err := pool.QueryRow(ctx, `SELECT data->>'bank_token' FROM loom_events WHERE type='PayeeRegistered'`).Scan(&evtTok); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(evtTok, "pii:v1:") {
		t.Fatalf("log at rest: bank_token=%q", evtTok)
	}
	if err := pool.QueryRow(ctx, `SELECT state->>'bank_token' FROM loom_snapshots WHERE aggregate_id=$1`, payee).Scan(&snapTok); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(snapTok, "pii:v1:") {
		t.Fatalf("snapshot at rest: bank_token=%q", snapTok)
	}

	// in-process reads see plaintext — this is how a sender/effect uses it
	state, _, err := cli.Load(ctx, "Payee", "default", payee)
	if err != nil {
		t.Fatal(err)
	}
	if got := state.(*billinggen.Payee).BankToken; got != "tok-live-abcdef123456" {
		t.Fatalf("in-process Load: bank_token=%q", got)
	}

	// the HTTP API redacts to a fingerprint; the sibling @pii field still
	// reads back in plaintext
	srv := httptest.NewServer(cli.HTTPHandler())
	defer srv.Close()

	res, err := http.Get(fmt.Sprintf("%s/aggregates/Payee/%s?namespace=default", srv.URL, payee))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	var out struct {
		State struct {
			Tin       string `json:"tin"`
			BankToken string `json:"bank_token"`
		} `json:"state"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("aggregate GET: %v: %s", err, body)
	}
	if out.State.Tin != "123-45-6789" {
		t.Fatalf("@pii field should read back: tin=%q", out.State.Tin)
	}
	if !strings.HasPrefix(out.State.BankToken, "secret:sha256:") || strings.Contains(string(body), "tok-live") {
		t.Fatalf("@secret field must be redacted: %s", body)
	}
	fp1 := out.State.BankToken

	// the fingerprint is stable across reads (clients can detect rotation)
	res2, err := http.Get(fmt.Sprintf("%s/aggregates/Payee/%s?namespace=default", srv.URL, payee))
	if err != nil {
		t.Fatal(err)
	}
	body2, _ := io.ReadAll(res2.Body)
	res2.Body.Close()
	if !strings.Contains(string(body2), fp1) {
		t.Fatalf("fingerprint not stable: %s vs %s", fp1, body2)
	}

	// the aggregate SSE stream redacts the same way
	sctx, scancel := context.WithTimeout(ctx, 5*time.Second)
	defer scancel()
	req, _ := http.NewRequestWithContext(sctx, "GET", fmt.Sprintf("%s/aggregates/Payee/%s/stream?namespace=default", srv.URL, payee), nil)
	sres, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer sres.Body.Close()
	buf := make([]byte, 4096)
	n, _ := sres.Body.Read(buf)
	first := string(buf[:n])
	if strings.Contains(first, "tok-live") || !strings.Contains(first, "secret:sha256:") {
		t.Fatalf("stream leaked the secret: %s", first)
	}
}
