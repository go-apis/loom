package e2e_test

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/billing"
	billinggen "github.com/go-apis/loom/internal/e2e/billing/loomgen"
)

// Rewrap swaps the KeyWrapper under sealed data: DEKs are re-wrapped in
// place, ciphertext is untouched, and a client holding the NEW wrapper
// reads everything — master-key rotation / LocalKeys→KMS migration.
func TestRewrap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := testDB(t, ctx)

	newKey := func() loom.KeyWrapper {
		raw := make([]byte, 32)
		if _, err := rand.Read(raw); err != nil {
			t.Fatal(err)
		}
		k, err := loom.LocalKeys(raw)
		if err != nil {
			t.Fatal(err)
		}
		return k
	}
	oldKeys, newKeys := newKey(), newKey()

	cli, err := loom.New(loom.Config{DB: pool, Registry: billing.NewRegistry(), Keys: oldKeys})
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
		BankToken:   "tok-rotate-me",
	})
	if err != nil {
		t.Fatal(err)
	}

	n, err := loom.Rewrap(ctx, pool, oldKeys, newKeys)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("rewrapped %d keys, want 1", n)
	}

	// a fresh client holding only the NEW wrapper reads the sealed fields
	cli2, err := loom.New(loom.Config{DB: pool, Registry: billing.NewRegistry(), Keys: newKeys})
	if err != nil {
		t.Fatal(err)
	}
	state, _, err := cli2.Load(ctx, "Payee", "default", payee)
	if err != nil {
		t.Fatal(err)
	}
	p := state.(*billinggen.Payee)
	if p.Tin != "123-45-6789" || p.BankToken != "tok-rotate-me" {
		t.Fatalf("after rewrap: tin=%q bank_token=%q", p.Tin, p.BankToken)
	}

	// and a client still holding the OLD wrapper cannot
	cli3, err := loom.New(loom.Config{DB: pool, Registry: billing.NewRegistry(), Keys: oldKeys})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := cli3.Load(ctx, "Payee", "default", payee); err == nil {
		t.Fatal("old wrapper still reads after rewrap — rotation did nothing")
	}
}
