package e2e_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/billing"
	billinggen "github.com/go-apis/loom/internal/e2e/billing/loomgen"
)

// testKeys is the master-key wrapper every billing client needs now that
// its schema declares @pii.
func testKeys(t *testing.T) loom.KeyWrapper {
	t.Helper()
	kw, err := loom.LocalKeys(bytes.Repeat([]byte("k"), 32))
	if err != nil {
		t.Fatal(err)
	}
	return kw
}

// TestPIIEncryption proves @pii fields are sealed everywhere they rest
// (log, snapshot, read model), plaintext on typed reads, and that Shred
// redacts them permanently — including through a projection rebuild.
func TestPIIEncryption(t *testing.T) {
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
	})
	if err != nil {
		t.Fatal(err)
	}

	// at rest: TIN sealed, name plaintext — in the log AND the snapshot
	var evtTin, evtName, snapTin string
	if err := pool.QueryRow(ctx, `SELECT data->>'tin', data->>'name' FROM loom_events WHERE type='PayeeRegistered'`).Scan(&evtTin, &evtName); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(evtTin, "pii:v1:") || evtName != "Jordan Rivers" {
		t.Fatalf("log at rest: tin=%q name=%q", evtTin, evtName)
	}
	if err := pool.QueryRow(ctx, `SELECT state->>'tin' FROM loom_snapshots WHERE aggregate_id=$1`, payee).Scan(&snapTin); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(snapTin, "pii:v1:") {
		t.Fatalf("snapshot at rest: tin=%q", snapTin)
	}

	// typed reads decrypt
	state, _, err := cli.Load(ctx, "Payee", "default", payee)
	if err != nil {
		t.Fatal(err)
	}
	if state.(*billinggen.Payee).Tin != "123-45-6789" {
		t.Fatalf("Load: %+v", state)
	}

	// the read model: sealed in the row, plaintext through Entity, and the
	// deliberately-plain last4 stays filterable
	waitFor(t, ctx, "payee directory projected", func() bool {
		e, err := cli.Entity(ctx, "PayeeDirectory", "default", payee)
		return err == nil && e != nil
	})
	var rowTin string
	if err := pool.QueryRow(ctx, `SELECT data->>'tin' FROM loom_entities WHERE entity_type='PayeeDirectory'`).Scan(&rowTin); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(rowTin, "pii:v1:") {
		t.Fatalf("entity at rest: tin=%q", rowTin)
	}
	entity, err := cli.Entity(ctx, "PayeeDirectory", "default", payee)
	if err != nil {
		t.Fatal(err)
	}
	dir := entity.(*billinggen.PayeeDirectory)
	if dir.Tin != "123-45-6789" || dir.TinLast4 != "6789" {
		t.Fatalf("Entity: %+v", dir)
	}
	rows, err := cli.QueryEntities(ctx, "PayeeDirectory", loom.Query{
		Namespace: "default",
		Filters:   []loom.Filter{{Field: "tin_last4", Value: "6789"}},
	})
	if err != nil || len(rows) != 1 {
		t.Fatalf("last4 filter: %d rows (%v)", len(rows), err)
	}

	// shred: the key dies, every copy redacts, non-PII survives
	if err := cli.Shred(ctx, "default", payee); err != nil {
		t.Fatal(err)
	}
	state, _, err = cli.Load(ctx, "Payee", "default", payee)
	if err != nil {
		t.Fatal(err)
	}
	shredded := state.(*billinggen.Payee)
	if shredded.Tin != "" || shredded.Name != "Jordan Rivers" {
		t.Fatalf("after shred: %+v", shredded)
	}
	entity, err = cli.Entity(ctx, "PayeeDirectory", "default", payee)
	if err != nil {
		t.Fatal(err)
	}
	dir = entity.(*billinggen.PayeeDirectory)
	if dir.Tin != "" || dir.Name != "Jordan Rivers" || dir.TinLast4 != "6789" {
		t.Fatalf("entity after shred: %+v", dir)
	}

	// a rebuild refolds redacted events: still no TIN, everything else back
	if err := cli.Rebuild(ctx, "payeeDirectory"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "rebuilt directory", func() bool {
		e, err := cli.Entity(ctx, "PayeeDirectory", "default", payee)
		if err != nil || e == nil {
			return false
		}
		d := e.(*billinggen.PayeeDirectory)
		return d.Name == "Jordan Rivers" && d.Tin == "" && d.TinLast4 == "6789"
	})

	// new streams mint new keys after a shred
	payee2 := uuid.New()
	err = cli.Dispatch(ctx, &billinggen.RegisterPayee{
		CommandBase: loom.CommandBase{AggregateID: payee2, Namespace: "default"},
		Name:        "Casey Reed",
		Tin:         "987-65-4321",
		TinLast4:    "4321",
	})
	if err != nil {
		t.Fatal(err)
	}
	state, _, err = cli.Load(ctx, "Payee", "default", payee2)
	if err != nil || state.(*billinggen.Payee).Tin != "987-65-4321" {
		t.Fatalf("post-shred registration: %+v (%v)", state, err)
	}
}
