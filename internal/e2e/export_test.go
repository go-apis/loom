package e2e_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/internal/e2e/clock"
	clockgen "github.com/go-apis/loom/internal/e2e/clock/loomgen"
)

// TestSignedExportVerifies: a service's events are exported in log
// order, chained and signed, and the export verifies against the
// signer's public key away from the service. Changing one event,
// dropping one, moving one, cutting the end off or adding to it, and
// verifying against another key each fail, and a changed event is named.
// An export of one namespace holds only that namespace's events.
func TestSignedExportVerifies(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Registry: clock.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	ctx = loom.WithMeta(ctx, loom.Metadata{Actor: "user:smith"})
	for i := 0; i < 6; i++ {
		ns := "forge"
		if i%3 == 2 {
			ns = "foundry"
		}
		if err := cli.Dispatch(ctx, &clockgen.StrikeAnvil{CommandBase: loom.CommandBase{Namespace: ns, AggregateID: uuid.New()}}); err != nil {
			t.Fatal(err)
		}
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer := loom.Ed25519Signer("audit-2026", priv)
	verify := loom.Ed25519Verifier(pub)

	var file bytes.Buffer
	wrote, err := cli.ExportEvents(ctx, &file, loom.ExportOptions{Signer: signer})
	if err != nil {
		t.Fatal(err)
	}
	if wrote.Footer.Count != 6 || wrote.KeyID != "audit-2026" {
		t.Fatalf("exported %+v", wrote)
	}
	export := file.String()

	// it verifies, and every event is there in order, as stored
	var seen []loom.ExportedEvent
	sum, err := loom.VerifyExport(strings.NewReader(export), verify, func(e loom.ExportedEvent) error { seen = append(seen, e); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if sum.Footer.Count != 6 || sum.Header.Service != "clock" || sum.KeyID != "audit-2026" || len(seen) != 6 {
		t.Fatalf("verified %+v with %d events", sum, len(seen))
	}
	for i, e := range seen {
		if e.Type != "AnvilStruck" || e.Actor != "user:smith" || e.AggregateType != "Anvil" || (i > 0 && e.Seq <= seen[i-1].Seq) {
			t.Errorf("event %d of the export: %+v", i, e)
		}
	}

	lines := strings.Split(strings.TrimRight(export, "\n"), "\n")
	join := func(l []string) string { return strings.Join(l, "\n") + "\n" }
	third := seen[2]
	for name, c := range map[string]struct {
		file string
		want string
	}{
		"an event changed":        {strings.Replace(export, `"actor":"user:smith"`, `"actor":"user:jones"`, 1), "does not verify at event"},
		"an event dropped":        {join(append(append([]string{}, lines[:3]...), lines[4:]...)), "does not verify at event"},
		"two events swapped":      {join(append(append(append([]string{}, lines[:2]...), lines[3], lines[2]), lines[4:]...)), "does not verify at event"},
		"the end cut off":         {join(lines[:len(lines)-1]), "has no footer"},
		"the last event cut out":  {join(append(append([]string{}, lines[:len(lines)-2]...), lines[len(lines)-1])), "events were dropped or added at the end"},
		"a line after the footer": {export + lines[2] + "\n", "follows the footer"},
	} {
		if _, err := loom.VerifyExport(strings.NewReader(c.file), verify, nil); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want an error saying %q", name, err, c.want)
		}
	}
	// a changed event is named by its sequence
	changed := strings.Replace(export, lines[3], strings.Replace(lines[3], `"cool":false`, `"cool":true`, 1), 1)
	if changed == export {
		t.Fatalf("the third event's line has no payload to change: %s", lines[3])
	}
	if _, err := loom.VerifyExport(strings.NewReader(changed), verify, nil); err == nil || !strings.Contains(err.Error(), fmt.Sprintf("event %d (AnvilStruck", third.Seq)) {
		t.Errorf("a changed payload: %v, want event %d named", err, third.Seq)
	}
	// another key's signature is not this one's
	other, _, _ := ed25519.GenerateKey(rand.Reader)
	if _, err := loom.VerifyExport(strings.NewReader(export), loom.Ed25519Verifier(other), nil); err == nil || !strings.Contains(err.Error(), "signature") {
		t.Errorf("verified against another key: %v", err)
	}

	// the same from the command line: `loom verify-export`
	if !testing.Short() {
		dir := t.TempDir()
		write := func(name, content string) string {
			t.Helper()
			path := filepath.Join(dir, name)
			if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
				t.Fatal(err)
			}
			return path
		}
		key := base64.StdEncoding.EncodeToString(pub)
		run := func(file string) (string, error) {
			cmd := exec.Command("go", "run", "../../cmd/loom", "verify-export", "--key", key, file)
			out, err := cmd.CombinedOutput()
			return string(out), err
		}
		if out, err := run(write("export.jsonl", export)); err != nil || !strings.Contains(out, "verified: 6 events of clock (every namespace)") || !strings.Contains(out, "signed by audit-2026") {
			t.Errorf("loom verify-export on the export: %v\n%s", err, out)
		}
		if out, err := run(write("changed.jsonl", changed)); err == nil || !strings.Contains(out, fmt.Sprintf("does not verify at event %d", third.Seq)) {
			t.Errorf("loom verify-export on a changed export: %v\n%s", err, out)
		}
	}

	// one namespace, and a range
	file.Reset()
	if _, err := cli.ExportEvents(ctx, &file, loom.ExportOptions{Signer: signer, Namespace: "foundry"}); err != nil {
		t.Fatal(err)
	}
	seen = nil
	if sum, err := loom.VerifyExport(&file, verify, func(e loom.ExportedEvent) error { seen = append(seen, e); return nil }); err != nil || sum.Footer.Count != 2 || sum.Header.Namespace != "foundry" {
		t.Fatalf("the foundry's export: %+v %v", sum, err)
	}
	for _, e := range seen {
		if e.Namespace != "foundry" {
			t.Errorf("the foundry's export holds an event of %s", e.Namespace)
		}
	}
	file.Reset()
	if sum, err := cli.ExportEvents(ctx, &file, loom.ExportOptions{Signer: signer, FromSeq: third.Seq, ToSeq: third.Seq + 1}); err != nil || sum.Footer.Count != 2 {
		t.Errorf("a range of two: %+v %v", sum, err)
	}
	// nobody signing is refused
	if _, err := cli.ExportEvents(ctx, &file, loom.ExportOptions{}); err == nil {
		t.Error("an export with no signer was written")
	}
}
