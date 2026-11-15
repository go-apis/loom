package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/go-apis/loom"
)

// runVerifyExport checks a signed export of a service's events
// (loom.Client.ExportEvents) against an Ed25519 public key, away from
// the service: every event's chain, the footer's count, and the
// signature. It prints what the export held, or fails naming the first
// event that does not check.
func runVerifyExport(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("verify-export", flag.ContinueOnError)
	key := fs.String("key", "", "the Ed25519 public key, base64, or a file holding it (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *key == "" || fs.NArg() != 1 {
		return fmt.Errorf("verify-export: usage: loom verify-export --key <public key> <export file>")
	}
	encoded := *key
	if raw, err := os.ReadFile(*key); err == nil {
		encoded = string(raw)
	}
	pub, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
	if err != nil || len(pub) != ed25519.PublicKeySize {
		return fmt.Errorf("verify-export: --key is not a base64 Ed25519 public key, nor a file holding one")
	}
	f, err := os.Open(fs.Arg(0))
	if err != nil {
		return err
	}
	defer f.Close()
	sum, err := loom.VerifyExport(f, loom.Ed25519Verifier(ed25519.PublicKey(pub)), nil)
	if err != nil {
		return err
	}
	scope := "every namespace"
	if sum.Header.Namespace != "" {
		scope = "namespace " + sum.Header.Namespace
	}
	fmt.Fprintf(out, "verified: %d events of %s (%s), ending at %d, exported %s, signed by %s\n",
		sum.Footer.Count, sum.Header.Service, scope, sum.Footer.LastSeq, sum.Header.ExportedAt.Format("2006-01-02T15:04:05Z"), sum.KeyID)
	return nil
}
