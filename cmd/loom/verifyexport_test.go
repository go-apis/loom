package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestVerifyExportCommand: the command reads its key from the flag or a
// file, and refuses a file that is not an export, a key that is not one,
// and a call with no key. A real export verifying, and failing when
// altered, is proved against a store in internal/e2e
// (TestSignedExportVerifies).
func TestVerifyExportCommand(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	// a file that is not an export
	bad := filepath.Join(dir, "bad.jsonl")
	if err := os.WriteFile(bad, []byte(`{"event":{"seq":1},"chain":"00"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	key := base64.StdEncoding.EncodeToString(pub)
	if err := runVerifyExport([]string{"--key", key, bad}, &out); err == nil || !strings.Contains(err.Error(), "before the header") {
		t.Errorf("a file with no header: %v", err)
	}
	if err := runVerifyExport([]string{"--key", "not-a-key", bad}, &out); err == nil || !strings.Contains(err.Error(), "not a base64 Ed25519 public key") {
		t.Errorf("a bad key: %v", err)
	}
	if err := runVerifyExport([]string{bad}, &out); err == nil || !strings.Contains(err.Error(), "usage") {
		t.Errorf("no key: %v", err)
	}
	// the key may be a file
	keyFile := filepath.Join(dir, "audit.pub")
	if err := os.WriteFile(keyFile, []byte(key+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := runVerifyExport([]string{"--key", keyFile, bad}, &out); err == nil || !strings.Contains(err.Error(), "before the header") {
		t.Errorf("a key file: %v", err)
	}
}
