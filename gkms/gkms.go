// Package gkms implements loom.KeyWrapper on Cloud KMS. Per-stream data
// keys are envelope-wrapped by a KMS key that never leaves Google's
// infrastructure: the service holds only short-lived DEKs in memory, key
// use is IAM-gated per service account and audit-logged, and revoking a
// compromised service is an IAM change, not a key rotation.
//
// Wiring: create a keyring + symmetric key and grant the runtime service
// account roles/cloudkms.cryptoKeyEncrypterDecrypter, then
//
//	keys, err := gkms.New(ctx, "projects/P/locations/L/keyRings/R/cryptoKeys/K")
//	loom.New(loom.Config{ ..., Keys: keys })
//
// Loom caches unwrapped DEKs, so KMS sees roughly one Decrypt per stream
// per process lifetime. Migrating existing data from LocalKeys is
// `loom rewrap` (see the CLI) — DEKs are re-wrapped in place; sealed
// fields themselves are untouched.
package gkms

import (
	"context"
	"fmt"

	kms "cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/kms/apiv1/kmspb"
)

// Keys wraps loom data keys with a Cloud KMS crypto key.
type Keys struct {
	client *kms.KeyManagementClient
	name   string // projects/P/locations/L/keyRings/R/cryptoKeys/K
}

// New connects to Cloud KMS with ambient credentials (ADC).
func New(ctx context.Context, cryptoKey string) (*Keys, error) {
	if cryptoKey == "" {
		return nil, fmt.Errorf("gkms: crypto key resource name is required")
	}
	client, err := kms.NewKeyManagementClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("gkms: %w", err)
	}
	return &Keys{client: client, name: cryptoKey}, nil
}

func (k *Keys) Wrap(ctx context.Context, dek []byte) ([]byte, error) {
	resp, err := k.client.Encrypt(ctx, &kmspb.EncryptRequest{Name: k.name, Plaintext: dek})
	if err != nil {
		return nil, fmt.Errorf("gkms: wrap: %w", err)
	}
	return resp.Ciphertext, nil
}

func (k *Keys) Unwrap(ctx context.Context, wrapped []byte) ([]byte, error) {
	resp, err := k.client.Decrypt(ctx, &kmspb.DecryptRequest{Name: k.name, Ciphertext: wrapped})
	if err != nil {
		return nil, fmt.Errorf("gkms: unwrap: %w", err)
	}
	return resp.Plaintext, nil
}

func (k *Keys) Close() error { return k.client.Close() }
