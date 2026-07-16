package loom

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// @pii fields are encrypted at rest under a per-stream data key
// (AES-256-GCM), so the immutable log stops being a liability: deleting the
// key — Shred — makes every copy of that stream's PII (events, snapshots,
// read models, records, parked envelopes) permanently unreadable, which is
// how you erase a person from an append-only store. Data keys are wrapped
// by a KeyWrapper (a local master key today, a KMS tomorrow) and stored in
// loom_keys.
//
// What decrypts and what doesn't: folds, typed reads (Load, Entity, Record,
// and their GET endpoints) see plaintext; raw list queries and the log
// browser return ciphertext as stored (filters can't match PII fields).
// After a shred, decrypting yields the field's zero value — replays and
// rebuilds keep working, redacted.

const piiPrefix = "pii:v1:"

// KeyWrapper protects per-stream data keys at rest. Implementations wrap
// with a KMS or a local master key.
type KeyWrapper interface {
	Wrap(ctx context.Context, dek []byte) ([]byte, error)
	Unwrap(ctx context.Context, wrapped []byte) ([]byte, error)
}

// LocalKeys wraps data keys with a local AES-256-GCM master key (32 bytes).
// Ship the master key from your secret manager; rotate by re-wrapping
// loom_keys rows.
func LocalKeys(masterKey []byte) (KeyWrapper, error) {
	if len(masterKey) != 32 {
		return nil, fmt.Errorf("loom: LocalKeys wants a 32-byte master key, got %d", len(masterKey))
	}
	return &localKeys{key: masterKey}, nil
}

type localKeys struct{ key []byte }

func (l *localKeys) Wrap(ctx context.Context, dek []byte) ([]byte, error) {
	return seal(l.key, dek)
}

func (l *localKeys) Unwrap(ctx context.Context, wrapped []byte) ([]byte, error) {
	return open(l.key, wrapped)
}

func seal(key, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

func open(key, sealed []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(sealed) < gcm.NonceSize() {
		return nil, fmt.Errorf("ciphertext too short")
	}
	return gcm.Open(nil, sealed[:gcm.NonceSize()], sealed[gcm.NonceSize():], nil)
}

// dek returns the stream's data key, minting one when create is set. A nil
// key with no error means the stream was shredded (or never had PII).
func (c *Client) dek(ctx context.Context, namespace string, id uuid.UUID, create bool) ([]byte, error) {
	cacheKey := namespace + "/" + id.String()
	c.dekMu.Lock()
	if k, ok := c.deks[cacheKey]; ok {
		c.dekMu.Unlock()
		return k, nil
	}
	c.dekMu.Unlock()

	var wrapped []byte
	err := c.db.QueryRow(ctx, `
		SELECT wrapped_dek FROM loom_keys WHERE service=$1 AND namespace=$2 AND stream_id=$3`,
		c.reg.Service, namespace, id).Scan(&wrapped)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		if !create {
			return nil, nil
		}
		dek := make([]byte, 32)
		if _, err := rand.Read(dek); err != nil {
			return nil, err
		}
		w, err := c.keys.Wrap(ctx, dek)
		if err != nil {
			return nil, err
		}
		// a losing racer reads the winner's key back
		if _, err := c.db.Exec(ctx, `
			INSERT INTO loom_keys (service, namespace, stream_id, wrapped_dek)
			VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`,
			c.reg.Service, namespace, id, w); err != nil {
			return nil, err
		}
		if err := c.db.QueryRow(ctx, `
			SELECT wrapped_dek FROM loom_keys WHERE service=$1 AND namespace=$2 AND stream_id=$3`,
			c.reg.Service, namespace, id).Scan(&wrapped); err != nil {
			return nil, err
		}
	case err != nil:
		return nil, err
	}

	dek, err := c.keys.Unwrap(ctx, wrapped)
	if err != nil {
		return nil, fmt.Errorf("loom: unwrap data key for %s/%s: %w", namespace, id, err)
	}
	c.dekMu.Lock()
	if len(c.deks) > 16384 { // crude cap; keys re-fetch cheaply
		c.deks = map[string][]byte{}
	}
	c.deks[cacheKey] = dek
	c.dekMu.Unlock()
	return dek, nil
}

func (c *Client) dropDEK(namespace, id string) {
	c.dekMu.Lock()
	delete(c.deks, namespace+"/"+id)
	c.dekMu.Unlock()
}

// encryptFields seals the named top-level fields of a JSON document under
// the stream's data key. Already-sealed and null fields pass through.
func (c *Client) encryptFields(ctx context.Context, namespace string, id uuid.UUID, data []byte, fields []string) ([]byte, error) {
	if len(fields) == 0 {
		return data, nil
	}
	if c.keys == nil {
		return nil, fmt.Errorf("loom: schema declares @pii but Config.Keys is not set")
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	var dek []byte
	touched := false
	for _, f := range fields {
		raw, ok := doc[f]
		if !ok || string(raw) == "null" || isSealed(raw) {
			continue
		}
		if dek == nil {
			var err error
			if dek, err = c.dek(ctx, namespace, id, true); err != nil {
				return nil, err
			}
		}
		ct, err := seal(dek, raw)
		if err != nil {
			return nil, err
		}
		enc, err := json.Marshal(piiPrefix + base64.StdEncoding.EncodeToString(ct))
		if err != nil {
			return nil, err
		}
		doc[f] = enc
		touched = true
	}
	if !touched {
		return data, nil
	}
	return json.Marshal(doc)
}

// decryptFields opens the named fields. A shredded stream (no key) redacts:
// the fields are dropped, decoding to zero values, so folds and rebuilds
// keep working without the PII.
func (c *Client) decryptFields(ctx context.Context, namespace string, id uuid.UUID, data []byte, fields []string) ([]byte, error) {
	if len(fields) == 0 {
		return data, nil
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	sealed := false
	for _, f := range fields {
		if raw, ok := doc[f]; ok && isSealed(raw) {
			sealed = true
			break
		}
	}
	if !sealed {
		return data, nil
	}
	dek, err := c.dek(ctx, namespace, id, false)
	if err != nil {
		return nil, err
	}
	for _, f := range fields {
		raw, ok := doc[f]
		if !ok || !isSealed(raw) {
			continue
		}
		if dek == nil { // shredded: redact
			delete(doc, f)
			continue
		}
		var s string
		_ = json.Unmarshal(raw, &s)
		ct, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(s, piiPrefix))
		if err != nil {
			return nil, fmt.Errorf("loom: field %s: bad pii ciphertext: %w", f, err)
		}
		plain, err := open(dek, ct)
		if err != nil {
			return nil, fmt.Errorf("loom: field %s: %w", f, err)
		}
		doc[f] = plain
	}
	return json.Marshal(doc)
}

func isSealed(raw json.RawMessage) bool {
	var s string
	return json.Unmarshal(raw, &s) == nil && strings.HasPrefix(s, piiPrefix)
}

// decryptEventData opens an event payload's @pii fields before decoding.
func (c *Client) decryptEventData(ctx context.Context, namespace string, id uuid.UUID, eventType string, data []byte) ([]byte, error) {
	def := c.reg.eventDef(eventType)
	if def == nil || len(def.PII) == 0 {
		return data, nil
	}
	return c.decryptFields(ctx, namespace, id, data, def.PII)
}

// Shred deletes a stream's data key: every @pii field ever written for it —
// events, snapshots, read models, records, parked envelopes — becomes
// permanently unreadable and reads as the zero value from here on. This is
// the erasure lever for an append-only store; there is no unshred.
func (c *Client) Shred(ctx context.Context, namespace string, id uuid.UUID) error {
	if _, err := c.db.Exec(ctx, `
		DELETE FROM loom_keys WHERE service=$1 AND namespace=$2 AND stream_id=$3`,
		c.reg.Service, namespace, id); err != nil {
		return err
	}
	c.dropDEK(namespace, id.String())
	// other instances drop their cached key via LISTEN/NOTIFY
	_, _ = c.db.Exec(ctx, `SELECT pg_notify($1, $2)`, "loom_"+c.reg.Service, "shred:"+namespace+":"+id.String())
	return nil
}

// piiForCommand names a command's sealed payload fields.
func (r *Registry) piiForCommand(name string) []string {
	if _, def := r.aggregateForCommand(name); def != nil {
		return def.PII
	}
	if _, def := r.recordForCommand(name); def != nil {
		return def.PII
	}
	return nil
}

// sealCommand encrypts a command's @pii fields under its target stream's
// key — commands rest in timers and batch items.
func (c *Client) sealCommand(ctx context.Context, cmd Command, raw []byte) ([]byte, error) {
	fields := c.reg.piiForCommand(cmd.LoomCommand())
	if len(fields) == 0 {
		return raw, nil
	}
	ns, id := cmd.CommandTarget()
	return c.encryptFields(ctx, ns, id, raw, fields)
}

// openCommand decrypts a stored command's @pii fields; the target rides
// plaintext inside the JSON.
func (c *Client) openCommand(ctx context.Context, cmdType string, raw []byte) ([]byte, error) {
	fields := c.reg.piiForCommand(cmdType)
	if len(fields) == 0 {
		return raw, nil
	}
	var target struct {
		Namespace   string    `json:"namespace"`
		AggregateID uuid.UUID `json:"aggregate_id"`
	}
	if err := json.Unmarshal(raw, &target); err != nil {
		return nil, err
	}
	return c.decryptFields(ctx, target.Namespace, target.AggregateID, raw, fields)
}

func (r *Registry) hasPII() bool {
	for _, e := range r.Events {
		if len(e.PII) > 0 {
			return true
		}
	}
	for _, a := range r.Aggregates {
		for _, def := range a.Commands {
			if len(def.PII) > 0 {
				return true
			}
		}
	}
	for _, rec := range r.Records {
		for _, def := range rec.Commands {
			if len(def.PII) > 0 {
				return true
			}
		}
	}
	for _, a := range r.Aggregates {
		if len(a.StatePII) > 0 {
			return true
		}
	}
	for _, rec := range r.Records {
		if len(rec.StatePII) > 0 {
			return true
		}
	}
	for _, p := range r.Projections {
		if len(p.PII) > 0 {
			return true
		}
	}
	return false
}
