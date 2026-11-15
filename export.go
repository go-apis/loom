package loom

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
)

// A signed export of a service's events, for audit. The log is the
// record of what the service did; an auditor needs a copy that can be
// checked without the service: that nothing in it was changed, dropped,
// added or reordered, and that it came from who it says.
//
// An export is lines of JSON:
//
//	{"header": {...}}                         what was exported, and when
//	{"event": {...}, "chain": "<hex>"}        one per event, in log order
//	{"footer": {...}, "signature": "<b64>"}   the count, the last chain, signed
//
// Each line's chain is SHA-256 over the chain before it and the line's
// own bytes, starting from the header, so a change to any event, or to
// their order, changes every chain after it. The footer names the last
// chain and how many events there were, and the signature is over the
// footer: a truncated or extended file does not verify.
//
// Events are exported as stored. A @pii field is exported sealed, as it
// rests: an export carries no more than the table does.

// Signer signs an export's footer. A product gives one over the key its
// identity service issued it; Ed25519Signer is one over a key in hand.
type Signer interface {
	// KeyID names the key, for whoever verifies to find its public half.
	KeyID() string
	// Algorithm names how it signs: "ed25519".
	Algorithm() string
	Sign(ctx context.Context, digest []byte) ([]byte, error)
}

type ed25519Signer struct {
	id  string
	key ed25519.PrivateKey
}

// Ed25519Signer signs with an Ed25519 private key.
func Ed25519Signer(keyID string, key ed25519.PrivateKey) Signer {
	return ed25519Signer{id: keyID, key: key}
}

func (s ed25519Signer) KeyID() string     { return s.id }
func (s ed25519Signer) Algorithm() string { return "ed25519" }
func (s ed25519Signer) Sign(ctx context.Context, digest []byte) ([]byte, error) {
	return ed25519.Sign(s.key, digest), nil
}

// VerifyFunc checks a signature over digest by the key named.
type VerifyFunc func(keyID, algorithm string, digest, signature []byte) error

// Ed25519Verifier verifies against one Ed25519 public key, whatever key
// id the export names.
func Ed25519Verifier(key ed25519.PublicKey) VerifyFunc {
	return func(keyID, algorithm string, digest, signature []byte) error {
		if algorithm != "ed25519" {
			return fmt.Errorf("signed with %s, and this key is ed25519", algorithm)
		}
		if !ed25519.Verify(key, digest, signature) {
			return errors.New("the signature is not this key's over this export")
		}
		return nil
	}
}

// ExportOptions says which events to export.
type ExportOptions struct {
	// Namespace exports one namespace's events; empty exports every
	// namespace's.
	Namespace string
	// FromSeq and ToSeq bound the export by global sequence, both
	// inclusive; zero leaves that end open.
	FromSeq, ToSeq int64
	// Signer signs the footer. Required: an export nobody signed proves
	// nothing.
	Signer Signer
}

// ExportHeader opens an export.
type ExportHeader struct {
	Version    int       `json:"loom_export"`
	Service    string    `json:"service"`
	Namespace  string    `json:"namespace,omitempty"`
	FromSeq    int64     `json:"from_seq,omitempty"`
	ToSeq      int64     `json:"to_seq,omitempty"`
	ExportedAt time.Time `json:"exported_at"`
}

// ExportedEvent is one event as the log holds it.
type ExportedEvent struct {
	Seq           int64           `json:"seq"`
	Namespace     string          `json:"namespace"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   uuid.UUID       `json:"aggregate_id"`
	Version       int             `json:"version"`
	Type          string          `json:"type"`
	SchemaVersion int             `json:"schema_version"`
	At            time.Time       `json:"at"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	CausationID   string          `json:"causation_id,omitempty"`
	Actor         string          `json:"actor,omitempty"`
	Data          json.RawMessage `json:"data"`
}

// ExportFooter closes an export: what the signature is over.
type ExportFooter struct {
	Count   int64  `json:"count"`
	LastSeq int64  `json:"last_seq"`
	Chain   string `json:"chain"`
}

// ExportSummary is what an export held.
type ExportSummary struct {
	Header ExportHeader
	Footer ExportFooter
	KeyID  string
}

const exportBatch = 1000

func chainOf(prev []byte, line []byte) []byte {
	h := sha256.New()
	h.Write(prev)
	h.Write(line)
	return h.Sum(nil)
}

func footerDigest(raw []byte) []byte {
	sum := sha256.Sum256(append([]byte("loom-export-v1\n"), raw...))
	return sum[:]
}

// ExportEvents writes a signed export of the service's events to w, in
// log order, and answers what it wrote.
func (c *Client) ExportEvents(ctx context.Context, w io.Writer, opts ExportOptions) (*ExportSummary, error) {
	if opts.Signer == nil {
		return nil, errors.New("loom: an export needs a Signer: an export nobody signed proves nothing")
	}
	out := bufio.NewWriter(w)
	header := ExportHeader{Version: 1, Service: c.reg.Service, Namespace: opts.Namespace, FromSeq: opts.FromSeq, ToSeq: opts.ToSeq, ExportedAt: nowUTC()}
	raw, err := json.Marshal(header)
	if err != nil {
		return nil, err
	}
	if _, err := fmt.Fprintf(out, "{\"header\":%s}\n", raw); err != nil {
		return nil, err
	}
	chain := chainOf(nil, raw)
	footer := ExportFooter{}
	after := opts.FromSeq - 1
	if after < 0 {
		after = 0
	}
	for {
		rows, err := c.db.Query(ctx, `
			SELECT global_seq, namespace, aggregate_type, aggregate_id, version, type, schema_version, at,
			       correlation_id, causation_id, actor, data::text
			FROM loom_events
			WHERE service=$1 AND global_seq > $2 AND ($3 = '' OR namespace = $3) AND ($4 = 0 OR global_seq <= $4)
			ORDER BY global_seq
			LIMIT $5`,
			c.reg.Service, after, opts.Namespace, opts.ToSeq, exportBatch)
		if err != nil {
			return nil, err
		}
		n := 0
		for rows.Next() {
			var (
				e    ExportedEvent
				data string
			)
			if err := rows.Scan(&e.Seq, &e.Namespace, &e.AggregateType, &e.AggregateID, &e.Version, &e.Type, &e.SchemaVersion, &e.At,
				&e.CorrelationID, &e.CausationID, &e.Actor, &data); err != nil {
				rows.Close()
				return nil, err
			}
			e.At, e.Data = e.At.UTC(), json.RawMessage(data)
			raw, err := json.Marshal(e)
			if err != nil {
				rows.Close()
				return nil, err
			}
			chain = chainOf(chain, raw)
			if _, err := fmt.Fprintf(out, "{\"event\":%s,\"chain\":%q}\n", raw, hex.EncodeToString(chain)); err != nil {
				rows.Close()
				return nil, err
			}
			footer.Count++
			footer.LastSeq, after = e.Seq, e.Seq
			n++
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
		if n < exportBatch {
			break
		}
	}
	footer.Chain = hex.EncodeToString(chain)
	raw, err = json.Marshal(footer)
	if err != nil {
		return nil, err
	}
	sig, err := opts.Signer.Sign(ctx, footerDigest(raw))
	if err != nil {
		return nil, fmt.Errorf("loom: signing the export: %w", err)
	}
	if _, err := fmt.Fprintf(out, "{\"footer\":%s,\"signature\":%q,\"key_id\":%q,\"algorithm\":%q}\n",
		raw, base64.StdEncoding.EncodeToString(sig), opts.Signer.KeyID(), opts.Signer.Algorithm()); err != nil {
		return nil, err
	}
	if err := out.Flush(); err != nil {
		return nil, err
	}
	return &ExportSummary{Header: header, Footer: footer, KeyID: opts.Signer.KeyID()}, nil
}

// exportLine is any line of an export.
type exportLine struct {
	Header    json.RawMessage `json:"header"`
	Event     json.RawMessage `json:"event"`
	Chain     string          `json:"chain"`
	Footer    json.RawMessage `json:"footer"`
	Signature string          `json:"signature"`
	KeyID     string          `json:"key_id"`
	Algorithm string          `json:"algorithm"`
}

// VerifyExport reads an export and checks it: every line's chain, the
// footer's count and last chain, and the signature over the footer. It
// answers what the export held, or an error naming the first event that
// does not check. each, when not nil, is given every event that checks,
// in order.
func VerifyExport(r io.Reader, verify VerifyFunc, each func(ExportedEvent) error) (*ExportSummary, error) {
	if verify == nil {
		return nil, errors.New("loom: verifying an export needs a key to verify against")
	}
	lines := bufio.NewReader(r)
	var (
		sum    ExportSummary
		chain  []byte
		count  int64
		last   int64
		opened bool
		closed bool
		n      int
	)
	for {
		text, err := lines.ReadBytes('\n')
		if len(bytes.TrimSpace(text)) > 0 {
			n++
			if closed {
				return nil, fmt.Errorf("loom: export line %d: something follows the footer", n)
			}
			var line exportLine
			if jerr := json.Unmarshal(text, &line); jerr != nil {
				return nil, fmt.Errorf("loom: export line %d is not JSON: %w", n, jerr)
			}
			switch {
			case line.Header != nil:
				if opened {
					return nil, fmt.Errorf("loom: export line %d: a second header", n)
				}
				if jerr := json.Unmarshal(line.Header, &sum.Header); jerr != nil || sum.Header.Version != 1 {
					return nil, fmt.Errorf("loom: export line %d: not a version 1 header", n)
				}
				opened, chain = true, chainOf(nil, line.Header)
			case line.Event != nil:
				if !opened {
					return nil, fmt.Errorf("loom: export line %d: an event before the header", n)
				}
				var e ExportedEvent
				if jerr := json.Unmarshal(line.Event, &e); jerr != nil {
					return nil, fmt.Errorf("loom: export line %d: not an event: %w", n, jerr)
				}
				chain = chainOf(chain, line.Event)
				if hex.EncodeToString(chain) != line.Chain {
					return nil, fmt.Errorf("loom: export does not verify at event %d (%s, line %d): it or an event before it was changed, dropped, added or moved", e.Seq, e.Type, n)
				}
				if e.Seq <= last {
					return nil, fmt.Errorf("loom: export line %d: event %d is out of order", n, e.Seq)
				}
				count, last = count+1, e.Seq
				if each != nil {
					if cerr := each(e); cerr != nil {
						return nil, cerr
					}
				}
			case line.Footer != nil:
				if !opened {
					return nil, fmt.Errorf("loom: export line %d: a footer before the header", n)
				}
				if jerr := json.Unmarshal(line.Footer, &sum.Footer); jerr != nil {
					return nil, fmt.Errorf("loom: export line %d: not a footer: %w", n, jerr)
				}
				if sum.Footer.Count != count || sum.Footer.Chain != hex.EncodeToString(chain) || sum.Footer.LastSeq != last {
					return nil, fmt.Errorf("loom: export does not verify: its footer counts %d events ending at %d, and it holds %d ending at %d — events were dropped or added at the end", sum.Footer.Count, sum.Footer.LastSeq, count, last)
				}
				sig, derr := base64.StdEncoding.DecodeString(line.Signature)
				if derr != nil {
					return nil, fmt.Errorf("loom: export line %d: the signature is not base64", n)
				}
				if verr := verify(line.KeyID, line.Algorithm, footerDigest(line.Footer), sig); verr != nil {
					return nil, fmt.Errorf("loom: export does not verify: %w", verr)
				}
				sum.KeyID, closed = line.KeyID, true
			default:
				return nil, fmt.Errorf("loom: export line %d is not a header, an event or a footer", n)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	if !closed {
		return nil, errors.New("loom: export does not verify: it has no footer, so nothing says where it ends or who signed it")
	}
	return &sum, nil
}
