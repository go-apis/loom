// Package s3blob implements loom.BlobStore on any S3-compatible store —
// Cloudflare R2, AWS S3, MinIO. Requests are path-style against the
// configured endpoint and hand-signed with SigV4 (no SDK).
//
// Uploads speak the s3-multipart dialect: S3 has no single
// self-authenticating session URL, so CreateUpload opens a multipart
// upload and hands the client a service-relative session token; the
// client asks the service for a presigned URL per part (POST
// {session}/parts?n=) and completes through the service (POST
// {session}/complete with the collected ETags), which verifies the
// object via Stat and doubles as the finalize signal — never client
// say-so. Session tokens are HMAC-bound to the key and upload id, so a
// tampered token dies at verification.
//
// Wiring for Cloudflare R2:
//
//	store, _ := s3blob.New(s3blob.Config{
//	  Bucket:   "uploads",
//	  Endpoint: "https://<account>.r2.cloudflarestorage.com",
//	  Region:   "auto",
//	  AccessKeyID: ..., SecretAccessKey: ...,
//	  PublicBaseURL: "https://files.example.com", // r2.dev or custom domain
//	})
//
// plus a CORS rule on the bucket allowing PUT from the UI origin.
package s3blob

import (
	"bytes"
	"context"
	"crypto/hmac"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-apis/loom"
)

type Config struct {
	Bucket string
	// Endpoint is the S3 API base: https://<account>.r2.cloudflarestorage.com,
	// https://s3.<region>.amazonaws.com, or a MinIO address.
	Endpoint string
	// Region signs the credential scope. R2 uses "auto" (the default);
	// AWS wants the bucket's real region.
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	// PublicBaseURL is the browser-facing base when the bucket (or a CDN
	// or custom domain over it) serves objects publicly; empty means no
	// public surface and PublicURL returns "".
	PublicBaseURL string
	// PartTTL bounds how long a presigned part URL stays valid. Default 1h.
	PartTTL    time.Duration
	HTTPClient *http.Client
	Logger     *slog.Logger

	// now overrides the clock in tests.
	now func() time.Time
}

type Store struct {
	bucket   string
	endpoint string
	public   string
	partTTL  time.Duration
	sig      signer
	http     *http.Client
	log      *slog.Logger
	now      func() time.Time
}

func New(cfg Config) (*Store, error) {
	if cfg.Bucket == "" || cfg.Endpoint == "" {
		return nil, fmt.Errorf("s3blob: Bucket and Endpoint are required")
	}
	if cfg.AccessKeyID == "" || cfg.SecretAccessKey == "" {
		return nil, fmt.Errorf("s3blob: AccessKeyID and SecretAccessKey are required")
	}
	if cfg.Region == "" {
		cfg.Region = "auto"
	}
	if cfg.PartTTL <= 0 {
		cfg.PartTTL = time.Hour
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.now == nil {
		cfg.now = time.Now
	}
	return &Store{
		bucket:   cfg.Bucket,
		endpoint: strings.TrimSuffix(cfg.Endpoint, "/"),
		public:   strings.TrimSuffix(cfg.PublicBaseURL, "/"),
		partTTL:  cfg.PartTTL,
		sig:      signer{accessKey: cfg.AccessKeyID, secretKey: cfg.SecretAccessKey, region: cfg.Region},
		http:     cfg.HTTPClient,
		log:      cfg.Logger,
		now:      cfg.now,
	}, nil
}

// PublicURL is the object's browser URL when a public base is
// configured; "" otherwise.
func (s *Store) PublicURL(key string) string {
	if s.public == "" {
		return ""
	}
	return s.public + "/" + key
}

func (s *Store) objectURL(key string, q url.Values) *url.URL {
	u, _ := url.Parse(s.endpoint)
	u.Path = "/" + s.bucket + "/" + key
	if q != nil {
		u.RawQuery = canonicalQuery(q)
	}
	return u
}

func (s *Store) do(ctx context.Context, method string, u *url.URL, header http.Header, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, err
	}
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	s.sig.sign(req, unsignedPayload, s.now())
	return s.http.Do(req)
}

func apiErr(op string, resp *http.Response) error {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	return fmt.Errorf("s3blob: %s: %s: %s", op, resp.Status, strings.TrimSpace(string(b)))
}

func drain(resp *http.Response) {
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

func (s *Store) Put(ctx context.Context, init loom.UploadInit, body io.Reader) error {
	h := http.Header{}
	if init.ContentType != "" {
		h.Set("Content-Type", init.ContentType)
	}
	if init.CacheControl != "" {
		h.Set("Cache-Control", init.CacheControl)
	}
	for k, v := range init.Metadata {
		h.Set("x-amz-meta-"+k, v)
	}
	if init.Name != "" {
		h.Set("x-amz-meta-loom_name", init.Name)
	}
	// S3 (R2 especially) refuses object PUTs without Content-Length, and
	// net/http only infers it for bytes/strings readers — an *os.File
	// body would 411. Put is documented memory-sized, so buffer anything
	// whose length the transport can't see.
	switch body.(type) {
	case *bytes.Reader, *bytes.Buffer, *strings.Reader:
	default:
		raw, err := io.ReadAll(body)
		if err != nil {
			return fmt.Errorf("s3blob: put %s: %w", init.Key, err)
		}
		body = bytes.NewReader(raw)
	}
	resp, err := s.do(ctx, http.MethodPut, s.objectURL(init.Key, nil), h, body)
	if err != nil {
		return err
	}
	defer drain(resp)
	if resp.StatusCode != http.StatusOK {
		return apiErr("put "+init.Key, resp)
	}
	return nil
}

func (s *Store) Stat(ctx context.Context, key string) (*loom.BlobInfo, error) {
	resp, err := s.do(ctx, http.MethodHead, s.objectURL(key, nil), nil, nil)
	if err != nil {
		return nil, err
	}
	defer drain(resp)
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, apiErr("stat "+key, resp)
	}
	info := &loom.BlobInfo{
		Key:         key,
		ContentType: resp.Header.Get("Content-Type"),
		Metadata:    map[string]string{},
	}
	info.Size, _ = strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	for name := range resp.Header {
		if meta, ok := strings.CutPrefix(strings.ToLower(name), "x-amz-meta-"); ok {
			info.Metadata[meta] = resp.Header.Get(name)
		}
	}
	info.Name = info.Metadata["loom_name"]
	return info, nil
}

func (s *Store) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	resp, err := s.do(ctx, http.MethodGet, s.objectURL(key, nil), nil, nil)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer drain(resp)
		return nil, apiErr("open "+key, resp)
	}
	return resp.Body, nil
}

func (s *Store) Delete(ctx context.Context, key string) error {
	resp, err := s.do(ctx, http.MethodDelete, s.objectURL(key, nil), nil, nil)
	if err != nil {
		return err
	}
	defer drain(resp)
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return apiErr("delete "+key, resp)
	}
	return nil
}

func (s *Store) DeletePrefix(ctx context.Context, prefix string) error {
	token := ""
	for {
		q := url.Values{"list-type": {"2"}, "prefix": {prefix}}
		if token != "" {
			q.Set("continuation-token", token)
		}
		u, _ := url.Parse(s.endpoint)
		u.Path = "/" + s.bucket
		u.RawQuery = canonicalQuery(q)
		resp, err := s.do(ctx, http.MethodGet, u, nil, nil)
		if err != nil {
			return err
		}
		var list struct {
			Contents              []struct{ Key string } `xml:"Contents"`
			IsTruncated           bool                   `xml:"IsTruncated"`
			NextContinuationToken string                 `xml:"NextContinuationToken"`
		}
		if resp.StatusCode != http.StatusOK {
			defer drain(resp)
			return apiErr("list "+prefix, resp)
		}
		err = xml.NewDecoder(resp.Body).Decode(&list)
		drain(resp)
		if err != nil {
			return fmt.Errorf("s3blob: list %s: %w", prefix, err)
		}
		for _, obj := range list.Contents {
			if err := s.Delete(ctx, obj.Key); err != nil {
				return err
			}
		}
		if !list.IsTruncated || list.NextContinuationToken == "" {
			return nil
		}
		token = list.NextContinuationToken
	}
}

// --- the s3-multipart session ---

type sessionClaims struct {
	Key      string `json:"k"`
	UploadID string `json:"u"`
	Exp      int64  `json:"e"`
}

// sessionKey derives the token-signing key from the store's secret, so
// deployments configure nothing extra and a forged token still needs
// the S3 credential.
func (s *Store) sessionKey() []byte {
	return hmacSHA256([]byte(s.SecretKeyForSessions()), []byte("loom-s3blob-session"))
}

// SecretKeyForSessions is exported for tests only.
func (s *Store) SecretKeyForSessions() string { return s.sig.secretKey }

func (s *Store) mintSession(c sessionClaims) string {
	payload, _ := json.Marshal(c)
	body := base64.RawURLEncoding.EncodeToString(payload)
	mac := hex.EncodeToString(hmacSHA256(s.sessionKey(), []byte(body)))
	return body + "." + mac
}

func (s *Store) verifySession(token string) (*sessionClaims, error) {
	body, mac, ok := strings.Cut(token, ".")
	if !ok {
		return nil, fmt.Errorf("s3blob: malformed session")
	}
	want := hex.EncodeToString(hmacSHA256(s.sessionKey(), []byte(body)))
	if !hmac.Equal([]byte(mac), []byte(want)) {
		return nil, fmt.Errorf("s3blob: session signature mismatch")
	}
	raw, err := base64.RawURLEncoding.DecodeString(body)
	if err != nil {
		return nil, fmt.Errorf("s3blob: malformed session")
	}
	var c sessionClaims
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("s3blob: malformed session")
	}
	if time.Unix(c.Exp, 0).Before(s.now()) {
		return nil, fmt.Errorf("s3blob: session expired")
	}
	return &c, nil
}

func (s *Store) CreateUpload(ctx context.Context, init loom.UploadInit) (*loom.UploadSession, error) {
	h := http.Header{}
	if init.ContentType != "" {
		h.Set("Content-Type", init.ContentType)
	}
	for k, v := range init.Metadata {
		h.Set("x-amz-meta-"+k, v)
	}
	if init.Name != "" {
		h.Set("x-amz-meta-loom_name", init.Name)
	}
	resp, err := s.do(ctx, http.MethodPost, s.objectURL(init.Key, url.Values{"uploads": {""}}), h, nil)
	if err != nil {
		return nil, err
	}
	defer drain(resp)
	if resp.StatusCode != http.StatusOK {
		return nil, apiErr("create upload "+init.Key, resp)
	}
	var out struct {
		UploadID string `xml:"UploadId"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&out); err != nil || out.UploadID == "" {
		return nil, fmt.Errorf("s3blob: create upload %s: no upload id (%v)", init.Key, err)
	}
	token := s.mintSession(sessionClaims{
		Key:      init.Key,
		UploadID: out.UploadID,
		// sessions outlive single parts: a day covers a slow multi-GB
		// upload; parts re-presign per request anyway
		Exp: s.now().Add(24 * time.Hour).Unix(),
	})
	return &loom.UploadSession{URL: "uploads/" + token, Protocol: loom.ProtocolS3Multipart}, nil
}

// SignPart hands the client part n's presigned PUT URL.
func (s *Store) SignPart(ctx context.Context, session string, part int) (string, error) {
	if part < 1 || part > 10000 {
		return "", fmt.Errorf("s3blob: part number out of range")
	}
	c, err := s.verifySession(session)
	if err != nil {
		return "", err
	}
	u := s.objectURL(c.Key, url.Values{
		"partNumber": {strconv.Itoa(part)},
		"uploadId":   {c.UploadID},
	})
	return s.sig.presign(http.MethodPut, *u, u.Host, s.partTTL, s.now()), nil
}

// CompleteUpload assembles the parts. S3 can answer 200 with an error
// document on completion, so the body is checked, not just the status.
func (s *Store) CompleteUpload(ctx context.Context, session string, parts []loom.CompletedPart) (string, error) {
	c, err := s.verifySession(session)
	if err != nil {
		return "", err
	}
	if len(parts) == 0 {
		return "", fmt.Errorf("s3blob: no parts to complete")
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].Number < parts[j].Number })
	var b bytes.Buffer
	b.WriteString("<CompleteMultipartUpload>")
	for _, p := range parts {
		fmt.Fprintf(&b, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", p.Number, escapeXML(p.ETag))
	}
	b.WriteString("</CompleteMultipartUpload>")

	resp, err := s.do(ctx, http.MethodPost, s.objectURL(c.Key, url.Values{"uploadId": {c.UploadID}}),
		http.Header{"Content-Type": {"application/xml"}}, &b)
	if err != nil {
		return "", err
	}
	defer drain(resp)
	if resp.StatusCode != http.StatusOK {
		return "", apiErr("complete upload "+c.Key, resp)
	}
	var out struct {
		XMLName xml.Name
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("s3blob: complete upload %s: %w", c.Key, err)
	}
	if out.XMLName.Local == "Error" {
		return "", fmt.Errorf("s3blob: complete upload %s: %s: %s", c.Key, out.Code, out.Message)
	}
	return c.Key, nil
}

func escapeXML(s string) string {
	var b bytes.Buffer
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}
