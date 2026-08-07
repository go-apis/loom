package s3blob

// AWS Signature Version 4, hand-rolled for the S3 dialect — the SDK is
// three hundred packages for what is one well-specified HMAC chain.
// Bodies are never buffered for hashing: requests declare
// UNSIGNED-PAYLOAD (fine over TLS, accepted by S3, R2, and MinIO), and
// presigned URLs always use it. Pinned against the worked examples in
// AWS's SigV4 documentation (see sigv4_test.go).

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	unsignedPayload = "UNSIGNED-PAYLOAD"
	algorithm       = "AWS4-HMAC-SHA256"
)

type signer struct {
	accessKey string
	secretKey string
	region    string
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// uriEncode is AWS's URI-encode: every byte percent-encoded except the
// unreserved set, with %XX uppercase — stricter than url.QueryEscape
// (spaces are %20, never +).
func uriEncode(s string, keepSlash bool) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '-', c == '.', c == '_', c == '~':
			b.WriteByte(c)
		case c == '/' && keepSlash:
			b.WriteByte(c)
		default:
			b.WriteString("%" + strings.ToUpper(hex.EncodeToString([]byte{c})))
		}
	}
	return b.String()
}

// canonicalQuery sorts and encodes query parameters AWS-style.
func canonicalQuery(q url.Values) string {
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		vs := append([]string(nil), q[k]...)
		sort.Strings(vs)
		for _, v := range vs {
			parts = append(parts, uriEncode(k, false)+"="+uriEncode(v, false))
		}
	}
	return strings.Join(parts, "&")
}

func (s signer) credentialScope(now time.Time) string {
	return now.UTC().Format("20060102") + "/" + s.region + "/s3/aws4_request"
}

func (s signer) signingKey(now time.Time) []byte {
	k := hmacSHA256([]byte("AWS4"+s.secretKey), []byte(now.UTC().Format("20060102")))
	k = hmacSHA256(k, []byte(s.region))
	k = hmacSHA256(k, []byte("s3"))
	return hmacSHA256(k, []byte("aws4_request"))
}

func (s signer) stringToSign(canonicalRequest string, now time.Time) string {
	return algorithm + "\n" + now.UTC().Format("20060102T150405Z") + "\n" +
		s.credentialScope(now) + "\n" + sha256Hex([]byte(canonicalRequest))
}

// sign authenticates a request with the Authorization header. The
// x-amz-date and x-amz-content-sha256 headers are set here; any header
// already on the request whose name starts with x-amz- is signed too,
// as are Host and the listed extras.
func (s signer) sign(req *http.Request, payloadHash string, now time.Time) {
	if payloadHash == "" {
		payloadHash = unsignedPayload
	}
	req.Header.Set("x-amz-date", now.UTC().Format("20060102T150405Z"))
	req.Header.Set("x-amz-content-sha256", payloadHash)

	headers := map[string]string{"host": req.Host}
	if req.Host == "" {
		headers["host"] = req.URL.Host
	}
	for name, vals := range req.Header {
		lower := strings.ToLower(name)
		if strings.HasPrefix(lower, "x-amz-") || lower == "content-type" || lower == "range" {
			headers[lower] = strings.TrimSpace(strings.Join(vals, ","))
		}
	}
	names := make([]string, 0, len(headers))
	for n := range headers {
		names = append(names, n)
	}
	sort.Strings(names)
	var canonHeaders strings.Builder
	for _, n := range names {
		canonHeaders.WriteString(n + ":" + headers[n] + "\n")
	}
	signedHeaders := strings.Join(names, ";")

	canonical := req.Method + "\n" +
		uriEncode(req.URL.Path, true) + "\n" +
		canonicalQuery(req.URL.Query()) + "\n" +
		canonHeaders.String() + "\n" +
		signedHeaders + "\n" +
		payloadHash

	sig := hex.EncodeToString(hmacSHA256(s.signingKey(now), []byte(s.stringToSign(canonical, now))))
	req.Header.Set("Authorization", algorithm+
		" Credential="+s.accessKey+"/"+s.credentialScope(now)+
		", SignedHeaders="+signedHeaders+
		", Signature="+sig)
}

// presign returns the URL with query-string authentication — the shape
// a browser can PUT to with no headers beyond Host. Only Host is
// signed, so the store must not demand content-type matching.
func (s signer) presign(method string, u url.URL, host string, ttl time.Duration, now time.Time) string {
	q := u.Query()
	q.Set("X-Amz-Algorithm", algorithm)
	q.Set("X-Amz-Credential", s.accessKey+"/"+s.credentialScope(now))
	q.Set("X-Amz-Date", now.UTC().Format("20060102T150405Z"))
	q.Set("X-Amz-Expires", strconv.Itoa(int(ttl.Seconds())))
	q.Set("X-Amz-SignedHeaders", "host")

	canonical := method + "\n" +
		uriEncode(u.Path, true) + "\n" +
		canonicalQuery(q) + "\n" +
		"host:" + host + "\n" + "\n" +
		"host\n" +
		unsignedPayload

	sig := hex.EncodeToString(hmacSHA256(s.signingKey(now), []byte(s.stringToSign(canonical, now))))
	q.Set("X-Amz-Signature", sig)
	u.RawQuery = strings.ReplaceAll(canonicalQuery(q), "+", "%20")
	return u.String()
}
