package s3blob

// Pinned against the worked examples in AWS's "Signature Calculations
// for the Authorization Header" and "Authenticating Requests: Using
// Query Parameters" documentation — same bucket, key, credentials, and
// frozen clock, byte-for-byte expected signatures.

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

var (
	docSigner = signer{
		accessKey: "AKIAIOSFODNN7EXAMPLE",
		secretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		region:    "us-east-1",
	}
	docTime = time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC)
)

const emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

func TestSignMatchesAWSDocExample(t *testing.T) {
	req, _ := http.NewRequest(http.MethodGet, "https://examplebucket.s3.amazonaws.com/test.txt", nil)
	req.Header.Set("Range", "bytes=0-9")
	docSigner.sign(req, emptySHA256, docTime)

	auth := req.Header.Get("Authorization")
	wantSig := "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
	if !strings.HasSuffix(auth, "Signature="+wantSig) {
		t.Fatalf("authorization = %q, want signature %s", auth, wantSig)
	}
	if !strings.Contains(auth, "SignedHeaders=host;range;x-amz-content-sha256;x-amz-date") {
		t.Fatalf("signed headers wrong: %q", auth)
	}
}

func TestPresignMatchesAWSDocExample(t *testing.T) {
	u, _ := url.Parse("https://examplebucket.s3.amazonaws.com/test.txt")
	got := docSigner.presign(http.MethodGet, *u, "examplebucket.s3.amazonaws.com", 86400*time.Second, docTime)

	wantSig := "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
	parsed, err := url.Parse(got)
	if err != nil {
		t.Fatal(err)
	}
	if sig := parsed.Query().Get("X-Amz-Signature"); sig != wantSig {
		t.Fatalf("signature = %s, want %s", sig, wantSig)
	}
}
