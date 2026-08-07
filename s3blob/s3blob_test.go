package s3blob

// A fake S3 speaking just enough of the XML API to drive the store end
// to end: object CRUD + prefix listing + the multipart initiate / part
// PUT / complete flow. It asserts every request carries SigV4 (header
// or query auth) — not the crypto, which sigv4_test pins against AWS's
// vectors, but that nothing goes out unsigned.

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-apis/loom"
)

type fakeS3 struct {
	mu       sync.Mutex
	objects  map[string][]byte
	meta     map[string]http.Header
	parts    map[string]map[string][]byte // uploadId -> partNumber -> bytes
	partKeys map[string]string            // uploadId -> key
	t        *testing.T
}

func newFakeS3(t *testing.T) *fakeS3 {
	return &fakeS3{objects: map[string][]byte{}, meta: map[string]http.Header{},
		parts: map[string]map[string][]byte{}, partKeys: map[string]string{}, t: t}
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") == "" && r.URL.Query().Get("X-Amz-Signature") == "" {
		f.t.Errorf("unsigned request: %s %s", r.Method, r.URL)
		http.Error(w, "unsigned", http.StatusForbidden)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	key := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/test-bucket"), "/")
	q := r.URL.Query()
	switch {
	case r.Method == http.MethodPost && q.Has("uploads"):
		id := fmt.Sprintf("up-%d", len(f.partKeys)+1)
		f.partKeys[id] = key
		f.parts[id] = map[string][]byte{}
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprintf(w, `<InitiateMultipartUploadResult><UploadId>%s</UploadId></InitiateMultipartUploadResult>`, id)
	case r.Method == http.MethodPut && q.Get("partNumber") != "":
		id := q.Get("uploadId")
		if f.parts[id] == nil {
			http.Error(w, "no such upload", http.StatusNotFound)
			return
		}
		b, _ := io.ReadAll(r.Body)
		f.parts[id][q.Get("partNumber")] = b
		w.Header().Set("ETag", `"etag-`+q.Get("partNumber")+`"`)
	case r.Method == http.MethodPost && q.Get("uploadId") != "":
		id := q.Get("uploadId")
		target, ok := f.partKeys[id]
		if !ok {
			http.Error(w, "no such upload", http.StatusNotFound)
			return
		}
		var joined []byte
		for n := 1; ; n++ {
			part, ok := f.parts[id][fmt.Sprint(n)]
			if !ok {
				break
			}
			joined = append(joined, part...)
		}
		f.objects[target] = joined
		f.meta[target] = http.Header{}
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, `<CompleteMultipartUploadResult><Key>`+target+`</Key></CompleteMultipartUploadResult>`)
	case r.Method == http.MethodPut:
		b, _ := io.ReadAll(r.Body)
		f.objects[key] = b
		h := http.Header{}
		for name := range r.Header {
			lower := strings.ToLower(name)
			if strings.HasPrefix(lower, "x-amz-meta-") || lower == "content-type" || lower == "cache-control" {
				h.Set(name, r.Header.Get(name))
			}
		}
		f.meta[key] = h
	case r.Method == http.MethodHead:
		b, ok := f.objects[key]
		if !ok {
			http.Error(w, "", http.StatusNotFound)
			return
		}
		for name, vals := range f.meta[key] {
			w.Header()[name] = vals
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(b)))
	case r.Method == http.MethodGet && q.Get("list-type") == "2":
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, "<ListBucketResult>")
		for k := range f.objects {
			if strings.HasPrefix(k, q.Get("prefix")) {
				fmt.Fprintf(w, "<Contents><Key>%s</Key></Contents>", k)
			}
		}
		fmt.Fprint(w, "<IsTruncated>false</IsTruncated></ListBucketResult>")
	case r.Method == http.MethodGet:
		b, ok := f.objects[key]
		if !ok {
			http.Error(w, "", http.StatusNotFound)
			return
		}
		_, _ = w.Write(b)
	case r.Method == http.MethodDelete:
		delete(f.objects, key)
		delete(f.meta, key)
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "unhandled: "+r.Method+" "+r.URL.String(), http.StatusNotImplemented)
	}
}

func testStore(t *testing.T) (*Store, *fakeS3) {
	fake := newFakeS3(t)
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	store, err := New(Config{
		Bucket: "test-bucket", Endpoint: srv.URL, Region: "auto",
		AccessKeyID: "test-access", SecretAccessKey: "test-secret",
		PublicBaseURL: "https://files.example.com",
	})
	if err != nil {
		t.Fatal(err)
	}
	return store, fake
}

func TestPutStatOpenDelete(t *testing.T) {
	ctx := context.Background()
	store, _ := testStore(t)

	err := store.Put(ctx, loom.UploadInit{
		Key: "places/abc/photo.jpg", Name: "photo.jpg", ContentType: "image/jpeg",
		CacheControl: "public, max-age=31536000, immutable",
		Metadata:     map[string]string{"loom_upload": "Photo"},
	}, strings.NewReader("jpeg-bytes"))
	if err != nil {
		t.Fatal(err)
	}

	info, err := store.Stat(ctx, "places/abc/photo.jpg")
	if err != nil || info == nil {
		t.Fatalf("stat: %v %v", info, err)
	}
	if info.ContentType != "image/jpeg" || info.Size != int64(len("jpeg-bytes")) ||
		info.Name != "photo.jpg" || info.Metadata["loom_upload"] != "Photo" {
		t.Fatalf("stat wrong: %+v", info)
	}

	rc, err := store.Open(ctx, "places/abc/photo.jpg")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(rc)
	rc.Close()
	if string(b) != "jpeg-bytes" {
		t.Fatalf("open: %q", b)
	}

	if got := store.PublicURL("places/abc/photo.jpg"); got != "https://files.example.com/places/abc/photo.jpg" {
		t.Fatalf("public url: %s", got)
	}

	if missing, err := store.Stat(ctx, "nope"); err != nil || missing != nil {
		t.Fatalf("missing stat should be nil,nil: %v %v", missing, err)
	}
	if err := store.Delete(ctx, "places/abc/photo.jpg"); err != nil {
		t.Fatal(err)
	}
	if info, _ := store.Stat(ctx, "places/abc/photo.jpg"); info != nil {
		t.Fatal("delete did not delete")
	}
}

func TestDeletePrefix(t *testing.T) {
	ctx := context.Background()
	store, fake := testStore(t)
	for _, k := range []string{"svc/ns/stream/a", "svc/ns/stream/b", "svc/ns/other/c"} {
		if err := store.Put(ctx, loom.UploadInit{Key: k}, strings.NewReader("x")); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.DeletePrefix(ctx, "svc/ns/stream/"); err != nil {
		t.Fatal(err)
	}
	if len(fake.objects) != 1 {
		t.Fatalf("prefix delete left %v", fake.objects)
	}
	if _, ok := fake.objects["svc/ns/other/c"]; !ok {
		t.Fatal("prefix delete overreached")
	}
}

func TestMultipartFlow(t *testing.T) {
	ctx := context.Background()
	store, fake := testStore(t)

	sess, err := store.CreateUpload(ctx, loom.UploadInit{
		Key: "svc/ns/id/W9/doc.pdf", Name: "doc.pdf", ContentType: "application/pdf", Size: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if sess.Protocol != loom.ProtocolS3Multipart || !strings.HasPrefix(sess.URL, "uploads/") {
		t.Fatalf("session: %+v", sess)
	}
	token := strings.TrimPrefix(sess.URL, "uploads/")

	// the client uploads two parts through presigned URLs
	for n := 1; n <= 2; n++ {
		partURL, err := store.SignPart(ctx, token, n)
		if err != nil {
			t.Fatal(err)
		}
		req, _ := http.NewRequest(http.MethodPut, partURL, strings.NewReader(fmt.Sprintf("part%d", n)))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("part %d: %d", n, resp.StatusCode)
		}
		if u, _ := url.Parse(partURL); u.Query().Get("X-Amz-Signature") == "" {
			t.Fatal("part URL is not presigned")
		}
	}

	key, err := store.CompleteUpload(ctx, token, []loom.CompletedPart{
		{Number: 2, ETag: `"etag-2"`}, {Number: 1, ETag: `"etag-1"`},
	})
	if err != nil {
		t.Fatal(err)
	}
	if key != "svc/ns/id/W9/doc.pdf" {
		t.Fatalf("key: %s", key)
	}
	if string(fake.objects[key]) != "part1part2" {
		t.Fatalf("assembled: %q", fake.objects[key])
	}

	// tampered and expired sessions die at verification
	if _, err := store.SignPart(ctx, token+"x", 1); err == nil {
		t.Fatal("tampered token accepted")
	}
	forged := store.mintSession(sessionClaims{Key: "other", UploadID: "up-1", Exp: time.Now().Add(-time.Minute).Unix()})
	if _, err := store.SignPart(ctx, forged, 1); err == nil {
		t.Fatal("expired token accepted")
	}
}
