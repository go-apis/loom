package gblob_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/gblob"
)

// fakeGCS covers the slice of the JSON API the store speaks: resumable
// init, object get (json + media), delete, and prefix list.
func fakeGCS(t *testing.T, objects map[string]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/storage/v1/b/bkt/o"):
			if r.URL.Query().Get("uploadType") != "resumable" {
				http.Error(w, "want resumable", http.StatusBadRequest)
				return
			}
			var body struct {
				Metadata map[string]string `json:"metadata"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body.Metadata["loom_upload"] == "" {
				http.Error(w, "metadata missing", http.StatusBadRequest)
				return
			}
			w.Header().Set("Location", "http://session.local/"+r.URL.Query().Get("name"))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Path == "/storage/v1/b/bkt/o":
			var items []map[string]string
			for name := range objects {
				if strings.HasPrefix(name, r.URL.Query().Get("prefix")) {
					items = append(items, map[string]string{"name": name})
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"items": items})
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/storage/v1/b/bkt/o/"):
			name := strings.TrimPrefix(r.URL.Path, "/storage/v1/b/bkt/o/")
			data, ok := objects[name]
			if !ok {
				http.NotFound(w, r)
				return
			}
			if r.URL.Query().Get("alt") == "media" {
				_, _ = io.WriteString(w, data)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"size":        fmt.Sprint(len(data)),
				"contentType": "application/pdf",
				"metadata":    map[string]string{"loom_name": "w9.pdf"},
			})
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/storage/v1/b/bkt/o/"):
			delete(objects, strings.TrimPrefix(r.URL.Path, "/storage/v1/b/bkt/o/"))
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected "+r.Method+" "+r.URL.String(), http.StatusTeapot)
		}
	}))
}

func TestStore(t *testing.T) {
	ctx := context.Background()
	objects := map[string]string{
		"svc/acme/s1/W9/f1": "pdf-bytes",
		"svc/acme/s1/W9/f2": "more-bytes",
		"svc/other/s2/W9/f": "keep",
	}
	srv := fakeGCS(t, objects)
	defer srv.Close()

	store, err := gblob.New(ctx, gblob.Config{Bucket: "bkt", Endpoint: srv.URL, HTTPClient: srv.Client()})
	if err != nil {
		t.Fatal(err)
	}

	sess, err := store.CreateUpload(ctx, loom.UploadInit{
		Key: "svc/acme/s1/W9/f3", Name: "w9.pdf", ContentType: "application/pdf", Size: 9,
		Metadata: map[string]string{"loom_upload": "W9"},
	})
	if err != nil || !strings.Contains(sess.URL, "svc/acme/s1/W9/f3") || sess.Protocol != loom.ProtocolGCSResumable {
		t.Fatalf("create upload: url=%v err=%v", sess, err)
	}

	info, err := store.Stat(ctx, "svc/acme/s1/W9/f1")
	if err != nil || info == nil || info.Size != 9 || info.Name != "w9.pdf" || info.ContentType != "application/pdf" {
		t.Fatalf("stat: %+v err=%v", info, err)
	}
	if missing, err := store.Stat(ctx, "svc/acme/s1/W9/nope"); err != nil || missing != nil {
		t.Fatalf("stat missing: %+v err=%v", missing, err)
	}

	body, err := store.Open(ctx, "svc/acme/s1/W9/f1")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(body)
	body.Close()
	if string(got) != "pdf-bytes" {
		t.Fatalf("open: %q", got)
	}

	if err := store.DeletePrefix(ctx, "svc/acme/s1/"); err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 {
		t.Fatalf("delete prefix left %v", objects)
	}
	if _, kept := objects["svc/other/s2/W9/f"]; !kept {
		t.Fatalf("delete prefix crossed streams: %v", objects)
	}
}
