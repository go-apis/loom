package loom

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakeCloudRun is the metadata server and the Cloud Run Admin API as an
// instance of revision mesh-api-00355-7rg sees them.
func fakeCloudRun(t *testing.T, revisionStatus int, createTime string) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/computeMetadata/v1/instance/region":
			if r.Header.Get("Metadata-Flavor") != "Google" {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			_, _ = io.WriteString(w, "projects/123456/regions/australia-southeast1")
		case "/computeMetadata/v1/instance/service-accounts/default/token":
			_, _ = io.WriteString(w, `{"access_token":"tok","expires_in":3599,"token_type":"Bearer"}`)
		case "/v2/projects/123456/locations/australia-southeast1/services/mesh-api/revisions/mesh-api-00355-7rg":
			if r.Header.Get("Authorization") != "Bearer tok" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.WriteHeader(revisionStatus)
			_, _ = io.WriteString(w, `{"name":"projects/123456/locations/australia-southeast1/services/mesh-api/revisions/mesh-api-00355-7rg","createTime":"`+createTime+`"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	t.Setenv("GCE_METADATA_HOST", strings.TrimPrefix(srv.URL, "http://"))
	prev := cloudRunAPI
	cloudRunAPI = srv.URL
	t.Cleanup(func() { cloudRunAPI = prev })
}

func lookupClient(t *testing.T) *Client {
	t.Helper()
	t.Setenv("K_SERVICE", "mesh-api")
	t.Setenv("K_REVISION", "mesh-api-00355-7rg")
	t.Setenv("LOOM_GENERATION", "")
	return &Client{reg: &Registry{Service: "mesh"}, log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		gen: generationFrom(0, "")}
}

func TestGenerationFromCloudRunRevision(t *testing.T) {
	fakeCloudRun(t, http.StatusOK, "2026-11-01T11:41:07.123456Z")
	c := lookupClient(t)
	c.resolveGeneration(context.Background())
	if c.gen.n != 1793533267 || c.gen.source != genFromCloudRun || c.gen.lookup {
		t.Fatalf("generation %+v", c.gen)
	}
}

// Any failure of the lookup is no generation: never preempts, never
// preempted wrongly.
func TestCloudRunLookupFailsSafe(t *testing.T) {
	for name, c := range map[string]struct {
		status int
		create string
	}{
		"no permission":  {http.StatusForbidden, "2026-11-01T11:41:07Z"},
		"no createTime":  {http.StatusOK, ""},
		"bad createTime": {http.StatusOK, "yesterday"},
	} {
		t.Run(name, func(t *testing.T) {
			fakeCloudRun(t, c.status, c.create)
			cli := lookupClient(t)
			cli.resolveGeneration(context.Background())
			if cli.gen.n != 0 || cli.gen.lookup || cli.gen.note == "" {
				t.Fatalf("generation %+v", cli.gen)
			}
		})
	}
	t.Run("no metadata server", func(t *testing.T) {
		t.Setenv("GCE_METADATA_HOST", "127.0.0.1:1")
		cli := lookupClient(t)
		cli.resolveGeneration(context.Background())
		if cli.gen.n != 0 || cli.gen.note == "" {
			t.Fatalf("generation %+v", cli.gen)
		}
	})
}

// LOOM_GENERATION, when set, is the generation: Cloud Run is not asked.
func TestLoomGenerationOverCloudRun(t *testing.T) {
	fakeCloudRun(t, http.StatusOK, "2026-11-01T11:41:07Z")
	c := lookupClient(t)
	t.Setenv("LOOM_GENERATION", "1793533000")
	c.gen = generationFrom(0, "")
	c.resolveGeneration(context.Background())
	if c.gen.n != 1793533000 || c.gen.source != genFromEnv {
		t.Fatalf("generation %+v", c.gen)
	}
}
