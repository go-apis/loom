package graphql

import (
	"bytes"
	"io"
	"net/http"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/mesh"
)

// MeshHooks receives Mesh's job hooks at the gateway, for the agents
// the services' @loop aggregates ask (loom.AskAgent), so the services
// stay private. Mount it at the URL the product's catalogue names for
// hooks.jobs, outside any user auth — Mesh signs each hook, and the
// owning service's Client.MeshHook verifies it under its own secret:
//
//	mux.Handle("POST /hooks/mesh", loomgraphql.MeshHooks(ticketsCli, ordersCli))
//
// A hook is routed by the service loom named in the job's refs. One for
// a job no service here offered is answered 200 and dropped, so Mesh
// does not retry it; a body that is too big or unreadable is 400.
func MeshHooks(services ...*loom.Client) http.Handler {
	byName := make(map[string]http.Handler, len(services))
	for _, cli := range services {
		byName[cli.Registry().Service] = cli.MeshHook()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "post a hook", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, mesh.MaxHookBody+1))
		if err != nil || len(body) > mesh.MaxHookBody {
			http.Error(w, "reading the body", http.StatusBadRequest)
			return
		}
		h := byName[loom.MeshHookService(body)]
		if h == nil {
			w.WriteHeader(http.StatusOK)
			return
		}
		r2 := r.Clone(r.Context())
		r2.Body = io.NopCloser(bytes.NewReader(body))
		r2.ContentLength = int64(len(body))
		h.ServeHTTP(w, r2)
	})
}
