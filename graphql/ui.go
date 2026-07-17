package graphql

import (
	"bytes"
	_ "embed"
	"net/http"
)

//go:embed ui.html
var uiHTML []byte

// UI serves the generated admin page: a self-contained HTML app that
// introspects the gateway at load time and builds itself from the
// schema — entity list views with filters and live ({x}sChanged)
// subscriptions, doc detail with a live watch, and mutation forms
// generated from the command input types. A Bearer-token box drives the
// gateway's Access model, so it doubles as the way to SEE the auth
// scoping work. No build step, no external assets, nothing to drift.
//
//	mux.Handle("/ui", loomgraphql.UI("/graphql"))
func UI(endpoint string) http.Handler {
	page := bytes.ReplaceAll(uiHTML, []byte("__LOOM_GRAPHQL_ENDPOINT__"), []byte(endpoint))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(page)
	})
}
