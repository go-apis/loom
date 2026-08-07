package loom

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The confirm phrase is the server-side arming switch: /reset must
// refuse anything but the exact service name before touching the
// database — exercised without a pool, so a slipped-through request
// would panic instead of passing silently.
func TestAPIResetDemandsServiceName(t *testing.T) {
	c := &Client{reg: &Registry{Service: "orders"}, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	for name, body := range map[string]string{
		"no body":       "",
		"empty confirm": `{"confirm":""}`,
		"wrong service": `{"confirm":"billing"}`,
		"case mismatch": `{"confirm":"Orders"}`,
		"padded":        `{"confirm":" orders "}`,
		"wrong field":   `{"service":"orders"}`,
	} {
		t.Run(name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c.apiReset(rec, httptest.NewRequest(http.MethodPost, "/reset", strings.NewReader(body)))
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status %d, want 400", rec.Code)
			}
		})
	}
}
