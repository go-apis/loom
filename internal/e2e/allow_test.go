package e2e_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	loomgql "github.com/go-apis/loom/graphql"
	"github.com/go-apis/loom/internal/e2e/orders"
)

// TestAllowRulesGateByWhatTheCallerIs: @allow on Parcel lets an admin
// do anything and a courier only what belongs to its own depot. The
// rule is checked against the stored parcel, so a courier cannot claim
// another depot's parcel by what it sends; a list is narrowed to the
// courier's rows whatever it filters by; a scoped caller with no
// attributes meets no rule; and god access with no attribute model
// keeps its meaning.
func TestAllowRulesGateByWhatTheCallerIs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Bus: loom.NewMemoryBus(), Registry: orders.NewRegistry(), Blobs: loom.NewDirBlobStore(t.TempDir(), "http://blobs.local")})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	north, south := uuid.NewString(), uuid.NewString()
	tokens := map[string]loomgql.Access{
		"admin":   {Namespaces: []string{"acme"}, Mutate: true, Attrs: map[string]string{"kind": "admin"}},
		"north":   {Namespaces: []string{"acme"}, Mutate: true, Attrs: map[string]string{"kind": "courier", "depot": north}},
		"south":   {Namespaces: []string{"acme"}, Mutate: true, Attrs: map[string]string{"kind": "courier", "depot": south}},
		"nodepot": {Namespaces: []string{"acme"}, Mutate: true, Attrs: map[string]string{"kind": "courier"}},
		"plain":   {Namespaces: []string{"acme"}, Mutate: true},
		"god":     {All: true, Mutate: true},
	}
	gateway, err := loomgql.New(loomgql.Config{
		Services: []*loom.Client{cli},
		Auth: func(r *http.Request) (loomgql.Access, error) {
			a, ok := tokens[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
			if !ok {
				return loomgql.Access{}, fmt.Errorf("bad token")
			}
			return a, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ex, _ := loomgql.AsExecutor(gateway)
	run := func(token, doc string, vars map[string]any) loomgql.Result {
		t.Helper()
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		r, err := ex.Authenticate(r)
		if err != nil {
			t.Fatal(err)
		}
		return ex.Exec(r.Context(), doc, vars)
	}
	const denied = "is not allowed for this caller"
	allowed := func(what string, res loomgql.Result) {
		t.Helper()
		if len(res.Errors) > 0 {
			t.Errorf("%s: refused: %v", what, res.Errors)
		}
	}
	refused := func(what string, res loomgql.Result) {
		t.Helper()
		if len(res.Errors) == 0 || !strings.Contains(res.Errors[0], denied) {
			t.Errorf("%s: errors = %v, data = %v; want it refused by its rules", what, res.Errors, res.Data)
		}
	}

	register := `mutation($in: RegisterParcelInput!) { registerParcel(input: $in) { status } }`
	relabel := `mutation($in: RelabelParcelInput!) { relabelParcel(input: $in) { status } }`
	get := `query($id: UUID!) { parcel(namespace: "acme", id: $id) { label depotId } }`
	list := `query($where: [FilterInput!]) { parcels(namespace: "acme", where: $where, order: "label") { label } }`
	reg := func(id, depot, label string) map[string]any {
		return map[string]any{"in": map[string]any{"aggregateId": id, "namespace": "acme", "depotId": depot, "label": label}}
	}
	lab := func(id, label string) map[string]any {
		return map[string]any{"in": map[string]any{"aggregateId": id, "namespace": "acme", "label": label}}
	}

	// creating: nothing is stored yet, so the payload says whose it is —
	// a courier registers at its own depot and at no other
	mine, theirs := uuid.NewString(), uuid.NewString()
	allowed("north registers at north", run("north", register, reg(mine, north, "n1")))
	refused("north registers at south", run("north", register, reg(uuid.NewString(), south, "x")))
	allowed("south registers at south", run("south", register, reg(theirs, south, "s1")))
	allowed("admin registers anywhere", run("admin", register, reg(uuid.NewString(), south, "s2")))
	refused("a courier with no depot", run("nodepot", register, reg(uuid.NewString(), north, "x")))
	refused("a scoped caller with no attributes", run("plain", register, reg(uuid.NewString(), north, "x")))
	allowed("god access with no attribute model", run("god", register, reg(uuid.NewString(), north, "n2")))

	// changing: the stored parcel says whose it is, and what the caller
	// sends cannot say otherwise
	allowed("north relabels its own", run("north", relabel, lab(mine, "n1b")))
	refused("north relabels south's", run("north", relabel, lab(theirs, "stolen")))
	refused("north re-registers south's parcel as its own", run("north", register, reg(theirs, north, "stolen")))
	allowed("admin relabels any", run("admin", relabel, lab(theirs, "s1b")))

	// uploading: opening an upload asks for its command to be run when
	// the bytes land, by the store and not the caller — so the caller
	// answers to that command's rules here, against the stored parcel
	upload := `mutation($in: CreateParcelPhotoUploadInput!) { createParcelPhotoUpload(input: $in) { url } }`
	photo := func(id string) map[string]any {
		return map[string]any{"in": map[string]any{"namespace": "acme", "id": id, "name": "label.jpg", "contentType": "image/jpeg", "size": 10}}
	}
	allowed("north uploads to its own", run("north", upload, photo(mine)))
	refused("north uploads to south's", run("north", upload, photo(theirs)))
	refused("north uploads to a parcel that does not exist", run("north", upload, photo(uuid.NewString())))
	allowed("admin uploads to any", run("admin", upload, photo(theirs)))
	refused("a scoped caller with no attributes uploads", run("plain", upload, photo(mine)))

	// reading one
	res := run("north", get, map[string]any{"id": mine})
	allowed("north reads its own", res)
	if got := fmt.Sprint(res.Data["parcel"]); !strings.Contains(got, "n1b") {
		t.Errorf("north's parcel = %s", got)
	}
	refused("north reads south's", run("north", get, map[string]any{"id": theirs}))
	refused("north reads one that does not exist", run("north", get, map[string]any{"id": uuid.NewString()}))
	allowed("admin reads south's", run("admin", get, map[string]any{"id": theirs}))
	if got := fmt.Sprint(run("south", get, map[string]any{"id": theirs}).Data["parcel"]); !strings.Contains(got, "s1b") {
		t.Errorf("south's parcel after the admin relabelled it = %s", got)
	}

	// listing: a courier sees its depot's rows, and filtering for
	// another depot's finds none of them
	labels := func(res loomgql.Result) string {
		t.Helper()
		if len(res.Errors) > 0 {
			t.Fatalf("list: %v", res.Errors)
		}
		var out []string
		for _, row := range res.Data["parcels"].([]any) {
			out = append(out, fmt.Sprint(row.(map[string]any)["label"]))
		}
		return strings.Join(out, " ")
	}
	if got := labels(run("north", list, nil)); got != "n1b n2" {
		t.Errorf("north lists %q, want its own two", got)
	}
	if got := labels(run("south", list, nil)); got != "s1b s2" {
		t.Errorf("south lists %q, want its own two", got)
	}
	southOnly := map[string]any{"where": []any{map[string]any{"field": "depot_id", "op": "EQ", "value": south}}}
	if got := labels(run("north", list, southOnly)); got != "" {
		t.Errorf("north filtering for south's rows lists %q", got)
	}
	if got := labels(run("admin", list, nil)); got != "n1b n2 s1b s2" {
		t.Errorf("admin lists %q, want all four", got)
	}
	refused("a courier with no depot lists", run("nodepot", list, nil))
	refused("a scoped caller with no attributes lists", run("plain", list, nil))
}
