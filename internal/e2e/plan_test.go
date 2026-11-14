package e2e_test

import (
	"context"
	"errors"
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
	ordersgen "github.com/go-apis/loom/internal/e2e/orders/loomgen"
)

// TestPlanThenRun: a @plan command's mutation runs nothing and answers
// what it would do; runPlan runs exactly that, once. A plan is refused
// once its target has moved, once it has been run, once it has expired,
// and for a caller the command's own gate refuses — and a Policy that
// reads the stage lets one caller plan what only another may run.
func TestPlanThenRun(t *testing.T) {
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
		"admin": {Namespaces: []string{"acme"}, Mutate: true, Attrs: map[string]string{"kind": "admin"}},
		// an agent is an admin by its rules, held to planning by the Policy below
		"agent": {Namespaces: []string{"acme"}, Mutate: true, Attrs: map[string]string{"kind": "admin", "agent": "yes"}},
		"north": {Namespaces: []string{"acme"}, Mutate: true, Attrs: map[string]string{"kind": "courier", "depot": north}},
		"south": {Namespaces: []string{"acme"}, Mutate: true, Attrs: map[string]string{"kind": "courier", "depot": south}},
		"other": {Namespaces: []string{"globex"}, Mutate: true, Attrs: map[string]string{"kind": "admin"}},
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
		Policy: func(ctx context.Context, d loomgql.Decision) error {
			if d.Stage == loomgql.StageRun && d.Access.Attrs["agent"] == "yes" {
				return fmt.Errorf("access denied: an agent plans; a person runs")
			}
			return loomgql.DefaultPolicy(d)
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
	in := func(id string, more map[string]any) map[string]any {
		input := map[string]any{"aggregateId": id, "namespace": "acme"}
		for k, v := range more {
			input[k] = v
		}
		return map[string]any{"in": input}
	}
	register := `mutation($in: RegisterParcelInput!) { registerParcel(input: $in) { status } }`
	relabel := `mutation($in: RelabelParcelInput!) { relabelParcel(input: $in) { status } }`
	discard := `mutation($in: DiscardParcelInput!) { discardParcel(input: $in) { id command version targetId events { type data } } }`
	runPlan := `mutation($id: UUID!, $ns: Namespace!) { runPlan(namespace: $ns, id: $id) { status } }`
	discarded := func(id string) bool {
		t.Helper()
		state, _, err := cli.Load(ctx, "Parcel", "acme", uuid.MustParse(id))
		if err != nil {
			t.Fatal(err)
		}
		return state.(*ordersgen.Parcel).Discarded
	}
	// plan answers the plan's id, failing the test if planning is refused
	plan := func(token, parcel string) string {
		t.Helper()
		res := run(token, discard, in(parcel, nil))
		if len(res.Errors) > 0 {
			t.Fatalf("%s planning: %v", token, res.Errors)
		}
		return res.Data["discardParcel"].(map[string]any)["id"].(string)
	}
	runIt := func(token, id string) string {
		t.Helper()
		return strings.Join(run(token, runPlan, map[string]any{"id": id, "ns": "acme"}).Errors, "\n")
	}
	newParcel := func(depot string) string {
		t.Helper()
		id := uuid.NewString()
		if res := run("admin", register, in(id, map[string]any{"depotId": depot, "label": "p"})); len(res.Errors) > 0 {
			t.Fatal(res.Errors)
		}
		return id
	}

	// the mutation runs nothing: it says what the command would emit
	parcel := newParcel(north)
	res := run("north", discard, in(parcel, nil))
	if len(res.Errors) > 0 {
		t.Fatal(res.Errors)
	}
	held := res.Data["discardParcel"].(map[string]any)
	events := held["events"].([]any)
	if held["command"] != "DiscardParcel" || held["targetId"] != parcel || held["version"] != 1 || len(events) != 1 ||
		events[0].(map[string]any)["type"] != "ParcelDiscarded" || fmt.Sprint(events[0].(map[string]any)["data"]) != "map[discarded:true]" {
		t.Fatalf("plan = %+v", held)
	}
	if discarded(parcel) {
		t.Fatal("planning discarded the parcel")
	}
	planID := held["id"].(string)

	// who may run it is the command's own gate, asked of the runner
	if got := runIt("south", planID); !strings.Contains(got, "is not allowed for this caller") {
		t.Errorf("another depot's courier running the plan: %q", got)
	}
	if got := runIt("other", planID); !strings.Contains(got, "access denied") {
		t.Errorf("another tenant running the plan: %q", got)
	}
	if got := run("other", runPlan, map[string]any{"id": planID, "ns": "globex"}).Errors; len(got) == 0 || !strings.Contains(got[0], "no such plan") {
		t.Errorf("the plan reached through another namespace: %v", got)
	}
	if discarded(parcel) {
		t.Fatal("a refused run discarded the parcel")
	}

	// run it: once
	if got := runIt("north", planID); got != "" {
		t.Fatalf("running the plan: %s", got)
	}
	if !discarded(parcel) {
		t.Fatal("the plan ran and the parcel is not discarded")
	}
	if got := runIt("north", planID); !strings.Contains(got, "no such plan") {
		t.Errorf("running a spent plan: %q", got)
	}
	// what the handler refuses is refused at planning: nothing to plan
	if got := run("north", discard, in(parcel, nil)).Errors; len(got) == 0 || !strings.Contains(got[0], "already discarded") {
		t.Errorf("planning what the handler refuses: %v", got)
	}

	// stale: the parcel moved after the plan was made
	parcel = newParcel(north)
	planID = plan("north", parcel)
	if res := run("north", relabel, in(parcel, map[string]any{"label": "moved"})); len(res.Errors) > 0 {
		t.Fatal(res.Errors)
	}
	if got := runIt("north", planID); !strings.Contains(got, "stale") || !strings.Contains(got, "plan again") {
		t.Errorf("running a stale plan: %q", got)
	}
	if discarded(parcel) {
		t.Fatal("a stale plan discarded the parcel")
	}
	if got := runIt("north", planID); !strings.Contains(got, "no such plan") {
		t.Errorf("a stale plan is spent: %q", got)
	}

	// one caller plans what only another may run
	parcel = newParcel(south)
	planID = plan("agent", parcel)
	if got := runIt("agent", planID); !strings.Contains(got, "a person runs") {
		t.Errorf("the agent running its own plan: %q", got)
	}
	if got := runIt("admin", planID); got != "" {
		t.Errorf("the admin running the agent's plan: %q", got)
	}
	if !discarded(parcel) {
		t.Error("the admin ran the agent's plan and the parcel is not discarded")
	}

	// expiry, and in-process dispatch: a plan past its lifetime is gone,
	// and Dispatch itself is never held back by @plan
	parcel = newParcel(north)
	cmd := &ordersgen.DiscardParcel{}
	cmd.Namespace, cmd.AggregateID = "acme", uuid.MustParse(parcel)
	short, err := cli.Plan(ctx, cmd, time.Nanosecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.RunPlan(ctx, "acme", short.ID); !errors.Is(err, loom.ErrNoPlan) {
		t.Errorf("running an expired plan: %v", err)
	}
	if err := cli.Dispatch(ctx, cmd); err != nil || !discarded(parcel) {
		t.Errorf("in-process dispatch of a @plan command: %v, discarded %v", err, discarded(parcel))
	}
}
