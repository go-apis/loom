package e2e_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	"github.com/go-apis/loom/contract"
	loomgql "github.com/go-apis/loom/graphql"
	"github.com/go-apis/loom/internal/e2e/orders"
)

// the depots and parcels the contract tests seed, by ids that are the
// same on every run
var (
	northDepot  = uuid.MustParse("00000000-0000-4000-8000-00000000000a")
	southDepot  = uuid.MustParse("00000000-0000-4000-8000-00000000000b")
	northParcel = uuid.MustParse("00000000-0000-4000-8000-0000000000a1")
	southParcel = uuid.MustParse("00000000-0000-4000-8000-0000000000b1")
)

// parcelSuite is the orders gateway under policy, over a store holding
// one parcel at each of two depots, and the principals its rules tell
// apart: a person (a courier at the north depot), an org admin, and
// estate staff, who no rule names.
func parcelSuite(t *testing.T, policy loomgql.Policy) contract.Suite {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Bus: loom.NewMemoryBus(), Registry: orders.NewRegistry(), Blobs: loom.NewDirBlobStore(t.TempDir(), "http://blobs.local")})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	gateway, err := loomgql.New(loomgql.Config{Services: []*loom.Client{cli}, Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	admin := loomgql.Access{Namespaces: []string{"acme"}, Mutate: true, Attrs: map[string]string{"kind": "admin"}}
	ex, _ := loomgql.AsExecutor(gateway)
	for parcel, depot := range map[uuid.UUID]uuid.UUID{northParcel: northDepot, southParcel: southDepot} {
		res := ex.Exec(loomgql.WithAccess(ctx, admin), `mutation($in: RegisterParcelInput!) { registerParcel(input: $in) { status } }`,
			map[string]any{"in": map[string]any{"aggregateId": parcel.String(), "namespace": "acme", "depotId": depot.String(), "label": "box"}})
		if len(res.Errors) > 0 {
			t.Fatal(res.Errors)
		}
	}
	return contract.Suite{
		Gateway: gateway,
		Principals: []contract.Principal{
			{Name: "person", Access: loomgql.Access{Namespaces: []string{"acme"}, Mutate: true, Attrs: map[string]string{"kind": "courier", "depot": northDepot.String()}}},
			{Name: "org admin", Access: admin},
			{Name: "estate staff", Access: loomgql.Access{Namespaces: []string{"acme"}, Mutate: true, Attrs: map[string]string{"kind": "estate"}}},
		},
		Namespaces: []string{"acme", "globex"},
		Labels:     map[string]string{northParcel.String(): "the north parcel", southParcel.String(): "the south parcel"},
		Golden:     filepath.Join(t.TempDir(), "contract.json"),
	}
}

func byRules(ctx context.Context, d loomgql.Decision) error { return loomgql.DefaultPolicy(d) }

// TestContractRunsEveryOperationAsEveryPrincipal: the suite reads every
// list as each principal and records who may run what, which rows each
// is shown and which fields are hidden from each. Recorded under the
// service's rules, the contract holds. A policy that skips those rules
// for one kind of principal changes what that principal is shown, and
// the difference names the query and the principal.
func TestContractRunsEveryOperationAsEveryPrincipal(t *testing.T) {
	good := parcelSuite(t, byRules)

	// nothing recorded yet: record it, as a first run does
	t.Setenv(contract.UpdateEnv, "1")
	contract.Run(t, good)
	t.Setenv(contract.UpdateEnv, "")
	recorded, err := contract.Read(good.Golden)
	if err != nil {
		t.Fatal(err)
	}
	parcels := recorded.Operations["parcels"]
	if parcels == nil || parcels.Kind != "query" {
		t.Fatalf("the contract has no parcels query: %+v", recorded.Operations)
	}
	for principal, want := range map[string]string{
		"person":       "shown [the north parcel]",
		"org admin":    "shown [the north parcel, the south parcel]",
		"estate staff": `refused (access denied: query "parcels" is not allowed for this caller)`,
	} {
		if got := parcels.Rows[principal]["acme"].String(); got != want {
			t.Errorf("parcels as %s in acme: %s, want %s", principal, got, want)
		}
		if got := parcels.Rows[principal]["globex"].Refused; !strings.Contains(got, `namespace "globex"`) {
			t.Errorf("parcels as %s in globex: %q, want a refusal of the namespace", principal, got)
		}
	}
	if !parcels.Allowed["person"] || !parcels.Allowed["org admin"] || parcels.Allowed["estate staff"] {
		t.Errorf("who may run parcels: %v", parcels.Allowed)
	}
	if register := recorded.Operations["registerParcel"]; register == nil || register.Kind != "mutation" || register.Allowed["estate staff"] || !register.Allowed["org admin"] {
		t.Errorf("registerParcel in the contract: %+v", register)
	}
	if got := strings.Join(recorded.Hidden["Parcel"]["person"], ","); got != "insuredCents" || len(recorded.Hidden["Parcel"]["org admin"]) != 0 {
		t.Errorf("Parcel's hidden fields: %v", recorded.Hidden["Parcel"])
	}
	// and it holds: the same gateway is checked against it and passes
	contract.Run(t, good)
	// two recordings of one behaviour are the same bytes
	first, _ := os.ReadFile(good.Golden)
	again, err := good.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := contract.Write(good.Golden, again); err != nil {
		t.Fatal(err)
	}
	if second, _ := os.ReadFile(good.Golden); string(first) != string(second) {
		t.Error("recording the contract twice wrote different bytes")
	}

	// a policy that lets estate staff past the rules everyone else is
	// held to
	skips := parcelSuite(t, func(ctx context.Context, d loomgql.Decision) error {
		if d.Access.Attrs["kind"] == "estate" {
			return nil
		}
		return loomgql.DefaultPolicy(d)
	})
	now, err := skips.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	diffs := contract.Differences(recorded, now)
	want := `query parcels as estate staff in acme: shown [the north parcel, the south parcel]; the contract says refused (access denied: query "parcels" is not allowed for this caller)`
	found := false
	for _, d := range diffs {
		found = found || d == want
		if !strings.Contains(d, "as estate staff") {
			t.Errorf("a difference for a principal whose rules did not change: %s", d)
		}
	}
	if !found {
		t.Errorf("the differences do not name the query and the principal:\n%s", strings.Join(diffs, "\n"))
	}
}

// TestContractChecksAClientsAsks: what a client publishes that it asks
// is checked for every principal the client serves. A query the policy
// refuses for org admins, a field hidden from a person and a query the
// gateway does not have are each named with the principal; an ask
// excused for a principal is not.
func TestContractChecksAClientsAsks(t *testing.T) {
	published := filepath.Join(t.TempDir(), "gateway-asks.json")
	if err := os.WriteFile(published, []byte(`{"version": 1,
		"queries": ["parcels", "parcel", "parcelsByMoonlight"],
		"mutations": ["registerParcel"],
		"inputs": ["RegisterParcelInput"],
		"fields": {"Parcel": ["label", "insuredCents"]}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	asks, err := contract.ReadAsks(published)
	if err != nil {
		t.Fatal(err)
	}

	// a policy that refuses the parcels list for org admins
	refuses := parcelSuite(t, func(ctx context.Context, d loomgql.Decision) error {
		if d.Field == "parcels" && d.Access.Attrs["kind"] == "admin" {
			return fmt.Errorf("access denied: admins use the depot view")
		}
		return loomgql.DefaultPolicy(d)
	})
	refuses.Asks, refuses.AsksFor = asks, []string{"person", "org admin"}
	now, err := refuses.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Join(refuses.Unserved(now), "\n")
	want := strings.Join([]string{
		"the client asks query parcels, which is refused as org admin",
		"the client asks query parcelsByMoonlight, which the gateway does not have",
		"the client reads Parcel.insuredCents, which is hidden as person",
	}, "\n")
	if got != want {
		t.Errorf("unserved asks:\n got %s\nwant %s", got, want)
	}

	// excused where the client asks only on behalf of some
	refuses.AsksExcept = map[string][]string{"parcels": {"org admin"}, "Parcel.insuredCents": {"person"}}
	if got := strings.Join(refuses.Unserved(now), "\n"); got != "the client asks query parcelsByMoonlight, which the gateway does not have" {
		t.Errorf("unserved asks with two excused:\n%s", got)
	}

	// a file that asks nothing is not a pass
	empty := filepath.Join(t.TempDir(), "empty.json")
	if err := os.WriteFile(empty, []byte(`{"version": 1}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := contract.ReadAsks(empty); err == nil || !strings.Contains(err.Error(), "asks nothing") {
		t.Errorf("an asks file that asks nothing: %v", err)
	}
}
