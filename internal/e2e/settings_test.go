package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-apis/loom"
	loomgql "github.com/go-apis/loom/graphql"
	"github.com/go-apis/loom/internal/e2e/clock"
	clockgen "github.com/go-apis/loom/internal/e2e/clock/loomgen"
	"github.com/go-apis/loom/internal/e2e/orders"
)

// TestSettingChangeNeedsAReason: a declared setting reads as its default
// until it is changed. A change with no reason is refused and changes
// nothing; one with a reason is stored with who made it, when, why and
// the value before. A namespace's own value stands over the one set for
// every namespace, which stands over the default; a reset takes the
// namespace's own away and is itself a change. A value of the wrong type
// and a setting the schema does not declare are refused.
func TestSettingChangeNeedsAReason(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Registry: clock.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	smith := loom.WithMeta(ctx, loom.Metadata{Actor: "user:smith"})
	jones := loom.WithMeta(ctx, loom.Metadata{Actor: "user:jones"})

	// the defaults, typed, through the generated readers
	if cap, err := clockgen.SettingStrikeCap(ctx, cli, "forge"); err != nil || cap != 3 {
		t.Fatalf("strike_cap = %d %v, want its default 3", cap, err)
	}
	if open, err := clockgen.SettingForgeOpen(ctx, cli, "forge"); err != nil || !open {
		t.Errorf("forge_open = %v %v", open, err)
	}
	if bellows, err := clockgen.SettingBellows(ctx, cli, "forge"); err != nil || bellows != "hand" {
		t.Errorf("bellows = %q %v", bellows, err)
	}
	if heat, err := clockgen.SettingHeat(ctx, cli, "forge"); err != nil || heat != clockgen.HeatWarm {
		t.Errorf("heat = %q %v", heat, err)
	}
	if draught, err := clockgen.SettingDraught(ctx, cli, "forge"); err != nil || draught != 0 {
		t.Errorf("draught = %v %v, want the zero a setting with no default has", draught, err)
	}

	// no reason: refused, and nothing changed or recorded
	for _, reason := range []string{"", "   "} {
		if err := cli.SetSetting(smith, "forge", "strike_cap", 5, reason); !errors.Is(err, loom.ErrNoReason) {
			t.Errorf("a change with reason %q: %v, want ErrNoReason", reason, err)
		}
	}
	if cap, _ := clockgen.SettingStrikeCap(ctx, cli, "forge"); cap != 3 {
		t.Fatalf("a refused change moved strike_cap to %d", cap)
	}
	if history, err := cli.SettingHistory(ctx, "forge", "strike_cap", 0); err != nil || len(history) != 0 {
		t.Errorf("a refused change left history: %+v %v", history, err)
	}

	// with a reason: stored with who, when, why and the old value
	began := time.Now().Add(-time.Second)
	if err := cli.SetSetting(smith, "forge", "strike_cap", 5, "the new anvil takes more"); err != nil {
		t.Fatal(err)
	}
	if err := cli.SetSetting(jones, "forge", "strike_cap", 8, "rush order"); err != nil {
		t.Fatal(err)
	}
	value, err := cli.SettingValue(ctx, "forge", "strike_cap")
	if err != nil {
		t.Fatal(err)
	}
	if value.Value != int64(8) || value.Default != int64(3) || value.From != loom.SettingFromNamespace || value.Actor != "user:jones" || value.Reason != "rush order" || value.At.Before(began) {
		t.Errorf("strike_cap as set: %+v", value)
	}
	history, err := cli.SettingHistory(ctx, "forge", "strike_cap", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 {
		t.Fatalf("history: %+v", history)
	}
	if h := history[0]; h.Old != int64(5) || h.New != int64(8) || h.Actor != "user:jones" || h.Reason != "rush order" || h.At.Before(began) {
		t.Errorf("the latest change: %+v", h)
	}
	if h := history[1]; h.Old != nil || h.New != int64(5) || h.Actor != "user:smith" || h.Reason != "the new anvil takes more" {
		t.Errorf("the first change: %+v", h)
	}

	// every namespace, then one namespace's own, then a reset
	if err := cli.SetSetting(smith, loom.EveryNamespace, "heat", "hot", "winter"); err != nil {
		t.Fatal(err)
	}
	if err := cli.SetSetting(smith, "foundry", "heat", "cold", "the foundry is resting"); err != nil {
		t.Fatal(err)
	}
	for ns, want := range map[string]struct {
		heat clockgen.Heat
		from string
	}{"forge": {clockgen.HeatHot, loom.SettingFromEvery}, "foundry": {clockgen.HeatCold, loom.SettingFromNamespace}} {
		heat, err := clockgen.SettingHeat(ctx, cli, ns)
		v, _ := cli.SettingValue(ctx, ns, "heat")
		if err != nil || heat != want.heat || v.From != want.from {
			t.Errorf("heat in %s: %q from %s (%v), want %q from %s", ns, heat, v.From, err, want.heat, want.from)
		}
	}
	if err := cli.ResetSetting(smith, "foundry", "heat", ""); !errors.Is(err, loom.ErrNoReason) {
		t.Errorf("a reset with no reason: %v", err)
	}
	if err := cli.ResetSetting(jones, "foundry", "heat", "work is back"); err != nil {
		t.Fatal(err)
	}
	if heat, _ := clockgen.SettingHeat(ctx, cli, "foundry"); heat != clockgen.HeatHot {
		t.Errorf("after its reset the foundry's heat is %q, want every namespace's hot", heat)
	}
	if history, _ := cli.SettingHistory(ctx, "foundry", "heat", 0); len(history) != 2 || history[0].New != nil || history[0].Old != "cold" || history[0].Reason != "work is back" {
		t.Errorf("the foundry's heat history: %+v", history)
	}

	// what is refused
	for name, c := range map[string]struct {
		setting string
		value   any
		want    string
	}{
		"a string for an int":      {"strike_cap", "five", "is an int"},
		"a fraction for an int":    {"strike_cap", 2.5, "is an int"},
		"a value not in the enum":  {"heat", "scorching", "one of cold, warm, hot"},
		"an int for a bool":        {"forge_open", 1, "is a bool"},
		"a setting never declared": {"anvil_colour", "black", "declares no setting"},
	} {
		if err := cli.SetSetting(smith, "forge", c.setting, c.value, "trying"); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want an error saying %q", name, err, c.want)
		}
	}
	// a whole number given as a float is an int; the list reads them all
	if err := cli.SetSetting(smith, "forge", "strike_cap", float64(4), "from JSON"); err != nil {
		t.Errorf("a whole float for an int: %v", err)
	}
	all, err := cli.Settings(ctx, "forge")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, s := range all {
		names = append(names, s.Name)
	}
	if got := strings.Join(names, " "); got != "bellows draught forge_open heat strike_cap" {
		t.Errorf("the forge's settings: %s", got)
	}
	if all[4].Value != int64(4) || all[4].Doc != "How many strikes an anvil takes before it is rested." {
		t.Errorf("strike_cap in the list: %+v", all[4])
	}
}

// TestSettingsThroughTheGateway: a service's settings are read and
// changed through the gateway with no code of the service's own. A
// change is recorded as made by the caller, needs a reason, and answers
// to the gateway's rules: a caller with no access to the namespace reads
// and changes nothing there, and one who may not mutate changes nothing.
func TestSettingsThroughTheGateway(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Registry: clock.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	callers := map[string]loomgql.Access{
		"smith":  {Namespaces: []string{"forge"}, Mutate: true, Actor: "user:smith"},
		"reader": {Namespaces: []string{"forge"}, Actor: "user:reader"},
		"other":  {Namespaces: []string{"foundry"}, Mutate: true, Actor: "user:other"},
	}
	gateway, err := loomgql.New(loomgql.Config{
		Services: []*loom.Client{cli},
		Auth: func(r *http.Request) (loomgql.Access, error) {
			a, ok := callers[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
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
	run := func(caller, doc string) loomgql.Result {
		t.Helper()
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		r.Header.Set("Authorization", "Bearer "+caller)
		r, err := ex.Authenticate(r)
		if err != nil {
			t.Fatal(err)
		}
		return ex.Exec(r.Context(), doc, nil)
	}
	said := func(res loomgql.Result) string { return strings.Join(res.Errors, "; ") }

	// the list: every declared setting, at its default
	res := run("smith", `{ settings(namespace: "forge") { service name type value default from values doc } }`)
	if len(res.Errors) > 0 {
		t.Fatal(res.Errors)
	}
	raw, _ := json.Marshal(res.Data["settings"])
	for _, want := range []string{
		`{"default":"3","doc":"How many strikes an anvil takes before it is rested.","from":"default","name":"strike_cap","service":"clock","type":"int","value":"3","values":[]}`,
		`"from":"default","name":"heat","service":"clock","type":"enum","value":"\"warm\"","values":["cold","warm","hot"]`,
	} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("the settings list lacks %s:\n%s", want, raw)
		}
	}

	// a change: with no reason refused, with one recorded as the caller's
	if res := run("smith", `mutation { setSetting(namespace: "forge", name: "strike_cap", value: "6", reason: "") { value } }`); !strings.Contains(said(res), "with a reason") {
		t.Errorf("a change with no reason: %v", res.Errors)
	}
	res = run("smith", `mutation { setSetting(namespace: "forge", name: "strike_cap", value: "6", reason: "bigger hammers") { value from actor reason } }`)
	if raw, _ := json.Marshal(res.Data["setSetting"]); len(res.Errors) > 0 || string(raw) != `{"actor":"user:smith","from":"namespace","reason":"bigger hammers","value":"6"}` {
		t.Errorf("setSetting: %s %v", raw, res.Errors)
	}
	// an enum by its bare word, and a wrong one refused
	if res := run("smith", `mutation { setSetting(namespace: "forge", name: "heat", value: "hot", reason: "winter") { value } }`); len(res.Errors) > 0 || fmt.Sprint(res.Data["setSetting"]) != `map[value:"hot"]` {
		t.Errorf("an enum by its bare word: %v %v", res.Data, res.Errors)
	}
	if res := run("smith", `mutation { setSetting(namespace: "forge", name: "heat", value: "scorching", reason: "x") { value } }`); !strings.Contains(said(res), "one of cold, warm, hot") {
		t.Errorf("a value not in the enum: %v", res.Errors)
	}
	if res := run("smith", `mutation { setSetting(namespace: "forge", name: "strike_cap", value: "many", reason: "x") { value } }`); !strings.Contains(said(res), "is an int") {
		t.Errorf("a word for an int: %v", res.Errors)
	}
	// the history, and a reset
	res = run("smith", `{ settingHistory(namespace: "forge", name: "strike_cap") { old new actor reason } }`)
	if raw, _ := json.Marshal(res.Data["settingHistory"]); len(res.Errors) > 0 || string(raw) != `[{"actor":"user:smith","new":"6","old":null,"reason":"bigger hammers"}]` {
		t.Errorf("settingHistory: %s %v", raw, res.Errors)
	}
	res = run("smith", `mutation { resetSetting(namespace: "forge", name: "strike_cap", reason: "back to normal") { value from } }`)
	if raw, _ := json.Marshal(res.Data["resetSetting"]); len(res.Errors) > 0 || string(raw) != `{"from":"default","value":"3"}` {
		t.Errorf("resetSetting: %s %v", raw, res.Errors)
	}

	// the gateway's rules hold
	if res := run("reader", `mutation { setSetting(namespace: "forge", name: "strike_cap", value: "9", reason: "x") { value } }`); len(res.Errors) == 0 {
		t.Error("a caller who may not mutate changed a setting")
	}
	if res := run("reader", `{ settings(namespace: "forge") { name } }`); len(res.Errors) > 0 {
		t.Errorf("a reader reading settings: %v", res.Errors)
	}
	for _, doc := range []string{
		`{ settings(namespace: "forge") { name } }`,
		`mutation { setSetting(namespace: "forge", name: "strike_cap", value: "9", reason: "x") { value } }`,
		`{ settingHistory(namespace: "forge", name: "strike_cap") { reason } }`,
	} {
		if res := run("other", doc); !strings.Contains(said(res), `namespace "forge"`) {
			t.Errorf("a caller of another namespace ran %s: %v", doc, res.Errors)
		}
	}
	if cap, _ := clockgen.SettingStrikeCap(ctx, cli, "forge"); cap != 3 {
		t.Errorf("after the refused changes strike_cap is %d", cap)
	}
	// a service with no settings has none of the fields
	plain, err := loom.New(loom.Config{DB: pool, Bus: loom.NewMemoryBus(), Registry: orders.NewRegistry(), Blobs: loom.NewDirBlobStore(t.TempDir(), "http://blobs.local")})
	if err != nil {
		t.Fatal(err)
	}
	bare, err := loomgql.New(loomgql.Config{Services: []*loom.Client{plain}})
	if err != nil {
		t.Fatal(err)
	}
	bex, _ := loomgql.AsExecutor(bare)
	if res := bex.Exec(ctx, `{ settings(namespace: "acme") { name } }`, nil); len(res.Errors) == 0 {
		t.Error("a gateway over a service with no settings serves settings")
	}
}
