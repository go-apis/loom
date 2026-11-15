package e2e_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
	loomgql "github.com/go-apis/loom/graphql"
	"github.com/go-apis/loom/internal/e2e/orders"
)

// TestGatewayListPagesByCursor: a list read through the gateway pages
// by cursor. Each row carries its cursor, the list's `after` takes it,
// and walking the pages visits every row once though every row ties on
// the order column — in the default order and in a caller's own, either
// direction. A cursor the gateway did not give out is refused, and so
// is the `offset` argument, which lists no longer take.
func TestGatewayListPagesByCursor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	pool := testDB(t, ctx)
	cli, err := loom.New(loom.Config{DB: pool, Bus: loom.NewMemoryBus(), Registry: orders.NewRegistry(), Blobs: loom.NewDirBlobStore(t.TempDir(), "http://blobs.local")})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	gateway, err := loomgql.New(loomgql.Config{Services: []*loom.Client{cli}})
	if err != nil {
		t.Fatal(err)
	}
	ex, _ := loomgql.AsExecutor(gateway)
	actx := loomgql.WithAccess(ctx, loomgql.Access{Namespaces: []string{"acme"}, Mutate: true, Attrs: map[string]string{"kind": "admin"}})

	// 25 parcels, three labels among them, all updated at one instant
	const total = 25
	want := map[string]bool{}
	depot := uuid.NewString()
	for i := 0; i < total; i++ {
		id := uuid.NewString()
		want[id] = true
		res := ex.Exec(actx, `mutation($in: RegisterParcelInput!) { registerParcel(input: $in) { status } }`,
			map[string]any{"in": map[string]any{"aggregateId": id, "namespace": "acme", "depotId": depot, "label": fmt.Sprintf("label-%d", i%3)}})
		if len(res.Errors) > 0 {
			t.Fatal(res.Errors)
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE loom_t_orders_parcel SET updated_at = '2026-11-15T00:00:00Z'`); err != nil {
		t.Fatal(err)
	}

	walk := func(order string) []string {
		t.Helper()
		var ids []string
		after := ""
		for page := 0; page < 10; page++ {
			vars := map[string]any{"limit": 10}
			if order != "" {
				vars["order"] = order
			}
			if after != "" {
				vars["after"] = after
			}
			res := ex.Exec(actx, `query($limit: Int, $order: String, $after: String) {
				parcels(namespace: "acme", limit: $limit, order: $order, after: $after) { id cursor } }`, vars)
			if len(res.Errors) > 0 {
				t.Fatalf("order %q page %d: %v", order, page, res.Errors)
			}
			rows, _ := res.Data["parcels"].([]any)
			if len(rows) == 0 {
				break
			}
			for _, r := range rows {
				row := r.(map[string]any)
				ids = append(ids, row["id"].(string))
				after, _ = row["cursor"].(string)
				if after == "" {
					t.Fatalf("order %q: a listed row has no cursor: %v", order, row)
				}
			}
			if len(rows) < 10 {
				break
			}
		}
		return ids
	}
	for _, order := range []string{"", "-updated_at", "label", "-label"} {
		ids := walk(order)
		seen := map[string]int{}
		for _, id := range ids {
			seen[id]++
		}
		if len(ids) != total || len(seen) != total {
			t.Errorf("order %q: paged %d rows, %d distinct, want %d each once", order, len(ids), len(seen), total)
		}
		for id := range want {
			if seen[id] != 1 {
				t.Errorf("order %q: parcel %s was listed %d times", order, id, seen[id])
				break
			}
		}
	}

	// the pages come in the order asked: by label, labels never go back
	res := ex.Exec(actx, `{ parcels(namespace: "acme", limit: 25, order: "label") { label } }`, nil)
	if len(res.Errors) > 0 {
		t.Fatal(res.Errors)
	}
	last := ""
	for _, r := range res.Data["parcels"].([]any) {
		label := r.(map[string]any)["label"].(string)
		if label < last {
			t.Fatalf("ordered by label, %q came after %q", label, last)
		}
		last = label
	}

	// a cursor that is not one, and the argument lists no longer take
	res = ex.Exec(actx, `{ parcels(namespace: "acme", after: "page-two") { id } }`, nil)
	if len(res.Errors) == 0 || !strings.Contains(res.Errors[0], "not a cursor") {
		t.Errorf("a made-up cursor: %v", res.Errors)
	}
	res = ex.Exec(actx, `{ parcels(namespace: "acme", offset: 10) { id } }`, nil)
	if len(res.Errors) == 0 || !strings.Contains(res.Errors[0], "offset") {
		t.Errorf("the offset argument: %v", res.Errors)
	}
	// a single read has no place in a list: no cursor
	one := ""
	for id := range want {
		one = id
		break
	}
	res = ex.Exec(actx, `query($id: UUID!) { parcel(namespace: "acme", id: $id) { id cursor } }`, map[string]any{"id": one})
	if len(res.Errors) > 0 || res.Data["parcel"].(map[string]any)["cursor"] != nil {
		t.Errorf("a single parcel's cursor: %v %v", res.Data["parcel"], res.Errors)
	}
}
