package gen_test

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/go-apis/loom/gen"
	"github.com/go-apis/loom/sdl"
)

func ordersSchema(t *testing.T) []byte {
	t.Helper()
	src, err := os.ReadFile("../internal/e2e/orders/schema/orders.loom")
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func TestOpenAPI(t *testing.T) {
	s, err := sdl.Parse(string(ordersSchema(t)))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := gen.OpenAPI(s)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Paths      map[string]any `json:"paths"`
		Components struct {
			Schemas map[string]any `json:"schemas"`
		} `json:"components"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/commands/PlaceOrder", "/entities/OrderSummary", "/entities/OrderSummary/{id}", "/aggregates/Order/{id}", "/uploads", "/files", "/series/SkuPrice", "/series/SkuPrice/buckets"} {
		if doc.Paths[path] == nil {
			t.Errorf("missing path %s", path)
		}
	}
	for _, s := range []string{"Order", "OrderSummary", "OrderItem", "PlaceOrderCommand", "FileRef", "Upload", "SkuPrice", "SeriesBucket"} {
		if doc.Components.Schemas[s] == nil {
			t.Errorf("missing component schema %s", s)
		}
	}
	// file fields render as FileRef refs
	if !strings.Contains(string(raw), `"contract": {
          "$ref": "#/components/schemas/FileRef"`) && !strings.Contains(string(raw), `"$ref": "#/components/schemas/FileRef"`) {
		t.Errorf("file field did not ref FileRef")
	}
}

func TestGraphQL(t *testing.T) {
	s, err := sdl.Parse(string(ordersSchema(t)))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := gen.GraphQL(s)
	if err != nil {
		t.Fatal(err)
	}
	out := string(raw)
	for _, want := range []string{
		"type Order {",
		"type OrderSummary {",
		"input PlaceOrderInput {",
		"aggregateId: UUID!",
		"placeOrder(input: PlaceOrderInput!): DispatchResult!",
		"orderSummarys(namespace: Namespace!, where: [FilterInput!]",
		"orderSummaryChanged(namespace: Namespace!, id: UUID!): OrderSummary!",
		"orderSummarysChanged(namespace: Namespace!, where: [FilterInput!], order: String, limit: Int, offset: Int): [OrderSummary!]!",
		"items: [OrderItemInput!]!",
		"customerId: UUID!",
		"scalar Long",
		"totalCents: Long", // schema int is int64: money rides Long, not Int
		"priceCents: Long!",
		"type FileRef {",
		"downloadUrl: String!",
		"input FileRefInput {",
		"type UploadSession {",
		"protocol: String!",
		"contract: FileRefInput!",
		"createContractUpload(input: CreateContractUploadInput!): UploadSession!",
		"directive @role(anyOf: [String!]!) on FIELD_DEFINITION",
		`shipOrder(input: ShipOrderInput!): DispatchResult! @role(anyOf: ["owner", "shipper"])`,
		"cancelOrder(input: CancelOrderInput!): DispatchResult!\n", // ungated: no directive
		// series: nullable row type, required-marked input, range +
		// bucket queries, append + retract mutations, shared shapes
		"type SkuPrice {",
		"input SkuPriceInput {",
		"observedAt: Time!",
		"enum SeriesBucketInterval {",
		"type SeriesBucket {",
		"type SeriesAppendResult {",
		"type SeriesRetractResult {",
		"skuPrices(namespace: Namespace!, where: [FilterInput!], since: Time, until: Time, order: String, limit: Int, offset: Int): [SkuPrice!]!",
		"skuPriceBuckets(namespace: Namespace!, value: String!, bucket: SeriesBucketInterval!, by: [String!], percentiles: [Float!], where: [FilterInput!], since: Time, until: Time, limit: Int): [SeriesBucket!]!",
		"appendSkuPrices(namespace: Namespace!, rows: [SkuPriceInput!]!): SeriesAppendResult!",
		"retractSkuPrices(namespace: Namespace!, rows: [SkuPriceInput!]!): SeriesRetractResult!",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestFoldersLayout(t *testing.T) {
	s, err := sdl.Parse(string(ordersSchema(t)))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	res, err := gen.Generate(s, gen.Config{Dir: dir, Package: "orders", Module: "example.com/orders", Layout: "folders"})
	if err != nil {
		t.Fatal(err)
	}
	// stubs land in per-kind packages, registry at the root
	for _, want := range []string{
		"aggregates/order.go",
		"policies/scheduleautocancel.go",
		"processes/shiponpayment.go",
		"projections/customerspend.go", // @fold projections get stubs
		"registry.go",
	} {
		found := false
		for _, w := range res.Written {
			if strings.HasSuffix(w, want) {
				found = true
			}
		}
		if !found {
			t.Fatalf("missing %s in %v", want, res.Written)
		}
	}
	stub, err := os.ReadFile(dir + "/aggregates/order.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(stub), "package aggregates") {
		t.Fatalf("stub package: %s", stub)
	}
	reg, err := os.ReadFile(dir + "/registry.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"package orders", `"example.com/orders/aggregates"`, "&aggregates.Order{}", "&processes.ShipOnPayment{}", "&policies.ScheduleAutoCancel{}", "&projections.CustomerSpend{}"} {
		if !strings.Contains(string(reg), want) {
			t.Fatalf("registry missing %q:\n%s", want, reg)
		}
	}
}

// TestIdempotentEffect proves `@idempotent` on an effect reaches the
// generated ReactorDef, where Once consults it before declaring doubt.
func TestIdempotentEffect(t *testing.T) {
	s, err := sdl.Parse(`
service s
aggregate A {
  state { x: string }
  command C -> E
  event E { x: string }
}
process p {
  on E -> C
  effect notify @idempotent
  effect charge
}
`)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	res, err := gen.Generate(s, gen.Config{Dir: dir, Package: "s", Module: "example.com/s"})
	if err != nil {
		t.Fatal(err)
	}
	var all strings.Builder
	for _, w := range res.Written {
		raw, err := os.ReadFile(w)
		if err != nil {
			t.Fatal(err)
		}
		all.Write(raw)
	}
	// gofmt aligns the values, so compare with spaces squeezed out
	flat := strings.Join(strings.Fields(all.String()), "")
	for _, want := range []string{`Effects:[]string{"charge","notify"}`, `IdempotentEffects:[]string{"notify"}`} {
		if !strings.Contains(flat, want) {
			t.Fatalf("generated code missing %q", want)
		}
	}
}

// TestRetryPolicy proves `@retry(max, min..max)` reaches the generated
// ReactorDef as a loom.RetryPolicy, and an undeclared process gets none.
func TestRetryPolicy(t *testing.T) {
	s, err := sdl.Parse(`
service s
aggregate A {
  state { x: string }
  command C -> E
  event E { x: string }
}
process p @retry(20, 5s..5m) {
  on E -> C
}
process q {
  on E -> C
}
`)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	res, err := gen.Generate(s, gen.Config{Dir: dir, Package: "s", Module: "example.com/s"})
	if err != nil {
		t.Fatal(err)
	}
	var all strings.Builder
	for _, w := range res.Written {
		raw, err := os.ReadFile(w)
		if err != nil {
			t.Fatal(err)
		}
		all.Write(raw)
	}
	flat := strings.Join(strings.Fields(all.String()), "")
	want := `Retry:&loom.RetryPolicy{Max:20,Min:5000000000,MaxBackoff:300000000000}`
	if n := strings.Count(flat, want); n != 1 {
		t.Fatalf("generated code has %q %d times, want once", want, n)
	}
}

// TestTypeDefsCarryRequired proves a schema type's required list reaches
// the registry as a loom.TypeDef, so the gateway serves a nested input's
// optional list nullable — the SDL already says so, and the runtime must
// agree with it.
func TestTypeDefsCarryRequired(t *testing.T) {
	s, err := sdl.Parse(`
service s
type Account { id: string! }
type Scaling {
  accounts: [Account]?
  tiers: [string]!
}
aggregate A {
  state { x: string }
  command C { scaling: Scaling? } -> E
  event E { x: string }
}
`)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	res, err := gen.Generate(s, gen.Config{Dir: dir, Package: "s", Module: "example.com/s"})
	if err != nil {
		t.Fatal(err)
	}
	var all strings.Builder
	for _, w := range res.Written {
		raw, err := os.ReadFile(w)
		if err != nil {
			t.Fatal(err)
		}
		all.Write(raw)
	}
	flat := strings.Join(strings.Fields(all.String()), "")
	for _, want := range []string{
		`Types:[]*loom.TypeDef{`,
		`{Name:"Account",Required:[]string{"id"}},`,
		`{Name:"Scaling",Required:[]string{"tiers"}},`,
	} {
		if !strings.Contains(flat, want) {
			t.Errorf("generated registry missing %q", want)
		}
	}

	raw, err := gen.GraphQL(s)
	if err != nil {
		t.Fatal(err)
	}
	sdlOut := string(raw)
	start := strings.Index(sdlOut, "input ScalingInput {")
	if start < 0 {
		t.Fatalf("no input ScalingInput in:\n%s", sdlOut)
	}
	block := sdlOut[start : start+strings.Index(sdlOut[start:], "}")]
	// The optional list is nullable (no `!` after the list) and the
	// required list is NON_NULL. Elements are non-null in both, as for
	// every loom list: the schema has no nullable-element form.
	fieldLine := func(name string) string {
		for _, l := range strings.Split(block, "\n") {
			if l = strings.TrimSpace(l); strings.HasPrefix(l, name+":") {
				return l
			}
		}
		t.Fatalf("ScalingInput has no %s in:\n%s", name, block)
		return ""
	}
	if got := fieldLine("accounts"); !strings.HasPrefix(got, "accounts: [AccountInput") || strings.HasSuffix(got, "]!") {
		t.Errorf("optional list: got %q, want accounts: [AccountInput…] with no `!` on the list", got)
	}
	if got := fieldLine("tiers"); got != "tiers: [String!]!" {
		t.Errorf("required list: got %q, want tiers: [String!]!", got)
	}
}
