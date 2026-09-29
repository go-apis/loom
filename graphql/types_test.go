package graphql

import (
	"encoding/json"
	"reflect"
	"testing"

	gql "github.com/graphql-go/graphql"

	"github.com/go-apis/loom"
)

type sampleRow struct {
	LegalName string        `json:"legal_name"`
	Verified  bool          `json:"verified"`
	Reason    *string       `json:"reason"`
	Address   *sampleNested `json:"address"`
}

type sampleNested struct {
	Line1 string  `json:"line1"`
	Line2 *string `json:"line2"`
}

// Row (entity/state) fields serve nullable regardless of Go
// pointer-ness: the SDL contract declares them nullable, and a column
// added by migration is NULL for pre-existing rows — NonNull made those
// rows unreadable ("Cannot return null for non-nullable field").
func TestRowFieldsAreNullable(t *testing.T) {
	b := &builder{types: map[string]*typeEntry{}}
	obj, err := b.objectFor("SampleRow", sampleRow{})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"legalName", "verified", "reason", "address"} {
		f, ok := obj.Fields()[name]
		if !ok {
			t.Fatalf("field %s missing", name)
		}
		if _, nonNull := f.Type.(*gql.NonNull); nonNull {
			t.Errorf("row field %s is NonNull — rows predating the column hold NULL", name)
		}
	}
}

// Nested payload types keep their declared requiredness: the DSL marks
// them with `!` and the SDL emits NonNull for those, so the runtime
// must agree (Address.line1 stays String!).
func TestNestedTypeKeepsRequiredness(t *testing.T) {
	b := &builder{types: map[string]*typeEntry{}}
	if _, err := b.objectFor("SampleRow", sampleRow{}); err != nil {
		t.Fatal(err)
	}
	nested := b.types["sampleNested"]
	if nested == nil {
		// nested type registers under its Go type name
		for k, v := range b.types {
			if k != "SampleRow" {
				nested = v
			}
		}
	}
	if nested == nil {
		t.Fatal("nested type not registered")
	}
	if _, nonNull := nested.obj.Fields()["line1"].Type.(*gql.NonNull); !nonNull {
		t.Error("nested required field line1 lost NonNull")
	}
	if _, nonNull := nested.obj.Fields()["line2"].Type.(*gql.NonNull); nonNull {
		t.Error("nested optional field line2 gained NonNull")
	}
}

// Schema `bytes` fields ([]byte) serve as base64 Strings, matching the
// JSON wire form — before this, an aggregate with a bytes field made
// the whole gateway fail to compose ("unsupported type uint8").
func TestBytesFieldsServeAsString(t *testing.T) {
	type keyRow struct {
		KeyId     string `json:"key_id"`
		PublicKey []byte `json:"public_key"`
	}
	b := &builder{types: map[string]*typeEntry{}}
	obj, err := b.objectFor("KeyRow", keyRow{})
	if err != nil {
		t.Fatal(err)
	}
	f, ok := obj.Fields()["publicKey"]
	if !ok {
		t.Fatal("publicKey missing")
	}
	if f.Type != gql.String {
		t.Fatalf("bytes field served as %v, want String", f.Type)
	}
}

// Command input NonNull follows the SCHEMA's required list, not Go
// value-ness: optional strings and required strings are both plain
// `string` in the generated struct, and before this every value field
// came out NonNull — clients following the emitted SDL (which declares
// optionals nullable) were rejected at the type layer.
func TestCommandInputRequiredness(t *testing.T) {
	b := &builder{types: map[string]*typeEntry{}, inputs: map[string]gql.Input{}}
	in, _, err := b.commandInput("Sample", &sampleCmd{}, []string{"name"})
	if err != nil {
		t.Fatal(err)
	}
	obj, ok := in.(*gql.InputObject)
	if !ok {
		t.Fatalf("not an input object: %T", in)
	}
	fields := obj.Fields()
	if _, nonNull := fields["name"].Type.(*gql.NonNull); !nonNull {
		t.Error("required field name should be NonNull")
	}
	for _, f := range []string{"secretHash", "tags"} {
		if _, nonNull := fields[f].Type.(*gql.NonNull); nonNull {
			t.Errorf("optional field %s must stay nullable", f)
		}
	}
	for _, f := range []string{"aggregateId", "namespace"} {
		if _, nonNull := fields[f].Type.(*gql.NonNull); !nonNull {
			t.Errorf("envelope field %s should be NonNull", f)
		}
	}
}

type sampleCmd struct {
	loom.CommandBase
	Name       string   `json:"name"`
	SecretHash string   `json:"secret_hash"`
	Tags       []string `json:"tags"`
}

func (sampleCmd) LoomCommand() string { return "Sample" }

type sampleScaling struct {
	Accounts []sampleAccount `json:"accounts"`
	Tiers    []string        `json:"tiers"`
}

type sampleAccount struct {
	Id string `json:"id"`
}

// A nested input's NonNull follows the schema's required list, as a
// command's top level does: runsheet's `type NodeScaling { accounts:
// [NodeScalingAccount]? }` was served as `[NodeScalingAccountInput!]!`,
// because the generated `[]T` looks required to pointer-ness, and a
// client omitting the list was refused with "got invalid value".
func TestNestedInputFollowsSchemaRequired(t *testing.T) {
	b := &builder{
		types:        map[string]*typeEntry{},
		inputs:       map[string]gql.Input{},
		typeRequired: map[string]map[string]bool{"sampleScaling": {"tiers": true}},
	}
	in, conv, err := b.nestedInput(reflect.TypeOf(sampleScaling{}))
	if err != nil {
		t.Fatal(err)
	}
	obj, ok := in.(*gql.InputObject)
	if !ok {
		t.Fatalf("not an input object: %T", in)
	}
	fields := obj.Fields()
	tiers, ok := fields["tiers"].Type.(*gql.NonNull)
	if !ok {
		t.Fatalf("required list tiers is %v, want NON_NULL", fields["tiers"].Type)
	}
	if _, isList := tiers.OfType.(*gql.List); !isList {
		t.Errorf("tiers wraps %v, want a list", tiers.OfType)
	}
	if _, isList := fields["accounts"].Type.(*gql.List); !isList {
		t.Errorf("optional list accounts is %v, want a nullable list", fields["accounts"].Type)
	}

	// omitted or null, the optional list converts to a nil slice
	for name, arg := range map[string]map[string]any{
		"omitted": {"tiers": []any{"gold"}},
		"null":    {"tiers": []any{"gold"}, "accounts": nil},
	} {
		raw, err := json.Marshal(conv(arg))
		if err != nil {
			t.Fatal(err)
		}
		var got sampleScaling
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got.Accounts != nil {
			t.Errorf("%s: accounts = %#v, want nil", name, got.Accounts)
		}
		if len(got.Tiers) != 1 || got.Tiers[0] != "gold" {
			t.Errorf("%s: tiers = %#v, want [gold]", name, got.Tiers)
		}
	}

	// a present list still converts, keyed snake
	out := conv(map[string]any{"tiers": []any{}, "accounts": []any{map[string]any{"id": "a"}}}).(map[string]any)
	if accts, _ := out["accounts"].([]any); len(accts) != 1 {
		t.Errorf("accounts = %#v, want one account", out["accounts"])
	}
}

// A struct the registry names no required list for (hand-written, or a
// registry generated before Types) keeps pointer-based nullability.
func TestNestedInputWithoutTypeDefKeepsPointerRule(t *testing.T) {
	b := &builder{types: map[string]*typeEntry{}, inputs: map[string]gql.Input{}}
	in, _, err := b.nestedInput(reflect.TypeOf(sampleNested{}))
	if err != nil {
		t.Fatal(err)
	}
	fields := in.(*gql.InputObject).Fields()
	if _, nonNull := fields["line1"].Type.(*gql.NonNull); !nonNull {
		t.Error("value field line1 should be NonNull without a TypeDef")
	}
	if _, nonNull := fields["line2"].Type.(*gql.NonNull); nonNull {
		t.Error("pointer field line2 should stay nullable without a TypeDef")
	}
}
