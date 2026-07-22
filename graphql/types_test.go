package graphql

import (
	"testing"

	gql "github.com/graphql-go/graphql"
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
