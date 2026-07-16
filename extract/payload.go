package extract

import (
	"go/types"
	"maps"
	"reflect"
	"sort"
	"strings"

	"github.com/go-apis/loom/schema"
)

// payload derives the JSON Schema subset for a root struct (command, event,
// aggregate state), following encoding/json semantics: json tags name
// properties, embedded structs flatten, unexported fields are skipped.
// `required` and `format` struct tags (the lib's convention) map to their
// JSON Schema equivalents. Nested named structs become shared Schema.Types
// entries referenced by Payload.Ref; the root itself is always inlined.
func (e *extractor) payload(n *types.Named) *schema.Payload {
	st, ok := n.Underlying().(*types.Struct)
	if !ok {
		return e.payloadFor(n, map[*types.Named]bool{})
	}
	p := e.structPayload(st, map[*types.Named]bool{n: true})
	if len(p.Properties) == 0 {
		return nil
	}
	return p
}

func (e *extractor) payloadFor(t types.Type, seen map[*types.Named]bool) *schema.Payload {
	switch tt := t.(type) {
	case *types.Pointer:
		p := e.payloadFor(tt.Elem(), seen)
		if p != nil {
			p.Nullable = true
		}
		return p
	case *types.Named:
		switch goPathOf(tt) {
		case "github.com/google/uuid.UUID":
			return &schema.Payload{Type: "string", Format: "uuid"}
		case "time.Time":
			return &schema.Payload{Type: "string", Format: "date-time"}
		}
		if st, ok := tt.Underlying().(*types.Struct); ok {
			return e.refOrInline(tt, st, seen)
		}
		if seen[tt] {
			return &schema.Payload{Type: "object"}
		}
		seen[tt] = true
		defer delete(seen, tt)
		return e.payloadFor(tt.Underlying(), seen)
	case *types.Basic:
		return basicPayload(tt)
	case *types.Slice:
		if b, ok := tt.Elem().(*types.Basic); ok && b.Kind() == types.Byte {
			return &schema.Payload{Type: "string", Format: "byte"}
		}
		return &schema.Payload{Type: "array", Items: e.payloadFor(tt.Elem(), seen)}
	case *types.Array:
		return &schema.Payload{Type: "array", Items: e.payloadFor(tt.Elem(), seen)}
	case *types.Map:
		return &schema.Payload{Type: "object"}
	case *types.Struct:
		return e.structPayload(tt, seen)
	case *types.Interface:
		return &schema.Payload{} // any
	default:
		return &schema.Payload{}
	}
}

// refOrInline registers a nested named struct as a shared type and returns a
// reference to it. Structs with no visible fields, and name collisions
// between distinct Go types, fall back to inline definitions.
func (e *extractor) refOrInline(n *types.Named, st *types.Struct, seen map[*types.Named]bool) *schema.Payload {
	name := n.Obj().Name()
	gp := goPathOf(n)

	if existing, ok := e.types[name]; ok {
		if existing.Go == gp {
			return &schema.Payload{Ref: name}
		}
		e.warnf("payload type name collision: %s vs %s; inlining", existing.Go, gp)
		if seen[n] {
			return &schema.Payload{Type: "object"}
		}
		seen[n] = true
		defer delete(seen, n)
		return e.structPayload(st, seen)
	}

	// placeholder first so recursive types resolve to the ref
	nt := &schema.NamedType{Name: name, Go: gp, Payload: &schema.Payload{Type: "object"}}
	e.types[name] = nt
	p := e.structPayload(st, seen)
	if len(p.Properties) == 0 {
		delete(e.types, name)
		return p
	}
	*nt.Payload = *p
	return &schema.Payload{Ref: name}
}

func (e *extractor) structPayload(st *types.Struct, seen map[*types.Named]bool) *schema.Payload {
	out := &schema.Payload{Type: "object", Properties: map[string]*schema.Payload{}}
	for i := 0; i < st.NumFields(); i++ {
		f := st.Field(i)
		tag := reflect.StructTag(st.Tag(i))

		if f.Anonymous() {
			// flatten embedded structs like encoding/json, bypassing the
			// shared-type registry — their fields belong to this object.
			inner := e.embeddedStruct(f.Type(), seen)
			if inner == nil {
				continue
			}
			maps.Copy(out.Properties, inner.Properties)
			out.Required = append(out.Required, inner.Required...)
			continue
		}
		if !f.Exported() {
			continue
		}

		name := f.Name()
		jsonTag := tag.Get("json")
		if jsonTag != "" {
			parts := strings.Split(jsonTag, ",")
			if parts[0] == "-" {
				continue
			}
			if parts[0] != "" {
				name = parts[0]
			}
		}

		p := e.payloadFor(f.Type(), seen)
		if p == nil {
			continue
		}
		if format := tag.Get("format"); format != "" && p.Ref == "" {
			p.Format = format
		}
		if isTrue(tag.Get("required")) {
			out.Required = append(out.Required, name)
		}
		out.Properties[name] = p
	}
	sort.Strings(out.Required)
	return out
}

func (e *extractor) embeddedStruct(t types.Type, seen map[*types.Named]bool) *schema.Payload {
	if n := concreteNamed(t); n != nil {
		st, ok := n.Underlying().(*types.Struct)
		if !ok || seen[n] {
			return nil
		}
		seen[n] = true
		defer delete(seen, n)
		return e.structPayload(st, seen)
	}
	if st, ok := t.Underlying().(*types.Struct); ok {
		return e.structPayload(st, seen)
	}
	return nil
}

func basicPayload(b *types.Basic) *schema.Payload {
	switch {
	case b.Info()&types.IsBoolean != 0:
		return &schema.Payload{Type: "boolean"}
	case b.Info()&types.IsInteger != 0:
		return &schema.Payload{Type: "integer"}
	case b.Info()&types.IsFloat != 0:
		return &schema.Payload{Type: "number"}
	case b.Info()&types.IsString != 0:
		return &schema.Payload{Type: "string"}
	default:
		return &schema.Payload{}
	}
}

func goPathOf(n *types.Named) string {
	if n.Obj().Pkg() == nil {
		return n.Obj().Name()
	}
	return n.Obj().Pkg().Path() + "." + n.Obj().Name()
}
