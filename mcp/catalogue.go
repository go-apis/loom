package mcp

import (
	"fmt"
	"sort"
	"strings"
)

// The catalogue is the composed schema as a caller names it: every
// command (a mutation) and every read (a query), with the types they
// take and return. It is read once from the gateway's own introspection,
// so an operation exists here exactly when it exists on the gateway —
// generated, declared or hand-written.

const (
	kindCommand = "command"
	kindRead    = "read"
)

type typeRef struct {
	// text is the reference as GraphQL writes it: "[OrderLine!]!".
	text string
	// name is the named type under the wrappers.
	name string
	list bool
	// required is a non-null outermost wrapper.
	required bool
}

type field struct {
	name string
	desc string
	typ  typeRef
	args []field
}

type typeDef struct {
	kind   string // OBJECT, INPUT_OBJECT, ENUM, SCALAR…
	name   string
	desc   string
	fields []field // an object's fields, or an input object's
	values []string
}

type operation struct {
	kind string // command or read
	field
}

type catalogue struct {
	ops   map[string]*operation
	names []string // sorted
	types map[string]*typeDef
}

func newCatalogue(schema map[string]any) (*catalogue, error) {
	c := &catalogue{ops: map[string]*operation{}, types: map[string]*typeDef{}}
	for _, raw := range list(schema["types"]) {
		t := object(raw)
		td := &typeDef{kind: str(t["kind"]), name: str(t["name"]), desc: str(t["description"])}
		for _, f := range list(t["fields"]) {
			td.fields = append(td.fields, fieldOf(object(f)))
		}
		for _, f := range list(t["inputFields"]) {
			td.fields = append(td.fields, fieldOf(object(f)))
		}
		for _, v := range list(t["enumValues"]) {
			td.values = append(td.values, str(object(v)["name"]))
		}
		c.types[td.name] = td
	}
	roots := []struct{ key, kind string }{{"queryType", kindRead}, {"mutationType", kindCommand}}
	for _, root := range roots {
		name := str(object(schema[root.key])["name"])
		if name == "" {
			continue
		}
		rt := c.types[name]
		if rt == nil {
			return nil, fmt.Errorf("mcp: the schema names root type %q and does not define it", name)
		}
		for _, f := range rt.fields {
			if _, dup := c.ops[f.name]; dup {
				return nil, fmt.Errorf("mcp: %q is both a command and a read", f.name)
			}
			c.ops[f.name] = &operation{kind: root.kind, field: f}
			c.names = append(c.names, f.name)
		}
	}
	sort.Strings(c.names)
	return c, nil
}

func fieldOf(f map[string]any) field {
	out := field{name: str(f["name"]), desc: str(f["description"]), typ: refOf(object(f["type"]))}
	for _, a := range list(f["args"]) {
		out.args = append(out.args, fieldOf(object(a)))
	}
	return out
}

func refOf(t map[string]any) typeRef {
	ref := typeRef{required: str(t["kind"]) == "NON_NULL"}
	ref.text = refText(t, &ref)
	return ref
}

func refText(t map[string]any, ref *typeRef) string {
	switch str(t["kind"]) {
	case "NON_NULL":
		return refText(object(t["ofType"]), ref) + "!"
	case "LIST":
		ref.list = true
		return "[" + refText(object(t["ofType"]), ref) + "]"
	default:
		ref.name = str(t["name"])
		return ref.name
	}
}

// leaf reports whether a field is selected with no sub-selection.
func (c *catalogue) leaf(t typeRef) bool {
	td := c.types[t.name]
	return td == nil || (td.kind != "OBJECT" && td.kind != "INTERFACE" && td.kind != "UNION")
}

// search ranks operations against the words of query: every word must
// appear in the name or the description, a name match first.
// Only operations visible reports true for are returned; it is asked
// last, of matches only, and no more often than the limit needs.
func (c *catalogue) search(query, kind string, limit int, visible func(*operation) bool) []*operation {
	words := strings.Fields(strings.ToLower(query))
	type hit struct {
		op    *operation
		score int
	}
	var hits []hit
	for _, name := range c.names {
		op := c.ops[name]
		if kind != "" && op.kind != kind {
			continue
		}
		lname, ldesc := strings.ToLower(name), strings.ToLower(op.desc)
		score, ok := 0, true
		for _, w := range words {
			switch {
			case strings.Contains(lname, w):
				score += 2
			case strings.Contains(ldesc, w):
				score++
			default:
				ok = false
			}
		}
		if ok {
			hits = append(hits, hit{op, score})
		}
	}
	sort.SliceStable(hits, func(i, j int) bool { return hits[i].score > hits[j].score })
	out := []*operation{}
	for _, h := range hits {
		if len(out) == limit {
			break
		}
		if visible(h.op) {
			out = append(out, h.op)
		}
	}
	return out
}

// describeType is a type as a caller fills it in or reads it: an input
// object's fields nest (to depth, since inputs may nest inputs), an
// enum lists its values, an object lists its fields by reference.
func (c *catalogue) describeType(name string, depth int) map[string]any {
	td := c.types[name]
	if td == nil {
		return nil
	}
	out := map[string]any{"type": td.name, "kind": strings.ToLower(td.kind)}
	if td.desc != "" {
		out["description"] = td.desc
	}
	if len(td.values) > 0 {
		out["values"] = td.values
	}
	if len(td.fields) > 0 {
		out["fields"] = c.describeFields(td.fields, td.kind == "INPUT_OBJECT", depth)
	}
	return out
}

func (c *catalogue) describeFields(fields []field, nest bool, depth int) []map[string]any {
	out := make([]map[string]any, 0, len(fields))
	for _, f := range fields {
		d := map[string]any{"name": f.name, "type": f.typ.text}
		if f.desc != "" {
			d["description"] = f.desc
		}
		if td := c.types[f.typ.name]; td != nil {
			switch {
			case td.kind == "ENUM":
				d["values"] = td.values
			case td.kind == "INPUT_OBJECT" && nest && depth > 0:
				d["fields"] = c.describeFields(td.fields, true, depth-1)
			}
		}
		out = append(out, d)
	}
	return out
}

// selection writes the sub-selection for a value of type t. With no
// paths it is every field readable without arguments and without a
// sub-selection of its own; with paths ("total", "lines.sku") it is
// exactly those, each segment checked against the type so nothing a
// caller wrote reaches the document unchecked.
func (c *catalogue) selection(t typeRef, paths []string) (string, error) {
	if c.leaf(t) {
		if len(paths) > 0 {
			return "", fmt.Errorf("%s has no fields to choose", t.name)
		}
		return "", nil
	}
	td := c.types[t.name]
	if len(paths) == 0 {
		var names []string
		for _, f := range td.fields {
			if c.leaf(f.typ) && !needsArgs(f) {
				names = append(names, f.name)
			}
		}
		if len(names) == 0 {
			names = []string{"__typename"}
		}
		return " { " + strings.Join(names, " ") + " }", nil
	}
	// group paths by their first segment, in the order first asked
	var order []string
	rest := map[string][]string{}
	for _, p := range paths {
		head, tail, nested := strings.Cut(strings.TrimSpace(p), ".")
		if head == "" {
			return "", fmt.Errorf("an empty field name in %q", p)
		}
		if _, seen := rest[head]; !seen {
			order = append(order, head)
			rest[head] = nil
		}
		if nested {
			rest[head] = append(rest[head], tail)
		}
	}
	var b strings.Builder
	b.WriteString(" {")
	for _, name := range order {
		var f *field
		for i := range td.fields {
			if td.fields[i].name == name {
				f = &td.fields[i]
			}
		}
		if f == nil {
			return "", fmt.Errorf("%s has no field %q", td.name, name)
		}
		if needsArgs(*f) {
			return "", fmt.Errorf("%s.%s takes arguments and cannot be chosen here", td.name, name)
		}
		sub, err := c.selection(f.typ, rest[name])
		if err != nil {
			return "", err
		}
		b.WriteString(" " + name + sub)
	}
	b.WriteString(" }")
	return b.String(), nil
}

func needsArgs(f field) bool {
	for _, a := range f.args {
		if a.typ.required {
			return true
		}
	}
	return false
}

// document writes the one-operation GraphQL document that calls op with
// the arguments given, each as a typed variable. Argument names come
// from the catalogue, never from the caller's text.
func (c *catalogue) document(op *operation, args map[string]any, paths []string) (string, error) {
	sel, err := c.selection(op.typ, paths)
	if err != nil {
		return "", err
	}
	var decl, pass []string
	for _, a := range op.args {
		if _, given := args[a.name]; !given {
			continue
		}
		decl = append(decl, "$"+a.name+": "+a.typ.text)
		pass = append(pass, a.name+": $"+a.name)
	}
	if len(decl) != len(args) {
		known := make([]string, len(op.args))
		for i, a := range op.args {
			known[i] = a.name
		}
		for name := range args {
			if !contains(known, name) {
				return "", fmt.Errorf("%s takes no argument %q; it takes: %s", op.name, name, strings.Join(known, ", "))
			}
		}
	}
	word := "query"
	if op.kind == kindCommand {
		word = "mutation"
	}
	var b strings.Builder
	b.WriteString(word)
	if len(decl) > 0 {
		b.WriteString("(" + strings.Join(decl, ", ") + ")")
	}
	b.WriteString(" { " + op.name)
	if len(pass) > 0 {
		b.WriteString("(" + strings.Join(pass, ", ") + ")")
	}
	b.WriteString(sel + " }")
	return b.String(), nil
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func object(v any) map[string]any { m, _ := v.(map[string]any); return m }
func list(v any) []any            { l, _ := v.([]any); return l }
func str(v any) string            { s, _ := v.(string); return s }
