package contract

import "sort"

// The schema as the suite needs it, read from the gateway's own
// introspection: an operation exists here exactly when it exists on the
// gateway — generated, declared or hand-written.

type schema struct {
	roots   []rootField // queries then mutations, each by name
	objects []string    // object types, by name
	fields  map[string]map[string]bool
}

type rootField struct {
	kind string // query or mutation
	name string
	// returns is the named type under the wrappers, and many whether a
	// list wraps it
	returns string
	many    bool
	args    map[string]string // argument name → its type as GraphQL writes it
	// required is the arguments a call cannot omit
	required []string
	// namespaceType is the namespace argument's type, "" when it has none
	namespaceType string
}

// list reports whether the suite can read the field as a list with no
// more than a namespace: many objects with an id, a namespace and a
// limit argument, and nothing else required.
func (f rootField) list(s *schema) bool {
	if f.kind != "query" || !f.many || !s.fields[f.returns]["id"] || f.namespaceType == "" || f.args["limit"] == "" {
		return false
	}
	for _, name := range f.required {
		if name != "namespace" {
			return false
		}
	}
	return true
}

func readSchema(raw map[string]any) *schema {
	s := &schema{fields: map[string]map[string]bool{}}
	types := map[string]map[string]any{}
	for _, t := range list(raw["types"]) {
		typ := object(t)
		name := str(typ["name"])
		types[name] = typ
		if str(typ["kind"]) != "OBJECT" || len(name) > 1 && name[:2] == "__" {
			continue
		}
		s.fields[name] = map[string]bool{}
		for _, f := range list(typ["fields"]) {
			s.fields[name][str(object(f)["name"])] = true
		}
	}
	rootNames := map[string]bool{}
	for _, root := range []struct{ key, kind string }{{"queryType", "query"}, {"mutationType", "mutation"}} {
		name := str(object(raw[root.key])["name"])
		rootNames[name] = true
		var fields []rootField
		for _, f := range list(types[name]["fields"]) {
			field := object(f)
			rf := rootField{kind: root.kind, name: str(field["name"]), args: map[string]string{}}
			rf.returns, rf.many, _ = unwrap(object(field["type"]))
			for _, a := range list(field["args"]) {
				arg := object(a)
				_, _, text := unwrap(object(arg["type"]))
				rf.args[str(arg["name"])] = text
				if str(object(arg["type"])["kind"]) == "NON_NULL" {
					rf.required = append(rf.required, str(arg["name"]))
				}
			}
			rf.namespaceType = rf.args["namespace"]
			fields = append(fields, rf)
		}
		sort.Slice(fields, func(i, j int) bool { return fields[i].name < fields[j].name })
		s.roots = append(s.roots, fields...)
	}
	if sub := str(object(raw["subscriptionType"])["name"]); sub != "" {
		rootNames[sub] = true
	}
	for name := range s.fields {
		if !rootNames[name] {
			s.objects = append(s.objects, name)
		}
	}
	sort.Strings(s.objects)
	return s
}

// unwrap reads a type reference: the named type under the wrappers,
// whether a list wraps it, and the reference as GraphQL writes it.
func unwrap(t map[string]any) (name string, many bool, text string) {
	switch str(t["kind"]) {
	case "NON_NULL":
		name, many, text = unwrap(object(t["ofType"]))
		return name, many, text + "!"
	case "LIST":
		name, _, text = unwrap(object(t["ofType"]))
		return name, true, "[" + text + "]"
	default:
		return str(t["name"]), false, str(t["name"])
	}
}

func list(v any) []any { l, _ := v.([]any); return l }

func object(v any) map[string]any { m, _ := v.(map[string]any); return m }

func str(v any) string { s, _ := v.(string); return s }
