package gen

import (
	"fmt"
	"sort"
	"strings"

	"github.com/go-apis/loom/schema"
)

// GraphQL renders the service as a GraphQL SDL fragment for a gqlgen
// gateway: state types, command inputs → mutations, list/get queries with a
// generic filter input, entity watches → subscriptions. The gateway's
// resolvers are thin calls to the service's Loom HTTP API.
func GraphQL(s *schema.Schema) ([]byte, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# Generated from the %s service's .loom schema by loom graphql. DO NOT EDIT.\n\n", s.Service)
	b.WriteString("scalar UUID\nscalar Time\nscalar Map\nscalar Long\n\n")

	b.WriteString(`input FilterInput {
  field: String!
  op: FilterOp!
  value: String!
}

enum FilterOp {
  EQ
  NE
  GT
  GTE
  LT
  LTE
  LIKE
}

type DispatchResult {
  status: String!
}

`)

	if usesFiles(s) {
		// FileRef is what a `file` field holds; UploadSession is what the
		// upload mutations return. size rides Long: GraphQL Int is 32-bit.
		b.WriteString(`type FileRef {
  id: UUID!
  key: String!
  name: String
  contentType: String
  size: Long!
  downloadUrl: String!
}

input FileRefInput {
  id: UUID!
  key: String!
  name: String
  contentType: String
  size: Long!
}

type UploadSession {
  file: FileRef!
  url: String!
  protocol: String!
}

`)
	}

	for _, t := range s.Types {
		gqlType(&b, "type", t.Name, t.Payload)
		gqlType(&b, "input", t.Name+"Input", t.Payload)
	}
	for _, a := range s.Aggregates {
		gqlType(&b, "type", a.Name, a.State)
	}
	for _, r := range s.Records {
		gqlType(&b, "type", r.Name, r.State)
	}
	for _, e := range s.Entities {
		gqlType(&b, "type", e.Name, e.State)
	}

	var mutations []string
	addCommands := func(cmds []*schema.Command) {
		for _, c := range cmds {
			gqlCommandInput(&b, c)
			mutations = append(mutations, fmt.Sprintf("  %s(input: %sInput!): DispatchResult!", lowerFirst(c.Name), c.Name))
		}
	}
	addUploads := func(uploads []*schema.Upload) {
		for _, u := range uploads {
			fmt.Fprintf(&b, "input Create%sUploadInput {\n  namespace: String!\n  id: UUID!\n  name: String\n  contentType: String\n  size: Long!\n}\n\n", u.Name)
			mutations = append(mutations, fmt.Sprintf("  create%sUpload(input: Create%sUploadInput!): UploadSession!", u.Name, u.Name))
		}
	}
	for _, a := range s.Aggregates {
		addCommands(a.Commands)
		addUploads(a.Uploads)
	}
	for _, r := range s.Records {
		addCommands(r.Commands)
		addUploads(r.Uploads)
	}

	var queries, subscriptions []string
	for _, a := range s.Aggregates {
		queries = append(queries, fmt.Sprintf("  %s(namespace: String!, id: UUID!): %s", lowerFirst(a.Name), a.Name))
		subscriptions = append(subscriptions, fmt.Sprintf("  %sChanged(namespace: String!, id: UUID!): %s!", lowerFirst(a.Name), a.Name))
	}
	for _, r := range s.Records {
		queries = append(queries,
			fmt.Sprintf("  %s(namespace: String!, id: UUID!): %s", lowerFirst(r.Name), r.Name),
			fmt.Sprintf("  %ss(namespace: String!, where: [FilterInput!], order: String, limit: Int, offset: Int): [%s!]!", lowerFirst(r.Name), r.Name))
	}
	for _, e := range s.Entities {
		queries = append(queries,
			fmt.Sprintf("  %s(namespace: String!, id: UUID!): %s", lowerFirst(e.Name), e.Name),
			fmt.Sprintf("  %ss(namespace: String!, where: [FilterInput!], order: String, limit: Int, offset: Int): [%s!]!", lowerFirst(e.Name), e.Name))
		subscriptions = append(subscriptions, fmt.Sprintf("  %sChanged(namespace: String!, id: UUID!): %s!", lowerFirst(e.Name), e.Name))
	}

	writeBlock(&b, "type Mutation", mutations)
	writeBlock(&b, "type Query", queries)
	writeBlock(&b, "type Subscription", subscriptions)
	return []byte(b.String()), nil
}

func gqlType(b *strings.Builder, kind, name string, pl *schema.Payload) {
	fmt.Fprintf(b, "%s %s {\n", kind, name)
	suffix := ""
	if kind == "input" {
		suffix = "Input"
	}
	for _, field := range sortedFieldNames(pl) {
		fmt.Fprintf(b, "  %s: %s\n", gqlFieldName(field), gqlFieldType(pl.Properties[field], pl, field, suffix))
	}
	b.WriteString("}\n\n")
}

func gqlCommandInput(b *strings.Builder, c *schema.Command) {
	fmt.Fprintf(b, "input %sInput {\n  aggregateId: UUID!\n  namespace: String!\n", c.Name)
	if c.Payload != nil {
		for _, field := range sortedFieldNames(c.Payload) {
			fmt.Fprintf(b, "  %s: %s\n", gqlFieldName(field), gqlFieldType(c.Payload.Properties[field], c.Payload, field, "Input"))
		}
	}
	b.WriteString("}\n\n")
}

func gqlFieldType(p *schema.Payload, parent *schema.Payload, field, refSuffix string) string {
	t := gqlTypeCore(p, refSuffix)
	required := false
	for _, r := range parent.Required {
		if r == field {
			required = true
		}
	}
	if required && !(p != nil && p.Nullable) {
		t += "!"
	}
	return t
}

func gqlTypeCore(p *schema.Payload, refSuffix string) string {
	if p == nil {
		return "Map"
	}
	if p.Ref != "" {
		return p.Ref + refSuffix
	}
	switch p.Type {
	case "string":
		switch p.Format {
		case "uuid":
			return "UUID"
		case "date-time":
			return "Time"
		default:
			return "String"
		}
	case "integer":
		return "Int"
	case "number":
		return "Float"
	case "boolean":
		return "Boolean"
	case "array":
		return "[" + gqlTypeCore(p.Items, refSuffix) + "!]"
	case "object":
		if p.Format == "file" {
			return "FileRef" + refSuffix
		}
		return "Map"
	default:
		return "Map"
	}
}

// usesFiles reports whether any payload has a `file` field or any
// aggregate/record declares an upload.
func usesFiles(s *schema.Schema) bool {
	var found bool
	var walk func(pl *schema.Payload)
	walk = func(pl *schema.Payload) {
		if pl == nil || found {
			return
		}
		if pl.IsFile() {
			found = true
			return
		}
		walk(pl.Items)
		for _, sub := range pl.Properties {
			walk(sub)
		}
	}
	for _, a := range s.Aggregates {
		if len(a.Uploads) > 0 {
			return true
		}
		walk(a.State)
		for _, c := range a.Commands {
			walk(c.Payload)
		}
	}
	for _, r := range s.Records {
		if len(r.Uploads) > 0 {
			return true
		}
		walk(r.State)
		for _, c := range r.Commands {
			walk(c.Payload)
		}
	}
	for _, e := range s.Entities {
		walk(e.State)
	}
	for _, e := range s.Events {
		walk(e.Payload)
	}
	for _, t := range s.Types {
		walk(t.Payload)
	}
	return found
}

func gqlFieldName(snake string) string {
	parts := strings.Split(snake, "_")
	out := parts[0]
	for _, p := range parts[1:] {
		if p == "" {
			continue
		}
		out += strings.ToUpper(p[:1]) + p[1:]
	}
	return out
}

func lowerFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToLower(s[:1]) + s[1:]
}

func sortedFieldNames(pl *schema.Payload) []string {
	if pl == nil {
		return nil
	}
	out := make([]string, 0, len(pl.Properties))
	for name := range pl.Properties {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func writeBlock(b *strings.Builder, header string, lines []string) {
	if len(lines) == 0 {
		return
	}
	sort.Strings(lines)
	fmt.Fprintf(b, "%s {\n%s\n}\n\n", header, strings.Join(lines, "\n"))
}
