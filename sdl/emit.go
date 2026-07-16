package sdl

import (
	"fmt"
	"sort"
	"strings"

	"github.com/go-apis/loom/schema"
)

// Emit renders a schema as .loom source. Events emitted by exactly one
// aggregate nest inside it for readability; everything else is top-level.
// Go metadata is extraction-only and does not appear in the SDL.
func Emit(s *schema.Schema) []byte {
	s.Sort()
	e := &emitter{s: s}
	e.printf("service %s\n", s.Service)

	nested := e.nestedEvents()

	for _, a := range s.Aggregates {
		e.aggregate(a, nested)
	}

	var wroteEvent bool
	for _, evt := range s.Events {
		if evt.Service != "" || nested[evt.Name] != "" {
			continue
		}
		if !wroteEvent {
			e.printf("\n")
			wroteEvent = true
		}
		e.event(evt, "")
	}

	var wroteConsume bool
	for _, evt := range s.Events {
		if evt.Service == "" {
			continue
		}
		if !wroteConsume {
			e.printf("\n")
			wroteConsume = true
		}
		e.printf("consume %s.%s", evt.Service, evt.Name)
		e.payloadBlock(evt.Payload, "")
		e.printf("\n")
	}

	for _, h := range s.Handlers {
		e.handler(h)
	}

	if len(s.Types) > 0 {
		e.printf("\n")
		for _, t := range s.Types {
			e.printf("type %s ", t.Name)
			e.fields(t.Payload, "")
			e.printf("\n")
		}
	}

	return []byte(e.b.String())
}

type emitter struct {
	s *schema.Schema
	b strings.Builder
}

func (e *emitter) printf(format string, args ...any) {
	fmt.Fprintf(&e.b, format, args...)
}

// nestedEvents maps each own event emitted by exactly one aggregate to that
// aggregate's name.
func (e *emitter) nestedEvents() map[string]string {
	emitters := map[string]map[string]bool{}
	for _, a := range e.s.Aggregates {
		for _, c := range a.Commands {
			for _, evt := range c.Emits {
				if emitters[evt] == nil {
					emitters[evt] = map[string]bool{}
				}
				emitters[evt][a.Name] = true
			}
		}
	}
	out := map[string]string{}
	for evt, aggs := range emitters {
		ev := e.s.FindEvent(evt)
		if ev == nil || ev.Service != "" || len(aggs) != 1 {
			continue
		}
		for a := range aggs {
			out[evt] = a
		}
	}
	return out
}

func (e *emitter) aggregate(a *schema.Aggregate, nested map[string]string) {
	keyword := map[string]string{"sourced": "aggregate", "holder": "holder", "entity": "entity"}[a.Kind]
	if keyword == "" {
		keyword = "aggregate"
	}
	e.printf("\n%s %s", keyword, a.Name)
	if a.Snapshot != nil {
		e.printf(" @snapshot(%d)", a.Snapshot.Every)
		if a.Snapshot.Revision != "" {
			e.printf(" @revision(%s)", a.Snapshot.Revision)
		}
	}
	if a.Project != nil && !*a.Project {
		e.printf(" @project(false)")
	}
	e.printf(" {\n")

	if a.State != nil && len(a.State.Properties) > 0 {
		e.printf("  state ")
		e.fields(a.State, "  ")
		e.printf("\n")
	}

	for _, c := range a.Commands {
		e.command(c, "  ")
	}

	var events []string
	for evt, agg := range nested {
		if agg == a.Name {
			events = append(events, evt)
		}
	}
	sort.Strings(events)
	for _, name := range events {
		if evt := e.s.FindEvent(name); evt != nil {
			e.event(evt, "  ")
		}
	}

	e.printf("}\n")
}

func (e *emitter) command(c *schema.Command, indent string) {
	e.printf("\n%scommand %s", indent, c.Name)
	e.payloadBlock(c.Payload, indent)
	if len(c.Emits) > 0 {
		e.printf(" -> %s", strings.Join(c.Emits, ", "))
	}
	e.printf("\n")
}

func (e *emitter) event(evt *schema.Event, indent string) {
	if indent != "" {
		e.printf("\n")
	}
	e.printf("%sevent %s", indent, evt.Name)
	if evt.Publish {
		e.printf(" @publish")
	}
	if len(evt.Aliases) > 0 {
		e.printf(" @alias(%s)", strings.Join(evt.Aliases, ", "))
	}
	e.payloadBlock(evt.Payload, indent)
	e.printf("\n")
}

func (e *emitter) handler(h *schema.Handler) {
	switch h.Kind {
	case "command_handler":
		e.printf("\ncommands %s {\n", h.Name)
		for _, c := range h.Commands {
			e.command(c, "  ")
		}
		e.printf("}\n")
		return
	case "projector":
		e.printf("\nprojector %s", h.Name)
		if h.Group != "internal" {
			e.printf(" @group(%s)", h.Group)
		}
	case "saga":
		e.printf("\nsaga %s", h.Name)
		if h.Group != "external" {
			e.printf(" @group(%s)", h.Group)
		}
	default:
		e.printf("\nhandler %s", h.Name)
		if h.Group != "external" {
			e.printf(" @group(%s)", h.Group)
		}
	}
	if h.Chunked {
		e.printf(" @chunked")
	}
	e.printf(" {\n")
	for _, sub := range h.Subscribes {
		e.printf("  on %s", e.eventRef(sub.Event))
		if h.Kind == "projector" {
			// schema stores projected entities at handler level; a
			// single-entity projector is the overwhelmingly common case.
			if len(h.Projects) == 1 {
				e.printf(" project %s", h.Projects[0])
			} else {
				e.printf(" project %s", strings.Join(h.Projects, ", "))
			}
		}
		if len(sub.Dispatches) > 0 {
			e.printf(" -> %s", strings.Join(sub.Dispatches, ", "))
		}
		e.printf("\n")
	}
	e.printf("}\n")
}

func (e *emitter) eventRef(name string) string {
	if evt := e.s.FindEvent(name); evt != nil && evt.Service != "" {
		return evt.Service + "." + name
	}
	return name
}

func (e *emitter) payloadBlock(p *schema.Payload, indent string) {
	if p == nil || len(p.Properties) == 0 {
		return
	}
	e.printf(" ")
	e.fields(p, indent)
}

// fields renders an object payload as a { name: type } block.
func (e *emitter) fields(p *schema.Payload, indent string) {
	e.printf("{\n")
	required := map[string]bool{}
	for _, r := range p.Required {
		required[r] = true
	}
	names := make([]string, 0, len(p.Properties))
	for name := range p.Properties {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		e.printf("%s  %s: %s", indent, name, e.typeString(p.Properties[name], indent+"  "))
		if required[name] {
			e.printf("!")
		}
		e.printf("\n")
	}
	e.printf("%s}", indent)
}

func (e *emitter) typeString(p *schema.Payload, indent string) string {
	if p == nil {
		return "any"
	}
	suffix := ""
	if p.Nullable {
		suffix = "?"
	}
	if p.Ref != "" {
		return p.Ref + suffix
	}
	switch p.Type {
	case "":
		return "any" + suffix
	case "array":
		return "[" + e.typeString(p.Items, indent) + "]" + suffix
	case "object":
		if len(p.Properties) == 0 {
			return "map" + suffix
		}
		sub := &emitter{s: e.s}
		sub.fields(p, indent)
		return sub.b.String() + suffix
	case "string":
		switch p.Format {
		case "uuid":
			return "uuid" + suffix
		case "date-time":
			return "timestamp" + suffix
		case "byte":
			return "bytes" + suffix
		case "":
			return "string" + suffix
		default:
			return "string(" + p.Format + ")" + suffix
		}
	case "integer":
		return withFormat("int", p.Format) + suffix
	case "number":
		return withFormat("float", p.Format) + suffix
	case "boolean":
		return withFormat("bool", p.Format) + suffix
	default:
		return p.Type + suffix
	}
}

func withFormat(name, format string) string {
	if format == "" {
		return name
	}
	return name + "(" + format + ")"
}
