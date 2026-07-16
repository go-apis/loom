package sdl

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/go-apis/loom/schema"
)

// Parse reads a .loom document into the compiled schema form. The SDL is
// authoring sugar: everything it expresses lands in the same schema.Schema
// the extractor emits (minus Go metadata, which only extraction knows).
func Parse(src string) (*schema.Schema, error) {
	toks, err := lex(src)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks, out: &schema.Schema{Loom: schema.Version}, events: map[string]*schema.Event{}}
	if err := p.schema(); err != nil {
		return nil, err
	}
	p.out.Sort()
	return p.out, nil
}

type parser struct {
	toks   []token
	pos    int
	out    *schema.Schema
	events map[string]*schema.Event
}

func (p *parser) peek() token { return p.toks[p.pos] }
func (p *parser) next() token { t := p.toks[p.pos]; p.pos++; return t }
func (p *parser) errf(t token, format string, args ...any) error {
	return fmt.Errorf("line %d: %s", t.line, fmt.Sprintf(format, args...))
}

func (p *parser) accept(text string) bool {
	if p.peek().text == text && p.peek().kind != tString {
		p.pos++
		return true
	}
	return false
}

func (p *parser) expect(text string) error {
	t := p.next()
	if t.text != text || t.kind == tString {
		return p.errf(t, "expected %q, got %q", text, t.text)
	}
	return nil
}

func (p *parser) ident() (string, error) {
	t := p.next()
	if t.kind != tIdent {
		return "", p.errf(t, "expected identifier, got %q", t.text)
	}
	return t.text, nil
}

func (p *parser) schema() error {
	if err := p.expect("service"); err != nil {
		return err
	}
	name, err := p.ident()
	if err != nil {
		return err
	}
	p.out.Service = name

	for p.peek().kind != tEOF {
		t := p.peek()
		var err error
		switch t.text {
		case "aggregate":
			err = p.aggregate("sourced")
		case "holder":
			err = p.aggregate("holder")
		case "entity":
			err = p.aggregate("entity")
		case "event":
			_, err = p.event()
		case "consume":
			err = p.consume()
		case "saga":
			err = p.subscriber("saga")
		case "handler":
			err = p.subscriber("event_handler")
		case "projector":
			err = p.projector()
		case "commands":
			err = p.commandHandler()
		case "type":
			err = p.typeDecl()
		default:
			return p.errf(t, "unexpected %q at top level", t.text)
		}
		if err != nil {
			return err
		}
	}
	return p.checkRefs()
}

// directives parses a run of @name or @name(arg, ...) annotations.
func (p *parser) directives() (map[string][]string, error) {
	out := map[string][]string{}
	for p.accept("@") {
		name, err := p.ident()
		if err != nil {
			return nil, err
		}
		var args []string
		if p.accept("(") {
			for {
				t := p.next()
				if t.kind != tIdent && t.kind != tNumber && t.kind != tString {
					return nil, p.errf(t, "bad directive argument %q", t.text)
				}
				args = append(args, t.text)
				if p.accept(")") {
					break
				}
				if err := p.expect(","); err != nil {
					return nil, err
				}
			}
		}
		out[name] = args
	}
	return out, nil
}

func (p *parser) aggregate(kind string) error {
	p.next() // keyword
	name, err := p.ident()
	if err != nil {
		return err
	}
	dirs, err := p.directives()
	if err != nil {
		return err
	}
	agg := &schema.Aggregate{Name: name, Kind: kind}
	if args, ok := dirs["snapshot"]; ok && len(args) == 1 {
		every, err := strconv.Atoi(args[0])
		if err != nil {
			return fmt.Errorf("aggregate %s: bad @snapshot(%s)", name, args[0])
		}
		agg.Snapshot = &schema.Snapshot{Every: every}
	}
	if args, ok := dirs["revision"]; ok && len(args) == 1 && agg.Snapshot != nil {
		agg.Snapshot.Revision = args[0]
	}
	if args, ok := dirs["project"]; ok && len(args) == 1 && args[0] == "false" {
		f := false
		agg.Project = &f
	}

	if err := p.expect("{"); err != nil {
		return err
	}
	for !p.accept("}") {
		t := p.peek()
		switch t.text {
		case "state":
			p.next()
			pl, err := p.fieldBlock()
			if err != nil {
				return err
			}
			agg.State = pl
		case "command":
			cmd, err := p.command()
			if err != nil {
				return err
			}
			agg.Commands = append(agg.Commands, cmd)
		case "event":
			if _, err := p.event(); err != nil {
				return err
			}
		default:
			return p.errf(t, "unexpected %q in aggregate %s", t.text, name)
		}
	}
	p.out.Aggregates = append(p.out.Aggregates, agg)
	return nil
}

func (p *parser) command() (*schema.Command, error) {
	p.next() // "command"
	name, err := p.ident()
	if err != nil {
		return nil, err
	}
	cmd := &schema.Command{Name: name}
	if p.peek().text == "{" {
		pl, err := p.fieldBlock()
		if err != nil {
			return nil, err
		}
		cmd.Payload = pl
	}
	if p.accept("->") {
		emits, err := p.identList()
		if err != nil {
			return nil, err
		}
		cmd.Emits = emits
	}
	return cmd, nil
}

func (p *parser) event() (*schema.Event, error) {
	p.next() // "event"
	name, err := p.ident()
	if err != nil {
		return nil, err
	}
	dirs, err := p.directives()
	if err != nil {
		return nil, err
	}
	evt := p.registerEvent(name, "")
	if _, ok := dirs["publish"]; ok {
		evt.Publish = true
	}
	if args, ok := dirs["alias"]; ok {
		evt.Aliases = args
	}
	if p.peek().text == "{" {
		pl, err := p.fieldBlock()
		if err != nil {
			return nil, err
		}
		evt.Payload = pl
	}
	return evt, nil
}

func (p *parser) consume() error {
	p.next() // "consume"
	service, err := p.ident()
	if err != nil {
		return err
	}
	if err := p.expect("."); err != nil {
		return err
	}
	name, err := p.ident()
	if err != nil {
		return err
	}
	evt := p.registerEvent(name, service)
	if p.peek().text == "{" {
		pl, err := p.fieldBlock()
		if err != nil {
			return err
		}
		evt.Payload = pl
	}
	return nil
}

// registerEvent returns the schema event for name, creating it if needed.
func (p *parser) registerEvent(name, service string) *schema.Event {
	if evt, ok := p.events[name]; ok {
		if service != "" && evt.Service == "" {
			evt.Service = service
		}
		return evt
	}
	evt := &schema.Event{Name: name, Service: service}
	p.events[name] = evt
	p.out.Events = append(p.out.Events, evt)
	return evt
}

func (p *parser) subscriber(kind string) error {
	p.next() // keyword
	name, err := p.ident()
	if err != nil {
		return err
	}
	dirs, err := p.directives()
	if err != nil {
		return err
	}
	group := "external"
	if args, ok := dirs["group"]; ok && len(args) == 1 {
		group = args[0]
	}
	h := &schema.Handler{Name: name, Kind: kind, Group: group, Execution: execution(group)}
	if _, ok := dirs["chunked"]; ok {
		h.Chunked = true
	}

	if err := p.expect("{"); err != nil {
		return err
	}
	for !p.accept("}") {
		if err := p.expect("on"); err != nil {
			return err
		}
		evtName, err := p.eventRef()
		if err != nil {
			return err
		}
		sub := &schema.Subscription{Event: evtName}
		if p.accept("->") {
			sub.Dispatches, err = p.identList()
			if err != nil {
				return err
			}
		}
		h.Subscribes = append(h.Subscribes, sub)
	}
	p.out.Handlers = append(p.out.Handlers, h)
	return nil
}

func (p *parser) projector() error {
	p.next() // "projector"
	name, err := p.ident()
	if err != nil {
		return err
	}
	dirs, err := p.directives()
	if err != nil {
		return err
	}
	group := "internal"
	if args, ok := dirs["group"]; ok && len(args) == 1 {
		group = args[0]
	}
	h := &schema.Handler{Name: name, Kind: "projector", Group: group, Execution: execution(group)}

	projects := map[string]bool{}
	if err := p.expect("{"); err != nil {
		return err
	}
	for !p.accept("}") {
		if err := p.expect("on"); err != nil {
			return err
		}
		evtName, err := p.eventRef()
		if err != nil {
			return err
		}
		if err := p.expect("project"); err != nil {
			return err
		}
		entities, err := p.identList()
		if err != nil {
			return err
		}
		h.Subscribes = append(h.Subscribes, &schema.Subscription{Event: evtName})
		for _, entity := range entities {
			projects[entity] = true
		}
	}
	for e := range projects {
		h.Projects = append(h.Projects, e)
	}
	sort.Strings(h.Projects)
	p.out.Handlers = append(p.out.Handlers, h)
	return nil
}

func (p *parser) commandHandler() error {
	p.next() // "commands"
	name, err := p.ident()
	if err != nil {
		return err
	}
	h := &schema.Handler{Name: name, Kind: "command_handler"}
	if err := p.expect("{"); err != nil {
		return err
	}
	for !p.accept("}") {
		if p.peek().text != "command" {
			return p.errf(p.peek(), "expected command in commands %s", name)
		}
		cmd, err := p.command()
		if err != nil {
			return err
		}
		h.Commands = append(h.Commands, cmd)
	}
	sort.Slice(h.Commands, func(i, j int) bool { return h.Commands[i].Name < h.Commands[j].Name })
	p.out.Handlers = append(p.out.Handlers, h)
	return nil
}

func (p *parser) typeDecl() error {
	p.next() // "type"
	name, err := p.ident()
	if err != nil {
		return err
	}
	pl, err := p.fieldBlock()
	if err != nil {
		return err
	}
	p.out.Types = append(p.out.Types, &schema.NamedType{Name: name, Payload: pl})
	return nil
}

// eventRef parses Name or service.Name; the qualified form implicitly
// declares the foreign event when no consume block does.
func (p *parser) eventRef() (string, error) {
	first, err := p.ident()
	if err != nil {
		return "", err
	}
	if !p.accept(".") {
		p.registerEvent(first, "")
		return first, nil
	}
	name, err := p.ident()
	if err != nil {
		return "", err
	}
	p.registerEvent(name, first)
	return name, nil
}

func (p *parser) identList() ([]string, error) {
	var out []string
	for {
		name, err := p.ident()
		if err != nil {
			return nil, err
		}
		out = append(out, name)
		if !p.accept(",") {
			return out, nil
		}
	}
}

func (p *parser) fieldBlock() (*schema.Payload, error) {
	if err := p.expect("{"); err != nil {
		return nil, err
	}
	out := &schema.Payload{Type: "object", Properties: map[string]*schema.Payload{}}
	for !p.accept("}") {
		name, err := p.ident()
		if err != nil {
			return nil, err
		}
		if err := p.expect(":"); err != nil {
			return nil, err
		}
		ft, err := p.fieldType()
		if err != nil {
			return nil, err
		}
		if p.accept("!") {
			out.Required = append(out.Required, name)
		}
		out.Properties[name] = ft
	}
	sort.Strings(out.Required)
	return out, nil
}

func (p *parser) fieldType() (*schema.Payload, error) {
	var pl *schema.Payload
	t := p.peek()
	switch {
	case p.accept("["):
		items, err := p.fieldType()
		if err != nil {
			return nil, err
		}
		if err := p.expect("]"); err != nil {
			return nil, err
		}
		pl = &schema.Payload{Type: "array", Items: items}
	case t.text == "{":
		var err error
		pl, err = p.fieldBlock()
		if err != nil {
			return nil, err
		}
	case t.kind == tIdent:
		p.next()
		format := ""
		if p.accept("(") {
			f, err := p.ident()
			if err != nil {
				return nil, err
			}
			format = f
			if err := p.expect(")"); err != nil {
				return nil, err
			}
		}
		pl = namedFieldType(t.text, format)
		if pl == nil {
			return nil, p.errf(t, "type %s takes no format argument", t.text)
		}
	default:
		return nil, p.errf(t, "expected a type, got %q", t.text)
	}
	if p.accept("?") {
		pl.Nullable = true
	}
	return pl, nil
}

func namedFieldType(name, format string) *schema.Payload {
	switch name {
	case "string":
		return &schema.Payload{Type: "string", Format: format}
	case "int":
		return &schema.Payload{Type: "integer", Format: format}
	case "float":
		return &schema.Payload{Type: "number", Format: format}
	case "bool":
		return &schema.Payload{Type: "boolean", Format: format}
	case "uuid":
		return &schema.Payload{Type: "string", Format: "uuid"}
	case "timestamp":
		return &schema.Payload{Type: "string", Format: "date-time"}
	case "bytes":
		return &schema.Payload{Type: "string", Format: "byte"}
	case "any":
		return &schema.Payload{}
	case "map":
		return &schema.Payload{Type: "object"}
	default:
		if format != "" {
			return nil
		}
		return &schema.Payload{Ref: name}
	}
}

func execution(group string) string {
	if group == "internal" {
		return "in-transaction"
	}
	return "async"
}

// checkRefs verifies every Ref resolves to a declared type.
func (p *parser) checkRefs() error {
	var missing []string
	seen := map[string]bool{}
	var walk func(pl *schema.Payload)
	walk = func(pl *schema.Payload) {
		if pl == nil {
			return
		}
		if pl.Ref != "" && p.out.FindType(pl.Ref) == nil && !seen[pl.Ref] {
			seen[pl.Ref] = true
			missing = append(missing, pl.Ref)
		}
		walk(pl.Items)
		for _, sub := range pl.Properties {
			walk(sub)
		}
	}
	for _, a := range p.out.Aggregates {
		walk(a.State)
		for _, c := range a.Commands {
			walk(c.Payload)
		}
	}
	for _, e := range p.out.Events {
		walk(e.Payload)
	}
	for _, h := range p.out.Handlers {
		for _, c := range h.Commands {
			walk(c.Payload)
		}
	}
	for _, t := range p.out.Types {
		walk(t.Payload)
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("undeclared types: %v", missing)
	}
	return nil
}
