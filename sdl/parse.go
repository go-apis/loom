package sdl

import (
	"fmt"
	"strconv"

	"github.com/go-apis/loom/schema"
)

// Parse reads .loom source into the compiled schema and validates it.
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
	if err := p.out.Validate(); err != nil {
		return nil, err
	}
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
	if p.peek().kind != tString && p.peek().text == text {
		p.pos++
		return true
	}
	return false
}

func (p *parser) expect(text string) error {
	t := p.next()
	if t.kind == tString || t.text != text {
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
			err = p.aggregate()
		case "record":
			err = p.record()
		case "entity":
			err = p.entity()
		case "event":
			_, err = p.event()
		case "consume":
			err = p.consume()
		case "policy":
			err = p.reactor(&p.out.Policies, "policy")
		case "process":
			err = p.reactor(&p.out.Processes, "process")
		case "projection":
			err = p.projection()
		case "type":
			err = p.typeDecl()
		default:
			return p.errf(t, "unexpected %q at top level", t.text)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

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

func (p *parser) aggregate() error {
	p.next()
	name, err := p.ident()
	if err != nil {
		return err
	}
	dirs, err := p.directives()
	if err != nil {
		return err
	}
	agg := &schema.Aggregate{Name: name}
	if args, ok := dirs["snapshot"]; ok && len(args) == 1 {
		every, err := strconv.Atoi(args[0])
		if err != nil || every < 1 {
			return fmt.Errorf("aggregate %s: @snapshot wants a positive count, got %q", name, args[0])
		}
		agg.Snapshot = every
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

// record parses state-of-record declarations: like an aggregate but with
// no snapshot directive (the row is the source of truth).
func (p *parser) record() error {
	p.next()
	name, err := p.ident()
	if err != nil {
		return err
	}
	rec := &schema.Record{Name: name}
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
			rec.State = pl
		case "command":
			cmd, err := p.command()
			if err != nil {
				return err
			}
			rec.Commands = append(rec.Commands, cmd)
		case "event":
			if _, err := p.event(); err != nil {
				return err
			}
		default:
			return p.errf(t, "unexpected %q in record %s", t.text, name)
		}
	}
	p.out.Records = append(p.out.Records, rec)
	return nil
}

func (p *parser) command() (*schema.Command, error) {
	p.next()
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
		cmd.Emits, err = p.identList()
		if err != nil {
			return nil, err
		}
	}
	return cmd, nil
}

func (p *parser) event() (*schema.Event, error) {
	p.next()
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
	if args, ok := dirs["v"]; ok && len(args) == 1 {
		v, err := strconv.Atoi(args[0])
		if err != nil || v < 1 {
			return nil, fmt.Errorf("event %s: @v wants a positive version, got %q", name, args[0])
		}
		evt.Version = v
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
	p.next()
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
	evt.Publish = true
	if p.peek().text == "{" {
		pl, err := p.fieldBlock()
		if err != nil {
			return err
		}
		evt.Payload = pl
	}
	return nil
}

func (p *parser) entity() error {
	p.next()
	name, err := p.ident()
	if err != nil {
		return err
	}
	pl, err := p.fieldBlock()
	if err != nil {
		return err
	}
	p.out.Entities = append(p.out.Entities, &schema.Entity{Name: name, State: pl})
	return nil
}

func (p *parser) reactor(into *[]*schema.Reactor, kind string) error {
	p.next()
	name, err := p.ident()
	if err != nil {
		return err
	}
	r := &schema.Reactor{Name: name}
	if err := p.expect("{"); err != nil {
		return err
	}
	for !p.accept("}") {
		if p.peek().text == "effect" {
			t := p.next()
			if kind != "process" {
				return p.errf(t, "%s %s cannot declare effects — external calls belong in a process", kind, name)
			}
			effect, err := p.ident()
			if err != nil {
				return err
			}
			r.Effects = append(r.Effects, effect)
			continue
		}
		if err := p.expect("on"); err != nil {
			return err
		}
		evt, err := p.eventRef()
		if err != nil {
			return err
		}
		sub := &schema.Subscription{Event: evt}
		if p.accept("->") {
			sub.Dispatches, err = p.identList()
			if err != nil {
				return err
			}
		}
		r.Subscriptions = append(r.Subscriptions, sub)
	}
	*into = append(*into, r)
	return nil
}

func (p *parser) projection() error {
	p.next()
	name, err := p.ident()
	if err != nil {
		return err
	}
	if err := p.expect("->"); err != nil {
		return err
	}
	entity, err := p.ident()
	if err != nil {
		return err
	}
	proj := &schema.Projection{Name: name, Entity: entity}
	if err := p.expect("{"); err != nil {
		return err
	}
	for !p.accept("}") {
		if err := p.expect("on"); err != nil {
			return err
		}
		evt, err := p.eventRef()
		if err != nil {
			return err
		}
		proj.Subscriptions = append(proj.Subscriptions, &schema.ProjectionShot{Event: evt})
	}
	p.out.Projections = append(p.out.Projections, proj)
	return nil
}

func (p *parser) typeDecl() error {
	p.next()
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

func (p *parser) registerEvent(name, service string) *schema.Event {
	if evt, ok := p.events[name]; ok {
		if service != "" && evt.Service == "" {
			evt.Service = service
		}
		return evt
	}
	evt := &schema.Event{Name: name, Service: service, Version: 1}
	p.events[name] = evt
	p.out.Events = append(p.out.Events, evt)
	return evt
}

// eventRef parses Name or service.Name; the qualified form implicitly
// declares the foreign event if no consume block did.
func (p *parser) eventRef() (string, error) {
	first, err := p.ident()
	if err != nil {
		return "", err
	}
	if !p.accept(".") {
		return first, nil
	}
	name, err := p.ident()
	if err != nil {
		return "", err
	}
	evt := p.registerEvent(name, first)
	evt.Publish = true
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
		pl = fieldTypeNamed(t.text, format)
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

func fieldTypeNamed(name, format string) *schema.Payload {
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
