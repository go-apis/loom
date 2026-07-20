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

// peekAt looks n tokens past the cursor, clamping to the trailing EOF.
func (p *parser) peekAt(n int) token {
	if p.pos+n >= len(p.toks) {
		return p.toks[len(p.toks)-1]
	}
	return p.toks[p.pos+n]
}
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
		case "upcast":
			err = p.upcast()
		case "policy":
			err = p.reactor(&p.out.Policies, "policy")
		case "process":
			err = p.reactor(&p.out.Processes, "process")
		case "projection":
			err = p.projection()
		case "type":
			err = p.typeDecl()
		case "enum":
			err = p.enumDecl()
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
	for d := range dirs {
		if d != "snapshot" && d != "table" {
			return fmt.Errorf("aggregate %s: unknown directive @%s (aggregates take @snapshot, @table)", name, d)
		}
	}
	if args, ok := dirs["snapshot"]; ok && len(args) == 1 {
		every, err := strconv.Atoi(args[0])
		if err != nil || every < 1 {
			return fmt.Errorf("aggregate %s: @snapshot wants a positive count, got %q", name, args[0])
		}
		agg.Snapshot = every
	}
	if _, ok := dirs["table"]; ok {
		agg.Table = true
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
		case "upload":
			up, err := p.upload()
			if err != nil {
				return err
			}
			agg.Uploads = append(agg.Uploads, up)
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
		case "upload":
			up, err := p.upload()
			if err != nil {
				return err
			}
			rec.Uploads = append(rec.Uploads, up)
		default:
			return p.errf(t, "unexpected %q in record %s", t.text, name)
		}
	}
	p.out.Records = append(p.out.Records, rec)
	return nil
}

// upload parses a resumable-upload declaration; the hooks dispatch the
// enclosing aggregate/record's commands:
//
//	upload W9 {
//	  on started -> RequestW9
//	  on uploaded -> AttachW9
//	}
func (p *parser) upload() (*schema.Upload, error) {
	p.next()
	name, err := p.ident()
	if err != nil {
		return nil, err
	}
	up := &schema.Upload{Name: name}
	if err := p.expect("{"); err != nil {
		return nil, err
	}
	for !p.accept("}") {
		if err := p.expect("on"); err != nil {
			return nil, err
		}
		t := p.peek()
		hook, err := p.ident()
		if err != nil {
			return nil, err
		}
		if err := p.expect("->"); err != nil {
			return nil, err
		}
		cmd, err := p.ident()
		if err != nil {
			return nil, err
		}
		switch hook {
		case "started":
			up.OnStarted = cmd
		case "uploaded":
			up.OnUploaded = cmd
		default:
			return nil, p.errf(t, "upload %s: unknown hook %q (started, uploaded)", name, hook)
		}
	}
	return up, nil
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

// entity parses a read-model declaration. `@table` stores it in a typed
// per-entity table (real columns, SQL/BI-friendly) instead of the shared
// jsonb doc table.
// upcast declares payload migration for stored events written under older
// schema versions; each @from(n) names a hand-written hop n → n+1:
//
//	upcast OrderPlaced @from(1)
//	upcast OrderPlaced @from(2)     // or @from(1, 2)
func (p *parser) upcast() error {
	t := p.next()
	name, err := p.eventRef()
	if err != nil {
		return err
	}
	evt := p.registerEvent(name, "")
	dirs, err := p.directives()
	if err != nil {
		return err
	}
	for d := range dirs {
		if d != "from" {
			return p.errf(t, "upcast %s: unknown directive @%s (upcasts take @from)", name, d)
		}
	}
	args := dirs["from"]
	if len(args) == 0 {
		return p.errf(t, "upcast %s needs @from(version)", name)
	}
	for _, a := range args {
		v, err := strconv.Atoi(a)
		if err != nil || v < 1 {
			return p.errf(t, "upcast %s: @from wants a positive version, got %q", name, a)
		}
		evt.Upcasts = append(evt.Upcasts, v)
	}
	return nil
}

func (p *parser) entity() error {
	p.next()
	name, err := p.ident()
	if err != nil {
		return err
	}
	dirs, err := p.directives()
	if err != nil {
		return err
	}
	ent := &schema.Entity{Name: name}
	for d := range dirs {
		if d != "table" {
			return fmt.Errorf("entity %s: unknown directive @%s (entities take @table)", name, d)
		}
	}
	if _, ok := dirs["table"]; ok {
		ent.Table = true
	}
	pl, err := p.entityBlock(ent)
	if err != nil {
		return err
	}
	ent.State = pl
	p.out.Entities = append(p.out.Entities, ent)
	return nil
}

// entityBlock is fieldBlock plus join declarations:
//
//	join recipient -> recipients.RecipientSummary via recipient_id
//	join forms -> [filings.FormList] via recipient_id
//
// `join` followed by `:` still parses as a field named join.
func (p *parser) entityBlock(ent *schema.Entity) (*schema.Payload, error) {
	if err := p.expect("{"); err != nil {
		return nil, err
	}
	out := &schema.Payload{Type: "object", Properties: map[string]*schema.Payload{}}
	for !p.accept("}") {
		if p.peek().text == "join" && p.peekAt(1).text != ":" {
			p.next()
			if err := p.join(ent); err != nil {
				return nil, err
			}
			continue
		}
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
		dirs, err := p.directives()
		if err != nil {
			return nil, err
		}
		for d := range dirs {
			if d != "pii" && d != "secret" {
				return nil, fmt.Errorf("field %s: unknown directive @%s (fields take @pii, @secret)", name, d)
			}
		}
		if _, ok := dirs["pii"]; ok {
			ft.PII = true
		}
		if _, ok := dirs["secret"]; ok {
			// @secret rides the @pii machinery (sealed at rest, shreddable)
			// and additionally never reads back over the API.
			ft.PII = true
			ft.Secret = true
		}
		out.Properties[name] = ft
	}
	return out, nil
}

func (p *parser) join(ent *schema.Entity) error {
	t := p.peek()
	field, err := p.ident()
	if err != nil {
		return err
	}
	if err := p.expect("->"); err != nil {
		return err
	}
	j := &schema.Join{Field: field}
	j.List = p.accept("[")
	first, err := p.ident()
	if err != nil {
		return err
	}
	if p.accept(".") {
		j.Service = first
		if j.Entity, err = p.ident(); err != nil {
			return err
		}
	} else {
		j.Entity = first
	}
	if j.List {
		if err := p.expect("]"); err != nil {
			return err
		}
	}
	if err := p.expect("via"); err != nil {
		return err
	}
	if j.Via, err = p.ident(); err != nil {
		return err
	}
	for _, prev := range ent.Joins {
		if prev.Field == field {
			return p.errf(t, "entity %s declares join %s twice", ent.Name, field)
		}
	}
	ent.Joins = append(ent.Joins, j)
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

// projection parses a read-model maintainer. `@fold` hands the fold to
// a stub; `key(field)` routes an event to the row named by that payload
// field instead of the event's aggregate id:
//
//	projection massPayoutProgress -> MassPayoutProgress @fold {
//	  on MassPayoutStarted
//	  on PaymentSettled key(mass_payout_id)
//	}
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
	dirs, err := p.directives()
	if err != nil {
		return err
	}
	proj := &schema.Projection{Name: name, Entity: entity}
	for d := range dirs {
		if d != "fold" {
			return fmt.Errorf("projection %s: unknown directive @%s (projections take @fold)", name, d)
		}
	}
	if _, ok := dirs["fold"]; ok {
		proj.Fold = true
	}
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
		shot := &schema.ProjectionShot{Event: evt}
		if p.accept("key") {
			if err := p.expect("("); err != nil {
				return err
			}
			if shot.Key, err = p.ident(); err != nil {
				return err
			}
			if err := p.expect(")"); err != nil {
				return err
			}
		}
		proj.Subscriptions = append(proj.Subscriptions, shot)
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

// enumDecl parses a closed value set:
//
//	enum TinStatus { unknown pending_match matched mismatched }
func (p *parser) enumDecl() error {
	t := p.next()
	name, err := p.ident()
	if err != nil {
		return err
	}
	if err := p.expect("{"); err != nil {
		return err
	}
	e := &schema.Enum{Name: name}
	for !p.accept("}") {
		v, err := p.ident()
		if err != nil {
			return err
		}
		e.Values = append(e.Values, v)
	}
	if len(e.Values) == 0 {
		return p.errf(t, "enum %s has no values", name)
	}
	p.out.Enums = append(p.out.Enums, e)
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
		dirs, err := p.directives()
		if err != nil {
			return nil, err
		}
		for d := range dirs {
			if d != "pii" && d != "secret" {
				return nil, fmt.Errorf("field %s: unknown directive @%s (fields take @pii, @secret)", name, d)
			}
		}
		if _, ok := dirs["pii"]; ok {
			ft.PII = true
		}
		if _, ok := dirs["secret"]; ok {
			// @secret rides the @pii machinery (sealed at rest, shreddable)
			// and additionally never reads back over the API.
			ft.PII = true
			ft.Secret = true
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
	case "file":
		return &schema.Payload{Type: "object", Format: "file"}
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
