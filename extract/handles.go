package extract

import (
	"go/ast"
	"go/types"
	"sort"
	"strconv"

	"github.com/go-apis/loom/schema"
	"golang.org/x/tools/go/packages"
)

type method struct {
	obj  *types.Func
	sig  *types.Signature
	decl *ast.FuncDecl     // nil for methods promoted from dependencies
	pkg  *packages.Package // package of decl
}

func (e *extractor) methods(n *types.Named) []method {
	var out []method
	for sel := range types.NewMethodSet(types.NewPointer(n)).Methods() {
		obj, ok := sel.Obj().(*types.Func)
		if !ok || !obj.Exported() {
			continue
		}
		out = append(out, method{
			obj:  obj,
			sig:  obj.Type().(*types.Signature),
			decl: e.decls[obj],
			pkg:  e.declPkg[obj],
		})
	}
	return out
}

func (e *extractor) isCtx(t types.Type) bool {
	return types.Identical(t, e.ctx) || types.AssignableTo(t, e.ctx)
}

func isErr(t types.Type) bool {
	return types.Identical(t, types.Universe.Lookup("error").Type())
}

// isEventPtr reports whether t is *es.Event (sagahandle.go's in3 rule).
func (e *extractor) isEventPtr(t types.Type) bool {
	ptr, ok := t.Underlying().(*types.Pointer)
	return ok && types.Identical(ptr.Elem(), e.event)
}

func (e *extractor) implementsCommand(t types.Type) bool {
	return types.Implements(t, e.ifaces["Command"]) ||
		types.Implements(types.NewPointer(t), e.ifaces["Command"])
}

// commandHandle mirrors NewCommandHandle: exported, not Apply,
// (ctx, Command) error.
func (e *extractor) commandHandle(m method) (*types.Named, bool) {
	if m.obj.Name() == "Apply" {
		return nil, false
	}
	p, r := m.sig.Params(), m.sig.Results()
	if p.Len() != 2 || r.Len() != 1 || !e.isCtx(p.At(0).Type()) || !isErr(r.At(0).Type()) {
		return nil, false
	}
	if !e.implementsCommand(p.At(1).Type()) {
		return nil, false
	}
	return concreteNamed(p.At(1).Type()), true
}

// sagaHandle mirrors NewSagaHandle: exported, not Run,
// (ctx, *es.Event, data) ([]es.Command, error).
func (e *extractor) sagaHandle(m method) (*types.Named, bool) {
	if m.obj.Name() == "Run" {
		return nil, false
	}
	p, r := m.sig.Params(), m.sig.Results()
	if p.Len() != 3 || r.Len() != 2 {
		return nil, false
	}
	if !e.isCtx(p.At(0).Type()) || !e.isEventPtr(p.At(1).Type()) || !isErr(r.At(1).Type()) {
		return nil, false
	}
	slice, ok := r.At(0).Type().Underlying().(*types.Slice)
	if !ok || !types.AssignableTo(slice.Elem(), e.es.Scope().Lookup("Command").Type()) {
		return nil, false
	}
	return concreteNamed(p.At(2).Type()), true
}

// eventHandlerHandle mirrors NewEventHandlerHandle: exported, not Run,
// (ctx, *es.Event, data) error.
func (e *extractor) eventHandlerHandle(m method) (*types.Named, bool) {
	if m.obj.Name() == "Run" {
		return nil, false
	}
	p, r := m.sig.Params(), m.sig.Results()
	if p.Len() != 3 || r.Len() != 1 {
		return nil, false
	}
	if !e.isCtx(p.At(0).Type()) || !e.isEventPtr(p.At(1).Type()) || !isErr(r.At(0).Type()) {
		return nil, false
	}
	return concreteNamed(p.At(2).Type()), true
}

// projectorHandle mirrors NewProjectorHandle: exported,
// (ctx, entity, data) error. Returns (entity, event).
func (e *extractor) projectorHandle(m method) (*types.Named, *types.Named, bool) {
	p, r := m.sig.Params(), m.sig.Results()
	if p.Len() != 3 || r.Len() != 1 {
		return nil, nil, false
	}
	if !e.isCtx(p.At(0).Type()) || !isErr(r.At(0).Type()) {
		return nil, nil, false
	}
	return concreteNamed(p.At(1).Type()), concreteNamed(p.At(2).Type()), true
}

// handlerConfig mirrors NewEventHandlerConfig: name/group from the es tag on
// an embedded BaseEventHandler/BaseSaga/BaseProjector, projector defaulting
// to the internal group.
func (e *extractor) handlerConfig(n *types.Named) (name, group string) {
	name = n.Obj().Name()
	group = "external"
	for _, fieldName := range []string{"BaseEventHandler", "BaseSaga", "BaseProjector"} {
		tag, ok := e.embeddedTag(n, fieldName)
		if !ok {
			continue
		}
		if fieldName == "BaseProjector" {
			group = "internal"
		}
		for _, item := range splitTag(tag) {
			key, val, hasVal := cutTag(item)
			switch {
			case key == "group" && hasVal:
				group = val
			case !hasVal && key != "":
				name = key
			}
		}
	}
	return name, group
}

func execution(group string) string {
	if group == "internal" {
		return "in-transaction"
	}
	return "async"
}

// addSubscriber handles sagas and event handlers, which share the
// subscription discovery shape and differ only in handle signature.
func (e *extractor) addSubscriber(n *types.Named, kind string) {
	name, group := e.handlerConfig(n)
	h := &schema.Handler{
		Name:      name,
		Kind:      kind,
		Go:        goPath(n),
		Group:     group,
		Execution: execution(group),
	}
	for _, m := range e.methods(n) {
		var data *types.Named
		var ok bool
		if kind == "saga" {
			data, ok = e.sagaHandle(m)
		} else {
			data, ok = e.eventHandlerHandle(m)
		}
		if !ok {
			continue
		}
		if data == nil {
			e.warnf("%s.%s: event parameter is not a named struct", name, m.obj.Name())
			continue
		}
		sub := &schema.Subscription{Event: e.registerEvent(data).Name}
		if kind == "saga" {
			sub.Dispatches = e.dispatches(m)
		}
		h.Subscribes = append(h.Subscribes, sub)
		if e.chunkedDispatch(m) {
			h.Chunked = true
		}
	}
	if len(h.Subscribes) == 0 {
		e.warnf("%s %s has no matching handle methods (never receives events)", kind, name)
	}
	e.out.Handlers = append(e.out.Handlers, h)
}

func (e *extractor) addProjector(n *types.Named) {
	name, group := e.handlerConfig(n)
	h := &schema.Handler{
		Name:      name,
		Kind:      "projector",
		Go:        goPath(n),
		Group:     group,
		Execution: execution(group),
	}
	projects := map[string]bool{}
	for _, m := range e.methods(n) {
		entity, data, ok := e.projectorHandle(m)
		if !ok {
			continue
		}
		if entity == nil || data == nil {
			e.warnf("projector %s.%s: unresolvable entity or event type", name, m.obj.Name())
			continue
		}
		h.Subscribes = append(h.Subscribes, &schema.Subscription{Event: e.registerEvent(data).Name})
		projects[e.entityName(entity)] = true
	}
	for p := range projects {
		h.Projects = append(h.Projects, p)
	}
	if len(h.Subscribes) == 0 {
		e.warnf("projector %s has no matching handle methods", name)
	}
	e.out.Handlers = append(e.out.Handlers, h)
}

func (e *extractor) addCommandHandler(n *types.Named) {
	h := &schema.Handler{
		Name: n.Obj().Name(),
		Kind: "command_handler",
		Go:   goPath(n),
	}
	for _, m := range e.methods(n) {
		cmd, ok := e.commandHandle(m)
		if !ok {
			continue
		}
		if cmd == nil {
			e.warnf("%s.%s: command parameter is not a named struct", h.Name, m.obj.Name())
			continue
		}
		h.Commands = append(h.Commands, &schema.Command{
			Name:    cmd.Obj().Name(),
			Go:      goPath(cmd),
			Payload: e.payload(cmd),
			Emits:   e.emits(m),
		})
	}
	sort.Slice(h.Commands, func(i, j int) bool { return h.Commands[i].Name < h.Commands[j].Name })
	if len(h.Commands) == 0 {
		e.warnf("command handler %s has no matching handle methods", h.Name)
	}
	e.out.Handlers = append(e.out.Handlers, h)
}

// entityName mirrors NewEntityOptions: type name, overridable by a bare item
// in the BaseAggregateSourced tag.
func (e *extractor) entityName(n *types.Named) string {
	name := n.Obj().Name()
	tag, ok := e.embeddedTag(n, "BaseAggregateSourced")
	if !ok {
		return name
	}
	for _, item := range splitTag(tag) {
		if key, _, hasVal := cutTag(item); !hasVal && key != "" && key != "rev" && key != "snapshot" && key != "project" {
			name = key
		}
	}
	return name
}

func (e *extractor) addAggregate(n *types.Named, withCommands bool) {
	kind := "entity"
	if _, ok := e.embeddedTag(n, "BaseAggregateSourced"); ok {
		kind = "sourced"
	} else if _, ok := e.embeddedTag(n, "BaseAggregateHolder"); ok {
		kind = "holder"
	}

	agg := &schema.Aggregate{Name: e.entityName(n), Kind: kind, Go: goPath(n), State: e.payload(n)}

	if tag, ok := e.embeddedTag(n, "BaseAggregateSourced"); ok {
		revision := ""
		for _, item := range splitTag(tag) {
			key, val, hasVal := cutTag(item)
			if !hasVal {
				continue
			}
			switch key {
			case "snapshot":
				every, err := strconv.Atoi(val)
				if err != nil {
					e.warnf("aggregate %s: bad snapshot tag %q", agg.Name, val)
					continue
				}
				agg.Snapshot = &schema.Snapshot{Every: every}
			case "rev":
				revision = val
			case "project":
				if val == "false" {
					f := false
					agg.Project = &f
				}
			}
		}
		if agg.Snapshot != nil && revision != "" {
			agg.Snapshot.Revision = revision
		}
	}

	if withCommands {
		for _, m := range e.methods(n) {
			cmd, ok := e.commandHandle(m)
			if !ok {
				continue
			}
			if cmd == nil {
				e.warnf("%s.%s: command parameter is not a named struct", agg.Name, m.obj.Name())
				continue
			}
			agg.Commands = append(agg.Commands, &schema.Command{
				Name:    cmd.Obj().Name(),
				Go:      goPath(cmd),
				Payload: e.payload(cmd),
				Emits:   e.emits(m),
			})
		}
	}
	e.out.Aggregates = append(e.out.Aggregates, agg)
}

// registerEvent adds an event to the catalog (or returns the existing entry),
// reading name/publish/service/aliases from the BaseEvent tag exactly like
// NewEventConfig.
func (e *extractor) registerEvent(n *types.Named) *schema.Event {
	name := n.Obj().Name()
	var publish bool
	var service string
	var aliases []string

	if tag, ok := e.embeddedTag(n, "BaseEvent"); ok {
		for _, item := range splitTag(tag) {
			key, val, hasVal := cutTag(item)
			switch {
			case key == "alias" && hasVal:
				aliases = splitComma(val)
			case key == "publish" && !hasVal:
				publish = true
			case key == "publish" && hasVal:
				publish = isTrue(val)
			case key == "service" && hasVal:
				service = val
			case !hasVal && key != "":
				name = key
			}
		}
	}
	if service == e.service {
		service = ""
	}

	if existing, ok := e.events[name]; ok {
		return existing
	}
	ev := &schema.Event{
		Name:    name,
		Go:      goPath(n),
		Service: service,
		Publish: publish,
		Aliases: aliases,
		Payload: e.payload(n),
	}
	e.events[name] = ev
	return ev
}
