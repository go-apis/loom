package docs

import (
	"fmt"
	"slices"
	"sort"

	"github.com/go-apis/loom/schema"
)

// Model is the cross-service join of all loaded schemas.
type Model struct {
	Schemas   []*schema.Schema
	ByService map[string]*schema.Schema

	// Consumers of each event, keyed by ownerService/eventName.
	Consumers map[string][]Consumer
	// Emitters of each event (aggregates and command handlers), keyed the
	// same way.
	Emitters map[string][]Emitter
	// HandlerOf maps service/commandName to the node handling that command.
	HandlerOf map[string]Emitter

	Drift []string
}

type Consumer struct {
	Service string
	Handler *schema.Handler
	Sub     *schema.Subscription
}

// Emitter is a node that produces events or handles commands: an aggregate
// (Kind "aggregate") or a command handler (Kind "handler").
type Emitter struct {
	Service string
	Kind    string
	Name    string
}

func key(service, name string) string { return service + "/" + name }

// ownerOf resolves the owning service of an event as seen from schema s:
// its explicit `service:` for foreign events, otherwise s itself.
func ownerOf(s *schema.Schema, e *schema.Event) string {
	if e != nil && e.Service != "" {
		return e.Service
	}
	return s.Service
}

func link(schemas []*schema.Schema) *Model {
	m := &Model{
		Schemas:   schemas,
		ByService: map[string]*schema.Schema{},
		Consumers: map[string][]Consumer{},
		Emitters:  map[string][]Emitter{},
		HandlerOf: map[string]Emitter{},
	}
	for _, s := range schemas {
		m.ByService[s.Service] = s
	}

	for _, s := range schemas {
		for _, a := range s.Aggregates {
			node := Emitter{Service: s.Service, Kind: "aggregate", Name: a.Name}
			for _, c := range a.Commands {
				m.HandlerOf[key(s.Service, c.Name)] = node
				m.addEmits(s, node, c.Emits)
			}
		}
		for _, h := range s.Handlers {
			for _, sub := range h.Subscribes {
				k := key(ownerOf(s, s.FindEvent(sub.Event)), sub.Event)
				m.Consumers[k] = append(m.Consumers[k], Consumer{Service: s.Service, Handler: h, Sub: sub})
			}
			if h.Kind == "command_handler" {
				node := Emitter{Service: s.Service, Kind: "handler", Name: h.Name}
				for _, c := range h.Commands {
					m.HandlerOf[key(s.Service, c.Name)] = node
					m.addEmits(s, node, c.Emits)
				}
			}
		}
	}

	m.checkDrift()
	return m
}

func (m *Model) addEmits(s *schema.Schema, node Emitter, emits []string) {
	for _, evt := range emits {
		k := key(ownerOf(s, s.FindEvent(evt)), evt)
		if !slices.Contains(m.Emitters[k], node) {
			m.Emitters[k] = append(m.Emitters[k], node)
		}
	}
}

// checkDrift validates every foreign-event redeclaration against the owning
// service's schema — the check that does not exist at runtime today.
func (m *Model) checkDrift() {
	for _, s := range m.Schemas {
		for _, e := range s.Events {
			if e.Service == "" {
				continue
			}
			owner, ok := m.ByService[e.Service]
			if !ok {
				m.driftf("%s: consumes %s from unknown service %q", s.Service, e.Name, e.Service)
				continue
			}
			oe := owner.FindEvent(e.Name)
			if oe == nil {
				m.driftf("%s: consumes %s from %s, which declares no such event", s.Service, e.Name, e.Service)
				continue
			}
			if oe.Service != "" {
				m.driftf("%s: consumes %s from %s, but %s itself declares it as foreign (from %s)", s.Service, e.Name, e.Service, e.Service, oe.Service)
				continue
			}
			if !oe.Publish {
				m.driftf("%s: consumes %s from %s, but the owner does not publish it", s.Service, e.Name, e.Service)
			}
			m.comparePayloads(s, e, owner, oe)
		}
	}
	sort.Strings(m.Drift)
}

// comparePayloads checks the local redeclaration is a compatible subset of
// the owner's payload: every local property must exist on the owner with the
// same type/format. Extra owner fields are fine (ignored on decode). Refs
// resolve against their own schema's shared types before comparing.
func (m *Model) comparePayloads(ls *schema.Schema, local *schema.Event, os *schema.Schema, owner *schema.Event) {
	lp := ls.Resolve(local.Payload)
	op := os.Resolve(owner.Payload)
	if lp == nil || op == nil {
		return
	}
	for name, l := range lp.Properties {
		o, ok := op.Properties[name]
		if !ok {
			m.driftf("%s: %s.%s not present in %s's payload (decodes as zero value)", ls.Service, local.Name, name, os.Service)
			continue
		}
		l, o = ls.Resolve(l), os.Resolve(o)
		if l.Type != o.Type || l.Format != o.Format {
			m.driftf("%s: %s.%s is %s/%s locally but %s/%s at the owner", ls.Service, local.Name, name, l.Type, l.Format, o.Type, o.Format)
		}
	}
}

func (m *Model) driftf(format string, args ...any) {
	m.Drift = append(m.Drift, fmt.Sprintf(format, args...))
}
