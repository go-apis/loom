package loom

import "sort"

// PlannedReaction is one thing the schema says reacts to an event: a
// policy (in the event's own transaction), a process (afterwards, with
// whatever it calls outside), or a projection (a read model it
// updates). It is what a plan can say about what a command sets off
// without running any of it: who reacts, what each is declared to
// dispatch in answer, and which outside calls a process declares. What
// a reaction would actually decide — which of its commands, with what
// in them, and what those then cause — is not known until it runs, and
// is not here.
type PlannedReaction struct {
	// Kind is policy, process or projection.
	Kind string `json:"kind"`
	Name string `json:"name"`
	// Service is the reactor's service when it is not the event's own:
	// a process there that consumes the event off the bus.
	Service string `json:"service,omitempty"`
	// Dispatches is the commands the reactor's schema says it may
	// dispatch on this event (`on X -> A, B`). It may dispatch any of
	// them, or none.
	Dispatches []string `json:"dispatches,omitempty"`
	// Effects is the outside calls a process declares (`effect`). The
	// schema does not say which event each belongs to, so they are the
	// process's, not this event's alone.
	Effects []string `json:"effects,omitempty"`
	// Entity is the read model a projection folds the event into.
	Entity string `json:"entity,omitempty"`
}

// Reactions is what this service's schema says reacts to one of its own
// events: policies, then processes, then projections, each by name.
func (c *Client) Reactions(eventType string) []PlannedReaction {
	out := []PlannedReaction{}
	for _, p := range c.sortedReactors(c.reg.Policies) {
		if contains(p.Events, eventType) && c.ownEvent(eventType) {
			out = append(out, PlannedReaction{Kind: "policy", Name: p.Name, Dispatches: dispatchesOn(p, eventType)})
		}
	}
	for _, p := range c.sortedReactors(c.reg.Processes) {
		if contains(p.Events, eventType) && c.ownEvent(eventType) {
			out = append(out, PlannedReaction{Kind: "process", Name: p.Name, Dispatches: dispatchesOn(p, eventType), Effects: p.Effects})
		}
	}
	projections := append([]*ProjectionDef(nil), c.reg.Projections...)
	sort.Slice(projections, func(i, j int) bool { return projections[i].Name < projections[j].Name })
	for _, p := range projections {
		if contains(p.Events, eventType) && c.ownEvent(eventType) {
			out = append(out, PlannedReaction{Kind: "projection", Name: p.Name, Entity: p.Entity})
		}
	}
	return out
}

// ConsumedReactions is what this service's schema says reacts to an
// event of another service's: its processes that consume that event off
// the bus. A gateway composing several services asks each of the others
// this about a planned event.
func (c *Client) ConsumedReactions(service, eventType string) []PlannedReaction {
	out := []PlannedReaction{}
	def := c.reg.eventDef(eventType)
	if def == nil || def.Service != service {
		return out
	}
	for _, p := range c.sortedReactors(c.reg.Processes) {
		if contains(p.Events, eventType) {
			out = append(out, PlannedReaction{Kind: "process", Name: p.Name, Service: c.reg.Service, Dispatches: dispatchesOn(p, eventType), Effects: p.Effects})
		}
	}
	return out
}

// ownEvent reports whether the event type is this service's own rather
// than one it consumes from another.
func (c *Client) ownEvent(eventType string) bool {
	def := c.reg.eventDef(eventType)
	return def == nil || def.Service == ""
}

func (c *Client) sortedReactors(list []*ReactorDef) []*ReactorDef {
	out := append([]*ReactorDef(nil), list...)
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func dispatchesOn(p *ReactorDef, eventType string) []string {
	for _, s := range p.Subs {
		if s.Event == eventType {
			return s.Dispatches
		}
	}
	return nil
}
