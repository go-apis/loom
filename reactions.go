package loom

import (
	"sort"
	"time"

	"github.com/google/uuid"
)

// PlannedReaction is one thing that reacts to an event: a policy (in the
// event's own transaction), a process (afterwards, with whatever it
// calls outside), or a projection (a read model it updates). The schema
// says who reacts, what each may dispatch and which outside calls a
// process declares. A plan also runs the policies and processes of its
// own service, and Outcome says how that went.
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

	// Outcome is what happened when a plan ran the reaction: one of the
	// Outcome constants. It is empty for what a plan does not run — a
	// projection, another service's process — and outside a plan.
	Outcome string `json:"outcome,omitempty"`
	// Detail says more where Outcome is not the whole answer: the error
	// a reaction fails with, why it was not followed.
	Detail string `json:"detail,omitempty"`
	// Commands is what the reaction decided to dispatch, in order, when
	// Outcome is OutcomeDecided — none when it decided on nothing. The
	// events of those that run are in the plan's Events, naming this
	// reaction as their Cause.
	Commands []PlannedDispatch `json:"commands,omitempty"`
	// Calls is the outside call a process stopped at (OutcomeCalls), by
	// its loom.Once key.
	Calls []string `json:"calls,omitempty"`
}

// What a plan found when it ran a reaction (PlannedReaction.Outcome).
const (
	// OutcomeDecided: it ran to its end, and Commands is what it would
	// dispatch.
	OutcomeDecided = "decided"
	// OutcomeCalls: a process reached an outside call (Calls). The call
	// was not made, so what the process does with its answer is not
	// known.
	OutcomeCalls = "calls"
	// OutcomeFails: the reaction returned an error, or a command it
	// dispatches is refused (Detail). At the run it would be retried,
	// then parked.
	OutcomeFails = "fails"
	// OutcomeNotYet: the process answered loom.NotYet (Detail): it would
	// look again later.
	OutcomeNotYet = "not_yet"
	// OutcomeNotFollowed: the plan did not run it (Detail): too many
	// processes deep, or out of time.
	OutcomeNotFollowed = "not_followed"
)

// PlannedDispatch is one command a reaction decided to dispatch.
type PlannedDispatch struct {
	Command   string    `json:"command"`
	Data      any       `json:"data"`
	Namespace string    `json:"namespace"`
	TargetID  uuid.UUID `json:"target_id"`
	// How is what becomes of it: one of the Dispatch constants.
	How string `json:"how"`
	// At is when a timer would fire (DispatchTimer).
	At *time.Time `json:"at,omitempty"`
}

// What becomes of a command a reaction dispatches (PlannedDispatch.How).
const (
	// DispatchRun: handled at once. Its events are in the plan.
	DispatchRun = "run"
	// DispatchTimer: held by a timer until At (loom.At, loom.After). The
	// plan does not follow it.
	DispatchTimer = "timer"
	// DispatchCancelTimer: the pending timer for this command is
	// dropped (loom.CancelTimer).
	DispatchCancelTimer = "cancel_timer"
	// DispatchBatch: queued as part of a batch (loom.AsBatch), to run
	// later in a unit of work of its own. The plan does not follow it.
	DispatchBatch = "batch"
)

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
