// Package schema is the compiled Loom schema: the language-neutral model
// behind the .loom SDL. It is authoring-shaped — hand-written schemas are
// the source of truth and `loom generate` consumes this form.
package schema

import (
	"fmt"
	"sort"
)

const Version = 2

type Schema struct {
	Loom        int           `yaml:"loom" json:"loom"`
	Service     string        `yaml:"service" json:"service"`
	Aggregates  []*Aggregate  `yaml:"aggregates,omitempty" json:"aggregates,omitempty"`
	Records     []*Record     `yaml:"records,omitempty" json:"records,omitempty"`
	Entities    []*Entity     `yaml:"entities,omitempty" json:"entities,omitempty"`
	Events      []*Event      `yaml:"events,omitempty" json:"events,omitempty"`
	Policies    []*Reactor    `yaml:"policies,omitempty" json:"policies,omitempty"`
	Processes   []*Reactor    `yaml:"processes,omitempty" json:"processes,omitempty"`
	Projections []*Projection `yaml:"projections,omitempty" json:"projections,omitempty"`
	Types       []*NamedType  `yaml:"types,omitempty" json:"types,omitempty"`
}

type Aggregate struct {
	Name     string     `yaml:"name" json:"name"`
	Snapshot int        `yaml:"snapshot,omitempty" json:"snapshot,omitempty"` // every N events; 0 = disabled
	State    *Payload   `yaml:"state" json:"state"`
	Commands []*Command `yaml:"commands,omitempty" json:"commands,omitempty"`
}

type Command struct {
	Name    string   `yaml:"name" json:"name"`
	Payload *Payload `yaml:"payload,omitempty" json:"payload,omitempty"`
	// Emits is a contract: the only events the handler may return.
	Emits []string `yaml:"emits" json:"emits"`
}

type Event struct {
	Name    string   `yaml:"name" json:"name"`
	Service string   `yaml:"service,omitempty" json:"service,omitempty"` // set = consumed foreign event
	Publish bool     `yaml:"publish,omitempty" json:"publish,omitempty"`
	Version int      `yaml:"version,omitempty" json:"version,omitempty"` // schema version, default 1
	Aliases []string `yaml:"aliases,omitempty" json:"aliases,omitempty"`
	Payload *Payload `yaml:"payload,omitempty" json:"payload,omitempty"`
}

// Record is state-of-record persistence without event sourcing (ledgers,
// balances): commands mutate state directly; emitted events are
// announcements into the log, never a rebuild source.
type Record struct {
	Name     string     `yaml:"name" json:"name"`
	State    *Payload   `yaml:"state" json:"state"`
	Commands []*Command `yaml:"commands,omitempty" json:"commands,omitempty"`
}

// Entity is a read model maintained by a projection.
type Entity struct {
	Name  string   `yaml:"name" json:"name"`
	State *Payload `yaml:"state" json:"state"`
}

// Reactor backs policies (in-transaction, local events only) and processes
// (async: local events from the log, foreign events from the bus).
type Reactor struct {
	Name          string          `yaml:"name" json:"name"`
	Subscriptions []*Subscription `yaml:"on" json:"on"`
	// Effects declares the journaled external calls a process may perform
	// (loom.Once keys). Processes only — policies run in the producing
	// transaction and must not touch the outside world.
	Effects []string `yaml:"effects,omitempty" json:"effects,omitempty"`
}

type Subscription struct {
	Event string `yaml:"event" json:"event"`
	// Dispatches is a contract: the only commands the reaction may return.
	Dispatches []string `yaml:"dispatches,omitempty" json:"dispatches,omitempty"`
}

type Projection struct {
	Name          string            `yaml:"name" json:"name"`
	Entity        string            `yaml:"entity" json:"entity"`
	Subscriptions []*ProjectionShot `yaml:"on" json:"on"`
}

type ProjectionShot struct {
	Event string `yaml:"event" json:"event"`
}

type NamedType struct {
	Name    string   `yaml:"name" json:"name"`
	Payload *Payload `yaml:"payload" json:"payload"`
}

// Payload is the JSON Schema subset for states and payloads — also the
// integration-contract format a non-Go consumer reads.
type Payload struct {
	Ref        string              `yaml:"ref,omitempty" json:"ref,omitempty"`
	Type       string              `yaml:"type,omitempty" json:"type,omitempty"`
	Format     string              `yaml:"format,omitempty" json:"format,omitempty"`
	Properties map[string]*Payload `yaml:"properties,omitempty" json:"properties,omitempty"`
	Items      *Payload            `yaml:"items,omitempty" json:"items,omitempty"`
	Required   []string            `yaml:"required,omitempty" json:"required,omitempty"`
	Nullable   bool                `yaml:"nullable,omitempty" json:"nullable,omitempty"`
	// PII marks a field encrypted at rest with the stream's data key
	// (crypto-shreddable). Only valid on top-level fields of local
	// unpublished events and aggregate/record/entity states.
	PII bool `yaml:"pii,omitempty" json:"pii,omitempty"`
}

// PIIFields lists a payload's @pii field names, sorted.
func (pl *Payload) PIIFields() []string {
	if pl == nil {
		return nil
	}
	var out []string
	for name, f := range pl.Properties {
		if f.PII {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

func (s *Schema) FindEvent(name string) *Event {
	for _, e := range s.Events {
		if e.Name == name {
			return e
		}
		for _, a := range e.Aliases {
			if a == name {
				return e
			}
		}
	}
	return nil
}

func (s *Schema) FindEntity(name string) *Entity {
	for _, e := range s.Entities {
		if e.Name == name {
			return e
		}
	}
	return nil
}

func (s *Schema) FindType(name string) *NamedType {
	for _, t := range s.Types {
		if t.Name == name {
			return t
		}
	}
	return nil
}

func (s *Schema) FindCommand(name string) (*Aggregate, *Command) {
	for _, a := range s.Aggregates {
		for _, c := range a.Commands {
			if c.Name == name {
				return a, c
			}
		}
	}
	return nil, nil
}

func (s *Schema) FindRecordCommand(name string) (*Record, *Command) {
	for _, r := range s.Records {
		for _, c := range r.Commands {
			if c.Name == name {
				return r, c
			}
		}
	}
	return nil, nil
}

func (s *Schema) Sort() {
	sort.Slice(s.Aggregates, func(i, j int) bool { return s.Aggregates[i].Name < s.Aggregates[j].Name })
	for _, a := range s.Aggregates {
		sort.Slice(a.Commands, func(i, j int) bool { return a.Commands[i].Name < a.Commands[j].Name })
	}
	sort.Slice(s.Records, func(i, j int) bool { return s.Records[i].Name < s.Records[j].Name })
	for _, r := range s.Records {
		sort.Slice(r.Commands, func(i, j int) bool { return r.Commands[i].Name < r.Commands[j].Name })
	}
	sort.Slice(s.Entities, func(i, j int) bool { return s.Entities[i].Name < s.Entities[j].Name })
	sort.Slice(s.Events, func(i, j int) bool { return s.Events[i].Name < s.Events[j].Name })
	sort.Slice(s.Policies, func(i, j int) bool { return s.Policies[i].Name < s.Policies[j].Name })
	sort.Slice(s.Processes, func(i, j int) bool { return s.Processes[i].Name < s.Processes[j].Name })
	sort.Slice(s.Projections, func(i, j int) bool { return s.Projections[i].Name < s.Projections[j].Name })
	sort.Slice(s.Types, func(i, j int) bool { return s.Types[i].Name < s.Types[j].Name })
	for _, r := range s.Policies {
		sort.Slice(r.Subscriptions, func(i, j int) bool { return r.Subscriptions[i].Event < r.Subscriptions[j].Event })
	}
	for _, r := range s.Processes {
		sort.Slice(r.Subscriptions, func(i, j int) bool { return r.Subscriptions[i].Event < r.Subscriptions[j].Event })
		sort.Strings(r.Effects)
	}
	for _, p := range s.Projections {
		sort.Slice(p.Subscriptions, func(i, j int) bool { return p.Subscriptions[i].Event < p.Subscriptions[j].Event })
	}
}

// Validate enforces the cross-references and the semantic rules the runtime
// depends on. Notably: policies may only subscribe to local events (they
// run inside the producing transaction), and every emit/dispatch contract
// must name declared things.
func (s *Schema) Validate() error {
	if s.Service == "" {
		return fmt.Errorf("schema has no service")
	}
	var errs []string
	fail := func(format string, args ...any) { errs = append(errs, fmt.Sprintf(format, args...)) }

	commandExists := func(name string) bool {
		if _, c := s.FindCommand(name); c != nil {
			return true
		}
		_, c := s.FindRecordCommand(name)
		return c != nil
	}

	for _, a := range s.Aggregates {
		if a.State == nil || len(a.State.Properties) == 0 {
			fail("aggregate %s has no state", a.Name)
		}
		for _, c := range a.Commands {
			if len(c.Emits) == 0 {
				fail("command %s emits nothing (a command with no effect is a schema error)", c.Name)
			}
			for _, e := range c.Emits {
				evt := s.FindEvent(e)
				if evt == nil {
					fail("command %s emits undeclared event %s", c.Name, e)
				} else if evt.Service != "" {
					fail("command %s emits foreign event %s (only %s can emit it)", c.Name, e, evt.Service)
				}
			}
		}
	}
	for _, r := range s.Records {
		if r.State == nil || len(r.State.Properties) == 0 {
			fail("record %s has no state", r.Name)
		}
		for _, c := range r.Commands {
			// unlike aggregate commands, record commands may emit nothing:
			// the state write is the effect
			for _, e := range c.Emits {
				evt := s.FindEvent(e)
				if evt == nil {
					fail("record command %s emits undeclared event %s", c.Name, e)
				} else if evt.Service != "" {
					fail("record command %s emits foreign event %s", c.Name, e)
				}
			}
		}
	}
	for _, p := range s.Policies {
		if len(p.Effects) > 0 {
			fail("policy %s declares effects — policies run in the producing transaction; external calls belong in a process", p.Name)
		}
		for _, sub := range p.Subscriptions {
			evt := s.FindEvent(sub.Event)
			if evt == nil {
				fail("policy %s subscribes to undeclared event %s", p.Name, sub.Event)
			} else if evt.Service != "" {
				fail("policy %s subscribes to foreign event %s — policies run in the producing transaction; use a process", p.Name, sub.Event)
			}
			for _, d := range sub.Dispatches {
				if !commandExists(d) {
					fail("policy %s dispatches undeclared command %s", p.Name, d)
				}
			}
		}
	}
	for _, p := range s.Processes {
		seenEffects := map[string]bool{}
		for _, e := range p.Effects {
			if seenEffects[e] {
				fail("process %s declares effect %s twice", p.Name, e)
			}
			seenEffects[e] = true
		}
		for _, sub := range p.Subscriptions {
			evt := s.FindEvent(sub.Event)
			if evt == nil {
				fail("process %s subscribes to undeclared event %s", p.Name, sub.Event)
			} else if evt.Service != "" && !evt.Publish {
				// foreign events are only reachable if the owner publishes;
				// the consuming side records them as published contracts.
				evt.Publish = true
			}
			for _, d := range sub.Dispatches {
				if !commandExists(d) {
					fail("process %s dispatches undeclared command %s", p.Name, d)
				}
			}
		}
	}
	for _, p := range s.Projections {
		if s.FindEntity(p.Entity) == nil {
			fail("projection %s projects into undeclared entity %s", p.Name, p.Entity)
		}
		for _, sub := range p.Subscriptions {
			if s.FindEvent(sub.Event) == nil {
				fail("projection %s subscribes to undeclared event %s", p.Name, sub.Event)
			}
		}
	}

	// @pii placement: encrypt-at-rest applies to what the runtime persists
	// under a stream's data key. Commands are transient inputs, published
	// events cross the bus in plaintext, foreign events belong to another
	// service's keys, and named types would smuggle PII anywhere.
	noPII := func(pl *Payload, what string) {
		for name, f := range payloadProps(pl) {
			if f.PII {
				fail("%s field %s: %s", what, name, "@pii is only valid on local unpublished events and aggregate/record/entity states")
			}
		}
	}
	for _, a := range s.Aggregates {
		for _, c := range a.Commands {
			noPII(c.Payload, "command "+c.Name)
		}
	}
	for _, r := range s.Records {
		for _, c := range r.Commands {
			noPII(c.Payload, "command "+c.Name)
		}
	}
	for _, t := range s.Types {
		noPII(t.Payload, "type "+t.Name)
	}
	for _, e := range s.Events {
		if e.Service != "" {
			noPII(e.Payload, "consumed event "+e.Name)
			continue
		}
		if e.Publish {
			for name, f := range payloadProps(e.Payload) {
				if f.PII {
					fail("event %s field %s: @publish and @pii are incompatible — published events cross the bus in plaintext; keep PII on a private event", e.Name, name)
				}
			}
		}
	}

	var missing []string
	seen := map[string]bool{}
	var walk func(pl *Payload, root bool, at string)
	walk = func(pl *Payload, root bool, at string) {
		if pl == nil {
			return
		}
		if pl.Ref != "" && s.FindType(pl.Ref) == nil && !seen[pl.Ref] {
			seen[pl.Ref] = true
			missing = append(missing, pl.Ref)
		}
		// nested object literals must be named types: generated code stays
		// flat and cross-language contracts stay referencable.
		if !root && len(pl.Properties) > 0 {
			fail("%s: nested object fields must use a declared type", at)
		}
		walk(pl.Items, false, at)
		for name, sub := range pl.Properties {
			walk(sub, false, at+"."+name)
		}
	}
	for _, a := range s.Aggregates {
		walk(a.State, true, "aggregate "+a.Name)
		for _, c := range a.Commands {
			walk(c.Payload, true, "command "+c.Name)
		}
	}
	for _, r := range s.Records {
		walk(r.State, true, "record "+r.Name)
		for _, c := range r.Commands {
			walk(c.Payload, true, "command "+c.Name)
		}
	}
	for _, e := range s.Entities {
		walk(e.State, true, "entity "+e.Name)
	}
	for _, e := range s.Events {
		walk(e.Payload, true, "event "+e.Name)
	}
	for _, t := range s.Types {
		walk(t.Payload, true, "type "+t.Name)
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		fail("undeclared types: %v", missing)
	}

	if len(errs) > 0 {
		return fmt.Errorf("schema %s:\n  - %s", s.Service, joinLines(errs))
	}
	return nil
}

func payloadProps(pl *Payload) map[string]*Payload {
	if pl == nil {
		return nil
	}
	return pl.Properties
}

func joinLines(lines []string) string {
	out := ""
	for i, l := range lines {
		if i > 0 {
			out += "\n  - "
		}
		out += l
	}
	return out
}
