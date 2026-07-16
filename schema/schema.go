// Package schema defines the Loom schema: a language-neutral description of
// a service's aggregates, commands, events and handlers. M1 emits it from
// existing code (see loom/extract); later milestones generate registries and
// typed interfaces from it.
package schema

import (
	"fmt"
	"os"
	"slices"
	"sort"

	"gopkg.in/yaml.v3"
)

const Version = 1

type Schema struct {
	Loom       int          `yaml:"loom"`
	Service    string       `yaml:"service"`
	Aggregates []*Aggregate `yaml:"aggregates,omitempty"`
	Events     []*Event     `yaml:"events,omitempty"`
	Handlers   []*Handler   `yaml:"handlers,omitempty"`
	Types      []*NamedType `yaml:"types,omitempty"`
}

// NamedType is a shared payload definition referenced by Payload.Ref —
// JSON Schema's $defs, with service-local scope.
type NamedType struct {
	Name    string   `yaml:"name"`
	Go      string   `yaml:"go,omitempty"`
	Payload *Payload `yaml:"payload"`
}

type Aggregate struct {
	Name     string     `yaml:"name"`
	Kind     string     `yaml:"kind"` // sourced | holder | entity
	Go       string     `yaml:"go,omitempty"`
	Snapshot *Snapshot  `yaml:"snapshot,omitempty"`
	Project  *bool      `yaml:"project,omitempty"` // omitted = lib default (true)
	State    *Payload   `yaml:"state,omitempty"`
	Commands []*Command `yaml:"commands,omitempty"`
}

type Snapshot struct {
	Every    int    `yaml:"every"`
	Revision string `yaml:"revision,omitempty"`
}

type Command struct {
	Name    string   `yaml:"name"`
	Go      string   `yaml:"go,omitempty"`
	Payload *Payload `yaml:"payload,omitempty"`
	Emits   []string `yaml:"emits,omitempty"`
}

type Event struct {
	Name    string   `yaml:"name"`
	Go      string   `yaml:"go,omitempty"`
	Service string   `yaml:"service,omitempty"` // set = foreign event consumed from that service
	Publish bool     `yaml:"publish,omitempty"`
	Aliases []string `yaml:"aliases,omitempty"`
	Payload *Payload `yaml:"payload,omitempty"`
}

type Handler struct {
	Name       string          `yaml:"name"`
	Kind       string          `yaml:"kind"` // saga | event_handler | projector | command_handler
	Go         string          `yaml:"go,omitempty"`
	Group      string          `yaml:"group,omitempty"`
	Execution  string          `yaml:"execution,omitempty"` // in-transaction | async
	Chunked    bool            `yaml:"chunked,omitempty"`
	Subscribes []*Subscription `yaml:"subscribes,omitempty"`
	Projects   []string        `yaml:"projects,omitempty"`
	Commands   []*Command      `yaml:"commands,omitempty"` // command_handler only
}

type Subscription struct {
	Event      string   `yaml:"event"`
	Dispatches []string `yaml:"dispatches,omitempty"`
}

// Payload is the JSON Schema subset used for command/event payloads. It is
// deliberately small: enough to describe today's structs and to serve as the
// integration-contract format later.
type Payload struct {
	Ref        string              `yaml:"ref,omitempty" json:"ref,omitempty"` // names a Schema.Types entry
	Type       string              `yaml:"type,omitempty" json:"type,omitempty"`
	Format     string              `yaml:"format,omitempty" json:"format,omitempty"`
	Properties map[string]*Payload `yaml:"properties,omitempty" json:"properties,omitempty"`
	Items      *Payload            `yaml:"items,omitempty" json:"items,omitempty"`
	Required   []string            `yaml:"required,omitempty" json:"required,omitempty"`
	Nullable   bool                `yaml:"nullable,omitempty" json:"nullable,omitempty"`
}

// Sort orders every collection so marshaled output is deterministic and
// schema changes show up as clean diffs.
func (s *Schema) Sort() {
	sort.Slice(s.Aggregates, func(i, j int) bool { return s.Aggregates[i].Name < s.Aggregates[j].Name })
	for _, a := range s.Aggregates {
		sort.Slice(a.Commands, func(i, j int) bool { return a.Commands[i].Name < a.Commands[j].Name })
	}
	sort.Slice(s.Events, func(i, j int) bool { return s.Events[i].Name < s.Events[j].Name })
	sort.Slice(s.Handlers, func(i, j int) bool { return s.Handlers[i].Name < s.Handlers[j].Name })
	sort.Slice(s.Types, func(i, j int) bool { return s.Types[i].Name < s.Types[j].Name })
	for _, h := range s.Handlers {
		sort.Slice(h.Subscribes, func(i, j int) bool { return h.Subscribes[i].Event < h.Subscribes[j].Event })
		sort.Strings(h.Projects)
	}
}

func (s *Schema) Marshal() ([]byte, error) {
	s.Sort()
	return yaml.Marshal(s)
}

func (s *Schema) WriteFile(path string) error {
	data, err := s.Marshal()
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func Load(path string) (*Schema, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s Schema
	if err := yaml.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if s.Loom != Version {
		return nil, fmt.Errorf("%s: unsupported loom version %d", path, s.Loom)
	}
	if s.Service == "" {
		return nil, fmt.Errorf("%s: missing service", path)
	}
	return &s, nil
}

// FindType returns the named shared payload type, or nil.
func (s *Schema) FindType(name string) *NamedType {
	for _, t := range s.Types {
		if t.Name == name {
			return t
		}
	}
	return nil
}

// Resolve follows a Ref payload to its definition; non-ref payloads (and
// dangling refs) return as-is.
func (s *Schema) Resolve(p *Payload) *Payload {
	if p == nil || p.Ref == "" {
		return p
	}
	if t := s.FindType(p.Ref); t != nil {
		return t.Payload
	}
	return p
}

// Event lookup by name, including aliases.
func (s *Schema) FindEvent(name string) *Event {
	for _, e := range s.Events {
		if e.Name == name {
			return e
		}
		if slices.Contains(e.Aliases, name) {
			return e
		}
	}
	return nil
}
