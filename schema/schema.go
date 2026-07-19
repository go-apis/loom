// Package schema is the compiled Loom schema: the language-neutral model
// behind the .loom SDL. It is authoring-shaped — hand-written schemas are
// the source of truth and `loom generate` consumes this form.
package schema

import (
	"fmt"
	"regexp"
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
	Enums       []*Enum       `yaml:"enums,omitempty" json:"enums,omitempty"`
}

type Aggregate struct {
	Name     string   `yaml:"name" json:"name"`
	Snapshot int      `yaml:"snapshot,omitempty" json:"snapshot,omitempty"` // every N events; 0 = disabled
	// Table (@table) materializes the aggregate's state (minus @pii
	// fields) into a typed per-aggregate table, written in the same
	// transaction as the events — a queryable state mirror with no
	// projection lag and no entity+projection boilerplate.
	Table    bool       `yaml:"table,omitempty" json:"table,omitempty"`
	State    *Payload   `yaml:"state" json:"state"`
	Commands []*Command `yaml:"commands,omitempty" json:"commands,omitempty"`
	Uploads  []*Upload  `yaml:"uploads,omitempty" json:"uploads,omitempty"`
}

// Upload declares a resumable file upload whose lifecycle dispatches the
// enclosing aggregate/record's commands: OnStarted (optional) when the
// session is created, OnUploaded (required) when storage reports the
// object finalized. Each referenced command must have exactly one
// payload field, required and typed `file`, which loom fills.
type Upload struct {
	Name       string `yaml:"name" json:"name"`
	OnStarted  string `yaml:"on_started,omitempty" json:"on_started,omitempty"`
	OnUploaded string `yaml:"on_uploaded" json:"on_uploaded"`
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
	// Upcasts lists the versions this event can be lifted FROM (`upcast X
	// @from(n)`): each n names a hand-written hop n → n+1 run at decode
	// time when a stored row's schema_version is behind. Declaring any
	// upcast makes version handling strict for this event — undecodable
	// old versions become loud errors instead of silent zero-value folds.
	Upcasts []int    `yaml:"upcasts,omitempty" json:"upcasts,omitempty"`
	Payload *Payload `yaml:"payload,omitempty" json:"payload,omitempty"`
}

// Record is state-of-record persistence without event sourcing (ledgers,
// balances): commands mutate state directly; emitted events are
// announcements into the log, never a rebuild source.
type Record struct {
	Name     string     `yaml:"name" json:"name"`
	State    *Payload   `yaml:"state" json:"state"`
	Commands []*Command `yaml:"commands,omitempty" json:"commands,omitempty"`
	Uploads  []*Upload  `yaml:"uploads,omitempty" json:"uploads,omitempty"`
}

// Entity is a read model maintained by a projection.
type Entity struct {
	Name string `yaml:"name" json:"name"`
	// Table (@table) stores the entity in a typed per-entity table with a
	// real column per state field, instead of the shared jsonb doc table —
	// filters and ORDER BY hit real columns, and the table is plain SQL for
	// BI. Opt-in; switching is create table + Rebuild (projections refold).
	Table bool     `yaml:"table,omitempty" json:"table,omitempty"`
	State *Payload `yaml:"state" json:"state"`
	// Joins are gateway-resolved edges to other entities (declared with
	// `join`); the runtime ignores them — only the GraphQL gateway wires
	// them up, erroring at compose time if the target isn't mounted.
	Joins []*Join `yaml:"joins,omitempty" json:"joins,omitempty"`
}

// Enum is a closed set of string values, declared top-level and
// referenced by fields like a named type:
//
//	enum TinStatus { unknown pending_match matched mismatched }
//	...
//	tin_status: TinStatus
//
// On the wire and at rest an enum is its string value; generated code
// gets a named string type with constants and validation, the GraphQL
// surfaces get real enum types, OpenAPI gets the value list.
type Enum struct {
	Name   string   `yaml:"name" json:"name"`
	Values []string `yaml:"values" json:"values"`
}

// Join declares a gateway edge from one entity to another, possibly in
// another service:
//
//	join recipient -> recipients.RecipientSummary via recipient_id
//	join forms -> [filings.FormList] via recipient_id
//
// A single join follows a local uuid field (Via) to the target's row id.
// A list join collects the target rows whose Via field equals this row's
// id. Cross-service targets resolve when the gateway composes the
// services; same-service targets are validated here.
type Join struct {
	Field   string `yaml:"field" json:"field"`
	Service string `yaml:"service,omitempty" json:"service,omitempty"` // empty = this service
	Entity  string `yaml:"entity" json:"entity"`
	List    bool   `yaml:"list,omitempty" json:"list,omitempty"`
	Via     string `yaml:"via" json:"via"`
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
	Name   string `yaml:"name" json:"name"`
	Entity string `yaml:"entity" json:"entity"`
	// Fold marks the projection's fold hand-written (@fold): loom
	// generates a Folds interface and a once-only stub instead of the
	// assignment fold — for shapes assignment can't express (counters,
	// per-child maps). Checkpointing/rebuild stay framework-owned.
	Fold          bool              `yaml:"fold,omitempty" json:"fold,omitempty"`
	Subscriptions []*ProjectionShot `yaml:"on" json:"on"`
}

type ProjectionShot struct {
	Event string `yaml:"event" json:"event"`
	// Key routes the event to the entity row named by this payload field
	// (a uuid) instead of the event's own aggregate id — how child
	// events land on a parent-keyed row (the 1-* read-model answer).
	Key string `yaml:"key,omitempty" json:"key,omitempty"`
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

// IsFile reports whether the payload is the builtin `file` type
// (loom.FileRef on the wire).
func (pl *Payload) IsFile() bool {
	return pl != nil && pl.Type == "object" && pl.Format == "file"
}

// FileField returns the name of a command payload's single required
// `file` field, or "" when the shape doesn't match — the shape upload
// lifecycle commands must have.
func (c *Command) FileField() string {
	if c.Payload == nil || len(c.Payload.Properties) != 1 {
		return ""
	}
	for name, p := range c.Payload.Properties {
		if p.IsFile() && len(c.Payload.Required) == 1 && c.Payload.Required[0] == name {
			return name
		}
	}
	return ""
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

func (s *Schema) FindEnum(name string) *Enum {
	for _, e := range s.Enums {
		if e.Name == name {
			return e
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
		sort.Slice(a.Uploads, func(i, j int) bool { return a.Uploads[i].Name < a.Uploads[j].Name })
	}
	sort.Slice(s.Records, func(i, j int) bool { return s.Records[i].Name < s.Records[j].Name })
	for _, r := range s.Records {
		sort.Slice(r.Commands, func(i, j int) bool { return r.Commands[i].Name < r.Commands[j].Name })
		sort.Slice(r.Uploads, func(i, j int) bool { return r.Uploads[i].Name < r.Uploads[j].Name })
	}
	sort.Slice(s.Entities, func(i, j int) bool { return s.Entities[i].Name < s.Entities[j].Name })
	sort.Slice(s.Events, func(i, j int) bool { return s.Events[i].Name < s.Events[j].Name })
	for _, e := range s.Events {
		sort.Ints(e.Upcasts)
	}
	sort.Slice(s.Policies, func(i, j int) bool { return s.Policies[i].Name < s.Policies[j].Name })
	sort.Slice(s.Processes, func(i, j int) bool { return s.Processes[i].Name < s.Processes[j].Name })
	sort.Slice(s.Projections, func(i, j int) bool { return s.Projections[i].Name < s.Projections[j].Name })
	sort.Slice(s.Enums, func(i, j int) bool { return s.Enums[i].Name < s.Enums[j].Name })
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
	// uploads: names are service-unique (they become API surface), and
	// their lifecycle commands must belong to the enclosing owner with
	// exactly one required `file` field for loom to fill.
	uploadNames := map[string]bool{}
	checkUploads := func(owner string, commands []*Command, uploads []*Upload) {
		ownCommand := func(name string) *Command {
			for _, c := range commands {
				if c.Name == name {
					return c
				}
			}
			return nil
		}
		for _, u := range uploads {
			if uploadNames[u.Name] {
				fail("upload %s declared twice — upload names are service-unique", u.Name)
			}
			uploadNames[u.Name] = true
			if u.OnUploaded == "" {
				fail("upload %s has no `on uploaded` command — an upload nobody records is a schema error", u.Name)
			}
			for hook, cmdName := range map[string]string{"started": u.OnStarted, "uploaded": u.OnUploaded} {
				if cmdName == "" {
					continue
				}
				cmd := ownCommand(cmdName)
				if cmd == nil {
					fail("upload %s: `on %s` dispatches %s, which is not a command of %s", u.Name, hook, cmdName, owner)
					continue
				}
				if cmd.FileField() == "" {
					fail("upload %s: command %s must have exactly one payload field, required and typed file", u.Name, cmdName)
				}
			}
		}
	}
	for _, a := range s.Aggregates {
		checkUploads(a.Name, a.Commands, a.Uploads)
	}
	for _, r := range s.Records {
		checkUploads(r.Name, r.Commands, r.Uploads)
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
	// upcast coverage: each hop must sit below the current version, and the
	// declared hops must run contiguously up to it — a gap would strand
	// every version below the gap with no path to current.
	for _, e := range s.Events {
		if len(e.Upcasts) == 0 {
			continue
		}
		version := e.Version
		if version == 0 {
			version = 1
		}
		seen := map[int]bool{}
		lowest := e.Upcasts[0]
		for _, from := range e.Upcasts {
			if seen[from] {
				fail("event %s declares upcast @from(%d) twice", e.Name, from)
			}
			seen[from] = true
			if from < 1 || from >= version {
				fail("event %s: upcast @from(%d) must name a version below the event's @v(%d)", e.Name, from, version)
			}
			if from < lowest {
				lowest = from
			}
		}
		for v := lowest; v < version; v++ {
			if !seen[v] {
				fail("event %s: upcast coverage has a gap — @from(%d) is missing, so versions %d and below cannot reach @v(%d)", e.Name, v, lowest, version)
			}
		}
	}

	// @table entities become real columns: field names must not shadow the
	// meta columns, and @pii is incompatible (sealed ciphertext in a typed
	// column defeats both — keep PII entities in the doc store).
	for _, e := range s.Entities {
		if !e.Table {
			continue
		}
		for name := range payloadProps(e.State) {
			switch name {
			case "service", "namespace", "id", "updated_at":
				fail("entity %s: field %s collides with a meta column of its @table — rename the field", e.Name, name)
			}
		}
		if pii := e.State.PIIFields(); len(pii) > 0 {
			fail("entity %s: @table and @pii are incompatible — typed columns cannot hold sealed values; keep this entity in the doc store (filters cannot match sealed fields anyway)", e.Name)
		}
	}

	// @table aggregates materialize state into typed columns too: the same
	// meta-column rules apply, @pii state fields are simply excluded from
	// the table (the sealed value stays in events/snapshots; keep a plain
	// derived field like ein_last4 for the queryable fact), and the
	// aggregate's name becomes a query surface — it must not collide with
	// an entity's.
	for _, a := range s.Aggregates {
		if !a.Table {
			continue
		}
		pii := map[string]bool{}
		for _, f := range a.State.PIIFields() {
			pii[f] = true
		}
		hasPlain := false
		for name := range payloadProps(a.State) {
			if pii[name] {
				continue
			}
			hasPlain = true
			switch name {
			case "service", "namespace", "id", "updated_at":
				fail("aggregate %s: state field %s collides with a meta column of its @table — rename the field", a.Name, name)
			}
		}
		if !hasPlain {
			fail("aggregate %s: @table needs at least one non-@pii state field — sealed values never land in typed columns", a.Name)
		}
		for _, e := range s.Entities {
			if e.Name == a.Name {
				fail("aggregate %s: @table shares the entity query surface — entity %s must be renamed or dropped", a.Name, e.Name)
			}
		}
	}

	for _, e := range s.Entities {
		for _, j := range e.Joins {
			if payloadProps(e.State)[j.Field] != nil {
				fail("entity %s: join %s collides with a state field", e.Name, j.Field)
			}
			if !j.List {
				// a single join follows a local field to the target's id
				f := payloadProps(e.State)[j.Via]
				if f == nil {
					fail("entity %s: join %s via %s — no such field on the entity", e.Name, j.Field, j.Via)
				} else if f.Type != "string" || f.Format != "uuid" {
					fail("entity %s: join %s via %s must follow a uuid field", e.Name, j.Field, j.Via)
				}
			}
			if j.Service != "" && j.Service != s.Service {
				continue // foreign target: the gateway validates at compose time
			}
			target := s.FindEntity(j.Entity)
			if target == nil {
				fail("entity %s: join %s targets undeclared entity %s", e.Name, j.Field, j.Entity)
			} else if j.List && payloadProps(target.State)[j.Via] == nil {
				fail("entity %s: join %s via %s — no such field on %s", e.Name, j.Field, j.Via, j.Entity)
			}
		}
	}

	for _, p := range s.Projections {
		if s.FindEntity(p.Entity) == nil {
			fail("projection %s projects into undeclared entity %s", p.Name, p.Entity)
		}
		for _, sub := range p.Subscriptions {
			evt := s.FindEvent(sub.Event)
			if evt == nil {
				fail("projection %s subscribes to undeclared event %s", p.Name, sub.Event)
			}
			if sub.Key == "" || evt == nil {
				continue
			}
			// key(field) routing needs a required uuid field on the event
			// payload — a missing key would route to the nil row
			field := payloadProps(evt.Payload)[sub.Key]
			required := false
			if evt.Payload != nil {
				for _, r := range evt.Payload.Required {
					required = required || r == sub.Key
				}
			}
			if field == nil {
				fail("projection %s: key(%s) is not a field of event %s", p.Name, sub.Key, sub.Event)
			} else if field.Type != "string" || field.Format != "uuid" || field.Nullable || !required {
				fail("projection %s: key(%s) on %s must be a required uuid field", p.Name, sub.Key, sub.Event)
			}
		}
	}

	// command payload fields must not collide with the dispatch envelope:
	// CommandBase marshals aggregate_id + namespace at the same JSON level,
	// so a payload field with either name is unreachable over HTTP/GraphQL.
	envelope := func(cmds []*Command, owner string) {
		for _, c := range cmds {
			for name := range payloadProps(c.Payload) {
				if name == "aggregate_id" || name == "namespace" {
					fail("%s command %s: field %s collides with the command envelope — rename the field", owner, c.Name, name)
				}
			}
		}
	}
	for _, a := range s.Aggregates {
		envelope(a.Commands, "aggregate "+a.Name)
	}
	for _, r := range s.Records {
		envelope(r.Commands, "record "+r.Name)
	}

	// enums: closed string sets. Names share the type namespace (a ref
	// resolves to exactly one of them); values must be legal GraphQL enum
	// value names so the gateway can serve them as real enum types.
	enumValueRe := regexp.MustCompile(`^[_A-Za-z][_0-9A-Za-z]*$`)
	for _, e := range s.Enums {
		if len(e.Values) == 0 {
			fail("enum %s has no values", e.Name)
		}
		if s.FindType(e.Name) != nil {
			fail("enum %s collides with type %s", e.Name, e.Name)
		}
		seenVals := map[string]bool{}
		for _, v := range e.Values {
			if !enumValueRe.MatchString(v) {
				fail("enum %s: value %q is not a legal enum value name", e.Name, v)
			}
			if seenVals[v] {
				fail("enum %s declares value %s twice", e.Name, v)
			}
			seenVals[v] = true
		}
	}

	// @pii placement: encrypt-at-rest applies to what the runtime persists
	// under a stream's data key — states, local unpublished events, and
	// command payloads (commands rest in timers and batch items).
	// Published events cross the bus in plaintext, foreign events belong
	// to another service's keys, and named types would smuggle PII
	// anywhere.
	noPII := func(pl *Payload, what string) {
		for name, f := range payloadProps(pl) {
			if f.PII {
				fail("%s field %s: %s", what, name, "@pii is only valid on commands, local unpublished events and aggregate/record/entity states")
			}
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
		if pl.Ref != "" && s.FindType(pl.Ref) == nil && s.FindEnum(pl.Ref) == nil && !seen[pl.Ref] {
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
