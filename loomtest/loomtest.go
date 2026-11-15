// Package loomtest is the given/when/then harness: exercise generated
// command handlers and reactions against the real registry with no
// database. Given events fold into state exactly as replay would; When
// runs the generated Handle/React; Then asserts by JSON equality, so
// tests read like the schema:
//
//	loomtest.Aggregate(t, reg, "Order").
//		Given(&loomgen.OrderPlaced{Status: "placed", TotalCents: 100}).
//		When(&loomgen.ShipOrder{CommandBase: base}).
//		Then(&loomgen.OrderShipped{Status: "shipped"})
//
// Not covered by given/when/then: policies-in-transaction wiring,
// projections, effects (loom.Once needs the journal), timers and batches
// — that harness tests domain decisions, not delivery. For those, and
// for anything else that needs the store, Postgres gives a test a
// database of its own on a real server (pg.go).
package loomtest

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/go-apis/loom"
)

// Reader is a fake StateReader for reactions that read state: register
// what Lookup should find.
type Reader struct {
	Aggregates map[string]loom.AggregateState // key: name/namespace/id
	Entities   map[string]loom.EntityState
	Records    map[string]any
}

func key(name, ns string, id uuid.UUID) string { return name + "/" + ns + "/" + id.String() }

// AddAggregate registers state (with version 1) for loom.Load.
func (r *Reader) AddAggregate(name, ns string, id uuid.UUID, state loom.AggregateState) *Reader {
	if r.Aggregates == nil {
		r.Aggregates = map[string]loom.AggregateState{}
	}
	r.Aggregates[key(name, ns, id)] = state
	return r
}

func (r *Reader) Load(ctx context.Context, name, ns string, id uuid.UUID) (loom.AggregateState, int, error) {
	if s, ok := r.Aggregates[key(name, ns, id)]; ok {
		return s, 1, nil
	}
	return nil, 0, nil
}

func (r *Reader) Record(ctx context.Context, name, ns string, id uuid.UUID) (any, error) {
	return r.Records[key(name, ns, id)], nil
}

func (r *Reader) Entity(ctx context.Context, name, ns string, id uuid.UUID) (loom.EntityState, error) {
	return r.Entities[key(name, ns, id)], nil
}

// --- aggregates and records ---

type AggScenario struct {
	t      *testing.T
	agg    *loom.AggregateDef
	rec    *loom.RecordDef
	state  any
	events []any
	err    error
	ran    bool
}

// Aggregate starts a scenario for one aggregate's (or record's) commands.
func Aggregate(t *testing.T, reg *loom.Registry, name string) *AggScenario {
	t.Helper()
	for _, a := range reg.Aggregates {
		if a.Name == name {
			return &AggScenario{t: t, agg: a, state: a.NewState()}
		}
	}
	for _, r := range reg.Records {
		if r.Name == name {
			return &AggScenario{t: t, rec: r, state: r.NewState()}
		}
	}
	t.Fatalf("loomtest: no aggregate or record %q", name)
	return nil
}

// Given folds prior events into the state, exactly as replay would.
func (s *AggScenario) Given(events ...loom.DomainEvent) *AggScenario {
	s.t.Helper()
	folder, ok := s.state.(interface{ Fold(string, any) error })
	if !ok {
		s.t.Fatalf("loomtest: state %T does not fold", s.state)
	}
	for _, evt := range events {
		if err := folder.Fold(evt.LoomEvent(), evt); err != nil {
			s.t.Fatalf("loomtest: given %s: %v", evt.LoomEvent(), err)
		}
	}
	return s
}

// When runs the command through the registry's generated handler. The ctx
// carries the reader (nil Reader is fine for pure handlers).
func (s *AggScenario) When(cmd loom.Command, opts ...*Reader) *AggScenario {
	s.t.Helper()
	ctx := context.Background()
	r := &Reader{}
	if len(opts) > 0 && opts[0] != nil {
		r = opts[0]
	}
	ctx = loom.WithStateReader(ctx, r)
	s.ran = true
	s.events, s.err = s.handle(ctx, cmd)
	return s
}

// handle finds the command in the scenario's def and runs the generated
// handler against the folded state.
func (s *AggScenario) handle(ctx context.Context, cmd loom.Command) ([]any, error) {
	name := cmd.LoomCommand()
	if s.agg != nil {
		for _, def := range s.agg.Commands {
			if def.Name == name {
				return def.Handle(ctx, s.state.(loom.AggregateState), cmd)
			}
		}
		return nil, fmt.Errorf("loomtest: aggregate %s has no command %s", s.agg.Name, name)
	}
	for _, def := range s.rec.Commands {
		if def.Name == name {
			return def.Handle(ctx, s.state, cmd)
		}
	}
	return nil, fmt.Errorf("loomtest: record %s has no command %s", s.rec.Name, name)
}

// Then asserts the exact emitted events, in order, by JSON equality.
func (s *AggScenario) Then(want ...loom.DomainEvent) *AggScenario {
	s.t.Helper()
	s.mustRun()
	if s.err != nil {
		s.t.Fatalf("loomtest: handler returned error: %v", s.err)
	}
	if len(s.events) != len(want) {
		s.t.Fatalf("loomtest: emitted %d events, want %d:\n got: %s\nwant: %s",
			len(s.events), len(want), dump(s.events), dumpDE(want))
	}
	for i := range want {
		if !sameJSON(s.events[i], want[i]) {
			s.t.Fatalf("loomtest: event %d differs:\n got: %s\nwant: %s", i, js(s.events[i]), js(want[i]))
		}
	}
	return s
}

// ThenNothing asserts the command was a clean no-op.
func (s *AggScenario) ThenNothing() *AggScenario {
	s.t.Helper()
	s.mustRun()
	if s.err != nil || len(s.events) != 0 {
		s.t.Fatalf("loomtest: want no-op, got events=%s err=%v", dump(s.events), s.err)
	}
	return s
}

// ThenError asserts the handler rejected the command.
func (s *AggScenario) ThenError(contains string) *AggScenario {
	s.t.Helper()
	s.mustRun()
	if s.err == nil {
		s.t.Fatalf("loomtest: want error containing %q, got events=%s", contains, dump(s.events))
	}
	if !strings.Contains(s.err.Error(), contains) {
		s.t.Fatalf("loomtest: error %q does not contain %q", s.err, contains)
	}
	return s
}

func (s *AggScenario) mustRun() {
	if !s.ran {
		s.t.Fatalf("loomtest: call When before Then")
	}
}

func js(v any) string { raw, _ := json.Marshal(v); return string(raw) }

func sameJSON(a, b any) bool {
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	var ma, mb any
	_ = json.Unmarshal(ja, &ma)
	_ = json.Unmarshal(jb, &mb)
	return reflect.DeepEqual(ma, mb) && fmt.Sprintf("%T", a) == fmt.Sprintf("%T", b)
}

func dump(events []any) string {
	parts := make([]string, 0, len(events))
	for _, e := range events {
		parts = append(parts, js(e))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func dumpDE(events []loom.DomainEvent) string {
	parts := make([]string, 0, len(events))
	for _, e := range events {
		parts = append(parts, js(e))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

// --- reactions ---

// ReactorScenario tests a policy's or process's React: an event in,
// commands out. Reactions that call loom.Once are e2e territory (the
// effect journal needs the database); reactions that read state get a
// Reader.
type ReactorScenario struct {
	t      *testing.T
	def    *loom.ReactorDef
	reader *Reader
	cmds   []loom.Command
	err    error
	ran    bool
}

// Reaction starts a scenario for one policy or process.
func Reaction(t *testing.T, reg *loom.Registry, name string) *ReactorScenario {
	t.Helper()
	for _, p := range append(append([]*loom.ReactorDef{}, reg.Policies...), reg.Processes...) {
		if p.Name == name {
			return &ReactorScenario{t: t, def: p, reader: &Reader{}}
		}
	}
	t.Fatalf("loomtest: no policy or process %q", name)
	return nil
}

// Reading injects the fake reader for reactions that loom.Load state.
func (s *ReactorScenario) Reading(r *Reader) *ReactorScenario {
	if r != nil {
		s.reader = r
	}
	return s
}

// When delivers one event to the reaction.
func (s *ReactorScenario) When(namespace string, id uuid.UUID, evt loom.DomainEvent) *ReactorScenario {
	s.t.Helper()
	ctx := loom.WithStateReader(context.Background(), s.reader)
	s.ran = true
	s.cmds, s.err = s.def.React(ctx, &loom.Event{
		Namespace:   namespace,
		AggregateID: id,
		Type:        evt.LoomEvent(),
		Data:        evt,
	})
	return s
}

// Then asserts the dispatched commands, in order, by JSON equality.
func (s *ReactorScenario) Then(want ...loom.Command) *ReactorScenario {
	s.t.Helper()
	s.mustRun()
	if s.err != nil {
		s.t.Fatalf("loomtest: reaction returned error: %v", s.err)
	}
	if len(s.cmds) != len(want) {
		s.t.Fatalf("loomtest: dispatched %d commands, want %d: got %s", len(s.cmds), len(want), dumpCmds(s.cmds))
	}
	for i := range want {
		if !sameJSON(s.cmds[i], want[i]) {
			s.t.Fatalf("loomtest: command %d differs:\n got: %s\nwant: %s", i, js(s.cmds[i]), js(want[i]))
		}
	}
	return s
}

// ThenError asserts the reaction failed.
func (s *ReactorScenario) ThenError(contains string) *ReactorScenario {
	s.t.Helper()
	s.mustRun()
	if s.err == nil || !strings.Contains(s.err.Error(), contains) {
		s.t.Fatalf("loomtest: want error containing %q, got err=%v cmds=%s", contains, s.err, dumpCmds(s.cmds))
	}
	return s
}

func (s *ReactorScenario) mustRun() {
	if !s.ran {
		s.t.Fatalf("loomtest: call When before Then")
	}
}

func dumpCmds(cmds []loom.Command) string {
	parts := make([]string, 0, len(cmds))
	for _, c := range cmds {
		parts = append(parts, js(c))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

// --- loops ---

// LoopScenario tests a @loop aggregate's look with no database and no
// clock: when it would be looked at next, what it is told when it is,
// and what it decides. The commands a look answers with are run through
// the registry's handlers and their events folded into the reader's
// state, so a second look sees what the first one did, as it would in
// the store.
type LoopScenario struct {
	t      *testing.T
	reg    *loom.Registry
	look   func(ctx context.Context) ([]loom.Command, error)
	reader *Reader
	state  loom.LoopState
	now    time.Time

	told   loom.Looking
	cmds   []loom.Command
	events []any
	err    error
	ran    bool
}

// Loop starts a scenario for one look: the method the generated Look
// interface gives a @loop aggregate (look.LookOrder).
func Loop(t *testing.T, reg *loom.Registry, look func(ctx context.Context) ([]loom.Command, error)) *LoopScenario {
	t.Helper()
	return &LoopScenario{t: t, reg: reg, look: look, reader: &Reader{}}
}

// Reading injects the state the look reads with loom.Load. The commands
// it answers with change that state.
func (s *LoopScenario) Reading(r *Reader) *LoopScenario {
	if r != nil {
		s.reader = r
	}
	return s
}

// LookedAt says when the loop was last looked at.
func (s *LoopScenario) LookedAt(at time.Time) *LoopScenario {
	s.state.Looked = at
	return s
}

// EventAt says an event the loop cares about arrived at at.
func (s *LoopScenario) EventAt(at time.Time) *LoopScenario {
	s.state.Event = at
	return s
}

// Waiting adds something the loop is waiting on.
func (s *LoopScenario) Waiting(w loom.Wait) *LoopScenario {
	s.state.Waits = append(s.state.Waits, w)
	return s
}

// NextLook is when the runtime would look next, by the same rule it
// uses (loom.LoopState.NextLook).
func (s *LoopScenario) NextLook() time.Time {
	return s.state.NextLook()
}

// Look calls the look as the runtime would at now: told whether an
// event arrived and which waits have expired. What it dispatches is
// handled and folded into the reader's state; the look is then noted as
// made, and the expired waits as told.
func (s *LoopScenario) Look(now time.Time) *LoopScenario {
	s.t.Helper()
	s.now, s.ran = now, true
	s.told = s.state.Look(now)
	ctx := loom.WithLooking(loom.WithStateReader(context.Background(), s.reader), s.told)
	s.cmds, s.err = s.look(ctx)
	s.events = nil
	if s.err == nil {
		for _, cmd := range s.cmds {
			events, err := s.apply(ctx, cmd)
			if err != nil {
				s.err = fmt.Errorf("%s: %w", cmd.LoomCommand(), err)
				break
			}
			s.events = append(s.events, events...)
		}
	}
	s.state.Looked = now
	s.state.Waits = s.told.Waiting
	return s
}

// apply runs one command through its aggregate's handler against the
// reader's state and folds what it emits back in.
func (s *LoopScenario) apply(ctx context.Context, cmd loom.Command) ([]any, error) {
	ns, id := cmd.CommandTarget()
	for _, agg := range s.reg.Aggregates {
		for _, def := range agg.Commands {
			if def.Name != cmd.LoomCommand() {
				continue
			}
			state := s.reader.Aggregates[key(agg.Name, ns, id)]
			if state == nil {
				state = agg.NewState()
			}
			events, err := def.Handle(ctx, state, cmd)
			if err != nil {
				return nil, err
			}
			for _, evt := range events {
				name := evt.(loom.DomainEvent).LoomEvent()
				if !containsString(def.Emits, name) {
					return nil, fmt.Errorf("emitted undeclared event %s", name)
				}
				if err := state.Fold(name, evt); err != nil {
					return nil, err
				}
			}
			s.reader.AddAggregate(agg.Name, ns, id, state)
			return events, nil
		}
	}
	return nil, fmt.Errorf("loomtest: no aggregate handles %s", cmd.LoomCommand())
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// Told is what the last look was told: why it was called.
func (s *LoopScenario) Told() loom.Looking {
	s.t.Helper()
	s.mustRun()
	return s.told
}

// Then asserts the commands the last look dispatched, in order, by JSON
// equality.
func (s *LoopScenario) Then(want ...loom.Command) *LoopScenario {
	s.t.Helper()
	s.mustRun()
	if s.err != nil {
		s.t.Fatalf("loomtest: look returned error: %v", s.err)
	}
	if len(s.cmds) != len(want) {
		s.t.Fatalf("loomtest: look dispatched %d commands, want %d: got %s", len(s.cmds), len(want), dumpCmds(s.cmds))
	}
	for i := range want {
		if !sameJSON(s.cmds[i], want[i]) {
			s.t.Fatalf("loomtest: command %d differs:\n got: %s\nwant: %s", i, js(s.cmds[i]), js(want[i]))
		}
	}
	return s
}

// ThenNothing asserts the last look dispatched nothing, and so emitted
// nothing: there was nothing to do.
func (s *LoopScenario) ThenNothing() *LoopScenario {
	s.t.Helper()
	s.mustRun()
	if s.err != nil {
		s.t.Fatalf("loomtest: look returned error: %v", s.err)
	}
	if len(s.cmds) != 0 || len(s.events) != 0 {
		s.t.Fatalf("loomtest: look dispatched %s and emitted %s, want nothing", dumpCmds(s.cmds), dump(s.events))
	}
	return s
}

// Emitted is the events the last look's commands emitted, in order.
func (s *LoopScenario) Emitted() []any {
	s.t.Helper()
	s.mustRun()
	if s.err != nil {
		s.t.Fatalf("loomtest: look returned error: %v", s.err)
	}
	return s.events
}

func (s *LoopScenario) mustRun() {
	s.t.Helper()
	if !s.ran {
		s.t.Fatal("loomtest: call Look first")
	}
}
