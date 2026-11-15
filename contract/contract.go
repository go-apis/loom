// Package contract checks a gateway's access rules by running them:
// every query and mutation of the composed schema, as every kind of
// principal the service names, against a seeded store. What each
// principal may run, which rows each list shows it and which fields
// are hidden from it are recorded in a file kept beside the code — the
// contract — and a change to any of them fails the test, naming the
// operation and the principal, until the file is recorded again and the
// change reviewed.
//
// It exists for the bug no ordinary test finds: a rule that holds for
// the principals someone thought to test and is skipped for one nobody
// ran. A suite runs them all, so a policy that drops a filter for one
// kind of principal changes that principal's rows and the test says so.
//
//	contract.Run(t, contract.Suite{
//		Gateway:    gateway, // loom/graphql.New(...)
//		Principals: []contract.Principal{
//			{Name: "person", Access: person},
//			{Name: "org admin", Access: orgAdmin},
//			{Name: "estate staff", Access: staff},
//		},
//		Namespaces: []string{"acme", "globex"},
//		Golden:     "testdata/contract.json",
//		Asks:       asks, // what a client publishes that it asks (ReadAsks)
//	})
//
// Record the contract the first time, and after a change that is meant,
// with LOOM_CONTRACT_UPDATE=1.
package contract

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	loomgql "github.com/go-apis/loom/graphql"
)

// Principal is one kind of caller the service's policy tells apart: a
// person, an org admin, estate staff. Access is what the gateway's Auth
// hook would resolve for one.
type Principal struct {
	Name   string
	Access loomgql.Access
}

// Suite is a gateway, the principals to run it as, and where its
// contract is kept.
type Suite struct {
	// Gateway is the handler loom/graphql.New returned, over services
	// whose store the test has seeded.
	Gateway http.Handler
	// Principals is every kind of caller, in the order the contract
	// lists them.
	Principals []Principal
	// Namespaces is where each list is read: the namespaces the test
	// seeded. A principal with no access to one is refused there, and
	// the contract says so.
	Namespaces []string
	// Labels names seeded rows by id, so the contract reads "the
	// archived project" where it would read a uuid. A row with no label
	// is recorded by its id: give seeded rows ids that are the same on
	// every run.
	Labels map[string]string
	// Golden is the contract's file.
	Golden string
	// Asks is what a client publishes that it asks of the gateway; nil
	// for none. Everything it asks must be served to every principal in
	// AsksFor.
	Asks *Asks
	// AsksFor names the principals the client serves; empty means every
	// principal of the suite.
	AsksFor []string
	// AsksExcept excuses an asked query or mutation for the principals
	// named: what the client asks only on behalf of some.
	AsksExcept map[string][]string
}

// Contract is what a gateway did for every principal.
type Contract struct {
	Principals []string `json:"principals"`
	// Operations is every query and mutation by name.
	Operations map[string]*Operation `json:"operations"`
	// Hidden is, for each type with a field hidden from someone, the
	// fields hidden from each principal.
	Hidden map[string]map[string][]string `json:"hidden,omitempty"`
}

// Operation is one query or mutation as each principal met it.
type Operation struct {
	// Kind is query or mutation.
	Kind string `json:"kind"`
	// Allowed is whether each principal could ever run it (the
	// gateway's own probe, Executor.Allowed).
	Allowed map[string]bool `json:"allowed"`
	// Rows is, for a list, what each principal was shown in each
	// namespace: the rows by label or id, sorted, or the refusal.
	Rows map[string]map[string]Listing `json:"rows,omitempty"`
}

// Listing is what one principal's read of a list in one namespace came
// to.
type Listing struct {
	Rows    []string `json:"rows,omitempty"`
	Refused string   `json:"refused,omitempty"`
}

// Observe runs the suite and answers what the gateway did.
func (s Suite) Observe(ctx context.Context) (*Contract, error) {
	ex, ok := loomgql.AsExecutor(s.Gateway)
	if !ok {
		return nil, fmt.Errorf("contract: Gateway is not a loom/graphql gateway")
	}
	if len(s.Principals) == 0 {
		return nil, fmt.Errorf("contract: no principals")
	}
	raw, err := ex.Introspect()
	if err != nil {
		return nil, err
	}
	sch := readSchema(raw)
	out := &Contract{Operations: map[string]*Operation{}, Hidden: map[string]map[string][]string{}}
	seen := map[string]bool{}
	for _, p := range s.Principals {
		if p.Name == "" || seen[p.Name] {
			return nil, fmt.Errorf("contract: principal %q is unnamed or named twice", p.Name)
		}
		seen[p.Name] = true
		out.Principals = append(out.Principals, p.Name)
	}
	for _, f := range sch.roots {
		op := &Operation{Kind: f.kind, Allowed: map[string]bool{}}
		out.Operations[f.name] = op
		for _, p := range s.Principals {
			pctx := loomgql.WithAccess(ctx, p.Access)
			op.Allowed[p.Name] = ex.Allowed(pctx, f.name)
			if !f.list(sch) {
				continue
			}
			if op.Rows == nil {
				op.Rows = map[string]map[string]Listing{}
			}
			op.Rows[p.Name] = map[string]Listing{}
			for _, ns := range s.Namespaces {
				op.Rows[p.Name][ns] = s.read(pctx, ex, f, ns)
			}
		}
	}
	for _, typ := range sch.objects {
		for _, p := range s.Principals {
			hidden := ex.Hidden(loomgql.WithAccess(ctx, p.Access), typ)
			if len(hidden) == 0 {
				continue
			}
			if out.Hidden[typ] == nil {
				out.Hidden[typ] = map[string][]string{}
			}
			for field := range hidden {
				out.Hidden[typ][p.Name] = append(out.Hidden[typ][p.Name], field)
			}
			sort.Strings(out.Hidden[typ][p.Name])
		}
	}
	return out, nil
}

// listLimit is how many rows a list is read for. A seed is small; a
// contract of more rows than this records the first of them.
const listLimit = 200

// read runs one list as one principal in one namespace.
func (s Suite) read(ctx context.Context, ex loomgql.Executor, f rootField, ns string) Listing {
	doc := fmt.Sprintf("query($ns: %s) { %s(namespace: $ns, limit: %d) { id } }", f.namespaceType, f.name, listLimit)
	res := ex.Exec(ctx, doc, map[string]any{"ns": ns})
	if len(res.Errors) > 0 {
		return Listing{Refused: strings.Join(res.Errors, "; ")}
	}
	rows, _ := res.Data[f.name].([]any)
	out := Listing{Rows: []string{}}
	for _, r := range rows {
		row, _ := r.(map[string]any)
		id := fmt.Sprint(row["id"])
		if label, ok := s.Labels[id]; ok {
			id = label
		}
		out.Rows = append(out.Rows, id)
	}
	sort.Strings(out.Rows)
	return out
}

// Differences compares what the gateway does now with the contract
// recorded: one line for each operation and principal that differs,
// naming both. Empty means the contract holds.
func Differences(recorded, now *Contract) []string {
	var out []string
	say := func(format string, args ...any) { out = append(out, fmt.Sprintf(format, args...)) }
	if strings.Join(recorded.Principals, ",") != strings.Join(now.Principals, ",") {
		say("the principals are %s; the contract was recorded for %s", strings.Join(now.Principals, ", "), strings.Join(recorded.Principals, ", "))
	}
	names := map[string]bool{}
	for name := range recorded.Operations {
		names[name] = true
	}
	for name := range now.Operations {
		names[name] = true
	}
	for _, name := range sortedNames(names) {
		was, is := recorded.Operations[name], now.Operations[name]
		switch {
		case was == nil:
			say("%s %s is new: the contract does not say who may run it", is.Kind, name)
			continue
		case is == nil:
			say("%s %s is gone", was.Kind, name)
			continue
		}
		for _, p := range now.Principals {
			wasAllowed, known := was.Allowed[p]
			if !known {
				continue // a new principal: said once, above
			}
			if wasAllowed != is.Allowed[p] {
				say("%s %s as %s: %s; the contract says %s", is.Kind, name, p, may(is.Allowed[p]), may(wasAllowed))
			}
			for _, ns := range sortedNames(keys(was.Rows[p], is.Rows[p])) {
				a, b := was.Rows[p][ns], is.Rows[p][ns]
				if a.Refused != b.Refused || strings.Join(a.Rows, "\x00") != strings.Join(b.Rows, "\x00") {
					say("%s %s as %s in %s: %s; the contract says %s", is.Kind, name, p, ns, b, a)
				}
			}
		}
	}
	for _, typ := range sortedNames(keys(recorded.Hidden, now.Hidden)) {
		for _, p := range now.Principals {
			a, b := strings.Join(recorded.Hidden[typ][p], ", "), strings.Join(now.Hidden[typ][p], ", ")
			if a != b {
				say("type %s as %s: hides [%s]; the contract says [%s]", typ, p, b, a)
			}
		}
	}
	return out
}

func may(allowed bool) string {
	if allowed {
		return "may run it"
	}
	return "is refused"
}

func (l Listing) String() string {
	if l.Refused != "" {
		return "refused (" + l.Refused + ")"
	}
	if len(l.Rows) == 0 {
		return "shown no rows"
	}
	return "shown [" + strings.Join(l.Rows, ", ") + "]"
}

// Unserved is what the client's published asks name that a principal
// the client serves is not given: an asked query or mutation the policy
// refuses it, one the gateway does not have, a field hidden from it.
// One line each, naming the ask and the principal.
func (s Suite) Unserved(now *Contract) []string {
	if s.Asks == nil {
		return nil
	}
	serves := s.AsksFor
	if len(serves) == 0 {
		serves = now.Principals
	}
	excused := func(ask, principal string) bool {
		for _, p := range s.AsksExcept[ask] {
			if p == principal {
				return true
			}
		}
		return false
	}
	var out []string
	asked := func(kind string, names []string) {
		for _, name := range names {
			op := now.Operations[name]
			if op == nil || op.Kind != kind {
				out = append(out, fmt.Sprintf("the client asks %s %s, which the gateway does not have", kind, name))
				continue
			}
			for _, p := range serves {
				if !op.Allowed[p] && !excused(name, p) {
					out = append(out, fmt.Sprintf("the client asks %s %s, which is refused as %s", kind, name, p))
				}
			}
		}
	}
	asked("query", s.Asks.Queries)
	asked("mutation", s.Asks.Mutations)
	for _, typ := range sortedNames(keys(s.Asks.Fields, nil)) {
		for _, field := range s.Asks.Fields[typ] {
			for _, p := range serves {
				for _, hidden := range now.Hidden[typ][p] {
					if hidden == field && !excused(typ+"."+field, p) {
						out = append(out, fmt.Sprintf("the client reads %s.%s, which is hidden as %s", typ, field, p))
					}
				}
			}
		}
	}
	return out
}

// Asks is what a client publishes that it asks of a gateway: the root
// queries and mutations it sends and the fields it reads on each type.
// It is the shape of runsheet's console manifest,
// /.well-known/gateway-asks.json; what else that file carries is not
// read here.
type Asks struct {
	Version   int                 `json:"version"`
	Queries   []string            `json:"queries"`
	Mutations []string            `json:"mutations"`
	Fields    map[string][]string `json:"fields"`
}

// ReadAsks reads a published asks file.
func ReadAsks(path string) (*Asks, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var a Asks
	if err := json.Unmarshal(raw, &a); err != nil {
		return nil, fmt.Errorf("contract: %s: %w", path, err)
	}
	if len(a.Queries)+len(a.Mutations)+len(a.Fields) == 0 {
		return nil, fmt.Errorf("contract: %s asks nothing: a check that could not be made is not a pass", path)
	}
	return &a, nil
}

// UpdateEnv is the environment variable that has Run record the
// contract in place of checking it.
const UpdateEnv = "LOOM_CONTRACT_UPDATE"

// Run observes the suite and fails t for every operation and principal
// that differs from the recorded contract, and for every ask a
// principal is not served. With LOOM_CONTRACT_UPDATE set it records the
// contract instead; the asks are checked either way.
func Run(t *testing.T, s Suite) {
	t.Helper()
	now, err := s.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, miss := range s.Unserved(now) {
		t.Error("contract: " + miss)
	}
	if s.Golden == "" {
		t.Fatal("contract: Suite.Golden names no file")
	}
	if os.Getenv(UpdateEnv) != "" {
		if err := Write(s.Golden, now); err != nil {
			t.Fatal(err)
		}
		t.Logf("contract: recorded %s", s.Golden)
		return
	}
	recorded, err := Read(s.Golden)
	if os.IsNotExist(err) {
		t.Fatalf("contract: %s is not recorded yet: run once with %s=1, read the file, and commit it", s.Golden, UpdateEnv)
	}
	if err != nil {
		t.Fatal(err)
	}
	for _, diff := range Differences(recorded, now) {
		t.Error("contract: " + diff)
	}
	if t.Failed() {
		t.Logf("contract: if the change is meant, record it with %s=1 and commit %s", UpdateEnv, s.Golden)
	}
}

// Read reads a recorded contract.
func Read(path string) (*Contract, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Contract
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("contract: %s: %w", path, err)
	}
	return &c, nil
}

// Write records a contract: indented JSON with its keys in order, so two
// recordings of one behaviour are the same bytes and a change reads as
// a small diff.
func Write(path string, c *Contract) error {
	raw, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o644)
}

func sortedNames(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for name := range set {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// keys is the keys of both maps.
func keys[V any](a, b map[string]V) map[string]bool {
	out := map[string]bool{}
	for k := range a {
		out[k] = true
	}
	for k := range b {
		out[k] = true
	}
	return out
}
