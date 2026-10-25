// Package gen turns a compiled schema into the service's Go code: typed
// models with folds, the registry wiring, handler interfaces (always
// regenerated), and implementation stubs (generated once, then owned by the
// developer — the gqlgen resolver rule).
package gen

import (
	"bytes"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/go-apis/loom/schema"
)

// gofmt formats generated source; on error (a generator bug) the raw text
// is written anyway so the compile error is inspectable.
func gofmt(path, content string) []byte {
	out, err := format.Source([]byte(content))
	if err != nil {
		fmt.Fprintf(os.Stderr, "warn: generated %s does not format: %v\n", path, err)
		return []byte(content)
	}
	return out
}

type Config struct {
	// Dir is the service root (where loom.yml lives). Stubs land here.
	Dir string
	// Package is the service package name (default: base of Dir).
	Package string
	// GenDir is the generated package directory under Dir (default "loomgen").
	GenDir string
	// Module is the service module path, used for the generated import.
	Module string
	// Layout picks where stubs land: "flat" (default) puts them in the
	// service root; "folders" gives each kind its own package —
	// aggregates/, records/, policies/, processes/ — with registry.go at
	// the root wiring them.
	Layout string
}

type Result struct {
	Written []string
	Skipped []string // stub files that already existed
	Removed []string // orphaned generated files no longer produced
}

// prepare validates the schema, fills the config defaults and returns the
// generator both Generate and Render/Check work from.
func prepare(s *schema.Schema, cfg Config) (*generator, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	if cfg.Package == "" {
		cfg.Package = identLower(filepath.Base(absOr(cfg.Dir)))
	}
	if cfg.GenDir == "" {
		cfg.GenDir = "loomgen"
	}
	if cfg.Module == "" {
		return nil, fmt.Errorf("gen: Config.Module is required (the service module path)")
	}
	return &generator{s: s, cfg: cfg, genPkg: filepath.Base(cfg.GenDir)}, nil
}

// render returns the regenerated loomgen files (path -> final bytes, gofmt
// applied). Stubs are not included: they belong to the user.
func (g *generator) render() map[string][]byte {
	genDir := filepath.Join(g.cfg.Dir, g.cfg.GenDir)
	files := map[string]string{
		filepath.Join(genDir, "models_gen.go"):   g.models(),
		filepath.Join(genDir, "registry_gen.go"): g.registry(),
	}
	for _, o := range g.owners() {
		files[filepath.Join(genDir, o.file)] = g.ownerFile(o)
	}
	if sql := g.tablesSQL(); sql != "" {
		files[filepath.Join(genDir, "tables_gen.sql")] = sql
	}
	out := make(map[string][]byte, len(files))
	for path, content := range files {
		if strings.HasSuffix(path, ".go") {
			out[path] = gofmt(path, content)
		} else {
			out[path] = []byte(content)
		}
	}
	return out
}

// Render returns the regenerated loomgen files (path -> final bytes) without
// touching disk. Stubs are never included.
func Render(s *schema.Schema, cfg Config) (map[string][]byte, error) {
	g, err := prepare(s, cfg)
	if err != nil {
		return nil, err
	}
	return g.render(), nil
}

// Check renders the regenerated files in memory and compares them to disk
// byte for byte. It writes nothing. stale files differ from disk, or are a
// generated file render no longer produces (an orphan, e.g. a renamed or
// deleted aggregate's file); missing files are absent. Stubs are the
// user's and are never compared. Both lists are sorted.
func Check(s *schema.Schema, cfg Config) (stale, missing []string, err error) {
	g, err := prepare(s, cfg)
	if err != nil {
		return nil, nil, err
	}
	files := g.render()
	for path, want := range files {
		got, err := os.ReadFile(path)
		if os.IsNotExist(err) {
			missing = append(missing, path)
		} else if err != nil {
			return nil, nil, err
		} else if !bytes.Equal(got, want) {
			stale = append(stale, path)
		}
	}
	orphans, err := orphanGenFiles(filepath.Join(g.cfg.Dir, g.cfg.GenDir), files)
	if err != nil {
		return nil, nil, err
	}
	stale = append(stale, orphans...)
	sort.Strings(stale)
	sort.Strings(missing)
	return stale, missing, nil
}

// orphanGenFiles lists generated files on disk, in dir, that render no
// longer produces: a _gen.go or _gen.sql file carrying the generated
// header, whose path is not a key of files. A file without the header —
// hand-written, or a stub — is never touched. The result is sorted.
func orphanGenFiles(dir string, files map[string][]byte) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !(strings.HasSuffix(name, "_gen.go") || strings.HasSuffix(name, "_gen.sql")) {
			continue
		}
		path := filepath.Join(dir, name)
		if _, ok := files[path]; ok {
			continue
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if !hasGeneratedHeader(path, content) {
			continue
		}
		out = append(out, path)
	}
	sort.Strings(out)
	return out, nil
}

// hasGeneratedHeader reports whether content opens with loom's generated
// header, in the comment style of path's extension.
func hasGeneratedHeader(path string, content []byte) bool {
	prefix := "// Code generated by loom generate. DO NOT EDIT.\n"
	if strings.HasSuffix(path, ".sql") {
		prefix = "-- Code generated by loom generate. DO NOT EDIT.\n"
	}
	return bytes.HasPrefix(content, []byte(prefix))
}

func Generate(s *schema.Schema, cfg Config) (*Result, error) {
	g, err := prepare(s, cfg)
	if err != nil {
		return nil, err
	}
	cfg = g.cfg

	res := &Result{}
	genDir := filepath.Join(cfg.Dir, cfg.GenDir)
	if err := os.MkdirAll(genDir, 0o755); err != nil {
		return nil, err
	}
	files := g.render()
	for path, out := range files {
		if err := os.WriteFile(path, out, 0o644); err != nil {
			return nil, err
		}
		res.Written = append(res.Written, path)
	}

	orphans, err := orphanGenFiles(genDir, files)
	if err != nil {
		return nil, err
	}
	for _, path := range orphans {
		if err := os.Remove(path); err != nil {
			return nil, err
		}
		res.Removed = append(res.Removed, path)
	}

	for name, content := range g.stubs() {
		path := filepath.Join(cfg.Dir, name)
		if _, err := os.Stat(path); err == nil {
			res.Skipped = append(res.Skipped, path)
			continue
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, err
		}
		if err := os.WriteFile(path, gofmt(path, content), 0o644); err != nil {
			return nil, err
		}
		res.Written = append(res.Written, path)
	}
	sort.Strings(res.Written)
	sort.Strings(res.Skipped)
	sort.Strings(res.Removed)
	return res, nil
}

type generator struct {
	s      *schema.Schema
	cfg    Config
	genPkg string
}

const header = "// Code generated by loom generate. DO NOT EDIT.\n\n"

// --- models ---

func (g *generator) models() string {
	var b strings.Builder
	b.WriteString(header)
	fmt.Fprintf(&b, "package %s\n\n", g.genPkg)
	b.WriteString(`import (
	"fmt"
	"time"

	"github.com/go-apis/loom"
	"github.com/google/uuid"
)

var (
	_ = fmt.Sprintf
	_ = time.Time{}
	_ = uuid.UUID{}
	_ = loom.CommandBase{}
)

`)

	for _, e := range g.s.Enums {
		g.enumDef(&b, e)
	}
	for _, t := range g.s.Types {
		g.structDef(&b, t.Name, t.Payload)
	}
	// an event emitted by one aggregate or record lives in its file; the
	// rest (emitted by several, by none, or foreign) stay here
	owned := g.eventOwners()
	for _, e := range g.s.Events {
		if owned[e.Name] == "" {
			g.eventDef(&b, e)
		}
	}
	for _, e := range g.s.Entities {
		g.structDef(&b, e.Name, e.State)
		g.foldDef(&b, e.Name, g.entityEvents(e))
	}
	for _, sr := range g.s.Series {
		// series rows don't fold: observations are appended, never rebuilt
		g.structDef(&b, sr.Name, sr.State)
		fmt.Fprintf(&b, "func (*%s) LoomSeries() string { return %q }\n\n", sr.Name, sr.Name)
	}
	return b.String()
}

// eventDefEntry renders an event's registry entry, the *loom.EventDef
// literal without its type: "{Name: ...}".
func (g *generator) eventDefEntry(e *schema.Event) string {
	version := e.Version
	if version == 0 {
		version = 1
	}
	pii := ""
	if fields := e.Payload.PIIFields(); len(fields) > 0 {
		pii = fmt.Sprintf(" PII: %s,", stringSlice(fields))
	}
	ups := ""
	if len(e.Upcasts) > 0 {
		var hops []string
		for _, from := range e.Upcasts {
			hops = append(hops, fmt.Sprintf("%d: impl.Upcasts.%s", from, upcastMethod(e, from)))
		}
		ups = fmt.Sprintf(" Upcasts: map[int]loom.UpcastFunc{%s},", strings.Join(hops, ", "))
	}
	ret := ""
	if e.Retired {
		ret = " Retired: true,"
	}
	return fmt.Sprintf("{Name: %q, SchemaVersion: %d, Publish: %v, Service: %q,%s Aliases: %s,%s%s New: func() any { return &%s{} }}",
		e.Name, version, e.Publish, e.Service, ret, stringSlice(e.Aliases), pii, ups, e.Name)
}

func (g *generator) eventDef(b *strings.Builder, e *schema.Event) {
	g.structDef(b, e.Name, e.Payload)
	fmt.Fprintf(b, "func (*%s) LoomEvent() string { return %q }\n\n", e.Name, e.Name)
}

// --- one file per aggregate and record ---

// owner is an aggregate or a record and the loomgen file it is rendered
// into. Exactly one of agg and rec is set.
type owner struct {
	name string
	file string
	agg  *schema.Aggregate
	rec  *schema.Record
}

func (o owner) commands() []*schema.Command {
	if o.agg != nil {
		return o.agg.Commands
	}
	return o.rec.Commands
}

// owners lists the aggregates, then the records, in schema order, each
// with its file: <snake name>_gen.go. A name whose file is already taken —
// by the shared models_gen.go or registry_gen.go, or by an earlier name
// that snakes the same way — takes its kind appended, then a number. The
// choice depends on the schema's slices only, so it is the same on every
// run (ADR 0008).
func (g *generator) owners() []owner {
	taken := map[string]bool{"models": true, "registry": true}
	pick := func(name, kind string) string {
		base := toSnake(name)
		if taken[base] {
			base += "_" + kind
		}
		file := base
		for n := 2; taken[file]; n++ {
			file = fmt.Sprintf("%s_%d", base, n)
		}
		taken[file] = true
		return file + "_gen.go"
	}
	var out []owner
	for _, a := range g.s.Aggregates {
		out = append(out, owner{name: a.Name, file: pick(a.Name, "aggregate"), agg: a})
	}
	for _, r := range g.s.Records {
		out = append(out, owner{name: r.Name, file: pick(r.Name, "record"), rec: r})
	}
	return out
}

// eventOwners maps each event emitted by exactly one aggregate or record
// to that owner's name. An event emitted by several, or by none (a
// @retired or a foreign event), has no owner and stays in models_gen.go.
func (g *generator) eventOwners() map[string]string {
	emitters := map[string]map[string]bool{}
	for _, o := range g.owners() {
		for _, c := range o.commands() {
			for _, e := range c.Emits {
				if emitters[e] == nil {
					emitters[e] = map[string]bool{}
				}
				emitters[e][o.name] = true
			}
		}
	}
	out := map[string]string{}
	for e, by := range emitters {
		if len(by) == 1 {
			out[e] = sortedKeys(by)[0]
		}
	}
	return out
}

// defFunc names the generated function that builds an owner's definition.
func (o owner) defFunc() string {
	if o.agg != nil {
		return "aggregateDef" + o.name
	}
	return "recordDef" + o.name
}

// ownerFile renders one aggregate's or record's file: its commands and
// their Validate, the events only it emits, its state (and fold), its
// Handlers interface, and the function returning its definition with the
// Handle closures over those handlers.
func (g *generator) ownerFile(o owner) string {
	var b strings.Builder
	b.WriteString(header)
	fmt.Fprintf(&b, "package %s\n\n", g.genPkg)
	b.WriteString(`import (
	"context"
	"fmt"
	"time"

	"github.com/go-apis/loom"
	"github.com/google/uuid"
)

var (
	_ context.Context
	_ = fmt.Sprintf
	_ = time.Time{}
	_ = uuid.UUID{}
)

`)
	for _, c := range o.commands() {
		g.commandDef(&b, c)
	}
	owned := g.eventOwners()
	for _, e := range g.s.Events {
		if owned[e.Name] == o.name {
			g.eventDef(&b, e)
		}
	}
	if o.agg != nil {
		g.structDef(&b, o.name, o.agg.State)
		g.foldDef(&b, o.name, g.aggregateEvents(o.agg))
	} else {
		// records don't fold: the state row is the source of truth
		g.structDef(&b, o.name, o.rec.State)
	}

	fmt.Fprintf(&b, "type %sHandlers interface {\n", o.name)
	for _, c := range o.commands() {
		fmt.Fprintf(&b, "\t%s(ctx context.Context, state *%s, cmd *%s) ([]loom.DomainEvent, error)\n", c.Name, o.name, c.Name)
	}
	b.WriteString("}\n\n")

	var state *schema.Payload
	var readRoles []string
	defType, cmdType, stateType := "AggregateDef", "CommandDef", "any"
	if o.agg != nil {
		state, readRoles, stateType = o.agg.State, o.agg.ReadRoles, "loom.AggregateState"
	} else {
		state, readRoles, defType, cmdType = o.rec.State, o.rec.ReadRoles, "RecordDef", "RecordCommandDef"
	}
	fmt.Fprintf(&b, "func %s(h %sHandlers) *loom.%s {\n", o.defFunc(), o.name, defType)
	fmt.Fprintf(&b, "\treturn &loom.%s{\n\t\tName: %q,\n", defType, o.name)
	if o.agg != nil {
		fmt.Fprintf(&b, "\t\tSnapshotEvery: %d,\n", o.agg.Snapshot)
	}
	if pii := state.PIIFields(); len(pii) > 0 {
		fmt.Fprintf(&b, "\t\tStatePII: %s,\n", stringSlice(pii))
	}
	if sec := state.SecretFields(); len(sec) > 0 {
		fmt.Fprintf(&b, "\t\tStateSecret: %s,\n", stringSlice(sec))
	}
	if len(readRoles) > 0 {
		fmt.Fprintf(&b, "\t\tReadRoles: %s,\n", stringSlice(readRoles))
	}
	fmt.Fprintf(&b, "\t\tNewState: func() %s { return &%s{} },\n", stateType, o.name)
	fmt.Fprintf(&b, "\t\tCommands: []*loom.%s{\n", cmdType)
	for _, c := range o.commands() {
		fmt.Fprintf(&b, "\t\t\t{\n\t\t\t\tName: %q,\n\t\t\t\tNew: func() loom.Command { return &%s{} },\n\t\t\t\tEmits: %s,\n", c.Name, c.Name, stringSlice(c.Emits))
		if pii := c.Payload.PIIFields(); len(pii) > 0 {
			fmt.Fprintf(&b, "\t\t\t\tPII: %s,\n", stringSlice(pii))
		}
		if len(c.Roles) > 0 {
			fmt.Fprintf(&b, "\t\t\t\tRoles: %s,\n", stringSlice(c.Roles))
		}
		if req := requiredFields(c.Payload); len(req) > 0 {
			fmt.Fprintf(&b, "\t\t\t\tRequired: %s,\n", stringSlice(req))
		}
		fmt.Fprintf(&b, "\t\t\t\tHandle: func(ctx context.Context, state %s, cmd loom.Command) ([]any, error) {\n", stateType)
		fmt.Fprintf(&b, "\t\t\t\t\tevts, err := h.%s(ctx, state.(*%s), cmd.(*%s))\n", c.Name, o.name, c.Name)
		b.WriteString("\t\t\t\t\treturn asAny(evts), err\n\t\t\t\t},\n\t\t\t},\n")
	}
	b.WriteString("\t\t},\n\t}\n}\n\n")
	g.ownerRegistration(&b, o)
	return b.String()
}

// ownerRegistration renders the owner's binding method on Impl and the
// init that adds it to loomOwners: its def, the events only it emits, its
// uploads and its @table. NewRegistry reads loomOwners, so the shared
// registry_gen.go never names an aggregate or record (ADR 0008).
func (g *generator) ownerRegistration(b *strings.Builder, o owner) {
	kind, field := "aggregate", "Aggregates"
	if o.rec != nil {
		kind, field = "record", "Records"
	}
	fmt.Fprintf(b, "// %s binds h as the %s %s's handlers. NewRegistry panics\n// if it is never called.\nfunc (impl *Impl) %s(h %sHandlers) { impl.bind(%q, h) }\n\n", o.name, o.name, kind, o.name, o.name, o.name)
	b.WriteString("func init() {\n")
	fmt.Fprintf(b, "\tloomRegister(loomOwner{kind: %q, name: %q, register: func(impl Impl, h any, r *loom.Registry) {\n", kind, o.name)
	fmt.Fprintf(b, "\t\tr.%s = append(r.%s, %s(h.(%sHandlers)))\n", field, field, o.defFunc(), o.name)
	owned := g.eventOwners()
	var events []string
	for _, e := range g.s.Events {
		if owned[e.Name] == o.name {
			events = append(events, g.eventDefEntry(e))
		}
	}
	if len(events) > 0 {
		b.WriteString("\t\tr.Events = append(r.Events,\n")
		for _, e := range events {
			fmt.Fprintf(b, "\t\t\t&loom.EventDef%s,\n", e)
		}
		b.WriteString("\t\t)\n")
	}
	if o.agg != nil {
		if uploads := g.ownerUploads(o.name, o.agg.Commands, o.agg.Uploads); uploads != "" {
			b.WriteString("\t\tr.Uploads = append(r.Uploads,\n" + uploads + "\t\t)\n")
		}
		if o.agg.Table {
			b.WriteString("\t\tr.Tables = append(r.Tables,\n")
			g.tableDef(b, "&loom.TableDef{", o.name, o.agg.State, nil)
			b.WriteString("\t\t)\n")
		}
	} else if uploads := g.ownerUploads(o.name, o.rec.Commands, o.rec.Uploads); uploads != "" {
		b.WriteString("\t\tr.Uploads = append(r.Uploads,\n" + uploads + "\t\t)\n")
	}
	b.WriteString("\t}})\n}\n")
}

// aggregateEvents is the union of the aggregate's command emit contracts —
// exactly the events its state can ever fold.
func (g *generator) aggregateEvents(a *schema.Aggregate) []string {
	set := map[string]bool{}
	for _, c := range a.Commands {
		for _, e := range c.Emits {
			set[e] = true
		}
	}
	return sortedKeys(set)
}

func (g *generator) entityEvents(e *schema.Entity) []string {
	set := map[string]bool{}
	for _, p := range g.s.Projections {
		if p.Entity != e.Name {
			continue
		}
		for _, sub := range p.Subscriptions {
			set[sub.Event] = true
		}
	}
	return sortedKeys(set)
}

func (g *generator) structDef(b *strings.Builder, name string, pl *schema.Payload) {
	fmt.Fprintf(b, "type %s struct {\n", name)
	if pl != nil {
		for _, field := range sortedKeys(boolKeys(pl.Properties)) {
			fmt.Fprintf(b, "\t%s %s `json:%q`\n", exported(field), g.goType(pl.Properties[field]), field)
		}
	}
	b.WriteString("}\n\n")
}

func (g *generator) commandDef(b *strings.Builder, c *schema.Command) {
	fmt.Fprintf(b, "type %s struct {\n\tloom.CommandBase\n\n", c.Name)
	if c.Payload != nil {
		for _, field := range sortedKeys(boolKeys(c.Payload.Properties)) {
			fmt.Fprintf(b, "\t%s %s `json:%q`\n", exported(field), g.goType(c.Payload.Properties[field]), field)
		}
	}
	b.WriteString("}\n\n")
	fmt.Fprintf(b, "func (*%s) LoomCommand() string { return %q }\n\n", c.Name, c.Name)
	g.commandValidate(b, c)
}

// enumDef generates the named string type, its constants, and Valid().
func (g *generator) enumDef(b *strings.Builder, e *schema.Enum) {
	fmt.Fprintf(b, "type %s string\n\nconst (\n", e.Name)
	for _, v := range e.Values {
		fmt.Fprintf(b, "\t%s%s %s = %q\n", e.Name, exported(v), e.Name, v)
	}
	b.WriteString(")\n\n")
	fmt.Fprintf(b, "func (v %s) Valid() bool {\n\tswitch v {\n\tcase ", e.Name)
	for i, v := range e.Values {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(b, "%s%s", e.Name, exported(v))
	}
	b.WriteString(":\n\t\treturn true\n\t}\n\treturn false\n}\n\n")
}

// commandValidate generates Validate() for commands with enum fields:
// the dispatcher rejects values outside the set before the handler runs.
// Optional (nullable, non-required) fields may be empty.
func (g *generator) commandValidate(b *strings.Builder, c *schema.Command) {
	if c.Payload == nil {
		return
	}
	required := map[string]bool{}
	for _, r := range c.Payload.Required {
		required[r] = true
	}
	var checks []string
	for _, field := range sortedKeys(boolKeys(c.Payload.Properties)) {
		pl := c.Payload.Properties[field]
		switch {
		case pl.Ref != "" && g.s.FindEnum(pl.Ref) != nil:
			cond := fmt.Sprintf("!c.%s.Valid()", exported(field))
			if !required[field] && !pl.Nullable {
				cond = fmt.Sprintf("c.%s != \"\" && ", exported(field)) + cond
			} else if pl.Nullable {
				cond = fmt.Sprintf("c.%s != nil && !c.%s.Valid()", exported(field), exported(field))
			}
			checks = append(checks, fmt.Sprintf("\tif %s {\n\t\treturn fmt.Errorf(\"%s must be one of the %s values\")\n\t}\n", cond, field, pl.Ref))
		case pl.Type == "array" && pl.Items != nil && pl.Items.Ref != "" && g.s.FindEnum(pl.Items.Ref) != nil:
			checks = append(checks, fmt.Sprintf("\tfor _, v := range c.%s {\n\t\tif !v.Valid() {\n\t\t\treturn fmt.Errorf(\"%s must hold %s values\")\n\t\t}\n\t}\n", exported(field), field, pl.Items.Ref))
		}
	}
	if len(checks) == 0 {
		return
	}
	fmt.Fprintf(b, "func (c *%s) Validate() error {\n", c.Name)
	for _, ch := range checks {
		b.WriteString(ch)
	}
	b.WriteString("\treturn nil\n}\n\n")
}

// foldDef generates the state's Fold: per event, assignments for every
// field the state and the event share by json name.
func (g *generator) foldDef(b *strings.Builder, stateName string, events []string) {
	fmt.Fprintf(b, "func (s *%s) Fold(eventType string, data any) error {\n\tswitch e := data.(type) {\n", stateName)
	state := g.stateOf(stateName)
	for _, evtName := range events {
		evt := g.s.FindEvent(evtName)
		if evt == nil || evt.Payload == nil {
			continue
		}
		fmt.Fprintf(b, "\tcase *%s:\n", evtName)
		wrote := false
		for _, field := range sortedKeys(boolKeys(evt.Payload.Properties)) {
			sp, ok := state.Properties[field]
			if !ok {
				continue
			}
			ep := evt.Payload.Properties[field]
			line := g.foldAssign(exported(field), sp, ep)
			if line == "" {
				continue
			}
			fmt.Fprintf(b, "\t\t%s\n", line)
			wrote = true
		}
		if !wrote {
			b.WriteString("\t\t_ = e // no shared fields\n")
		}
	}
	b.WriteString("\tcase nil:\n\t\t_ = e\n\t\treturn loomErrNilEvent(eventType)\n")
	b.WriteString("\t}\n\treturn nil\n}\n\n")
}

func (g *generator) stateOf(name string) *schema.Payload {
	for _, a := range g.s.Aggregates {
		if a.Name == name {
			return a.State
		}
	}
	for _, e := range g.s.Entities {
		if e.Name == name {
			return e.State
		}
	}
	return &schema.Payload{}
}

// foldAssign emits one state-field assignment, bridging optionality between
// the event's and the state's Go types.
func (g *generator) foldAssign(field string, state, evt *schema.Payload) string {
	st, et := g.goType(state), g.goType(evt)
	switch {
	case st == et:
		return fmt.Sprintf("s.%s = e.%s", field, field)
	case st == "*"+et:
		return fmt.Sprintf("v%s := e.%s\n\t\ts.%s = &v%s", field, field, field, field)
	case et == "*"+st:
		return fmt.Sprintf("if e.%s != nil {\n\t\t\ts.%s = *e.%s\n\t\t}", field, field, field)
	default:
		return "" // incompatible shapes: schema author's explicit mismatch, skipped
	}
}

func (g *generator) goType(pl *schema.Payload) string {
	t := g.goTypeCore(pl)
	if pl != nil && pl.Nullable && !strings.HasPrefix(t, "[]") && !strings.HasPrefix(t, "map[") && t != "any" {
		return "*" + t
	}
	return t
}

func (g *generator) goTypeCore(pl *schema.Payload) string {
	if pl == nil {
		return "any"
	}
	if pl.Ref != "" {
		return pl.Ref
	}
	switch pl.Type {
	case "string":
		switch pl.Format {
		case "uuid":
			return "uuid.UUID"
		case "date-time":
			return "time.Time"
		case "byte":
			return "[]byte"
		default:
			return "string"
		}
	case "integer":
		return "int64"
	case "number":
		return "float64"
	case "boolean":
		return "bool"
	case "array":
		return "[]" + g.goType(pl.Items)
	case "object":
		if pl.Format == "file" {
			return "loom.FileRef"
		}
		return "map[string]any"
	default:
		return "any"
	}
}

// --- typed entity tables (@table) ---

// tableColumns derives the column shape of an @table entity: one column
// per state field, in sorted field order. Scalars get real SQL types;
// arrays, maps, named types, files, and bytes ride jsonb. All data columns
// are nullable — upserts always write every column, so NOT NULL would only
// buy ADD COLUMN pain on shape changes.
type tableColumn struct{ Name, Type string }

func (g *generator) tableColumns(pl *schema.Payload) []tableColumn {
	var out []tableColumn
	if pl == nil {
		return out
	}
	// @pii fields never become columns: entities reject the combination
	// outright, and @table aggregates keep sealed state out of the mirror
	pii := map[string]bool{}
	for _, f := range pl.PIIFields() {
		pii[f] = true
	}
	for _, field := range sortedKeys(boolKeys(pl.Properties)) {
		if pii[field] {
			continue
		}
		out = append(out, tableColumn{Name: field, Type: g.sqlType(pl.Properties[field])})
	}
	return out
}

func (g *generator) sqlType(pl *schema.Payload) string {
	if pl != nil && pl.Ref != "" && g.s.FindEnum(pl.Ref) != nil {
		return "text" // an enum is its string value at rest
	}
	if pl == nil || pl.Ref != "" {
		return "jsonb"
	}
	switch pl.Type {
	case "string":
		switch pl.Format {
		case "uuid":
			return "uuid"
		case "date-time":
			return "timestamptz"
		case "byte":
			return "jsonb" // base64 via the json round-trip, like the doc store
		default:
			return "text"
		}
	case "integer":
		return "bigint"
	case "number":
		return "double precision"
	case "boolean":
		return "boolean"
	default:
		return "jsonb"
	}
}

func (g *generator) tableName(name string) string {
	return "loom_t_" + identLower(g.s.Service) + "_" + toSnake(name)
}

func (g *generator) tableDDL(name string, pl *schema.Payload) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE IF NOT EXISTS %s (\n", g.tableName(name))
	b.WriteString("\tservice    text NOT NULL,\n\tnamespace  text NOT NULL,\n\tid         uuid NOT NULL,\n")
	for _, col := range g.tableColumns(pl) {
		fmt.Fprintf(&b, "\t%q %s,\n", col.Name, col.Type)
	}
	b.WriteString("\tupdated_at timestamptz NOT NULL DEFAULT now(),\n\tPRIMARY KEY (service, namespace, id)\n);")
	return b.String()
}

// tablePayloads lists every @table shape in declaration order: entities
// first, then aggregates whose state mirrors into a typed table. Only an
// entity may declare indexes (schema.Validate refuses them on an
// aggregate's mirror table).
func (g *generator) tablePayloads() []struct {
	Name    string
	State   *schema.Payload
	Indexes []*schema.Index
} {
	var out []struct {
		Name    string
		State   *schema.Payload
		Indexes []*schema.Index
	}
	for _, e := range g.s.Entities {
		if e.Table {
			out = append(out, struct {
				Name    string
				State   *schema.Payload
				Indexes []*schema.Index
			}{e.Name, e.State, e.Indexes})
		}
	}
	for _, a := range g.s.Aggregates {
		if a.Table {
			out = append(out, struct {
				Name    string
				State   *schema.Payload
				Indexes []*schema.Index
			}{a.Name, a.State, nil})
		}
	}
	return out
}

// indexDDL renders the full CREATE INDEX CONCURRENTLY statement for one
// declared index. CONCURRENTLY cannot run inside a transaction, so Migrate
// (item 3) issues it on its own connection after the migration commits.
func (g *generator) indexDDL(tableName string, idx *schema.Index) string {
	cols := make([]string, len(idx.Columns))
	for i, c := range idx.Columns {
		col := fmt.Sprintf("%q", c.Name)
		if c.Desc {
			col += " DESC"
		}
		switch c.Nulls {
		case "first":
			col += " NULLS FIRST"
		case "last":
			col += " NULLS LAST"
		}
		cols[i] = col
	}
	return fmt.Sprintf("CREATE INDEX CONCURRENTLY IF NOT EXISTS %s ON %s (%s);", idx.Name, tableName, strings.Join(cols, ", "))
}

// tablesSQL renders the reviewable DDL artifact (loomgen/tables_gen.sql) —
// the same statements Migrate executes.
func (g *generator) tablesSQL() string {
	var b strings.Builder
	writeHeader := func() {
		if b.Len() == 0 {
			b.WriteString("-- Code generated by loom generate. DO NOT EDIT.\n--\n-- Typed tables for @table entities/aggregates and series. Migrate\n-- executes these and then applies an additive column diff; incompatible\n-- drift is an error. @table remediation is drop table, Migrate, Rebuild;\n-- series data is NOT rebuildable — incompatible series drift is migrated\n-- by hand. On TimescaleDB, Migrate turns each series table into a\n-- hypertable on its time column (with a retention policy for @retain);\n-- on plain Postgres a @retain series is range-partitioned by day.\n\n")
		}
	}
	for _, t := range g.tablePayloads() {
		writeHeader()
		b.WriteString(g.tableDDL(t.Name, t.State))
		b.WriteString("\n\n")
		for _, idx := range t.Indexes {
			b.WriteString(g.indexDDL(g.tableName(t.Name), idx))
			b.WriteString("\n\n")
		}
	}
	for _, sr := range g.s.Series {
		writeHeader()
		b.WriteString(g.seriesDDL(sr))
		b.WriteString("\n\n")
		if sr.RetainDays > 0 {
			fmt.Fprintf(&b, "-- @retain(%dd): on plain Postgres Migrate creates the partitioned form instead and drops day partitions past retention;\n-- on TimescaleDB it adds a retention policy to the hypertable.\n", sr.RetainDays)
			b.WriteString(g.seriesDDLForm(sr, true))
			b.WriteString("\n\n")
		}
	}
	return b.String()
}

// --- series tables ---

func (g *generator) seriesTableName(name string) string {
	return "loom_s_" + identLower(g.s.Service) + "_" + toSnake(name)
}

// seriesDDL renders a series' CREATE TABLE: identity columns (dims/keys
// and the time field) NOT NULL, value columns nullable (additive shape
// changes leave older rows NULL), primary key (service, namespace,
// identity..., time) — the time column last, which is also what
// TimescaleDB requires of a hypertable's unique index.
func (g *generator) seriesDDL(sr *schema.Series) string {
	return g.seriesDDLForm(sr, false)
}

// seriesDDLForm renders the plain or the range-partitioned form.
func (g *generator) seriesDDLForm(sr *schema.Series, partitioned bool) string {
	notNull := map[string]bool{sr.Time: true}
	for _, d := range sr.Dims {
		notNull[d] = true
	}
	for _, k := range sr.Keys {
		notNull[k] = true
	}
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE IF NOT EXISTS %s (\n", g.seriesTableName(sr.Name))
	b.WriteString("\tservice    text NOT NULL,\n\tnamespace  text NOT NULL,\n")
	for _, col := range g.tableColumns(sr.State) {
		fmt.Fprintf(&b, "\t%q %s", col.Name, col.Type)
		if notNull[col.Name] {
			b.WriteString(" NOT NULL")
		}
		b.WriteString(",\n")
	}
	pk := []string{"service", "namespace"}
	for _, f := range sr.IdentityFields() {
		pk = append(pk, fmt.Sprintf("%q", f))
	}
	pk = append(pk, fmt.Sprintf("%q", sr.Time))
	fmt.Fprintf(&b, "\tPRIMARY KEY (%s)\n)", strings.Join(pk, ", "))
	if partitioned {
		// The time column closes the primary key, so it is a valid
		// partition key; Migrate picks this form on plain Postgres for a
		// @retain series and drops whole day partitions on expiry.
		fmt.Fprintf(&b, " PARTITION BY RANGE (%q)", sr.Time)
	}
	b.WriteString(";")
	return b.String()
}

// --- registry ---

func (g *generator) registry() string {
	var b strings.Builder
	b.WriteString(header)
	fmt.Fprintf(&b, "package %s\n\n", g.genPkg)
	b.WriteString(`import (
	"context"
	"fmt"
	"sort"

	"github.com/go-apis/loom"
	"github.com/google/uuid"
)

var (
	_ context.Context
	_ = uuid.UUID{}
)

func loomErrNilEvent(eventType string) error {
	return fmt.Errorf("nil payload for event %s", eventType)
}

`)

	// the aggregates' and records' Handlers interfaces live in their own files
	for _, r := range append(append([]*schema.Reactor{}, g.s.Policies...), g.s.Processes...) {
		fmt.Fprintf(&b, "type %sReactions interface {\n", exported(r.Name))
		for _, sub := range r.Subscriptions {
			fmt.Fprintf(&b, "\tOn%s(ctx context.Context, evt *loom.Event, data *%s) ([]loom.Command, error)\n", sub.Event, sub.Event)
		}
		b.WriteString("}\n\n")
	}
	// @fold projections: the fold is hand-written against this interface
	for _, p := range g.s.Projections {
		if !p.Fold {
			continue
		}
		fmt.Fprintf(&b, "type %sFolds interface {\n", exported(p.Name))
		for _, sub := range p.Subscriptions {
			fmt.Fprintf(&b, "\tOn%s(state *%s, evt *loom.Event, data *%s) error\n", sub.Event, p.Entity, sub.Event)
		}
		b.WriteString("}\n\n")
	}

	// upcasts: the hand-written migration hops, one interface service-wide
	if ups := g.upcastHops(); len(ups) > 0 {
		b.WriteString("// EventUpcasts lifts stored payloads written under older @v versions:\n// raw JSON of version n in, raw JSON of version n+1 out. Hops chain at\n// decode time.\ntype EventUpcasts interface {\n")
		for _, u := range ups {
			fmt.Fprintf(&b, "\t%s(data []byte) ([]byte, error)\n", u.method)
		}
		b.WriteString("}\n\n")
	}

	// Impl: one field per reactor and @fold projection the developer
	// implements; aggregates and records bind through methods their own
	// files declare (impl.Order(h)), so adding one never edits this file
	b.WriteString("type Impl struct {\n")
	for _, r := range append(append([]*schema.Reactor{}, g.s.Policies...), g.s.Processes...) {
		fmt.Fprintf(&b, "\t%s %sReactions\n", g.implField(r.Name), exported(r.Name))
	}
	for _, p := range g.s.Projections {
		if p.Fold {
			fmt.Fprintf(&b, "\t%s %sFolds\n", g.implField(p.Name), exported(p.Name))
		}
	}
	if len(g.upcastHops()) > 0 {
		b.WriteString("\tUpcasts EventUpcasts\n")
	}
	b.WriteString("\n\t// handlers holds what the aggregates' and records' binding methods set,\n\t// by name.\n\thandlers map[string]any\n}\n\n")

	fmt.Fprintf(&b, "func NewRegistry(impl Impl) *loom.Registry {\n")
	if len(g.upcastHops()) > 0 {
		// an older hand-owned registry.go compiles with the new field unset;
		// fail at boot with a pointed message instead of a nil-deref later
		b.WriteString("\tif impl.Upcasts == nil {\n\t\tpanic(\"loomgen: the schema declares upcasts — wire Impl.Upcasts (see the Upcasts stub; your registry.go predates it)\")\n\t}\n")
	}
	fmt.Fprintf(&b, "\tr := &loom.Registry{\n\t\tService: %q,\n", g.s.Service)

	// each aggregate and record adds its def, its events, uploads and
	// @table from its own file, through loomRegisterOwners below
	b.WriteString("\t\tAggregates: []*loom.AggregateDef{},\n\t\tRecords: []*loom.RecordDef{},\n")

	g.seriesDefs(&b)

	// the shared events only: one emitted by a single aggregate or record
	// is added by that owner's file
	owned := g.eventOwners()
	b.WriteString("\t\tEvents: []*loom.EventDef{\n")
	for _, e := range g.s.Events {
		if owned[e.Name] == "" {
			fmt.Fprintf(&b, "\t\t\t%s,\n", g.eventDefEntry(e))
		}
	}
	b.WriteString("\t\t},\n")

	g.reactorDefs(&b, "Policies", g.s.Policies)
	g.reactorDefs(&b, "Processes", g.s.Processes)

	if len(g.s.Enums) > 0 {
		b.WriteString("\t\tEnums: []*loom.EnumDef{\n")
		for _, e := range g.s.Enums {
			fmt.Fprintf(&b, "\t\t\t{Name: %q, Values: %s},\n", e.Name, stringSlice(e.Values))
		}
		b.WriteString("\t\t},\n")
	}

	// Types is written even when empty: every registry generated from
	// here on states its schema types, so the gateway's nested inputs
	// follow the schema's required lists (ADR 0004).
	types := append([]*schema.NamedType(nil), g.s.Types...)
	sort.Slice(types, func(i, j int) bool { return types[i].Name < types[j].Name })
	b.WriteString("\t\tTypes: []*loom.TypeDef{\n")
	for _, t := range types {
		if req := requiredFields(t.Payload); len(req) > 0 {
			fmt.Fprintf(&b, "\t\t\t{Name: %q, Required: %s},\n", t.Name, stringSlice(req))
		} else {
			fmt.Fprintf(&b, "\t\t\t{Name: %q},\n", t.Name)
		}
	}
	b.WriteString("\t\t},\n")

	joins := false
	for _, e := range g.s.Entities {
		joins = joins || len(e.Joins) > 0
	}
	if joins {
		b.WriteString("\t\tJoins: []*loom.JoinDef{\n")
		for _, e := range g.s.Entities {
			for _, j := range e.Joins {
				fmt.Fprintf(&b, "\t\t\t{OnEntity: %q, Field: %q, Service: %q, Entity: %q, List: %v, Via: %q},\n",
					e.Name, j.Field, j.Service, j.Entity, j.List, j.Via)
			}
		}
		b.WriteString("\t\t},\n")
	}

	g.tableDefs(&b)

	b.WriteString("\t\tProjections: []*loom.ProjectionDef{\n")
	for _, p := range g.s.Projections {
		fmt.Fprintf(&b, "\t\t\t{\n\t\t\t\tName: %q,\n\t\t\t\tEntity: %q,\n\t\t\t\tEvents: %s,\n", p.Name, p.Entity, stringSlice(eventNames(p)))
		if ent := g.s.FindEntity(p.Entity); ent != nil {
			if pii := ent.State.PIIFields(); len(pii) > 0 {
				fmt.Fprintf(&b, "\t\t\t\tPII: %s,\n", stringSlice(pii))
			}
			if len(ent.ReadRoles) > 0 {
				fmt.Fprintf(&b, "\t\t\t\tEntityReadRoles: %s,\n", stringSlice(ent.ReadRoles))
			}
		}
		fmt.Fprintf(&b, "\t\t\t\tNewState: func() loom.EntityState { return &%s{} },\n", p.Entity)
		keyed := false
		for _, sub := range p.Subscriptions {
			keyed = keyed || sub.Key != ""
		}
		if keyed {
			// key(field): child events land on the parent-keyed row
			b.WriteString("\t\t\t\tEntityID: func(evt *loom.Event) uuid.UUID {\n\t\t\t\t\tswitch e := evt.Data.(type) {\n")
			for _, sub := range p.Subscriptions {
				if sub.Key == "" {
					continue
				}
				fmt.Fprintf(&b, "\t\t\t\t\tcase *%s:\n\t\t\t\t\t\treturn e.%s\n", sub.Event, exported(sub.Key))
			}
			b.WriteString("\t\t\t\t\t}\n\t\t\t\t\treturn evt.AggregateID\n\t\t\t\t},\n")
		} else {
			b.WriteString("\t\t\t\tEntityID: func(evt *loom.Event) uuid.UUID { return evt.AggregateID },\n")
		}
		if p.Fold {
			fmt.Fprintf(&b, "\t\t\t\tFold: func(state loom.EntityState, evt *loom.Event) error {\n\t\t\t\t\tswitch data := evt.Data.(type) {\n")
			for _, sub := range p.Subscriptions {
				fmt.Fprintf(&b, "\t\t\t\t\tcase *%s:\n\t\t\t\t\t\treturn impl.%s.On%s(state.(*%s), evt, data)\n", sub.Event, g.implField(p.Name), sub.Event, p.Entity)
			}
			fmt.Fprintf(&b, "\t\t\t\t\t}\n\t\t\t\t\treturn fmt.Errorf(\"projection %s: unroutable event %%s\", evt.Type)\n\t\t\t\t},\n", p.Name)
		}
		b.WriteString("\t\t\t},\n")
	}
	b.WriteString("\t\t},\n\t}\n\tloomRegisterOwners(impl, r)\n\treturn r\n}\n\n")

	b.WriteString(`// loomOwner is one aggregate's or record's part of the registry. Each
// <name>_gen.go adds its own to loomOwners in init, so this file never
// lists them.
type loomOwner struct {
	kind     string // "aggregate" or "record"
	name     string
	register func(impl Impl, h any, r *loom.Registry)
}

var loomOwners []loomOwner

func loomRegister(o loomOwner) { loomOwners = append(loomOwners, o) }

func (impl *Impl) bind(name string, h any) {
	if impl.handlers == nil {
		impl.handlers = map[string]any{}
	}
	impl.handlers[name] = h
}

// loomRegisterOwners adds every aggregate, then every record, in name
// order — never init order — so the registry is the same on every boot.
// An owner whose handlers were never bound panics with its name.
func loomRegisterOwners(impl Impl, r *loom.Registry) {
	owners := append([]loomOwner(nil), loomOwners...)
	sort.Slice(owners, func(i, j int) bool {
		if owners[i].kind != owners[j].kind {
			return owners[i].kind == "aggregate"
		}
		return owners[i].name < owners[j].name
	})
	for _, o := range owners {
		h := impl.handlers[o.name]
		if h == nil {
			panic(fmt.Sprintf("loomgen: %s %s has no handlers — call impl.%s(...) on the Impl you pass to NewRegistry (your registry.go predates it)", o.kind, o.name, o.name))
		}
		o.register(impl, h, r)
	}
	sort.Slice(r.Events, func(i, j int) bool { return r.Events[i].Name < r.Events[j].Name })
}

func asAny[T any](in []T) []any {
	out := make([]any, len(in))
	for i, v := range in {
		out[i] = v
	}
	return out
}

func checkDispatches(reactor, event string, allowed []string, cmds []loom.Command) error {
	for _, cmd := range cmds {
		ok := false
		for _, a := range allowed {
			if cmd.LoomCommand() == a {
				ok = true
				break
			}
		}
		if !ok {
			return fmt.Errorf("%s on %s dispatched undeclared command %s", reactor, event, cmd.LoomCommand())
		}
	}
	return nil
}
`)
	return b.String()
}

// tableDefs renders the registry entries for @table entities and
// aggregates: the DDL and column shape Migrate applies, and the typed
// value extractor upserts ride on (jsonb columns pre-marshaled via
// loom.JSONValue so the driver never guesses at encoding). Aggregate
// state structs satisfy loom.EntityState (same Fold shape), so both kinds
// share the extractor signature.
func (g *generator) tableDefs(b *strings.Builder) {
	var tables []*schema.Entity
	for _, e := range g.s.Entities {
		if e.Table {
			tables = append(tables, e)
		}
	}
	if len(tables) == 0 {
		return
	}
	// a @table aggregate's entry is added by its own file
	b.WriteString("\t\tTables: []*loom.TableDef{\n")
	for _, t := range tables {
		g.tableDef(b, "{", t.Name, t.State, t.Indexes)
	}
	b.WriteString("\t\t},\n")
}

// tableDef renders one TableDef entry; open is how it begins: "{" in a
// slice literal, "&loom.TableDef{" in an append. indexes is nil for an
// aggregate's mirror table — only an entity may declare them.
func (g *generator) tableDef(b *strings.Builder, open, name string, state *schema.Payload, indexes []*schema.Index) {
	fmt.Fprintf(b, "\t\t\t%s\n\t\t\t\tEntity: %q,\n\t\t\t\tName: %q,\n\t\t\t\tDDL: `%s`,\n", open, name, g.tableName(name), g.tableDDL(name, state))
	b.WriteString("\t\t\t\tColumns: []loom.TableColumn{\n")
	for _, col := range g.tableColumns(state) {
		fmt.Fprintf(b, "\t\t\t\t\t{Name: %q, Type: %q},\n", col.Name, col.Type)
	}
	b.WriteString("\t\t\t\t},\n")
	fmt.Fprintf(b, "\t\t\t\tValues: func(state loom.EntityState) []any {\n\t\t\t\t\te := state.(*%s)\n\t\t\t\t\treturn []any{\n", name)
	for _, col := range g.tableColumns(state) {
		if col.Type == "jsonb" {
			fmt.Fprintf(b, "\t\t\t\t\t\tloom.JSONValue(e.%s),\n", exported(col.Name))
		} else {
			fmt.Fprintf(b, "\t\t\t\t\t\te.%s,\n", exported(col.Name))
		}
	}
	b.WriteString("\t\t\t\t\t}\n\t\t\t\t},\n")
	if len(indexes) > 0 {
		b.WriteString("\t\t\t\tIndexes: []loom.TableIndex{\n")
		for _, idx := range indexes {
			fmt.Fprintf(b, "\t\t\t\t\t{Name: %q, DDL: `%s`},\n", idx.Name, g.indexDDL(g.tableName(name), idx))
		}
		b.WriteString("\t\t\t\t},\n")
	}
	b.WriteString("\t\t\t},\n")
}

// seriesDefs renders the registry entries for series: the DDL and column
// shape Migrate applies, and the typed value extractor appends ride on
// (jsonb columns pre-marshaled via loom.JSONValue, like tableDefs).
func (g *generator) seriesDefs(b *strings.Builder) {
	if len(g.s.Series) == 0 {
		return
	}
	b.WriteString("\t\tSeries: []*loom.SeriesDef{\n")
	for _, sr := range g.s.Series {
		fmt.Fprintf(b, "\t\t\t{\n\t\t\t\tName: %q,\n\t\t\t\tTable: %q,\n\t\t\t\tDDL: `%s`,\n\t\t\t\tTime: %q,\n\t\t\t\tDims: %s,\n",
			sr.Name, g.seriesTableName(sr.Name), g.seriesDDL(sr), sr.Time, stringSlice(sr.Dims))
		if len(sr.Keys) > 0 {
			fmt.Fprintf(b, "\t\t\t\tKeys: %s,\n", stringSlice(sr.Keys))
		}
		if sr.RetainDays > 0 {
			fmt.Fprintf(b, "\t\t\t\tRetainDays: %d,\n\t\t\t\tPartitionDDL: `%s`,\n", sr.RetainDays, g.seriesDDLForm(sr, true))
		}
		b.WriteString("\t\t\t\tColumns: []loom.TableColumn{\n")
		for _, col := range g.tableColumns(sr.State) {
			fmt.Fprintf(b, "\t\t\t\t\t{Name: %q, Type: %q},\n", col.Name, col.Type)
		}
		b.WriteString("\t\t\t\t},\n")
		if req := requiredFields(sr.State); len(req) > 0 {
			fmt.Fprintf(b, "\t\t\t\tRequired: %s,\n", stringSlice(req))
		}
		fmt.Fprintf(b, "\t\t\t\tNew: func() loom.SeriesRow { return &%s{} },\n", sr.Name)
		fmt.Fprintf(b, "\t\t\t\tValues: func(row loom.SeriesRow) []any {\n\t\t\t\t\te := row.(*%s)\n\t\t\t\t\treturn []any{\n", sr.Name)
		for _, col := range g.tableColumns(sr.State) {
			if col.Type == "jsonb" {
				fmt.Fprintf(b, "\t\t\t\t\t\tloom.JSONValue(e.%s),\n", exported(col.Name))
			} else {
				fmt.Fprintf(b, "\t\t\t\t\t\te.%s,\n", exported(col.Name))
			}
		}
		b.WriteString("\t\t\t\t\t}\n\t\t\t\t},\n\t\t\t},\n")
	}
	b.WriteString("\t\t},\n")
}

type upcastHop struct {
	event  *schema.Event
	from   int
	method string
}

func upcastMethod(e *schema.Event, from int) string {
	return fmt.Sprintf("%sFromV%d", exported(e.Name), from)
}

// upcastHops lists every declared migration hop across all events, in
// schema order — the shape behind the EventUpcasts interface and stub.
func (g *generator) upcastHops() []upcastHop {
	var out []upcastHop
	for _, e := range g.s.Events {
		for _, from := range e.Upcasts {
			out = append(out, upcastHop{event: e, from: from, method: upcastMethod(e, from)})
		}
	}
	return out
}

// ownerUploads renders the registry entries for one owner's `upload`
// blocks; the file-field json names come from the shape Validate enforced.
func (g *generator) ownerUploads(owner string, commands []*schema.Command, uploads []*schema.Upload) string {
	var b strings.Builder
	fileField := func(name string) string {
		for _, c := range commands {
			if c.Name == name {
				return c.FileField()
			}
		}
		return ""
	}
	for _, u := range uploads {
		fmt.Fprintf(&b, "\t\t\t&loom.UploadDef{Name: %q, Owner: %q,", u.Name, owner)
		if u.OnStarted != "" {
			fmt.Fprintf(&b, " OnStarted: %q, StartedField: %q,", u.OnStarted, fileField(u.OnStarted))
		}
		fmt.Fprintf(&b, " OnUploaded: %q, UploadedField: %q},\n", u.OnUploaded, fileField(u.OnUploaded))
	}
	return b.String()
}

func (g *generator) reactorDefs(b *strings.Builder, field string, reactors []*schema.Reactor) {
	fmt.Fprintf(b, "\t\t%s: []*loom.ReactorDef{\n", field)
	for _, r := range reactors {
		var events []string
		for _, sub := range r.Subscriptions {
			events = append(events, sub.Event)
		}
		fmt.Fprintf(b, "\t\t\t{\n\t\t\t\tName: %q,\n\t\t\t\tEvents: %s,\n", r.Name, stringSlice(events))
		b.WriteString("\t\t\t\tSubs: []loom.SubscriptionDef{\n")
		for _, sub := range r.Subscriptions {
			fmt.Fprintf(b, "\t\t\t\t\t{Event: %q, Dispatches: %s},\n", sub.Event, stringSlice(sub.Dispatches))
		}
		b.WriteString("\t\t\t\t},\n")
		if len(r.Effects) > 0 {
			fmt.Fprintf(b, "\t\t\t\tEffects: %s,\n", stringSlice(r.Effects))
		}
		if len(r.Idempotent) > 0 {
			fmt.Fprintf(b, "\t\t\t\tIdempotentEffects: %s,\n", stringSlice(r.Idempotent))
		}
		if r.From != "" {
			fmt.Fprintf(b, "\t\t\t\tFrom: %q,\n", r.From)
		}
		if r.Retry != nil {
			// Validate has parsed the bounds; emit them as nanoseconds so
			// the generated file needs no time import
			lo, hi, _ := r.Retry.Durations()
			fmt.Fprintf(b, "\t\t\t\tRetry: &loom.RetryPolicy{Max: %d, Min: %d, MaxBackoff: %d}, // @retry(%d, %s..%s)\n",
				r.Retry.Max, int64(lo), int64(hi), r.Retry.Max, r.Retry.Min, r.Retry.MaxBackoff)
		}
		fmt.Fprintf(b, "\t\t\t\tReact: func(ctx context.Context, evt *loom.Event) ([]loom.Command, error) {\n\t\t\t\t\tswitch data := evt.Data.(type) {\n")
		for _, sub := range r.Subscriptions {
			fmt.Fprintf(b, "\t\t\t\t\tcase *%s:\n", sub.Event)
			fmt.Fprintf(b, "\t\t\t\t\t\tcmds, err := impl.%s.On%s(ctx, evt, data)\n", g.implField(r.Name), sub.Event)
			fmt.Fprintf(b, "\t\t\t\t\t\tif err != nil {\n\t\t\t\t\t\t\treturn nil, err\n\t\t\t\t\t\t}\n")
			fmt.Fprintf(b, "\t\t\t\t\t\treturn cmds, checkDispatches(%q, %q, %s, cmds)\n", r.Name, sub.Event, stringSlice(sub.Dispatches))
		}
		b.WriteString("\t\t\t\t\t}\n\t\t\t\t\treturn nil, fmt.Errorf(\"unroutable event %s\", evt.Type)\n\t\t\t\t},\n\t\t\t},\n")
	}
	b.WriteString("\t\t},\n")
}

// --- stubs ---

// stubKinds maps schema kinds to their folder/package when Layout is
// "folders". Only @fold projections generate stubs.
const (
	kindAggregates  = "aggregates"
	kindRecords     = "records"
	kindPolicies    = "policies"
	kindProcesses   = "processes"
	kindProjections = "projections"
)

func (g *generator) folders() bool { return g.cfg.Layout == "folders" }

// place returns the stub file path and its package name for a kind.
func (g *generator) place(kind, file string) (path, pkg string) {
	if g.folders() {
		return filepath.Join(kind, file), kind
	}
	return file, g.cfg.Package
}

// qualify renders a stub constructor for registry.go: kind-qualified
// under the folders layout, bare when flat.
func (g *generator) qualify(kind, typeName string) string {
	if g.folders() {
		return "&" + kind + "." + typeName + "{}"
	}
	return "&" + typeName + "{}"
}

func (g *generator) stubs() map[string]string {
	genImport := g.cfg.Module + "/" + filepath.ToSlash(g.cfg.GenDir)
	out := map[string]string{}

	for _, a := range g.s.Aggregates {
		path, pkg := g.place(kindAggregates, strings.ToLower(a.Name)+".go")
		var b strings.Builder
		fmt.Fprintf(&b, "package %s\n\n", pkg)
		fmt.Fprintf(&b, "import (\n\t\"context\"\n\n\t\"github.com/go-apis/loom\"\n\n\t%q\n)\n\n", genImport)
		fmt.Fprintf(&b, "// %s implements %s.%sHandlers. Yours to edit — loom generate never\n// rewrites this file.\n", a.Name, g.genPkg, a.Name)
		fmt.Fprintf(&b, "type %s struct{}\n\n", a.Name)
		for _, c := range a.Commands {
			fmt.Fprintf(&b, "func (h *%s) %s(ctx context.Context, state *%s.%s, cmd *%s.%s) ([]loom.DomainEvent, error) {\n\tpanic(\"not implemented\")\n}\n\n",
				a.Name, c.Name, g.genPkg, a.Name, g.genPkg, c.Name)
		}
		out[path] = b.String()
	}

	for _, r := range g.s.Records {
		path, pkg := g.place(kindRecords, strings.ToLower(r.Name)+".go")
		var b strings.Builder
		fmt.Fprintf(&b, "package %s\n\n", pkg)
		fmt.Fprintf(&b, "import (\n\t\"context\"\n\n\t\"github.com/go-apis/loom\"\n\n\t%q\n)\n\n", genImport)
		fmt.Fprintf(&b, "// %s implements %s.%sHandlers (a record: mutate state, optionally\n// announce events). Yours to edit.\n", r.Name, g.genPkg, r.Name)
		fmt.Fprintf(&b, "type %s struct{}\n\n", r.Name)
		for _, c := range r.Commands {
			fmt.Fprintf(&b, "func (h *%s) %s(ctx context.Context, state *%s.%s, cmd *%s.%s) ([]loom.DomainEvent, error) {\n\tpanic(\"not implemented\")\n}\n\n",
				r.Name, c.Name, g.genPkg, r.Name, g.genPkg, c.Name)
		}
		out[path] = b.String()
	}

	reactorStub := func(kind string, r *schema.Reactor) {
		path, pkg := g.place(kind, strings.ToLower(r.Name)+".go")
		var b strings.Builder
		fmt.Fprintf(&b, "package %s\n\n", pkg)
		fmt.Fprintf(&b, "import (\n\t\"context\"\n\n\t\"github.com/go-apis/loom\"\n\n\t%q\n)\n\n", genImport)
		fmt.Fprintf(&b, "// %s implements %s.%sReactions. Yours to edit.\n", exported(r.Name), g.genPkg, exported(r.Name))
		fmt.Fprintf(&b, "type %s struct{}\n\n", exported(r.Name))
		for _, sub := range r.Subscriptions {
			fmt.Fprintf(&b, "func (h *%s) On%s(ctx context.Context, evt *loom.Event, data *%s.%s) ([]loom.Command, error) {\n\tpanic(\"not implemented\")\n}\n\n",
				exported(r.Name), sub.Event, g.genPkg, sub.Event)
		}
		out[path] = b.String()
	}
	for _, r := range g.s.Policies {
		reactorStub(kindPolicies, r)
	}
	for _, r := range g.s.Processes {
		reactorStub(kindProcesses, r)
	}

	for _, p := range g.s.Projections {
		if !p.Fold {
			continue
		}
		path, pkg := g.place(kindProjections, strings.ToLower(p.Name)+".go")
		var b strings.Builder
		fmt.Fprintf(&b, "package %s\n\n", pkg)
		fmt.Fprintf(&b, "import (\n\t\"github.com/go-apis/loom\"\n\n\t%q\n)\n\n", genImport)
		fmt.Fprintf(&b, "// %s implements %s.%sFolds — the @fold projection's hand-written\n// fold. Runs on the checkpointed projection runner; rebuild refolds the\n// whole log, so keep it deterministic. Yours to edit.\n", exported(p.Name), g.genPkg, exported(p.Name))
		fmt.Fprintf(&b, "type %s struct{}\n\n", exported(p.Name))
		for _, sub := range p.Subscriptions {
			fmt.Fprintf(&b, "func (f *%s) On%s(state *%s.%s, evt *loom.Event, data *%s.%s) error {\n\tpanic(\"not implemented\")\n}\n\n",
				exported(p.Name), sub.Event, g.genPkg, p.Entity, g.genPkg, sub.Event)
		}
		out[path] = b.String()
	}

	// upcasts stub: service-wide, at the root in both layouts (hops cross
	// events, so no kind package fits)
	if ups := g.upcastHops(); len(ups) > 0 {
		var b strings.Builder
		fmt.Fprintf(&b, "package %s\n\n", g.cfg.Package)
		fmt.Fprintf(&b, "// Upcasts implements %s.EventUpcasts — migration for stored events\n// written under older @v versions. Each hop takes the payload JSON of one\n// version and returns the payload JSON of the next; loom chains hops at\n// decode time, so folds and reactions only ever see the current shape.\n// Keep hops deterministic: replays and rebuilds run them again.\n// Yours to edit.\ntype Upcasts struct{}\n\n", g.genPkg)
		for _, u := range ups {
			fmt.Fprintf(&b, "// %s lifts %s v%d → v%d.\nfunc (u *Upcasts) %s(data []byte) ([]byte, error) {\n\tpanic(\"not implemented\")\n}\n\n", u.method, u.event.Name, u.from, u.from+1, u.method)
		}
		out["upcasts.go"] = b.String()
	}

	// wiring, generated once so additions appear as compile errors on Impl
	var b strings.Builder
	fmt.Fprintf(&b, "package %s\n\n", g.cfg.Package)
	b.WriteString("import (\n\t\"github.com/go-apis/loom\"\n\n")
	foldProjections := 0
	for _, p := range g.s.Projections {
		if p.Fold {
			foldProjections++
		}
	}
	if g.folders() {
		for _, kind := range []struct {
			name string
			used bool
		}{
			{kindAggregates, len(g.s.Aggregates) > 0},
			{kindPolicies, len(g.s.Policies) > 0},
			{kindProcesses, len(g.s.Processes) > 0},
			{kindProjections, foldProjections > 0},
			{kindRecords, len(g.s.Records) > 0},
		} {
			if kind.used {
				fmt.Fprintf(&b, "\t%q\n", g.cfg.Module+"/"+kind.name)
			}
		}
		b.WriteString("\n")
	}
	fmt.Fprintf(&b, "\t%q\n)\n\n", genImport)
	fmt.Fprintf(&b, "// NewRegistry wires implementations into the generated registry. Yours\n// to edit.\nfunc NewRegistry() *loom.Registry {\n\timpl := %s.Impl{\n", g.genPkg)
	for _, r := range g.s.Policies {
		fmt.Fprintf(&b, "\t\t%s: %s,\n", g.implField(r.Name), g.qualify(kindPolicies, exported(r.Name)))
	}
	for _, r := range g.s.Processes {
		fmt.Fprintf(&b, "\t\t%s: %s,\n", g.implField(r.Name), g.qualify(kindProcesses, exported(r.Name)))
	}
	for _, p := range g.s.Projections {
		if p.Fold {
			fmt.Fprintf(&b, "\t\t%s: %s,\n", g.implField(p.Name), g.qualify(kindProjections, exported(p.Name)))
		}
	}
	if len(g.upcastHops()) > 0 {
		b.WriteString("\t\tUpcasts: &Upcasts{},\n")
	}
	b.WriteString("\t}\n")
	// aggregates and records bind through their methods on Impl (ADR 0008)
	for _, a := range g.s.Aggregates {
		fmt.Fprintf(&b, "\timpl.%s(%s)\n", a.Name, g.qualify(kindAggregates, a.Name))
	}
	for _, r := range g.s.Records {
		fmt.Fprintf(&b, "\timpl.%s(%s)\n", r.Name, g.qualify(kindRecords, r.Name))
	}
	fmt.Fprintf(&b, "\treturn %s.NewRegistry(impl)\n}\n", g.genPkg)
	out["registry.go"] = b.String()

	return out
}

// --- helpers ---

func eventNames(p *schema.Projection) []string {
	var out []string
	for _, sub := range p.Subscriptions {
		out = append(out, sub.Event)
	}
	return out
}

func stringSlice(list []string) string {
	if len(list) == 0 {
		return "nil"
	}
	var b strings.Builder
	b.WriteString("[]string{")
	for i, s := range list {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%q", s)
	}
	b.WriteString("}")
	return b.String()
}

// toSnake converts CamelCase schema names to snake_case SQL identifiers.
func toSnake(name string) string {
	var b strings.Builder
	for i, r := range name {
		if r >= 'A' && r <= 'Z' {
			if i > 0 && !(name[i-1] >= 'A' && name[i-1] <= 'Z') {
				b.WriteByte('_')
			}
			b.WriteRune(r + ('a' - 'A'))
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// exported converts snake_case / lowerCamel schema names to Go exported
// identifiers.
// implField is the Impl field a policy, process or @fold projection is
// wired through: its exported name, unless an aggregate or a record of
// the schema already holds that field — `aggregate FleetStall` beside
// `process fleetStall` — when it is the name with its kind appended
// (FleetStallProcess, FleetStallPolicy, FleetStallProjection), so the
// two compile side by side. Only the Impl field and the generated calls
// through it move: the reactor's Reactions or Folds interface, its stub
// type and its runner name keep the declared name, so a reactor already
// running is not renamed. A schema that avoids the collision never meets
// this (ADR 0006).
func (g *generator) implField(name string) string {
	field := exported(name)
	taken := false
	for _, a := range g.s.Aggregates {
		taken = taken || a.Name == field
	}
	for _, r := range g.s.Records {
		taken = taken || r.Name == field
	}
	if !taken {
		return field
	}
	for _, r := range g.s.Policies {
		if r.Name == name {
			return field + "Policy"
		}
	}
	for _, r := range g.s.Processes {
		if r.Name == name {
			return field + "Process"
		}
	}
	return field + "Projection"
}

func exported(name string) string {
	parts := strings.Split(name, "_")
	var b strings.Builder
	for _, p := range parts {
		if p == "" {
			continue
		}
		if p == "id" {
			b.WriteString("Id")
			continue
		}
		b.WriteString(strings.ToUpper(p[:1]) + p[1:])
	}
	return b.String()
}

func identLower(s string) string {
	return strings.ToLower(strings.ReplaceAll(s, "-", ""))
}

func absOr(dir string) string {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return dir
	}
	return abs
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func boolKeys(m map[string]*schema.Payload) map[string]bool {
	out := make(map[string]bool, len(m))
	for k := range m {
		out[k] = true
	}
	return out
}

// requiredFields is a payload's required list, sorted for deterministic
// emission.
func requiredFields(pl *schema.Payload) []string {
	if pl == nil || len(pl.Required) == 0 {
		return nil
	}
	out := append([]string(nil), pl.Required...)
	sort.Strings(out)
	return out
}
