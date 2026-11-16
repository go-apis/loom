// Package adopt finds code a loom service writes by hand that a loom
// feature already does: a ticker goroutine where a schedule would do, a
// command that re-arms itself with loom.After, a timer policy where a
// @loop would keep the wait. `loom adopt` runs it over a repository and
// prints what it found. It is advice: it reads, never writes, and a
// finding is something to look at, not a fault.
//
// Every rule names a feature this loom has, so a repository is only ever
// told about what the loom it pins can do. A release that adds a feature
// adds its rule here and its entry in docs/features.md.
package adopt

import (
	"bufio"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Rule is one pattern adopt looks for.
type Rule struct {
	// ID names the rule in findings and in docs/features.md.
	ID string
	// Pattern is what the repository writes by hand.
	Pattern string
	// Feature is what to use instead, and Since the loom release that
	// brought it.
	Feature string
	Since   string
	// Advice is the one line a reader needs to decide.
	Advice string
}

// Rules is every rule, in the order findings are reported.
var Rules = []Rule{
	{
		ID:      "ticker-goroutine",
		Pattern: "a goroutine looping on time.NewTicker",
		Feature: "Client.Every, or a `schedule` in the schema",
		Since:   "v0.95.0",
		Advice:  "work the service does once, wherever it runs, belongs on one instance of the newest generation, with a failed tick retried at the next; a cache each instance keeps in memory keeps its ticker",
	},
	{
		ID:      "self-rearming-timer",
		Pattern: "a tick that schedules the next tick with loom.After",
		Feature: "a `schedule`, or a @loop aggregate with WaitFor",
		Since:   "v0.110.0",
		Advice:  "a chain of loom.After timers ends the first time a link fails; a schedule ticks whatever came of the last tick, and a loop's wait is kept by loom with an owner and a deadline",
	},
	{
		ID:      "timer-policy",
		Pattern: "a `policy <name>Timer` block arming and cancelling a timer on events",
		Feature: "a @loop aggregate with WaitFor (and its deadline)",
		Since:   "v0.110.0",
		Advice:  "a loop looks at how the instance stands and keeps its wait itself, so no list of events arms and cancels it, and no sweep rescues a lost timer",
	},
}

// Finding is one place a rule matched.
type Finding struct {
	Rule string `json:"rule"`
	File string `json:"file"`
	Line int    `json:"line"`
	// Lines is how many lines the matched code spans: the enclosing
	// function for Go, the block for a schema; Start is where that span
	// begins, so a span holding two matches is counted once.
	Lines   int    `json:"lines"`
	Start   int    `json:"start"`
	Feature string `json:"feature"`
	What    string `json:"what"`
}

// Scan walks root and returns what it finds, ordered by rule, file and
// line. Test files, generated code (loomgen, *_gen.go), vendor,
// node_modules and dot-directories are skipped. It reads only.
func Scan(root string) ([]Finding, error) {
	var out []Finding
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := d.Name()
		if d.IsDir() {
			if path != root && (strings.HasPrefix(name, ".") || name == "vendor" || name == "node_modules" || name == "loomgen" || name == "testdata") {
				return filepath.SkipDir
			}
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		switch {
		case strings.HasSuffix(name, ".go") && !strings.HasSuffix(name, "_test.go") && !strings.HasSuffix(name, "_gen.go"):
			fs, err := scanGo(path, rel)
			if err != nil {
				return nil // a file that does not parse is not ours to judge
			}
			out = append(out, fs...)
		case strings.HasSuffix(name, ".loom"):
			fs, err := scanSchema(path, rel)
			if err != nil {
				return err
			}
			out = append(out, fs...)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	order := map[string]int{}
	for i, r := range Rules {
		order[r.ID] = i
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if order[a.Rule] != order[b.Rule] {
			return order[a.Rule] < order[b.Rule]
		}
		if a.File != b.File {
			return a.File < b.File
		}
		return a.Line < b.Line
	})
	return out, nil
}

func ruleByID(id string) Rule {
	for _, r := range Rules {
		if r.ID == id {
			return r
		}
	}
	return Rule{ID: id}
}

func finding(id, file string, line, start, lines int, what string) Finding {
	return Finding{Rule: id, File: filepath.ToSlash(file), Line: line, Start: start, Lines: lines, Feature: ruleByID(id).Feature, What: what}
}

// scanGo finds ticker goroutines and loom.After calls. A file's import
// name for loom is whatever it imports github.com/go-apis/loom as.
func scanGo(path, rel string) ([]Finding, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}
	loomName, timeName := "", ""
	for _, imp := range file.Imports {
		p, _ := strconv.Unquote(imp.Path.Value)
		local := ""
		if imp.Name != nil {
			local = imp.Name.Name
		}
		switch p {
		case "github.com/go-apis/loom":
			loomName = orDefault(local, "loom")
		case "time":
			timeName = orDefault(local, "time")
		}
	}
	if loomName == "" && timeName == "" {
		return nil, nil
	}
	var out []Finding
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		start := fset.Position(fn.Pos()).Line
		span := fset.Position(fn.End()).Line - start + 1
		name := fn.Name.Name
		if fn.Recv != nil && len(fn.Recv.List) > 0 {
			name = recvName(fn.Recv.List[0].Type) + "." + name
		}
		tickerSeen := false
		reactsToTick := sigMentions(fn.Type, "Tick")
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			pkg, ok := sel.X.(*ast.Ident)
			if !ok {
				return true
			}
			line := fset.Position(call.Pos()).Line
			switch {
			case timeName != "" && pkg.Name == timeName && sel.Sel.Name == "NewTicker" && !tickerSeen && loops(fn.Body):
				tickerSeen = true
				out = append(out, finding("ticker-goroutine", rel, line, start, span, name+" loops on a ticker"))
			case loomName != "" && pkg.Name == loomName && sel.Sel.Name == "After" && len(call.Args) > 0:
				// a one-shot timeout (cancel an order left unpaid) is what
				// loom.After is for; a tick scheduling the next tick is a
				// chain a failed link ends
				cmd := literalType(call.Args[0])
				if strings.HasPrefix(cmd, "Tick") || reactsToTick {
					what := name + " re-arms itself with loom.After"
					if cmd != "" {
						what = name + " re-arms " + cmd + " with loom.After"
					}
					out = append(out, finding("self-rearming-timer", rel, line, start, span, what))
				}
			}
			return true
		})
	}
	return out, nil
}

// literalType is the type name of a command written as &pkg.T{...} or
// pkg.T{...}; empty for anything else.
func literalType(e ast.Expr) string {
	if u, ok := e.(*ast.UnaryExpr); ok {
		e = u.X
	}
	lit, ok := e.(*ast.CompositeLit)
	if !ok {
		return ""
	}
	switch t := lit.Type.(type) {
	case *ast.SelectorExpr:
		return t.Sel.Name
	case *ast.Ident:
		return t.Name
	}
	return ""
}

// sigMentions reports whether any parameter type of the function names
// word: a reaction to a tick's event takes its event type.
func sigMentions(ft *ast.FuncType, word string) bool {
	if ft == nil || ft.Params == nil {
		return false
	}
	for _, p := range ft.Params.List {
		if strings.Contains(typeName(p.Type), word) {
			return true
		}
	}
	return false
}

func typeName(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.StarExpr:
		return typeName(t.X)
	case *ast.SelectorExpr:
		return t.Sel.Name
	case *ast.Ident:
		return t.Name
	}
	return ""
}

// loops reports whether body has a for statement: a ticker made and only
// read once is not a polling loop.
func loops(body *ast.BlockStmt) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if _, ok := n.(*ast.ForStmt); ok {
			found = true
		}
		if _, ok := n.(*ast.RangeStmt); ok {
			found = true
		}
		return !found
	})
	return found
}

func recvName(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.StarExpr:
		return recvName(t.X)
	case *ast.Ident:
		return t.Name
	case *ast.IndexExpr:
		return recvName(t.X)
	}
	return "?"
}

func orDefault(s, d string) string {
	if s == "" {
		return d
	}
	return s
}

var timerPolicy = regexp.MustCompile(`^\s*policy\s+([A-Za-z0-9_]*[Tt]imer)\s*\{`)

// scanSchema finds timer policies in a .loom file: the block from its
// `policy …Timer {` line to its closing brace.
func scanSchema(path, rel string) ([]Finding, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var (
		out   []Finding
		lines []string
	)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	for i, l := range lines {
		m := timerPolicy.FindStringSubmatch(l)
		if m == nil {
			continue
		}
		depth, end := 0, i
		for j := i; j < len(lines); j++ {
			depth += strings.Count(lines[j], "{") - strings.Count(lines[j], "}")
			if depth <= 0 {
				end = j
				break
			}
		}
		ons := 0
		for _, b := range lines[i : end+1] {
			if strings.HasPrefix(strings.TrimSpace(b), "on ") {
				ons++
			}
		}
		out = append(out, finding("timer-policy", rel, i+1, i+1, end-i+1, fmt.Sprintf("policy %s arms or cancels its timer on %d event(s)", m[1], ons)))
	}
	return out, nil
}

// Report writes findings for a reader: each finding, then each rule's
// count and lines with its feature and advice.
func Report(w *strings.Builder, findings []Finding) {
	if len(findings) == 0 {
		w.WriteString("loom adopt: nothing found — no hand-written code a loom feature already covers.\n")
		return
	}
	type tally struct{ n, lines int }
	by := map[string]*tally{}
	counted := map[string]bool{} // rule, file and span start: a span is counted once
	for _, f := range findings {
		fmt.Fprintf(w, "%s:%d\t%s\t%s\t(%d lines)\n", f.File, f.Line, f.Rule, f.What, f.Lines)
		t := by[f.Rule]
		if t == nil {
			t = &tally{}
			by[f.Rule] = t
		}
		t.n++
		if key := f.Rule + "\x00" + f.File + "\x00" + strconv.Itoa(f.Start); !counted[key] {
			counted[key] = true
			t.lines += f.Lines
		}
	}
	w.WriteString("\n")
	for _, r := range Rules {
		t := by[r.ID]
		if t == nil {
			continue
		}
		fmt.Fprintf(w, "%s: %d place(s), %d lines — use %s (since %s).\n  %s\n", r.ID, t.n, t.lines, r.Feature, r.Since, r.Advice)
	}
	w.WriteString("\nSee docs/features.md in go-apis/loom. This is advice: nothing here is a fault.\n")
}
