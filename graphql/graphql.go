// Package graphql serves one GraphQL endpoint over any number of loom
// services — the gateway. The schema is built at boot from the services'
// registries and matches the SDL contract `loom graphql` emits: state
// types (camelCase fields), command inputs → mutations returning
// DispatchResult, entity/record list+get queries with FilterInput,
// {x}Changed subscriptions (served over SSE on the same endpoint), plus
// runtime extras the fragments don't carry (id/namespace/updatedAt on
// rows) and hand-written cross-service Join fields. Together with Files
// and Streams, the gateway is the whole public surface — the services'
// own HTTP handlers can stay on the private network.
package graphql

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	gql "github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"

	"github.com/go-apis/loom"
)

// Join adds a hand-written cross-service field: the one part of a graph
// no generator should guess.
type Join struct {
	OnType  string // GraphQL type name the field hangs off, e.g. "FormList"
	Field   string // e.g. "recipient"
	Returns string // result type name; must exist in the composed schema
	List    bool
	Resolve func(ctx context.Context, source map[string]any) (any, error)
}

type Config struct {
	Services []*loom.Client
	Joins    []Join
}

// New composes the services into one executable schema and returns the
// /graphql handler (POST {query, variables, operationName}).
func New(cfg Config) (http.Handler, error) {
	if len(cfg.Services) == 0 {
		return nil, fmt.Errorf("graphql: no services")
	}
	b := &builder{
		types:   map[string]*typeEntry{},
		inputs:  map[string]gql.Input{},
		queries: gql.Fields{},
		muts:    gql.Fields{},
		subs:    gql.Fields{},
	}
	for _, cli := range cfg.Services {
		if err := b.service(cli); err != nil {
			return nil, err
		}
	}
	for _, j := range cfg.Joins {
		if err := b.join(j); err != nil {
			return nil, err
		}
	}
	schemaCfg := gql.SchemaConfig{
		Query: gql.NewObject(gql.ObjectConfig{Name: "Query", Fields: b.queries}),
	}
	if len(b.muts) > 0 {
		schemaCfg.Mutation = gql.NewObject(gql.ObjectConfig{Name: "Mutation", Fields: b.muts})
	}
	if len(b.subs) > 0 {
		schemaCfg.Subscription = gql.NewObject(gql.ObjectConfig{Name: "Subscription", Fields: b.subs})
	}
	schema, err := gql.NewSchema(schemaCfg)
	if err != nil {
		return nil, err
	}
	return &handler{schema: schema}, nil
}

// --- scalars shared with the SDL contract ---

func passthrough(name, desc string) *gql.Scalar {
	return gql.NewScalar(gql.ScalarConfig{
		Name: name, Description: desc,
		Serialize:  func(v any) any { return v },
		ParseValue: func(v any) any { return v },
		ParseLiteral: func(valueAST ast.Value) any {
			return valueAST.GetValue()
		},
	})
}

// coerceLong accepts what JSON transports actually deliver for 64-bit
// ints: numbers (float64 from encoding/json), native ints, or strings.
func coerceLong(v any) any {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	case string:
		if i, err := strconv.ParseInt(n, 10, 64); err == nil {
			return i
		}
	}
	return nil
}

var (
	scalarUUID = passthrough("UUID", "UUID as a string")
	scalarTime = passthrough("Time", "RFC 3339 timestamp")
	scalarMap  = passthrough("Map", "arbitrary JSON object")
	// Long carries schema `int` (int64): GraphQL Int is 32-bit and cent
	// totals are not. Real coercion (not passthrough) so inline literals
	// in queries parse to int64.
	scalarLong = gql.NewScalar(gql.ScalarConfig{
		Name: "Long", Description: "64-bit integer as a JSON number (GraphQL Int is 32-bit)",
		Serialize:  func(v any) any { return v },
		ParseValue: coerceLong,
		ParseLiteral: func(valueAST ast.Value) any {
			if iv, ok := valueAST.(*ast.IntValue); ok {
				if n, err := strconv.ParseInt(iv.Value, 10, 64); err == nil {
					return n
				}
			}
			return nil
		},
	})

	filterOp = gql.NewEnum(gql.EnumConfig{Name: "FilterOp", Values: gql.EnumValueConfigMap{
		"EQ": {Value: ""}, "NE": {Value: "ne"}, "GT": {Value: "gt"}, "GTE": {Value: "gte"},
		"LT": {Value: "lt"}, "LTE": {Value: "lte"}, "LIKE": {Value: "like"},
	}})
	filterInput = gql.NewInputObject(gql.InputObjectConfig{Name: "FilterInput", Fields: gql.InputObjectConfigFieldMap{
		"field": {Type: gql.NewNonNull(gql.String)},
		"op":    {Type: gql.NewNonNull(filterOp)},
		"value": {Type: gql.NewNonNull(gql.String)},
	}})
	dispatchResult = gql.NewObject(gql.ObjectConfig{Name: "DispatchResult", Fields: gql.Fields{
		"status": {Type: gql.NewNonNull(gql.String)},
	}})
)

// --- builder ---

type typeEntry struct {
	obj    *gql.Object
	fields []string // sorted json field names, for collision checks
}

type builder struct {
	types   map[string]*typeEntry
	inputs  map[string]gql.Input
	queries gql.Fields
	muts    gql.Fields
	subs    gql.Fields
}

func (b *builder) service(cli *loom.Client) error {
	reg := cli.Registry()

	for _, agg := range reg.Aggregates {
		obj, err := b.objectFor(agg.Name, agg.NewState())
		if err != nil {
			return err
		}
		if err := b.addQuery(lowerFirst(agg.Name), aggregateGet(cli, agg.Name, obj)); err != nil {
			return err
		}
		name := agg.Name
		if err := b.addSub(lowerFirst(agg.Name)+"Changed", docChanged(cli, obj, func(ctx context.Context, ns string, id uuid.UUID) (map[string]any, error) {
			state, version, err := cli.Load(ctx, name, ns, id)
			if err != nil || version == 0 {
				return nil, err
			}
			return toDoc(state, ns, id.String())
		})); err != nil {
			return err
		}
		for _, def := range agg.Commands {
			if err := b.mutation(cli, def.Name, def.New); err != nil {
				return err
			}
		}
	}
	for _, rec := range reg.Records {
		obj, err := b.objectFor(rec.Name, rec.NewState())
		if err != nil {
			return err
		}
		if err := b.addQuery(lowerFirst(rec.Name), docGet(obj, func(ctx context.Context, ns string, id uuid.UUID) (any, error) {
			return cli.Record(ctx, rec.Name, ns, id)
		})); err != nil {
			return err
		}
		recName := rec.Name
		recQuery := func(ctx context.Context, q loom.Query) ([]loom.Row, error) {
			return cli.QueryRecords(ctx, recName, q)
		}
		if err := b.addQuery(lowerFirst(rec.Name)+"s", docList(obj, recQuery)); err != nil {
			return err
		}
		if err := b.addSub(lowerFirst(rec.Name)+"sChanged", listChanged(cli, obj, recQuery)); err != nil {
			return err
		}
		for _, def := range rec.Commands {
			if err := b.mutation(cli, def.Name, def.New); err != nil {
				return err
			}
		}
	}
	for _, u := range reg.Uploads {
		if err := b.uploadMutation(cli, u); err != nil {
			return err
		}
	}
	seen := map[string]bool{}
	for _, p := range reg.Projections {
		if seen[p.Entity] {
			continue
		}
		seen[p.Entity] = true
		entity := p.Entity
		obj, err := b.objectFor(entity, p.NewState())
		if err != nil {
			return err
		}
		if err := b.addQuery(lowerFirst(entity), docGet(obj, func(ctx context.Context, ns string, id uuid.UUID) (any, error) {
			state, err := cli.Entity(ctx, entity, ns, id)
			if state == nil {
				return nil, err
			}
			return state, err
		})); err != nil {
			return err
		}
		entityQuery := func(ctx context.Context, q loom.Query) ([]loom.Row, error) {
			return cli.QueryEntities(ctx, entity, q)
		}
		if err := b.addQuery(lowerFirst(entity)+"s", docList(obj, entityQuery)); err != nil {
			return err
		}
		if err := b.addSub(lowerFirst(entity)+"sChanged", listChanged(cli, obj, entityQuery)); err != nil {
			return err
		}
		if err := b.addSub(lowerFirst(entity)+"Changed", docChanged(cli, obj, func(ctx context.Context, ns string, id uuid.UUID) (map[string]any, error) {
			state, err := cli.Entity(ctx, entity, ns, id)
			if err != nil || state == nil {
				return nil, err
			}
			return toDoc(state, ns, id.String())
		})); err != nil {
			return err
		}
	}
	return nil
}

func (b *builder) addSub(name string, f *gql.Field) error {
	if _, dup := b.subs[name]; dup {
		return fmt.Errorf("graphql: subscription %q defined by two services — rename one side", name)
	}
	b.subs[name] = f
	return nil
}

// docChanged is the {x}Changed subscription: the current doc immediately
// (once it exists), then a fresh copy on every change — the same
// semantics as the services' SSE doc streams, driven by the same log
// wake-ups. graphql-go re-executes the selection per emitted payload.
func docChanged(cli *loom.Client, obj *gql.Object, load func(ctx context.Context, ns string, id uuid.UUID) (map[string]any, error)) *gql.Field {
	return &gql.Field{
		Type: gql.NewNonNull(obj),
		Args: nsIDArgs(),
		Resolve: func(p gql.ResolveParams) (any, error) {
			return p.Source, nil // the payload docSubscribe emitted
		},
		Subscribe: func(p gql.ResolveParams) (any, error) {
			ns, id, err := parseNsID(p)
			if err != nil {
				return nil, err
			}
			ctx := p.Context
			out := make(chan any)
			go func() {
				defer close(out)
				wake, cancel := cli.Watch()
				defer cancel()
				// poll fallback, mirroring the services' stream loops
				poll := time.NewTicker(2 * time.Second)
				defer poll.Stop()
				var last []byte
				step := func() bool {
					doc, err := load(ctx, ns, id)
					if err != nil || doc == nil {
						return err == nil // absent row: keep waiting; error: end
					}
					raw, err := json.Marshal(doc)
					if err != nil || bytes.Equal(raw, last) {
						return err == nil
					}
					last = raw
					select {
					case out <- any(doc):
						return true
					case <-ctx.Done():
						return false
					}
				}
				if !step() {
					return
				}
				for {
					select {
					case <-ctx.Done():
						return
					case <-wake:
					case <-poll.C:
					}
					if !step() {
						return
					}
				}
			}()
			return out, nil
		},
	}
}

func (b *builder) addQuery(name string, f *gql.Field) error {
	if _, dup := b.queries[name]; dup {
		return fmt.Errorf("graphql: query %q defined by two services — rename one side", name)
	}
	b.queries[name] = f
	return nil
}

func (b *builder) join(j Join) error {
	on, ok := b.types[j.OnType]
	if !ok {
		return fmt.Errorf("graphql: join on unknown type %s", j.OnType)
	}
	ret, ok := b.types[j.Returns]
	if !ok {
		return fmt.Errorf("graphql: join %s.%s returns unknown type %s", j.OnType, j.Field, j.Returns)
	}
	var out gql.Output = ret.obj
	if j.List {
		out = gql.NewList(gql.NewNonNull(ret.obj))
	}
	resolve := j.Resolve
	on.obj.AddFieldConfig(j.Field, &gql.Field{
		Type: out,
		Resolve: func(p gql.ResolveParams) (any, error) {
			src, _ := p.Source.(map[string]any)
			if src == nil {
				return nil, nil
			}
			return resolve(p.Context, src)
		},
	})
	return nil
}

// --- resolvers ---

// toDoc renders any state as the resolver source: a map keyed by the json
// (snake) field names, with row identity injected.
func toDoc(state any, ns string, id string) (map[string]any, error) {
	raw, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}
	doc := map[string]any{}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	if ns != "" {
		doc["namespace"] = ns
	}
	if id != "" {
		doc["id"] = id
	}
	return doc, nil
}

func nsIDArgs() gql.FieldConfigArgument {
	return gql.FieldConfigArgument{
		"namespace": {Type: gql.NewNonNull(gql.String)},
		"id":        {Type: gql.NewNonNull(scalarUUID)},
	}
}

func parseNsID(p gql.ResolveParams) (string, uuid.UUID, error) {
	ns, _ := p.Args["namespace"].(string)
	raw := fmt.Sprint(p.Args["id"])
	id, err := uuid.Parse(raw)
	if err != nil {
		return "", uuid.Nil, fmt.Errorf("bad id %q", raw)
	}
	return ns, id, nil
}

func aggregateGet(cli *loom.Client, name string, obj *gql.Object) *gql.Field {
	return &gql.Field{
		Type: obj,
		Args: nsIDArgs(),
		Resolve: func(p gql.ResolveParams) (any, error) {
			ns, id, err := parseNsID(p)
			if err != nil {
				return nil, err
			}
			state, version, err := cli.Load(p.Context, name, ns, id)
			if err != nil || version == 0 {
				return nil, err
			}
			return toDoc(state, ns, id.String())
		},
	}
}

func docGet(obj *gql.Object, load func(ctx context.Context, ns string, id uuid.UUID) (any, error)) *gql.Field {
	return &gql.Field{
		Type: obj,
		Args: nsIDArgs(),
		Resolve: func(p gql.ResolveParams) (any, error) {
			ns, id, err := parseNsID(p)
			if err != nil {
				return nil, err
			}
			state, err := load(p.Context, ns, id)
			if err != nil || state == nil || reflect.ValueOf(state).IsNil() {
				return nil, err
			}
			return toDoc(state, ns, id.String())
		},
	}
}

func listArgs() gql.FieldConfigArgument {
	return gql.FieldConfigArgument{
		"namespace": {Type: gql.NewNonNull(gql.String)},
		"where":     {Type: gql.NewList(gql.NewNonNull(filterInput))},
		"order":     {Type: gql.String},
		"limit":     {Type: gql.Int},
		"offset":    {Type: gql.Int},
	}
}

func queryFromArgs(args map[string]any) loom.Query {
	q := loom.Query{Namespace: fmt.Sprint(args["namespace"])}
	if order, ok := args["order"].(string); ok {
		q.OrderBy = order
	}
	if limit, ok := args["limit"].(int); ok {
		q.Limit = limit
	}
	if offset, ok := args["offset"].(int); ok {
		q.Offset = offset
	}
	if where, ok := args["where"].([]any); ok {
		for _, w := range where {
			f, _ := w.(map[string]any)
			if f == nil {
				continue
			}
			q.Filters = append(q.Filters, loom.Filter{
				Field: fmt.Sprint(f["field"]),
				Op:    fmt.Sprint(f["op"]),
				Value: fmt.Sprint(f["value"]),
			})
		}
	}
	return q
}

func rowDocs(rows []loom.Row) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		doc := map[string]any{}
		_ = json.Unmarshal(r.Data, &doc)
		doc["id"] = r.ID
		doc["namespace"] = r.Namespace
		doc["updatedAt"] = r.UpdatedAt.Format(time.RFC3339Nano)
		out = append(out, doc)
	}
	return out
}

func docList(obj *gql.Object, query func(ctx context.Context, q loom.Query) ([]loom.Row, error)) *gql.Field {
	return &gql.Field{
		Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(obj))),
		Args: listArgs(),
		Resolve: func(p gql.ResolveParams) (any, error) {
			rows, err := query(p.Context, queryFromArgs(p.Args))
			if err != nil {
				return nil, err
			}
			return rowDocs(rows), nil
		},
	}
}

// listChanged is the {x}sChanged subscription: the filtered list as it
// stands, then the whole fresh list on every change — a live query.
// Requery-and-diff per log wake-up; no delta bookkeeping, so rows
// entering and leaving the filter just work. Sized for UI tables, not
// unbounded feeds — use where/limit.
func listChanged(cli *loom.Client, obj *gql.Object, query func(ctx context.Context, q loom.Query) ([]loom.Row, error)) *gql.Field {
	return &gql.Field{
		Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(obj))),
		Args: listArgs(),
		Resolve: func(p gql.ResolveParams) (any, error) {
			return p.Source, nil
		},
		Subscribe: func(p gql.ResolveParams) (any, error) {
			q := queryFromArgs(p.Args)
			ctx := p.Context
			out := make(chan any)
			go func() {
				defer close(out)
				wake, cancel := cli.Watch()
				defer cancel()
				poll := time.NewTicker(2 * time.Second)
				defer poll.Stop()
				var last []byte
				step := func() bool {
					rows, err := query(ctx, q)
					if err != nil {
						return false
					}
					docs := rowDocs(rows)
					raw, err := json.Marshal(docs)
					if err != nil || bytes.Equal(raw, last) {
						return err == nil
					}
					last = raw
					select {
					case out <- any(docs):
						return true
					case <-ctx.Done():
						return false
					}
				}
				if !step() {
					return
				}
				for {
					select {
					case <-ctx.Done():
						return
					case <-wake:
					case <-poll.C:
					}
					if !step() {
						return
					}
				}
			}()
			return out, nil
		},
	}
}

func (b *builder) mutation(cli *loom.Client, name string, newCmd func() loom.Command) error {
	field := lowerFirst(name)
	if _, dup := b.muts[field]; dup {
		return fmt.Errorf("graphql: mutation %q defined by two services — rename one side", field)
	}
	input, conv, err := b.commandInput(name, newCmd())
	if err != nil {
		return err
	}
	b.muts[field] = &gql.Field{
		Type: gql.NewNonNull(dispatchResult),
		Args: gql.FieldConfigArgument{"input": {Type: gql.NewNonNull(input)}},
		Resolve: func(p gql.ResolveParams) (any, error) {
			args, _ := p.Args["input"].(map[string]any)
			raw, err := json.Marshal(conv(args))
			if err != nil {
				return nil, err
			}
			cmd := newCmd()
			if err := json.Unmarshal(raw, cmd); err != nil {
				return nil, err
			}
			if err := cli.Dispatch(p.Context, cmd); err != nil {
				return nil, err
			}
			return map[string]any{"status": "ok"}, nil
		},
	}
	return nil
}

// uploadMutation serves create{Name}Upload: open a resumable session on
// the owning service (which dispatches the upload's `on started` command)
// and hand the URL back for the browser to PUT chunks directly.
func (b *builder) uploadMutation(cli *loom.Client, u *loom.UploadDef) error {
	field := "create" + u.Name + "Upload"
	if _, dup := b.muts[field]; dup {
		return fmt.Errorf("graphql: mutation %q defined by two services — rename one side", field)
	}
	input := gql.NewInputObject(gql.InputObjectConfig{Name: "Create" + u.Name + "UploadInput", Fields: gql.InputObjectConfigFieldMap{
		"namespace":   {Type: gql.NewNonNull(gql.String)},
		"id":          {Type: gql.NewNonNull(scalarUUID)},
		"name":        {Type: gql.String},
		"contentType": {Type: gql.String},
		"size":        {Type: gql.NewNonNull(scalarLong)},
	}})
	session, err := b.uploadSession()
	if err != nil {
		return err
	}
	upload := u.Name
	b.muts[field] = &gql.Field{
		Type: gql.NewNonNull(session),
		Args: gql.FieldConfigArgument{"input": {Type: gql.NewNonNull(input)}},
		Resolve: func(p gql.ResolveParams) (any, error) {
			args, _ := p.Args["input"].(map[string]any)
			id, err := uuid.Parse(fmt.Sprint(args["id"]))
			if err != nil {
				return nil, fmt.Errorf("bad id %q", args["id"])
			}
			req := loom.UploadRequest{
				Upload:    upload,
				Namespace: fmt.Sprint(args["namespace"]),
				StreamID:  id,
				Origin:    originFrom(p.Context),
			}
			req.Name, _ = args["name"].(string)
			req.ContentType, _ = args["contentType"].(string)
			req.Size = asInt64(args["size"])
			up, err := cli.CreateUpload(p.Context, req)
			if err != nil {
				return nil, err
			}
			return toDoc(up, "", "")
		},
	}
	return nil
}

// uploadSession lazily builds the shared UploadSession result type.
func (b *builder) uploadSession() (*gql.Object, error) {
	if e, ok := b.types["UploadSession"]; ok {
		return e.obj, nil
	}
	ref, err := b.fileRefObject()
	if err != nil {
		return nil, err
	}
	obj := gql.NewObject(gql.ObjectConfig{Name: "UploadSession", Fields: gql.Fields{
		"file":     {Type: gql.NewNonNull(ref), Resolve: mapField("file")},
		"url":      {Type: gql.NewNonNull(gql.String), Resolve: mapField("url")},
		"protocol": {Type: gql.NewNonNull(gql.String), Resolve: mapField("protocol")},
	}})
	b.types["UploadSession"] = &typeEntry{obj: obj, fields: []string{"file", "protocol", "url"}}
	return obj, nil
}

func asInt64(v any) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	default:
		return 0
	}
}

type originKey struct{}

func originFrom(ctx context.Context) string {
	s, _ := ctx.Value(originKey{}).(string)
	return s
}

// Streams exposes the services' watch endpoints (entity/aggregate SSE,
// resumable via Last-Event-ID) at the gateway, routed by service — the
// raw-EventSource sibling of the GraphQL subscriptions. Only the
// read-model watch paths pass through; ops surfaces (events log, stats,
// console) stay private.
//
//	mux.Handle("/streams/", http.StripPrefix("/streams", loomgraphql.Streams(recipientsCli, filingsCli)))
//	// → GET /streams/{service}/entities/{Name}/{id}/stream?namespace=…
//	// → GET /streams/{service}/aggregates/{Name}/{id}/stream?namespace=…
func Streams(services ...*loom.Client) http.Handler {
	byName := make(map[string]http.Handler, len(services))
	for _, cli := range services {
		byName[cli.Registry().Service] = cli.HTTPHandler()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		service, rest, ok := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
		h := byName[service]
		if !ok || h == nil {
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
			return
		}
		rest = "/" + rest
		watch := (strings.HasPrefix(rest, "/entities/") || strings.HasPrefix(rest, "/aggregates/")) &&
			strings.HasSuffix(rest, "/stream")
		if r.Method != http.MethodGet || !watch {
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
			return
		}
		r2 := r.Clone(r.Context())
		r2.URL.Path = rest
		h.ServeHTTP(w, r2)
	})
}

// Files serves file downloads at the gateway, so the services' own HTTP
// surfaces can stay private: object keys are service-prefixed, and the
// gateway reaches storage through the owning service's client. Mount it
// beside the graph — it is the path FileRef.downloadUrl points at:
//
//	mux.Handle("/graphql", gateway)
//	mux.Handle("/files", loomgraphql.Files(recipientsCli, filingsCli))
func Files(services ...*loom.Client) http.Handler {
	byName := make(map[string]*loom.Client, len(services))
	for _, cli := range services {
		byName[cli.Registry().Service] = cli
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"GET only"}`, http.StatusMethodNotAllowed)
			return
		}
		key := r.URL.Query().Get("key")
		service, _, ok := strings.Cut(key, "/")
		cli := byName[service]
		if !ok || cli == nil {
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
			return
		}
		cli.ServeFile(w, r, key)
	})
}

func lowerFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToLower(s[:1]) + s[1:]
}

// --- http ---

//go:embed playground.html
var playgroundHTML []byte

type handler struct{ schema gql.Schema }

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Query         string         `json:"query"`
		Variables     map[string]any `json:"variables"`
		OperationName string         `json:"operationName"`
	}
	switch r.Method {
	case http.MethodPost:
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"errors":[{"message":"bad request body"}]}`, http.StatusBadRequest)
			return
		}
	case http.MethodGet:
		p := r.URL.Query()
		req.Query = p.Get("query")
		req.OperationName = p.Get("operationName")
		if v := p.Get("variables"); v != "" {
			_ = json.Unmarshal([]byte(v), &req.Variables)
		}
		// a browser landing here gets the playground; ?query= still works
		// for quick curl checks
		if req.Query == "" && strings.Contains(r.Header.Get("Accept"), "text/html") {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Header().Set("Cache-Control", "no-store")
			_, _ = w.Write(playgroundHTML)
			return
		}
	default:
		http.Error(w, `{"errors":[{"message":"POST a query"}]}`, http.StatusMethodNotAllowed)
		return
	}
	params := gql.Params{
		Schema:         h.schema,
		RequestString:  req.Query,
		VariableValues: req.Variables,
		OperationName:  req.OperationName,
		// the browser origin rides the context so upload sessions can be
		// CORS-bound to it
		Context: context.WithValue(r.Context(), originKey{}, r.Header.Get("Origin")),
	}
	// subscriptions ride SSE on this same endpoint (graphql-sse shape:
	// `next` events carrying execution results, `complete` at the end) —
	// browser-native EventSource, no websocket stack
	if strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
		h.serveSSE(w, r, params)
		return
	}
	result := gql.Do(params)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

func (h *handler) serveSSE(w http.ResponseWriter, r *http.Request, params gql.Params) {
	f, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, `{"errors":[{"message":"streaming unsupported"}]}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	f.Flush()

	results := gql.Subscribe(params)
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			fmt.Fprint(w, ": ping\n\n")
			f.Flush()
		case res, more := <-results:
			if !more {
				fmt.Fprint(w, "event: complete\ndata:\n\n")
				f.Flush()
				return
			}
			raw, err := json.Marshal(res)
			if err != nil {
				raw = []byte(`{"errors":[{"message":"unencodable result"}]}`)
			}
			fmt.Fprintf(w, "event: next\ndata: %s\n\n", raw)
			f.Flush()
		}
	}
}
