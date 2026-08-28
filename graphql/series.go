package graphql

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	gql "github.com/graphql-go/graphql"

	"github.com/go-apis/loom"
)

// Series on the gateway: a {name}s range query, a {name}Buckets
// aggregation query, and an append{Name}s mutation — matching the SDL
// contract `loom graphql` emits. No subscriptions: observations arrive
// in bulk, not row by row.

// seriesService wires one service's series into the composed schema.
func (b *builder) seriesService(cli *loom.Client) error {
	for _, def := range cli.Registry().Series {
		obj, err := b.seriesObjectFor(def)
		if err != nil {
			return err
		}
		if err := b.addQuery(lowerFirst(def.Name)+"s", seriesList(cli, def.Name, obj)); err != nil {
			return err
		}
		bucketObj := b.seriesBucketObject()
		if err := b.addQuery(lowerFirst(def.Name)+"Buckets", seriesBuckets(cli, def.Name, bucketObj, b.seriesBucketInterval())); err != nil {
			return err
		}
		if err := b.seriesAppendMutation(cli, def); err != nil {
			return err
		}
	}
	return nil
}

// seriesObjectFor builds the row type: namespace plus the declared
// fields. Rows have no id or updatedAt — identity is (dims, time) and
// rows never update. Fields stay nullable like entity rows (a column
// added later is NULL for older rows).
func (b *builder) seriesObjectFor(def *loom.SeriesDef) (*gql.Object, error) {
	fields, err := structFields(reflect.TypeOf(def.New()))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", def.Name, err)
	}
	sig := signature(fields)
	if existing, ok := b.types[def.Name]; ok {
		if strings.Join(existing.fields, ",") != strings.Join(sig, ",") {
			return nil, fmt.Errorf("graphql: two services declare type %s with different fields — rename one side", def.Name)
		}
		return existing.obj, nil
	}
	gqlFields := gql.Fields{
		"namespace": {Type: scalarNamespace, Resolve: mapField("namespace")},
	}
	for _, f := range fields {
		out, err := b.outputType(f.t)
		if err != nil {
			return nil, fmt.Errorf("%s.%s: %w", def.Name, f.snake, err)
		}
		gqlFields[f.camel] = &gql.Field{Type: out, Resolve: mapField(f.snake)}
	}
	obj := gql.NewObject(gql.ObjectConfig{Name: def.Name, Fields: gqlFields})
	b.types[def.Name] = &typeEntry{obj: obj, fields: sig}
	return obj, nil
}

func seriesListArgs() gql.FieldConfigArgument {
	return gql.FieldConfigArgument{
		"namespace": {Type: gql.NewNonNull(scalarNamespace)},
		"where":     {Type: gql.NewList(gql.NewNonNull(filterInput))},
		"since":     {Type: scalarTime},
		"until":     {Type: scalarTime},
		"order":     {Type: gql.String, Description: `"asc" for oldest-first; the default is newest-first`},
		"limit":     {Type: gql.Int},
		"offset":    {Type: gql.Int},
	}
}

// seriesQueryFromArgs authorizes the operation and builds the query —
// queryFromArgs' series twin.
func seriesQueryFromArgs(p gql.ResolveParams) (loom.SeriesQuery, error) {
	args := p.Args
	ns := fmt.Sprint(args["namespace"])
	if err := decide(p.Context, Decision{Kind: opKind(p), Field: p.Info.FieldName, Namespace: ns, Args: args, Fields: selectedFields(p)}); err != nil {
		return loom.SeriesQuery{}, err
	}
	if ns == AllNamespaces {
		ns = ""
	}
	q := loom.SeriesQuery{Namespace: ns, AllNamespaces: ns == ""}
	if order, ok := args["order"].(string); ok {
		q.Ascending = order == "asc"
	}
	if limit, ok := args["limit"].(int); ok {
		q.Limit = limit
	}
	if offset, ok := args["offset"].(int); ok {
		q.Offset = offset
	}
	var err error
	if q.Since, err = timeArg(args, "since"); err != nil {
		return q, err
	}
	if q.Until, err = timeArg(args, "until"); err != nil {
		return q, err
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
	return q, nil
}

func timeArg(args map[string]any, name string) (time.Time, error) {
	v, ok := args[name]
	if !ok || v == nil {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339, fmt.Sprint(v))
	if err != nil {
		return time.Time{}, fmt.Errorf("bad %s: %v", name, err)
	}
	return t, nil
}

func seriesDocs(points []loom.SeriesPoint) []map[string]any {
	out := make([]map[string]any, 0, len(points))
	for _, pt := range points {
		doc := map[string]any{}
		_ = json.Unmarshal(pt.Data, &doc)
		doc["namespace"] = pt.Namespace
		out = append(out, doc)
	}
	return out
}

func seriesList(cli *loom.Client, series string, obj *gql.Object) *gql.Field {
	return &gql.Field{
		Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(obj))),
		Args: seriesListArgs(),
		Resolve: func(p gql.ResolveParams) (any, error) {
			q, err := seriesQueryFromArgs(p)
			if err != nil {
				return nil, err
			}
			points, err := cli.QuerySeries(p.Context, series, q)
			if err != nil {
				return nil, err
			}
			return seriesDocs(points), nil
		},
	}
}

// seriesBucketObject builds (once) the shared bucket row type.
func (b *builder) seriesBucketObject() *gql.Object {
	if e, ok := b.types["SeriesBucket"]; ok {
		return e.obj
	}
	obj := gql.NewObject(gql.ObjectConfig{Name: "SeriesBucket", Fields: gql.Fields{
		"t":     {Type: gql.NewNonNull(scalarTime), Resolve: mapField("t")},
		"group": {Type: scalarMap, Resolve: mapField("group")},
		"count": {Type: gql.NewNonNull(scalarLong), Resolve: mapField("count")},
		"avg":   {Type: gql.Float, Resolve: mapField("avg")},
		"min":   {Type: gql.Float, Resolve: mapField("min")},
		"max":   {Type: gql.Float, Resolve: mapField("max")},
		"last":  {Type: gql.Float, Resolve: mapField("last")},
	}})
	b.types["SeriesBucket"] = &typeEntry{obj: obj, fields: []string{"avg", "count", "group", "last", "max", "min", "t"}}
	return obj
}

// seriesBucketInterval builds (once) the shared bucket-unit enum.
func (b *builder) seriesBucketInterval() *gql.Enum {
	if b.enums == nil {
		b.enums = map[string]*gql.Enum{}
		b.enumVal = map[string][]string{}
	}
	if e, ok := b.enums["SeriesBucketInterval"]; ok {
		return e
	}
	e := gql.NewEnum(gql.EnumConfig{Name: "SeriesBucketInterval", Values: gql.EnumValueConfigMap{
		"HOUR": {Value: "hour"}, "DAY": {Value: "day"}, "WEEK": {Value: "week"},
		"MONTH": {Value: "month"}, "YEAR": {Value: "year"},
	}})
	b.enums["SeriesBucketInterval"] = e
	b.enumVal["SeriesBucketInterval"] = []string{"hour", "day", "week", "month", "year"}
	return e
}

func seriesBuckets(cli *loom.Client, series string, obj *gql.Object, interval *gql.Enum) *gql.Field {
	args := seriesListArgs()
	delete(args, "order")
	delete(args, "offset")
	args["value"] = &gql.ArgumentConfig{Type: gql.NewNonNull(gql.String), Description: "numeric column to aggregate"}
	args["bucket"] = &gql.ArgumentConfig{Type: gql.NewNonNull(interval)}
	args["by"] = &gql.ArgumentConfig{Type: gql.NewList(gql.NewNonNull(gql.String)), Description: "dim columns to group by"}
	return &gql.Field{
		Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(obj))),
		Args: args,
		Resolve: func(p gql.ResolveParams) (any, error) {
			rq, err := seriesQueryFromArgs(p)
			if err != nil {
				return nil, err
			}
			q := loom.SeriesBucketQuery{
				Namespace:     rq.Namespace,
				AllNamespaces: rq.AllNamespaces,
				Value:         fmt.Sprint(p.Args["value"]),
				Bucket:        fmt.Sprint(p.Args["bucket"]),
				Filters:       rq.Filters,
				Since:         rq.Since,
				Until:         rq.Until,
				Limit:         rq.Limit,
			}
			if by, ok := p.Args["by"].([]any); ok {
				for _, d := range by {
					q.By = append(q.By, fmt.Sprint(d))
				}
			}
			buckets, err := cli.QuerySeriesBuckets(p.Context, series, q)
			if err != nil {
				return nil, err
			}
			docs := make([]map[string]any, 0, len(buckets))
			for _, bkt := range buckets {
				doc := map[string]any{
					"t":     bkt.T.Format(time.RFC3339Nano),
					"count": bkt.Count,
					"avg":   floatOrNil(bkt.Avg),
					"min":   floatOrNil(bkt.Min),
					"max":   floatOrNil(bkt.Max),
					"last":  floatOrNil(bkt.Last),
				}
				if bkt.Group != nil {
					group := map[string]any{}
					for k, v := range bkt.Group {
						group[k] = v
					}
					doc["group"] = group
				}
				docs = append(docs, doc)
			}
			return docs, nil
		},
	}
}

func floatOrNil(f *float64) any {
	if f == nil {
		return nil
	}
	return *f
}

// seriesAppendResult builds (once) the shared append result type.
func (b *builder) seriesAppendResult() *gql.Object {
	if e, ok := b.types["SeriesAppendResult"]; ok {
		return e.obj
	}
	obj := gql.NewObject(gql.ObjectConfig{Name: "SeriesAppendResult", Fields: gql.Fields{
		"inserted": {Type: gql.NewNonNull(scalarLong), Resolve: mapField("inserted")},
		"total":    {Type: gql.NewNonNull(scalarLong), Resolve: mapField("total")},
	}})
	b.types["SeriesAppendResult"] = &typeEntry{obj: obj, fields: []string{"inserted", "total"}}
	return obj
}

// seriesRowInput builds the append input from the generated row struct;
// NonNull follows the SCHEMA's required list, like command inputs.
func (b *builder) seriesRowInput(def *loom.SeriesDef) (gql.Input, converter, error) {
	fields, err := structFields(reflect.TypeOf(def.New()))
	if err != nil {
		return nil, nil, fmt.Errorf("%s: %w", def.Name, err)
	}
	requiredSet := map[string]bool{}
	for _, r := range def.Required {
		requiredSet[r] = true
	}
	cfg := gql.InputObjectConfigFieldMap{}
	convs := map[string]fieldConv{}
	for _, f := range fields {
		in, conv, err := b.inputType(f.t)
		if err != nil {
			return nil, nil, fmt.Errorf("%s.%s: %w", def.Name, f.snake, err)
		}
		if _, isEnum := in.(*gql.Enum); requiredSet[f.snake] && !isEnum {
			in = gql.NewNonNull(in)
		}
		cfg[f.camel] = &gql.InputObjectFieldConfig{Type: in}
		convs[f.camel] = fieldConv{snake: f.snake, conv: conv}
	}
	name := def.Name + "Input"
	if existing, ok := b.inputs[name]; ok {
		return existing, structConv(convs), nil
	}
	input := gql.NewInputObject(gql.InputObjectConfig{Name: name, Fields: cfg})
	b.inputs[name] = input
	return input, structConv(convs), nil
}

func (b *builder) seriesAppendMutation(cli *loom.Client, def *loom.SeriesDef) error {
	field := "append" + def.Name + "s"
	if _, dup := b.muts[field]; dup {
		return fmt.Errorf("graphql: mutation %q defined by two services — rename one side", field)
	}
	input, conv, err := b.seriesRowInput(def)
	if err != nil {
		return err
	}
	result := b.seriesAppendResult()
	newRow := def.New
	b.muts[field] = &gql.Field{
		Type: gql.NewNonNull(result),
		Args: gql.FieldConfigArgument{
			"namespace": {Type: gql.NewNonNull(scalarNamespace)},
			"rows":      {Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(input)))},
		},
		Resolve: func(p gql.ResolveParams) (any, error) {
			ns := fmt.Sprint(p.Args["namespace"])
			if err := decide(p.Context, Decision{Kind: "mutation", Field: field, Namespace: ns, Args: p.Args}); err != nil {
				return nil, err
			}
			if ns == AllNamespaces {
				return nil, fmt.Errorf("append needs a concrete namespace")
			}
			rawRows, _ := p.Args["rows"].([]any)
			rows := make([]loom.SeriesRow, 0, len(rawRows))
			for i, rr := range rawRows {
				raw, err := json.Marshal(conv(rr))
				if err != nil {
					return nil, err
				}
				row := newRow()
				if err := json.Unmarshal(raw, row); err != nil {
					return nil, fmt.Errorf("row %d: %v", i, err)
				}
				rows = append(rows, row)
			}
			inserted, err := cli.AppendSeries(p.Context, ns, rows...)
			if err != nil {
				return nil, err
			}
			return map[string]any{"inserted": inserted, "total": int64(len(rows))}, nil
		},
	}
	return nil
}
