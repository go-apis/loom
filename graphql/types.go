package graphql

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
	gql "github.com/graphql-go/graphql"

	"github.com/go-apis/loom"
)

// The schema shapes come from the generated state and command structs,
// reflected once at boot (dispatch stays reflection-free — this is
// boundary-surface construction, like JSON marshaling). Field names are
// the json (snake) tags rendered camelCase, exactly as `loom graphql`
// emits them; values resolve out of snake-keyed document maps.

var (
	uuidType    = reflect.TypeOf(uuid.UUID{})
	timeType    = reflect.TypeOf(time.Time{})
	cmdBaseType = reflect.TypeOf(loom.CommandBase{})
)

type fieldInfo struct {
	snake    string
	camel    string
	t        reflect.Type
	nullable bool
}

func structFields(t reflect.Type) ([]fieldInfo, error) {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("graphql: %s is not a struct", t)
	}
	var out []fieldInfo
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.Anonymous && f.Type == cmdBaseType {
			continue // aggregateId + namespace are declared explicitly
		}
		tag := strings.Split(f.Tag.Get("json"), ",")[0]
		if tag == "-" || tag == "" {
			continue
		}
		ft, nullable := f.Type, false
		if ft.Kind() == reflect.Pointer {
			ft, nullable = ft.Elem(), true
		}
		out = append(out, fieldInfo{snake: tag, camel: camel(tag), t: ft, nullable: nullable})
	}
	return out, nil
}

func camel(snake string) string {
	parts := strings.Split(snake, "_")
	out := parts[0]
	for _, p := range parts[1:] {
		if p == "" {
			continue
		}
		out += strings.ToUpper(p[:1]) + p[1:]
	}
	return out
}

// objectFor builds (or verifies) the named top-level type for a state
// struct, with row identity fields added.
func (b *builder) objectFor(name string, sample any) (*gql.Object, error) {
	fields, err := structFields(reflect.TypeOf(sample))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	sig := signature(fields)
	if existing, ok := b.types[name]; ok {
		if strings.Join(existing.fields, ",") != strings.Join(sig, ",") {
			return nil, fmt.Errorf("graphql: two services declare type %s with different fields — rename one side", name)
		}
		return existing.obj, nil
	}
	gqlFields := gql.Fields{
		"id":        {Type: scalarUUID, Resolve: mapField("id")},
		"namespace": {Type: gql.String, Resolve: mapField("namespace")},
		"updatedAt": {Type: scalarTime, Resolve: mapField("updatedAt")},
	}
	for _, f := range fields {
		out, err := b.outputType(f.t)
		if err != nil {
			return nil, fmt.Errorf("%s.%s: %w", name, f.snake, err)
		}
		if !f.nullable {
			out = gql.NewNonNull(out)
		}
		gqlFields[f.camel] = &gql.Field{Type: out, Resolve: mapField(f.snake)}
	}
	obj := gql.NewObject(gql.ObjectConfig{Name: name, Fields: gqlFields})
	b.types[name] = &typeEntry{obj: obj, fields: sig}
	return obj, nil
}

func signature(fields []fieldInfo) []string {
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		out = append(out, f.snake)
	}
	return out
}

func mapField(key string) gql.FieldResolveFn {
	return func(p gql.ResolveParams) (any, error) {
		src, _ := p.Source.(map[string]any)
		if src == nil {
			return nil, nil
		}
		return src[key], nil
	}
}

// outputType maps a Go type to a GraphQL output, registering nested
// object types (named payload types like Address) on the way.
func (b *builder) outputType(t reflect.Type) (gql.Output, error) {
	switch {
	case t == uuidType:
		return scalarUUID, nil
	case t == timeType:
		return scalarTime, nil
	}
	switch t.Kind() {
	case reflect.String:
		return gql.String, nil
	case reflect.Int, reflect.Int32, reflect.Int64:
		return gql.Int, nil
	case reflect.Float32, reflect.Float64:
		return gql.Float, nil
	case reflect.Bool:
		return gql.Boolean, nil
	case reflect.Map:
		return scalarMap, nil
	case reflect.Slice:
		inner := t.Elem()
		if inner.Kind() == reflect.Pointer {
			inner = inner.Elem()
		}
		out, err := b.outputType(inner)
		if err != nil {
			return nil, err
		}
		return gql.NewList(gql.NewNonNull(out)), nil
	case reflect.Pointer:
		return b.outputType(t.Elem())
	case reflect.Struct:
		return b.nestedObject(t)
	default:
		return nil, fmt.Errorf("unsupported type %s", t)
	}
}

// nestedObject builds a named payload type (no identity fields).
func (b *builder) nestedObject(t reflect.Type) (*gql.Object, error) {
	name := t.Name()
	fields, err := structFields(t)
	if err != nil {
		return nil, err
	}
	sig := signature(fields)
	if existing, ok := b.types[name]; ok {
		if strings.Join(existing.fields, ",") != strings.Join(sig, ",") {
			return nil, fmt.Errorf("graphql: two services declare type %s with different fields — rename one side", name)
		}
		return existing.obj, nil
	}
	gqlFields := gql.Fields{}
	for _, f := range fields {
		out, err := b.outputType(f.t)
		if err != nil {
			return nil, err
		}
		if !f.nullable {
			out = gql.NewNonNull(out)
		}
		gqlFields[f.camel] = &gql.Field{Type: out, Resolve: mapField(f.snake)}
	}
	obj := gql.NewObject(gql.ObjectConfig{Name: name, Fields: gqlFields})
	b.types[name] = &typeEntry{obj: obj, fields: sig}
	return obj, nil
}

// --- inputs ---

// converter rewrites a GraphQL argument map (camel keys) into the json
// shape commands unmarshal from (snake keys), recursing into nested
// inputs but never into Map-scalar values, whose keys belong to the user.
type converter func(any) any

func (b *builder) commandInput(name string, cmd loom.Command) (gql.Input, converter, error) {
	fields, err := structFields(reflect.TypeOf(cmd))
	if err != nil {
		return nil, nil, fmt.Errorf("%s: %w", name, err)
	}
	cfg := gql.InputObjectConfigFieldMap{
		"aggregateId": {Type: gql.NewNonNull(scalarUUID)},
		"namespace":   {Type: gql.NewNonNull(gql.String)},
	}
	convs := map[string]fieldConv{
		"aggregateId": {snake: "aggregate_id", conv: identity},
		"namespace":   {snake: "namespace", conv: identity},
	}
	for _, f := range fields {
		in, conv, err := b.inputType(f.t)
		if err != nil {
			return nil, nil, fmt.Errorf("%s.%s: %w", name, f.snake, err)
		}
		if !f.nullable {
			in = gql.NewNonNull(in)
		}
		cfg[f.camel] = &gql.InputObjectFieldConfig{Type: in}
		convs[f.camel] = fieldConv{snake: f.snake, conv: conv}
	}
	input := gql.NewInputObject(gql.InputObjectConfig{Name: name + "Input", Fields: cfg})
	return input, structConv(convs), nil
}

type fieldConv struct {
	snake string
	conv  converter
}

func identity(v any) any { return v }

func structConv(fields map[string]fieldConv) converter {
	return func(v any) any {
		src, _ := v.(map[string]any)
		if src == nil {
			return v
		}
		out := make(map[string]any, len(src))
		for camelKey, val := range src {
			if fc, ok := fields[camelKey]; ok {
				out[fc.snake] = fc.conv(val)
			} else {
				out[camelKey] = val
			}
		}
		return out
	}
}

func listConv(inner converter) converter {
	return func(v any) any {
		src, _ := v.([]any)
		if src == nil {
			return v
		}
		out := make([]any, len(src))
		for i, item := range src {
			out[i] = inner(item)
		}
		return out
	}
}

func (b *builder) inputType(t reflect.Type) (gql.Input, converter, error) {
	switch {
	case t == uuidType:
		return scalarUUID, identity, nil
	case t == timeType:
		return scalarTime, identity, nil
	}
	switch t.Kind() {
	case reflect.String:
		return gql.String, identity, nil
	case reflect.Int, reflect.Int32, reflect.Int64:
		return gql.Int, identity, nil
	case reflect.Float32, reflect.Float64:
		return gql.Float, identity, nil
	case reflect.Bool:
		return gql.Boolean, identity, nil
	case reflect.Map:
		return scalarMap, identity, nil // user keys pass through untouched
	case reflect.Slice:
		inner := t.Elem()
		if inner.Kind() == reflect.Pointer {
			inner = inner.Elem()
		}
		in, conv, err := b.inputType(inner)
		if err != nil {
			return nil, nil, err
		}
		return gql.NewList(gql.NewNonNull(in)), listConv(conv), nil
	case reflect.Pointer:
		return b.inputType(t.Elem())
	case reflect.Struct:
		return b.nestedInput(t)
	default:
		return nil, nil, fmt.Errorf("unsupported input type %s", t)
	}
}

func (b *builder) nestedInput(t reflect.Type) (gql.Input, converter, error) {
	name := t.Name() + "Input"
	fields, err := structFields(t)
	if err != nil {
		return nil, nil, err
	}
	convs := map[string]fieldConv{}
	cfg := gql.InputObjectConfigFieldMap{}
	for _, f := range fields {
		in, conv, err := b.inputType(f.t)
		if err != nil {
			return nil, nil, err
		}
		if !f.nullable {
			in = gql.NewNonNull(in)
		}
		cfg[f.camel] = &gql.InputObjectFieldConfig{Type: in}
		convs[f.camel] = fieldConv{snake: f.snake, conv: conv}
	}
	if existing, ok := b.inputs[name]; ok {
		return existing, structConv(convs), nil
	}
	input := gql.NewInputObject(gql.InputObjectConfig{Name: name, Fields: cfg})
	b.inputs[name] = input
	return input, structConv(convs), nil
}
