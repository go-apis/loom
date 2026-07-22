package graphql

import (
	"fmt"
	"net/url"
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
	fileRefType = reflect.TypeOf(loom.FileRef{})
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
		// Row fields stay nullable regardless of Go pointer-ness: the
		// SDL contract (`loom graphql`) declares entity/state fields
		// nullable (the DSL has no `!` in state or entity blocks), and a
		// column added by migration is NULL for rows written before it
		// existed — a NonNull here made those rows unreadable.
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
	case t == fileRefType:
		return b.fileRefObject()
	}
	switch t.Kind() {
	case reflect.String:
		// generated enums are named string types; resolve by type name
		if t.Name() != "string" {
			if e, ok := b.enums[t.Name()]; ok {
				return e, nil
			}
		}
		return gql.String, nil
	case reflect.Int, reflect.Int32, reflect.Int64:
		// schema int is int64: Long, matching the emitted SDL
		return scalarLong, nil
	case reflect.Float32, reflect.Float64:
		return gql.Float, nil
	case reflect.Bool:
		return gql.Boolean, nil
	case reflect.Map:
		return scalarMap, nil
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			// schema `bytes` ([]byte): base64 on the wire, like JSON
			return gql.String, nil
		}
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

// fileRefObject builds (once) the FileRef type `file` schema fields
// resolve to. It is hand-shaped rather than reflected so size rides the
// Long scalar — GraphQL Int is 32-bit and files are not.
func (b *builder) fileRefObject() (*gql.Object, error) {
	if e, ok := b.types["FileRef"]; ok {
		return e.obj, nil
	}
	obj := gql.NewObject(gql.ObjectConfig{Name: "FileRef", Fields: gql.Fields{
		"id":          {Type: gql.NewNonNull(scalarUUID), Resolve: mapField("id")},
		"key":         {Type: gql.NewNonNull(gql.String), Resolve: mapField("key")},
		"name":        {Type: gql.String, Resolve: mapField("name")},
		"contentType": {Type: gql.String, Resolve: mapField("content_type")},
		"size":        {Type: gql.NewNonNull(scalarLong), Resolve: mapField("size")},
		// where to fetch the bytes, relative to the gateway host (the
		// Files handler). Opaque to clients — a signed-URL upgrade later
		// changes what this returns, not who calls it.
		"downloadUrl": {Type: gql.NewNonNull(gql.String), Resolve: func(p gql.ResolveParams) (any, error) {
			src, _ := p.Source.(map[string]any)
			if src == nil {
				return nil, nil
			}
			key, _ := src["key"].(string)
			return "/files?key=" + url.QueryEscape(key), nil
		}},
	}})
	b.types["FileRef"] = &typeEntry{obj: obj, fields: []string{"content_type", "download_url", "id", "key", "name", "size"}}
	return obj, nil
}

func (b *builder) fileRefInput() (gql.Input, converter, error) {
	conv := structConv(map[string]fieldConv{
		"id":          {snake: "id", conv: identity},
		"key":         {snake: "key", conv: identity},
		"name":        {snake: "name", conv: identity},
		"contentType": {snake: "content_type", conv: identity},
		"size":        {snake: "size", conv: identity},
	})
	if existing, ok := b.inputs["FileRefInput"]; ok {
		return existing, conv, nil
	}
	input := gql.NewInputObject(gql.InputObjectConfig{Name: "FileRefInput", Fields: gql.InputObjectConfigFieldMap{
		"id":          {Type: gql.NewNonNull(scalarUUID)},
		"key":         {Type: gql.NewNonNull(gql.String)},
		"name":        {Type: gql.String},
		"contentType": {Type: gql.String},
		"size":        {Type: gql.NewNonNull(scalarLong)},
	}})
	b.inputs["FileRefInput"] = input
	return input, conv, nil
}

// --- inputs ---

// converter rewrites a GraphQL argument map (camel keys) into the json
// shape commands unmarshal from (snake keys), recursing into nested
// inputs but never into Map-scalar values, whose keys belong to the user.
type converter func(any) any

func (b *builder) commandInput(name string, cmd loom.Command, required []string) (gql.Input, converter, error) {
	fields, err := structFields(reflect.TypeOf(cmd))
	if err != nil {
		return nil, nil, fmt.Errorf("%s: %w", name, err)
	}
	requiredSet := map[string]bool{}
	for _, r := range required {
		requiredSet[r] = true
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
		// NonNull follows the SCHEMA's required list, matching the
		// emitted SDL — Go value-ness can't tell optional from required.
		// Enum fields stay nullable either way: their zero value means
		// "unset", and the generated Validate() rejects an empty
		// required enum at dispatch.
		if _, isEnum := in.(*gql.Enum); requiredSet[f.snake] && !isEnum {
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
	case t == fileRefType:
		return b.fileRefInput()
	}
	switch t.Kind() {
	case reflect.String:
		if t.Name() != "string" {
			if e, ok := b.enums[t.Name()]; ok {
				return e, identity, nil
			}
		}
		return gql.String, identity, nil
	case reflect.Int, reflect.Int32, reflect.Int64:
		return scalarLong, identity, nil
	case reflect.Float32, reflect.Float64:
		return gql.Float, identity, nil
	case reflect.Bool:
		return gql.Boolean, identity, nil
	case reflect.Map:
		return scalarMap, identity, nil // user keys pass through untouched
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			// schema `bytes` ([]byte): base64 strings in, like JSON
			return gql.String, identity, nil
		}
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
