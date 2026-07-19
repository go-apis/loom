package gen

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/go-apis/loom/schema"
)

// OpenAPI renders the service's HTTP surface as an OpenAPI 3.0 document —
// the schema's payloads are already a JSON Schema subset, so this is a
// direct projection. Feed it to oapi-codegen (or any generator) for typed
// clients, exactly like the pre-Loom services did.
func OpenAPI(s *schema.Schema) ([]byte, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}

	components := map[string]any{}
	for _, e := range s.Enums {
		vals := make([]any, len(e.Values))
		for i, v := range e.Values {
			vals[i] = v
		}
		components[e.Name] = map[string]any{"type": "string", "enum": vals}
	}
	for _, t := range s.Types {
		components[t.Name] = oaSchema(t.Payload)
	}
	for _, a := range s.Aggregates {
		components[a.Name] = oaSchema(a.State)
	}
	for _, r := range s.Records {
		components[r.Name] = oaSchema(r.State)
	}
	for _, e := range s.Entities {
		components[e.Name] = oaSchema(e.State)
	}
	components["DispatchResult"] = map[string]any{
		"type": "object", "properties": map[string]any{"status": map[string]any{"type": "string"}},
	}
	components["Error"] = map[string]any{
		"type": "object", "properties": map[string]any{"error": map[string]any{"type": "string"}},
	}
	if usesFiles(s) {
		components["FileRef"] = map[string]any{
			"type": "object",
			"properties": map[string]any{
				"id":           map[string]any{"type": "string", "format": "uuid"},
				"key":          map[string]any{"type": "string"},
				"name":         map[string]any{"type": "string"},
				"content_type": map[string]any{"type": "string"},
				"size":         map[string]any{"type": "integer", "format": "int64"},
			},
			"required": []string{"id", "key", "size"},
		}
	}

	paths := map[string]any{}
	addCommand := func(c *schema.Command) {
		body := map[string]any{"type": "object", "properties": map[string]any{
			"aggregate_id": map[string]any{"type": "string", "format": "uuid"},
			"namespace":    map[string]any{"type": "string"},
		}, "required": []string{"aggregate_id", "namespace"}}
		if c.Payload != nil {
			props := body["properties"].(map[string]any)
			for name, p := range c.Payload.Properties {
				props[name] = oaSchema(p)
			}
			body["required"] = append(body["required"].([]string), c.Payload.Required...)
		}
		components[c.Name+"Command"] = body
		paths["/commands/"+c.Name] = map[string]any{
			"post": map[string]any{
				"operationId": "dispatch" + c.Name,
				"requestBody": map[string]any{"required": true, "content": jsonContent(ref(c.Name + "Command"))},
				"responses": map[string]any{
					"202": responseOf("accepted", ref("DispatchResult")),
					"409": responseOf("version conflict", ref("Error")),
					"422": responseOf("rejected by the domain", ref("Error")),
				},
			},
		}
	}
	for _, a := range s.Aggregates {
		for _, c := range a.Commands {
			addCommand(c)
		}
		paths["/aggregates/"+a.Name+"/{id}"] = map[string]any{
			"get": map[string]any{
				"operationId": "get" + a.Name,
				"parameters":  idParams(),
				"responses": map[string]any{"200": responseOf(a.Name+" state", map[string]any{
					"type": "object",
					"properties": map[string]any{
						"version": map[string]any{"type": "integer"},
						"state":   ref(a.Name),
					},
				}), "404": responseOf("not found", ref("Error"))},
			},
		}
	}
	for _, r := range s.Records {
		for _, c := range r.Commands {
			addCommand(c)
		}
		paths["/records/"+r.Name+"/{id}"] = getDocPath("get"+r.Name, r.Name)
		paths["/records/"+r.Name] = listPath("list"+r.Name+"s", r.Name)
	}
	for _, e := range s.Entities {
		paths["/entities/"+e.Name+"/{id}"] = getDocPath("get"+e.Name, e.Name)
		paths["/entities/"+e.Name] = listPath("list"+e.Name+"s", e.Name)
	}
	for _, a := range s.Aggregates {
		if a.Table { // the state mirror lists like an entity
			paths["/entities/"+a.Name] = listPath("list"+a.Name+"s", a.Name)
		}
	}

	if hasUploads(s) {
		components["Upload"] = map[string]any{
			"type": "object",
			"properties": map[string]any{
				"file": ref("FileRef"),
				"url":  map[string]any{"type": "string", "description": "upload session URL"},
				"protocol": map[string]any{"type": "string", "enum": []string{"gcs-resumable"},
					"description": "the chunk dialect to speak at url; switch on this, never assume"},
			},
			"required": []string{"file", "url", "protocol"},
		}
		var kinds []string
		for _, a := range s.Aggregates {
			for _, u := range a.Uploads {
				kinds = append(kinds, u.Name)
			}
		}
		for _, r := range s.Records {
			for _, u := range r.Uploads {
				kinds = append(kinds, u.Name)
			}
		}
		sort.Strings(kinds)
		paths["/uploads"] = map[string]any{
			"post": map[string]any{
				"operationId": "createUpload",
				"requestBody": map[string]any{"required": true, "content": jsonContent(map[string]any{
					"type": "object",
					"properties": map[string]any{
						"upload":       map[string]any{"type": "string", "enum": kinds},
						"namespace":    map[string]any{"type": "string"},
						"id":           map[string]any{"type": "string", "format": "uuid", "description": "target stream id"},
						"name":         map[string]any{"type": "string"},
						"content_type": map[string]any{"type": "string"},
						"size":         map[string]any{"type": "integer", "format": "int64"},
					},
					"required": []string{"upload", "namespace", "id", "size"},
				})},
				"responses": map[string]any{
					"201": responseOf("session created", ref("Upload")),
					"400": responseOf("bad request", ref("Error")),
					"422": responseOf("rejected by the domain", ref("Error")),
				},
			},
		}
		paths["/files"] = map[string]any{
			"get": map[string]any{
				"operationId": "getFile",
				"parameters": []any{
					map[string]any{"name": "key", "in": "query", "required": true, "schema": map[string]any{"type": "string"}},
				},
				"responses": map[string]any{
					"200": map[string]any{"description": "file bytes", "content": map[string]any{
						"application/octet-stream": map[string]any{"schema": map[string]any{"type": "string", "format": "binary"}},
					}},
					"404": responseOf("not found", ref("Error")),
				},
			},
		}
	}

	doc := map[string]any{
		"openapi": "3.0.3",
		"info": map[string]any{
			"title":   s.Service,
			"version": fmt.Sprintf("loom-%d", s.Loom),
			"description": "Generated from the service's .loom schema. List endpoints also accept " +
				"dynamic filter params: field=value with .gte/.lte/.gt/.lt/.ne/.like suffixes.",
		},
		"paths":      paths,
		"components": map[string]any{"schemas": components},
	}
	return json.MarshalIndent(doc, "", "  ")
}

func oaSchema(p *schema.Payload) map[string]any {
	if p == nil {
		return map[string]any{}
	}
	if p.Ref != "" {
		return ref(p.Ref)
	}
	if p.IsFile() {
		return ref("FileRef")
	}
	out := map[string]any{}
	if p.Type != "" {
		out["type"] = p.Type
	}
	if p.Format != "" {
		out["format"] = p.Format
	}
	if p.Nullable {
		out["nullable"] = true
	}
	if p.Items != nil {
		out["items"] = oaSchema(p.Items)
	}
	if len(p.Properties) > 0 {
		props := map[string]any{}
		for name, sub := range p.Properties {
			props[name] = oaSchema(sub)
		}
		out["properties"] = props
	}
	if len(p.Required) > 0 {
		req := append([]string{}, p.Required...)
		sort.Strings(req)
		out["required"] = req
	}
	return out
}

func hasUploads(s *schema.Schema) bool {
	for _, a := range s.Aggregates {
		if len(a.Uploads) > 0 {
			return true
		}
	}
	for _, r := range s.Records {
		if len(r.Uploads) > 0 {
			return true
		}
	}
	return false
}

func ref(name string) map[string]any {
	return map[string]any{"$ref": "#/components/schemas/" + name}
}

func jsonContent(s map[string]any) map[string]any {
	return map[string]any{"application/json": map[string]any{"schema": s}}
}

func responseOf(desc string, s map[string]any) map[string]any {
	return map[string]any{"description": desc, "content": jsonContent(s)}
}

func idParams() []any {
	return []any{
		map[string]any{"name": "id", "in": "path", "required": true, "schema": map[string]any{"type": "string", "format": "uuid"}},
		map[string]any{"name": "namespace", "in": "query", "required": true, "schema": map[string]any{"type": "string"}},
	}
}

func getDocPath(op, typeName string) map[string]any {
	return map[string]any{
		"get": map[string]any{
			"operationId": op,
			"parameters":  idParams(),
			"responses": map[string]any{
				"200": responseOf(typeName, ref(typeName)),
				"404": responseOf("not found", ref("Error")),
			},
		},
	}
}

func listPath(op, typeName string) map[string]any {
	params := []any{
		map[string]any{"name": "namespace", "in": "query", "required": true, "schema": map[string]any{"type": "string"}},
		map[string]any{"name": "order", "in": "query", "schema": map[string]any{"type": "string"}},
		map[string]any{"name": "limit", "in": "query", "schema": map[string]any{"type": "integer"}},
		map[string]any{"name": "offset", "in": "query", "schema": map[string]any{"type": "integer"}},
	}
	item := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"id":         map[string]any{"type": "string", "format": "uuid"},
			"namespace":  map[string]any{"type": "string"},
			"updated_at": map[string]any{"type": "string", "format": "date-time"},
			"data":       ref(typeName),
		},
	}
	return map[string]any{
		"get": map[string]any{
			"operationId": op,
			"parameters":  params,
			"responses": map[string]any{
				"200": responseOf("results", map[string]any{
					"type": "object",
					"properties": map[string]any{
						"items": map[string]any{"type": "array", "items": item},
					},
				}),
			},
		},
	}
}
