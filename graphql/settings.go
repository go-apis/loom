package graphql

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	gql "github.com/graphql-go/graphql"

	"github.com/go-apis/loom"
)

// Settings through the gateway. A service that declares settings
// (loom/settings.go) gets them read and changed here with no code of
// its own: `settings` and `settingHistory` to read, `setSetting` and
// `resetSetting` to change, each a change with a reason. They answer to
// the gateway's rules like any other field — the namespace gate for the
// reads, the mutation gate for the changes — and the caller's actor is
// who the change is recorded as made by.
//
// A value crosses as its JSON: 8, true, "hot". setSetting also takes a
// bare word for a string or enum setting, so `value: "hot"` and
// `value: "\"hot\""` say the same.

var (
	settingObject = gql.NewObject(gql.ObjectConfig{
		Name:        "Setting",
		Description: "A value the service's behaviour reads and a person may change while it runs, as one namespace has it.",
		Fields: gql.Fields{
			"service": {Type: gql.NewNonNull(gql.String)},
			"name":    {Type: gql.NewNonNull(gql.String)},
			"type":    {Type: gql.NewNonNull(gql.String), Description: "string, int, bool, float or enum."},
			"value":   {Type: gql.NewNonNull(gql.String), Description: "The value in force, as JSON."},
			"default": {Type: gql.NewNonNull(gql.String), Description: "The schema's default, as JSON."},
			"values":  {Type: gql.NewNonNull(gql.NewList(gql.NewNonNull(gql.String))), Description: "An enum setting's values."},
			"from":    {Type: gql.NewNonNull(gql.String), Description: "Where the value comes from: namespace (set here), every_namespace (set for all that have not set their own) or default."},
			"actor":   {Type: gql.String, Description: "Who made the change behind the value."},
			"reason":  {Type: gql.String, Description: "Why."},
			"at":      {Type: scalarTime, Description: "When."},
			"doc":     {Type: gql.String, Description: "What the setting is."},
		},
	})
	settingChangeObject = gql.NewObject(gql.ObjectConfig{
		Name:        "SettingChange",
		Description: "One change to a setting in a namespace.",
		Fields: gql.Fields{
			"name":      {Type: gql.NewNonNull(gql.String)},
			"namespace": {Type: gql.NewNonNull(scalarNamespace)},
			"old":       {Type: gql.String, Description: "The value before, as JSON; null when the namespace had not set it."},
			"new":       {Type: gql.String, Description: "The value after, as JSON; null for a reset."},
			"actor":     {Type: gql.NewNonNull(gql.String)},
			"reason":    {Type: gql.NewNonNull(gql.String)},
			"at":        {Type: gql.NewNonNull(scalarTime)},
		},
	})
)

func jsonText(v any) string {
	raw, _ := json.Marshal(v)
	return string(raw)
}

func settingDoc(service string, def *loom.SettingDef, v loom.SettingValue) map[string]any {
	doc := map[string]any{
		"service": service, "name": v.Name, "type": v.Type, "value": jsonText(v.Value), "default": jsonText(v.Default),
		"values": orEmpty(def.Values), "from": v.From,
	}
	if v.From != loom.SettingFromDefault {
		doc["actor"], doc["reason"], doc["at"] = v.Actor, v.Reason, v.At.UTC().Format(time.RFC3339Nano)
	}
	if v.Doc != "" {
		doc["doc"] = v.Doc
	}
	return doc
}

// settingsFields adds the settings fields once any composed service
// declares a setting.
func (b *builder) settingsFields() error {
	var with []*loom.Client
	for _, cli := range b.services {
		if len(cli.Registry().Settings) > 0 {
			with = append(with, cli)
		}
	}
	if len(with) == 0 {
		return nil
	}
	def := func(cli *loom.Client, name string) *loom.SettingDef {
		for _, d := range cli.Registry().Settings {
			if d.Name == name {
				return d
			}
		}
		return nil
	}
	// owner finds the service a setting belongs to: the one named, or
	// the only one that declares it
	owner := func(args map[string]any) (*loom.Client, *loom.SettingDef, error) {
		name, _ := args["name"].(string)
		service, _ := args["service"].(string)
		var found []*loom.Client
		for _, cli := range with {
			if (service == "" || cli.Registry().Service == service) && def(cli, name) != nil {
				found = append(found, cli)
			}
		}
		switch len(found) {
		case 0:
			return nil, nil, fmt.Errorf("no setting %q", name)
		case 1:
			return found[0], def(found[0], name), nil
		}
		var names []string
		for _, cli := range found {
			names = append(names, cli.Registry().Service)
		}
		return nil, nil, fmt.Errorf("setting %q is declared by %s: say which with `service`", name, strings.Join(names, " and "))
	}
	one := func(p gql.ResolveParams, cli *loom.Client, d *loom.SettingDef, ns string) (any, error) {
		v, err := cli.SettingValue(p.Context, ns, d.Name)
		if err != nil {
			return nil, err
		}
		return settingDoc(cli.Registry().Service, d, *v), nil
	}

	b.gated("query", gate{}, true, "settings", "settingHistory")
	if err := b.addQuery("settings", &gql.Field{
		Description: "Every setting the services declare, as a namespace has it: the value in force, where it comes from and the change behind it.",
		Type:        gql.NewNonNull(gql.NewList(gql.NewNonNull(settingObject))),
		Args: gql.FieldConfigArgument{
			"namespace": {Type: gql.NewNonNull(scalarNamespace)},
			"service":   {Type: gql.String, Description: "Only this service's settings."},
		},
		Resolve: func(p gql.ResolveParams) (any, error) {
			ns, _ := p.Args["namespace"].(string)
			if err := decide(p.Context, Decision{Kind: "query", Field: "settings", Namespace: ns, Args: p.Args}); err != nil {
				return nil, err
			}
			service, _ := p.Args["service"].(string)
			out := []any{}
			for _, cli := range with {
				if service != "" && cli.Registry().Service != service {
					continue
				}
				values, err := cli.Settings(p.Context, ns)
				if err != nil {
					return nil, err
				}
				for _, v := range values {
					out = append(out, settingDoc(cli.Registry().Service, def(cli, v.Name), v))
				}
			}
			return out, nil
		},
	}); err != nil {
		return err
	}
	if err := b.addQuery("settingHistory", &gql.Field{
		Description: "The changes to a setting in a namespace, newest first: who, when, why, and the value before and after.",
		Type:        gql.NewNonNull(gql.NewList(gql.NewNonNull(settingChangeObject))),
		Args: gql.FieldConfigArgument{
			"namespace": {Type: gql.NewNonNull(scalarNamespace)},
			"name":      {Type: gql.NewNonNull(gql.String)},
			"service":   {Type: gql.String},
			"limit":     {Type: gql.Int},
		},
		Resolve: func(p gql.ResolveParams) (any, error) {
			ns, _ := p.Args["namespace"].(string)
			if err := decide(p.Context, Decision{Kind: "query", Field: "settingHistory", Namespace: ns, Args: p.Args}); err != nil {
				return nil, err
			}
			cli, d, err := owner(p.Args)
			if err != nil {
				return nil, err
			}
			limit, _ := p.Args["limit"].(int)
			changes, err := cli.SettingHistory(p.Context, ns, d.Name, limit)
			if err != nil {
				return nil, err
			}
			out := make([]any, len(changes))
			for i, ch := range changes {
				doc := map[string]any{"name": ch.Name, "namespace": ch.Namespace, "actor": ch.Actor, "reason": ch.Reason, "at": ch.At.UTC().Format(time.RFC3339Nano)}
				if ch.Old != nil {
					doc["old"] = jsonText(ch.Old)
				}
				if ch.New != nil {
					doc["new"] = jsonText(ch.New)
				}
				out[i] = doc
			}
			return out, nil
		},
	}); err != nil {
		return err
	}

	change := func(field, description string, reset bool) {
		args := gql.FieldConfigArgument{
			"namespace": {Type: gql.NewNonNull(scalarNamespace), Description: "The namespace to set it for; \"*\" sets it for every namespace that has not set its own."},
			"name":      {Type: gql.NewNonNull(gql.String)},
			"reason":    {Type: gql.NewNonNull(gql.String), Description: "Why. A change with no reason is refused."},
			"service":   {Type: gql.String, Description: "The service that declares it, when more than one does."},
		}
		if !reset {
			args["value"] = &gql.ArgumentConfig{Type: gql.NewNonNull(gql.String), Description: "The new value, as JSON: 8, true, \"hot\". A string or enum setting also takes the bare word."}
		}
		b.gated("mutation", gate{}, true, field)
		b.muts[field] = &gql.Field{
			Description: description,
			Type:        gql.NewNonNull(settingObject),
			Args:        args,
			Resolve: func(p gql.ResolveParams) (any, error) {
				ns, _ := p.Args["namespace"].(string)
				if err := decide(p.Context, Decision{Kind: "mutation", Field: field, Namespace: ns, Args: p.Args}); err != nil {
					return nil, err
				}
				cli, d, err := owner(p.Args)
				if err != nil {
					return nil, err
				}
				reason, _ := p.Args["reason"].(string)
				if reset {
					err = cli.ResetSetting(p.Context, ns, d.Name, reason)
				} else {
					text, _ := p.Args["value"].(string)
					var value any
					dec := json.NewDecoder(strings.NewReader(text))
					dec.UseNumber()
					if jerr := dec.Decode(&value); jerr != nil || dec.More() {
						value = text // a bare word, for a string or enum setting
					}
					// a string setting takes its text whatever it looks like
					if _, quoted := value.(string); !quoted && d.Type == "string" {
						value = text
					}
					err = cli.SetSetting(p.Context, ns, d.Name, value, reason)
				}
				if err != nil {
					return nil, err
				}
				return one(p, cli, d, ns)
			},
		}
	}
	change("setSetting", "Change a setting for a namespace, with the reason. The change is kept: who, when, why and the value before.", false)
	change("resetSetting", "Take a namespace's own value of a setting away, with the reason, so it reads as set for every namespace or by default again.", true)
	return nil
}
