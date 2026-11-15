package loom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Settings are the values a service's behaviour reads and a person may
// change while it runs: a cap, a switch, a choice. The schema declares
// each with its type and default (`setting build_cap: int @default(2)`),
// and the runtime keeps what each namespace has it set to.
//
// A change always says why. SetSetting and ResetSetting refuse an empty
// reason, and every change is kept — who made it, when, why, the value
// before and the value after — so a setting that surprises someone can
// be traced to the person and the reason.
//
// A setting is read for a namespace. A namespace that has not set it
// takes the value set for every namespace (EveryNamespace), and failing
// that the schema's default.

// SettingDef is one declared setting.
type SettingDef struct {
	Name string
	// Type is string, int, bool, float, or enum.
	Type string
	// Default is of the Go type the setting reads as: string (an enum's
	// too), int64, bool or float64.
	Default any
	// Values is an enum setting's values.
	Values []string
	// Doc — see AggregateDef.Doc.
	Doc string
}

// EveryNamespace is the namespace a setting is set in to apply to every
// namespace that has not set it itself.
const EveryNamespace = "*"

// ErrNoReason is a change to a setting that does not say why.
var ErrNoReason = errors.New("loom: a setting is changed with a reason: say why")

// SettingValue is a setting as a namespace has it.
type SettingValue struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Value   any    `json:"value"`
	Default any    `json:"default"`
	// From is where Value comes from: the namespace's own setting, the
	// one set for every namespace, or the schema's default.
	From string `json:"from"`
	// Actor, Reason and At are the last change behind Value; empty for a
	// default.
	Actor  string    `json:"actor,omitempty"`
	Reason string    `json:"reason,omitempty"`
	At     time.Time `json:"at,omitempty"`
	Doc    string    `json:"doc,omitempty"`
}

// Where a setting's value comes from (SettingValue.From).
const (
	SettingFromNamespace = "namespace"
	SettingFromEvery     = "every_namespace"
	SettingFromDefault   = "default"
)

// SettingChange is one change to a setting.
type SettingChange struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	// Old and New are the values before and after; nil is "not set here"
	// — the first change has no Old, and a reset has no New.
	Old    any       `json:"old"`
	New    any       `json:"new"`
	Actor  string    `json:"actor"`
	Reason string    `json:"reason"`
	At     time.Time `json:"at"`
}

func (r *Registry) setting(name string) *SettingDef {
	for _, s := range r.Settings {
		if s.Name == name {
			return s
		}
	}
	return nil
}

// coerce checks v against the setting's type and answers it as the Go
// type the setting reads as.
func (d *SettingDef) coerce(v any) (any, error) {
	wrong := func() (any, error) {
		return nil, fmt.Errorf("loom: setting %s is %s, and %v (%T) is not", d.Name, d.typeText(), v, v)
	}
	if n, ok := v.(json.Number); ok {
		f, err := n.Float64()
		if err != nil {
			return wrong()
		}
		v = f
	}
	switch d.Type {
	case "string":
		if s, ok := v.(string); ok {
			return s, nil
		}
	case "enum":
		if s, ok := v.(string); ok {
			if contains(d.Values, s) {
				return s, nil
			}
			return nil, fmt.Errorf("loom: setting %s is one of %s, and %q is not", d.Name, strings.Join(d.Values, ", "), s)
		}
	case "bool":
		if b, ok := v.(bool); ok {
			return b, nil
		}
	case "int":
		switch n := v.(type) {
		case int:
			return int64(n), nil
		case int32:
			return int64(n), nil
		case int64:
			return n, nil
		case float64:
			if n == math.Trunc(n) && math.Abs(n) < 1<<53 {
				return int64(n), nil
			}
		}
	case "float":
		switch n := v.(type) {
		case float64:
			return n, nil
		case float32:
			return float64(n), nil
		case int:
			return float64(n), nil
		case int64:
			return float64(n), nil
		}
	}
	return wrong()
}

func (d *SettingDef) typeText() string {
	if d.Type == "enum" {
		return "one of " + strings.Join(d.Values, ", ")
	}
	if d.Type == "int" {
		return "an int"
	}
	return "a " + d.Type
}

// stored decodes a value as it rests, to the setting's Go type.
func (d *SettingDef) stored(raw []byte) (any, error) {
	if raw == nil {
		return nil, nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	return d.coerce(v)
}

func (c *Client) settingDef(name string) (*SettingDef, error) {
	d := c.reg.setting(name)
	if d == nil {
		return nil, fmt.Errorf("loom: %s declares no setting %q", c.reg.Service, name)
	}
	return d, nil
}

// SettingValue reads a setting as a namespace has it: its value, where
// that comes from, and the change behind it.
func (c *Client) SettingValue(ctx context.Context, namespace, name string) (*SettingValue, error) {
	d, err := c.settingDef(name)
	if err != nil {
		return nil, err
	}
	out := &SettingValue{Name: d.Name, Type: d.Type, Value: d.Default, Default: d.Default, From: SettingFromDefault, Doc: d.Doc}
	// the namespace's own row sorts before every namespace's
	rows, err := c.db.Query(ctx, `
		SELECT namespace, value, actor, reason, updated_at FROM loom_settings
		WHERE service=$1 AND name=$2 AND namespace IN ($3, $4)
		ORDER BY (namespace = $3) DESC
		LIMIT 1`,
		c.reg.Service, name, namespace, EveryNamespace)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if rows.Next() {
		var (
			ns  string
			raw []byte
		)
		if err := rows.Scan(&ns, &raw, &out.Actor, &out.Reason, &out.At); err != nil {
			return nil, err
		}
		if out.Value, err = d.stored(raw); err != nil {
			return nil, fmt.Errorf("loom: setting %s in %s: %w", name, ns, err)
		}
		out.From = SettingFromNamespace
		if ns != namespace {
			out.From = SettingFromEvery
		}
	}
	return out, rows.Err()
}

// Setting reads a setting's value for a namespace: string (an enum's
// too), int64, bool or float64.
func (c *Client) Setting(ctx context.Context, namespace, name string) (any, error) {
	v, err := c.SettingValue(ctx, namespace, name)
	if err != nil {
		return nil, err
	}
	return v.Value, nil
}

// SettingString reads a string or enum setting.
func (c *Client) SettingString(ctx context.Context, namespace, name string) (string, error) {
	v, err := c.Setting(ctx, namespace, name)
	if err != nil {
		return "", err
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("loom: setting %s is not a string", name)
	}
	return s, nil
}

// SettingInt reads an int setting.
func (c *Client) SettingInt(ctx context.Context, namespace, name string) (int64, error) {
	v, err := c.Setting(ctx, namespace, name)
	if err != nil {
		return 0, err
	}
	n, ok := v.(int64)
	if !ok {
		return 0, fmt.Errorf("loom: setting %s is not an int", name)
	}
	return n, nil
}

// SettingBool reads a bool setting.
func (c *Client) SettingBool(ctx context.Context, namespace, name string) (bool, error) {
	v, err := c.Setting(ctx, namespace, name)
	if err != nil {
		return false, err
	}
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("loom: setting %s is not a bool", name)
	}
	return b, nil
}

// SettingFloat reads a float setting.
func (c *Client) SettingFloat(ctx context.Context, namespace, name string) (float64, error) {
	v, err := c.Setting(ctx, namespace, name)
	if err != nil {
		return 0, err
	}
	f, ok := v.(float64)
	if !ok {
		return 0, fmt.Errorf("loom: setting %s is not a float", name)
	}
	return f, nil
}

// Settings lists every declared setting as a namespace has it, by name.
func (c *Client) Settings(ctx context.Context, namespace string) ([]SettingValue, error) {
	out := make([]SettingValue, 0, len(c.reg.Settings))
	for _, d := range c.reg.Settings {
		v, err := c.SettingValue(ctx, namespace, d.Name)
		if err != nil {
			return nil, err
		}
		out = append(out, *v)
	}
	return out, nil
}

// SetSetting sets a setting for a namespace (EveryNamespace for all that
// have not set their own), and keeps the change: who (the context's
// Metadata.Actor), when, the reason and the value before. An empty
// reason is ErrNoReason; a value of the wrong type is refused.
func (c *Client) SetSetting(ctx context.Context, namespace, name string, value any, reason string) error {
	d, err := c.settingDef(name)
	if err != nil {
		return err
	}
	v, err := d.coerce(value)
	if err != nil {
		return err
	}
	return c.changeSetting(ctx, d, namespace, v, reason)
}

// ResetSetting takes a namespace's own value away, so it reads as set
// for every namespace or by default again. The reset is kept as a
// change, and needs a reason like any other.
func (c *Client) ResetSetting(ctx context.Context, namespace, name, reason string) error {
	d, err := c.settingDef(name)
	if err != nil {
		return err
	}
	return c.changeSetting(ctx, d, namespace, nil, reason)
}

// changeSetting writes a value (nil for a reset) and its history row in
// one transaction, under the setting's row lock.
func (c *Client) changeSetting(ctx context.Context, d *SettingDef, namespace string, value any, reason string) error {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return ErrNoReason
	}
	if namespace == "" {
		return fmt.Errorf("loom: setting %s: no namespace (use loom.EveryNamespace for all)", d.Name)
	}
	actor := MetaFrom(ctx).Actor
	var next []byte
	if value != nil {
		var err error
		if next, err = json.Marshal(value); err != nil {
			return err
		}
	}
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var prev []byte
	err = tx.QueryRow(ctx, `
		SELECT value FROM loom_settings WHERE service=$1 AND namespace=$2 AND name=$3 FOR UPDATE`,
		c.reg.Service, namespace, d.Name).Scan(&prev)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if next == nil {
		if prev == nil {
			return nil // nothing set here: nothing to reset, nothing to record
		}
		if _, err := tx.Exec(ctx, `DELETE FROM loom_settings WHERE service=$1 AND namespace=$2 AND name=$3`,
			c.reg.Service, namespace, d.Name); err != nil {
			return err
		}
	} else if _, err := tx.Exec(ctx, `
		INSERT INTO loom_settings (service, namespace, name, value, actor, reason, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6, now())
		ON CONFLICT (service, namespace, name)
		DO UPDATE SET value = EXCLUDED.value, actor = EXCLUDED.actor, reason = EXCLUDED.reason, updated_at = EXCLUDED.updated_at`,
		c.reg.Service, namespace, d.Name, next, actor, reason); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO loom_setting_changes (service, namespace, name, old, new, actor, reason)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		c.reg.Service, namespace, d.Name, prev, next, actor, reason); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// SettingHistory lists the changes to a setting in a namespace, newest
// first.
func (c *Client) SettingHistory(ctx context.Context, namespace, name string, limit int) ([]SettingChange, error) {
	d, err := c.settingDef(name)
	if err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := c.db.Query(ctx, `
		SELECT old, new, actor, reason, at FROM loom_setting_changes
		WHERE service=$1 AND namespace=$2 AND name=$3
		ORDER BY id DESC LIMIT $4`,
		c.reg.Service, namespace, name, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []SettingChange{}
	for rows.Next() {
		ch := SettingChange{Name: name, Namespace: namespace}
		var prev, next []byte
		if err := rows.Scan(&prev, &next, &ch.Actor, &ch.Reason, &ch.At); err != nil {
			return nil, err
		}
		// a value kept under an older declaration of the setting may no
		// longer be of its type: history shows it as it was written
		if ch.Old, err = d.stored(prev); err != nil {
			_ = json.Unmarshal(prev, &ch.Old)
		}
		if ch.New, err = d.stored(next); err != nil {
			_ = json.Unmarshal(next, &ch.New)
		}
		out = append(out, ch)
	}
	return out, rows.Err()
}
