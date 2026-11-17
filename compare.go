package loom

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// Comparing a fork with its source is the eval half of forking: a
// recorded run is re-run on changed code (a new look, a new agent step,
// a new reaction) by forking it at a step, and CompareFork lists where
// the two runs part after that step: the commands each issued and the
// events each wrote. An empty list says the new code does what the
// recorded run did.
//
// The two runs are read from the log, step by step after the fork
// point, and lined up by position: the first event each wrote after the
// fork point against each other, then the second, and so on. A step
// differs in its command (the command whose handling wrote it), its
// event type, or its payload, compared as JSON with @pii fields opened
// and the fields named in Ignore left out (times and fresh ids, which
// differ in every run).

// CompareOptions names a fork and its source.
type CompareOptions struct {
	Aggregate string
	// Namespace is the source's; ToNamespace the fork's.
	Namespace   string
	ID          uuid.UUID
	ToNamespace string
	// Ignore names top-level payload fields left out of the comparison.
	Ignore []string
}

// Step is one event a run wrote after the fork point.
type Step struct {
	Version int    `json:"version"`
	Command string `json:"command"`
	Event   string `json:"event"`
	// Cause is what issued the command: loop, evt (a reaction or
	// policy), timer, plan; empty for a direct dispatch.
	Cause string          `json:"cause"`
	Data  json.RawMessage `json:"data"`
}

// Difference is one step where the runs part. Source or Fork is nil when
// only the other run reached that step.
type Difference struct {
	// Step counts from 1 after the fork point.
	Step   int   `json:"step"`
	Source *Step `json:"source,omitempty"`
	Fork   *Step `json:"fork,omitempty"`
	// What names what differs: command, event, data, or missing (a step
	// one run has and the other has not).
	What []string `json:"what"`
}

// Comparison is a fork read against its source.
type Comparison struct {
	// ForkedAt is the last version the fork copied.
	ForkedAt    int          `json:"forked_at"`
	Source      []Step       `json:"source"`
	Fork        []Step       `json:"fork"`
	Differences []Difference `json:"differences"`
}

// Same says the fork did what its source did after the fork point.
func (c *Comparison) Same() bool { return len(c.Differences) == 0 }

// CompareFork reads the fork in opts.ToNamespace against its source in
// opts.Namespace after the fork point and lists where they differ.
func (c *Client) CompareFork(ctx context.Context, opts CompareOptions) (*Comparison, error) {
	agg := c.reg.aggregateDef(opts.Aggregate)
	if agg == nil {
		return nil, fmt.Errorf("loom: compare: %s is not an aggregate of %s", opts.Aggregate, c.reg.Service)
	}
	cmp := &Comparison{Differences: []Difference{}}
	// the fork point: the copies are the fork's events caused by a fork
	// of this source
	if err := c.db.QueryRow(ctx, `
		SELECT COALESCE(max(version), 0) FROM loom_events
		WHERE service=$1 AND namespace=$2 AND aggregate_type=$3 AND aggregate_id=$4 AND causation_id LIKE $5`,
		c.reg.Service, opts.ToNamespace, agg.Name, opts.ID, "fork:"+likeEscape(opts.Namespace)+"/"+opts.ID.String()+"@%").Scan(&cmp.ForkedAt); err != nil {
		return nil, err
	}
	if cmp.ForkedAt == 0 {
		return nil, fmt.Errorf("loom: compare: %s %s in %s is not a fork of %s", agg.Name, opts.ID, opts.ToNamespace, opts.Namespace)
	}
	var err error
	if cmp.Source, err = c.stepsAfter(ctx, agg.Name, opts.Namespace, opts.ID, cmp.ForkedAt, opts.Ignore); err != nil {
		return nil, err
	}
	if cmp.Fork, err = c.stepsAfter(ctx, agg.Name, opts.ToNamespace, opts.ID, cmp.ForkedAt, opts.Ignore); err != nil {
		return nil, err
	}
	for i := 0; i < max(len(cmp.Source), len(cmp.Fork)); i++ {
		d := Difference{Step: i + 1}
		switch {
		case i >= len(cmp.Fork):
			d.Source, d.What = &cmp.Source[i], []string{"missing"}
		case i >= len(cmp.Source):
			d.Fork, d.What = &cmp.Fork[i], []string{"missing"}
		default:
			s, f := &cmp.Source[i], &cmp.Fork[i]
			d.Source, d.Fork = s, f
			if s.Command != f.Command {
				d.What = append(d.What, "command")
			}
			if s.Event != f.Event {
				d.What = append(d.What, "event")
			}
			if !bytes.Equal(s.Data, f.Data) {
				d.What = append(d.What, "data")
			}
		}
		if len(d.What) > 0 {
			cmp.Differences = append(cmp.Differences, d)
		}
	}
	return cmp, nil
}

// stepsAfter reads one run's events after version from, payloads opened
// and normalised for comparison.
func (c *Client) stepsAfter(ctx context.Context, aggType, namespace string, id uuid.UUID, from int, ignore []string) ([]Step, error) {
	conn, err := c.db.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()
	rows, err := conn.Query(ctx, `
		SELECT version, command, type, causation_id, data FROM loom_events
		WHERE service=$1 AND namespace=$2 AND aggregate_type=$3 AND aggregate_id=$4 AND version > $5
		ORDER BY version`,
		c.reg.Service, namespace, aggType, id, from)
	if err != nil {
		return nil, err
	}
	type row struct {
		s    Step
		data []byte
	}
	var all []row
	for rows.Next() {
		var r row
		var cause string
		if err := rows.Scan(&r.s.Version, &r.s.Command, &r.s.Event, &cause, &r.data); err != nil {
			rows.Close()
			return nil, err
		}
		if kind, _, ok := strings.Cut(cause, ":"); ok {
			r.s.Cause = kind
		}
		all = append(all, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	steps := make([]Step, 0, len(all))
	for _, r := range all {
		// opened after the rows are closed: dek reads on this connection
		data, err := c.decryptEventData(ctx, conn, namespace, id, r.s.Event, r.data)
		if err != nil {
			return nil, err
		}
		var doc map[string]any
		if err := json.Unmarshal(data, &doc); err != nil {
			return nil, err
		}
		for _, f := range ignore {
			delete(doc, f)
		}
		// map keys marshal sorted: equal payloads compare byte-equal
		if r.s.Data, err = json.Marshal(doc); err != nil {
			return nil, err
		}
		steps = append(steps, r.s)
	}
	return steps, nil
}

func likeEscape(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}
