package loom

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
)

// Supervision: a @loop aggregate that declares `@supervised_by(Parent,
// field)` is a child of the Parent instance whose id its state holds.
// When a child is parked (its looks failed past the limit), reaches its
// end (LookDone) or is told a wait ran out, the runner leaves a
// ChildNote on its parent's row and brings the parent's look to within
// LoopSoon. The parent's look is told its live children (Looking.
// Children) and the notes since its last look (Looking.ChildNotes, each
// told once), and decides with ordinary commands, or restarts a child
// with RestartChild. A parent that fails its own looks is parked and
// told to its own supervisor the same way: escalation is failing.

// ParentDef is a child aggregate's supervisor: the parent aggregate, and
// how its id is read from the child's state (a uuid.UUID field, or a
// pointer to one; nil or the zero uuid for none).
type ParentDef struct {
	Aggregate string
	ID        func(AggregateState) any
}

// ChildRef names one child instance.
type ChildRef struct {
	Aggregate string    `json:"aggregate"`
	Namespace string    `json:"namespace"`
	ID        uuid.UUID `json:"id"`
}

// Child is how a live child stands, as its parent's look is told.
type Child struct {
	ChildRef
	Failures  int    `json:"failures"`
	LastError string `json:"last_error,omitempty"`
	Parked    bool   `json:"parked"`
	Waits     []Wait `json:"waits,omitempty"`
}

// What a ChildNote says happened.
const (
	ChildParked      = "parked"
	ChildDone        = "done"
	ChildWaitExpired = "wait expired"
)

// ChildNote is something that happened to a child since its parent's
// last look: it was parked, it reached its end, or a wait of its ran
// out.
type ChildNote struct {
	ChildRef
	What      string    `json:"what"`
	Wait      string    `json:"wait,omitempty"`
	LastError string    `json:"last_error,omitempty"`
	At        time.Time `json:"at"`
}

// RestartChild, in a parent's look, starts a child of its again: the
// child's failures are cleared, it is unparked (its wait on operators
// ended) and looked at at once, and its row records who restarted it.
// Only the parent's own children are restarted. Outside a look the
// runtime is making it does nothing.
func RestartChild(ctx context.Context, child ChildRef) {
	if s, ok := ctx.Value(lookSaidKey{}).(*lookSaid); ok {
		s.restarts = append(s.restarts, child)
	}
}

// uuidOf reads a parent id from a state field: a uuid.UUID, a pointer to
// one, or anything else as no parent.
func uuidOf(v any) uuid.UUID {
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return uuid.Nil
		}
		rv = rv.Elem()
	}
	if id, ok := rv.Interface().(uuid.UUID); ok {
		return id
	}
	return uuid.Nil
}

// supervises reports whether any aggregate of the registry declares
// aggregate as its supervisor.
func (c *Client) supervises(aggregate string) bool {
	for _, a := range c.reg.Aggregates {
		if a.Parent != nil && a.Parent.Aggregate == aggregate {
			return true
		}
	}
	return false
}

// childrenOf reads the live children of a parent instance.
func (c *Client) childrenOf(ctx context.Context, aggregate, namespace string, id uuid.UUID) ([]Child, error) {
	rows, err := c.db.Query(ctx, `
		SELECT aggregate_type, aggregate_id, failures, last_error, parked, waits
		FROM loom_loop_due
		WHERE service=$1 AND namespace=$2 AND parent_type=$3 AND parent_id=$4
		ORDER BY aggregate_type, aggregate_id`,
		c.reg.Service, namespace, aggregate, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Child
	for rows.Next() {
		var (
			ch    Child
			waits []byte
		)
		if err := rows.Scan(&ch.Aggregate, &ch.ID, &ch.Failures, &ch.LastError, &ch.Parked, &waits); err != nil {
			return nil, err
		}
		ch.Namespace = namespace
		_ = json.Unmarshal(waits, &ch.Waits)
		out = append(out, ch)
	}
	return out, rows.Err()
}

// noteParent leaves notes on a parent's row and brings its look to
// within LoopSoon; a parent with no row is given one, due at once. It
// runs on its own, outside the child's row lock, so a parent and a child
// never wait on each other's rows.
func (c *Client) noteParent(ctx context.Context, parent ChildRef, notes []ChildNote) error {
	if len(notes) == 0 || parent.ID == uuid.Nil {
		return nil
	}
	raw, err := json.Marshal(notes)
	if err != nil {
		return err
	}
	_, err = c.db.Exec(ctx, `
		INSERT INTO loom_loop_due (service, aggregate_type, namespace, aggregate_id, next_look, event_at, child_notes)
		VALUES ($1,$2,$3,$4, clock_timestamp(), clock_timestamp(), $5)
		ON CONFLICT (service, aggregate_type, namespace, aggregate_id) DO UPDATE
		SET child_notes = loom_loop_due.child_notes || EXCLUDED.child_notes,
		    event_at = clock_timestamp(),
		    next_look = LEAST(loom_loop_due.next_look, clock_timestamp() + $6 * interval '1 microsecond')`,
		c.reg.Service, parent.Aggregate, parent.Namespace, parent.ID, raw, LoopSoon.Microseconds())
	return err
}

// restartChildren starts again the children a parent's look asked to
// restart: only its own, each cleared, unparked and due at once.
func (c *Client) restartChildren(ctx context.Context, parent ChildRef, children []ChildRef) error {
	by := parent.Aggregate + "/" + parent.ID.String()
	for _, ch := range children {
		_, err := c.db.Exec(ctx, `
			UPDATE loom_loop_due SET
				failures = 0, last_error = '', parked = false,
				waits = (SELECT coalesce(jsonb_agg(w), '[]'::jsonb) FROM jsonb_array_elements(waits) w WHERE w->>'name' <> $7),
				next_look = clock_timestamp(),
				restarted_at = clock_timestamp(), restarted_by = $8
			WHERE service=$1 AND aggregate_type=$2 AND namespace=$3 AND aggregate_id=$4
			  AND parent_type=$5 AND parent_id=$6`,
			c.reg.Service, ch.Aggregate, ch.Namespace, ch.ID, parent.Aggregate, parent.ID, LoopParkedWait, by)
		if err != nil {
			return fmt.Errorf("loom: restart %s %s: %w", ch.Aggregate, ch.ID, err)
		}
	}
	return nil
}
