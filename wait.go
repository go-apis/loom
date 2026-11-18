package loom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// A wait is how a loop says it is waiting on someone: what for, who is
// to answer, and until when. It belongs to one instance of a @loop
// aggregate and is kept with that instance's looks (loom_loop_due), so
// nothing waits on nobody and nothing waits for ever: when the deadline
// passes with the wait still open, the instance's look is called and
// told the wait has expired (Looking.Expired), once, and decides what
// happens instead.
//
// A look starts a wait with WaitFor and ends one that has been answered
// with StopWaiting. Code outside a look — a handler's caller, a process
// — uses Client.Wait and Client.EndWait.

// check refuses a wait with no name, no owner or no deadline.
func (w Wait) check() error {
	switch {
	case w.Name == "":
		return errors.New("loom: a wait needs a name")
	case w.Owner == "":
		return fmt.Errorf("loom: wait %q has no owner: say who is to answer", w.Name)
	case w.Deadline.IsZero():
		return fmt.Errorf("loom: wait %q has no deadline: say when to stop waiting", w.Name)
	}
	return nil
}

// WaitFor starts a wait for the instance being looked at, or replaces
// its wait of the same name. The look is called again when the deadline
// passes, and told the wait has expired. It is an error outside a look
// the runtime is making, and for a wait with no name, owner or
// deadline.
func WaitFor(ctx context.Context, w Wait) error {
	s, ok := ctx.Value(lookSaidKey{}).(*lookSaid)
	if !ok {
		return errors.New("loom: WaitFor outside a look: use Client.Wait")
	}
	if err := w.check(); err != nil {
		return err
	}
	w.Deadline = w.Deadline.UTC()
	s.waits = append(s.waits, w)
	return nil
}

// StopWaiting ends the wait of that name for the instance being looked
// at: it has been answered, and its deadline no longer calls the look.
// Outside a look the runtime is making it does nothing.
func StopWaiting(ctx context.Context, name string) {
	if s, ok := ctx.Value(lookSaidKey{}).(*lookSaid); ok {
		s.stopped = append(s.stopped, name)
	}
}

// settleWaits is the instance's waits after a look: those it held, less
// the ones the look was told had expired and the ones it stopped, with
// the ones it started added or replacing their namesakes.
func settleWaits(held []Wait, told Looking, said *lookSaid) []Wait {
	gone := map[string]bool{}
	for _, w := range told.Expired {
		gone[w.Name+"\x00"+w.Deadline.UTC().Format(time.RFC3339Nano)] = true
	}
	stopped := map[string]bool{}
	started := map[string]Wait{}
	if said != nil {
		for _, name := range said.stopped {
			stopped[name] = true
		}
		for _, w := range said.waits {
			started[w.Name] = w
		}
	}
	out := []Wait{}
	for _, w := range held {
		if _, replaced := started[w.Name]; replaced || stopped[w.Name] || gone[w.Name+"\x00"+w.Deadline.UTC().Format(time.RFC3339Nano)] {
			continue
		}
		out = append(out, w)
	}
	if said != nil {
		seen := map[string]bool{}
		for i := len(said.waits) - 1; i >= 0; i-- { // the last of a name stands
			w := said.waits[i]
			if !seen[w.Name] && !stopped[w.Name] {
				seen[w.Name] = true
				out = append(out, w)
			}
		}
	}
	return out
}

// Wait starts a wait for an instance of a @loop aggregate, or replaces
// its wait of the same name: the instance's look is called when the
// deadline passes and told the wait has expired. An instance with no
// looks due — never created, or done — is given them.
func (c *Client) Wait(ctx context.Context, aggregate, namespace string, id uuid.UUID, w Wait) error {
	if err := w.check(); err != nil {
		return err
	}
	if c.looping(aggregate) == nil {
		return fmt.Errorf("loom: %s declares no @loop: a wait belongs to a loop", aggregate)
	}
	w.Deadline = w.Deadline.UTC()
	return c.changeWaits(ctx, aggregate, namespace, id, true, func(s LoopState) (LoopState, bool) {
		return s.Waited(w), false
	})
}

// EndWait ends an instance's wait of that name: it has been answered.
// Ending a wait that is not there does nothing. Ending a parked
// instance's LoopParkedWait unparks it: its failures are cleared and it
// is looked at again at once.
func (c *Client) EndWait(ctx context.Context, aggregate, namespace string, id uuid.UUID, name string) error {
	return c.changeWaits(ctx, aggregate, namespace, id, false, func(s LoopState) (LoopState, bool) {
		return s.WaitEnded(name)
	})
}

// changeWaits rewrites an instance's waits under its row's lock and
// brings its next look to the first deadline, when that is sooner.
// create writes the row for an instance that has none. A parked
// instance left without its LoopParkedWait is unparked and due at once.
func (c *Client) changeWaits(ctx context.Context, aggregate, namespace string, id uuid.UUID, create bool, change func(LoopState) (LoopState, bool)) error {
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if create {
		// due a minute on unless a wait or an event says sooner
		if _, err := tx.Exec(ctx, `
			INSERT INTO loom_loop_due (service, aggregate_type, namespace, aggregate_id, next_look, looked_at)
			VALUES ($1,$2,$3,$4, clock_timestamp() + $5 * interval '1 microsecond', clock_timestamp())
			ON CONFLICT DO NOTHING`,
			c.reg.Service, aggregate, namespace, id, LoopEvery.Microseconds()); err != nil {
			return err
		}
	}
	var (
		raw    []byte
		parked bool
		acting int
	)
	err = tx.QueryRow(ctx, `
		SELECT waits, parked, acting FROM loom_loop_due
		WHERE service=$1 AND aggregate_type=$2 AND namespace=$3 AND aggregate_id=$4
		FOR UPDATE`,
		c.reg.Service, aggregate, namespace, id).Scan(&raw, &parked, &acting)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	var held LoopState
	_ = json.Unmarshal(raw, &held.Waits)
	held.Parked = parked
	held.Acting = acting
	changed, lookNow := change(held)
	waits := changed.Waits
	out, err := json.Marshal(waits)
	if err != nil {
		return err
	}
	var first *time.Time
	for i := range waits {
		if first == nil || waits[i].Deadline.Before(*first) {
			first = &waits[i].Deadline
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE loom_loop_due SET waits=$5, next_look = LEAST(next_look, $6::timestamptz), acting=$7
		WHERE service=$1 AND aggregate_type=$2 AND namespace=$3 AND aggregate_id=$4`,
		c.reg.Service, aggregate, namespace, id, out, first, changed.Acting); err != nil {
		return err
	}
	if lookNow {
		// its operators answered: look again now, from a clean count
		if _, err := tx.Exec(ctx, `
			UPDATE loom_loop_due SET parked=false, failures=0, next_look=clock_timestamp()
			WHERE service=$1 AND aggregate_type=$2 AND namespace=$3 AND aggregate_id=$4`,
			c.reg.Service, aggregate, namespace, id); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func holdsWait(waits []Wait, name string) bool {
	for _, w := range waits {
		if w.Name == name {
			return true
		}
	}
	return false
}
