package loom

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// Event floors. A build that predates an event type cannot decode it, so
// a store that holds the type must never be started by such a build —
// the rule "never roll back past new event types". loom keeps, for each
// event type in a service's log, the release that first wrote it
// (loom_event_floors), and a Client whose Config.Release is older than
// any of them refuses to Start, naming the type and the release to
// start instead.
//
// A release is not a generation. Config.Generation grows with every
// deploy, a rollback included: it orders instances. Config.Release
// belongs to the code: the same number every time one build is
// deployed, and a larger one for every later build.

// EventFloor is the release that first wrote an event type to the store.
type EventFloor struct {
	EventType string `json:"event_type"`
	// Release is the first writer's Config.Release. Zero is a type first
	// written by a build that stated none, or before floors were kept:
	// it holds no build back.
	Release int64     `json:"release"`
	At      time.Time `json:"at"`
}

// FloorError is a Start refused because the store holds an event type
// first written by a later release than this build's.
type FloorError struct {
	Service string
	// Release is this build's; Floor the oldest release that may start.
	Release int64
	Floor   int64
	// EventType is an event type release Floor first wrote, and Others
	// how many more types this build is also too old for.
	EventType string
	Others    int
}

func (e *FloorError) Error() string {
	more := ""
	if e.Others > 0 {
		more = fmt.Sprintf(" (and %d more event types)", e.Others)
	}
	return fmt.Sprintf("loom: %s release %d cannot start: the store holds %s, first written by release %d%s — a build older than release %d does not know every event stored. Start release %d or newer",
		e.Service, e.Release, e.EventType, e.Floor, more, e.Floor, e.Floor)
}

// releaseFrom is Config.Release, or LOOM_RELEASE when it is not set.
func releaseFrom(release int64) int64 {
	if release != 0 {
		return release
	}
	n, err := strconv.ParseInt(os.Getenv("LOOM_RELEASE"), 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// floors is what a Client knows of the floors table.
type floors struct {
	mu sync.Mutex
	// kept is whether the table is there: 0 not yet looked, 1 yes, -1 no
	// (a store Migrate has not run on since floors arrived)
	kept int
	// seen is the event types whose floor is known to be committed
	seen map[string]bool
}

// floorsKept reports whether the store has the floors table, looking
// once. A store without it records nothing and holds no build back.
func (c *Client) floorsKept(ctx context.Context, q querier) (bool, error) {
	c.floors.mu.Lock()
	defer c.floors.mu.Unlock()
	if c.floors.kept != 0 {
		return c.floors.kept > 0, nil
	}
	var there bool
	if err := q.QueryRow(ctx, `SELECT to_regclass('loom_event_floors') IS NOT NULL`).Scan(&there); err != nil {
		return false, err
	}
	c.floors.kept = -1
	if there {
		c.floors.kept = 1
	} else {
		c.log.WarnContext(ctx, "loom: no loom_event_floors table — run Migrate; until then no event floor is recorded or checked", "service", c.reg.Service)
	}
	return there, nil
}

// recordFloors writes the floor of each event type in evts that has
// none, on the transaction appending them: the floor commits with the
// type's first event or not at all. The first write wins, so a later
// release appending the same type changes nothing.
func (c *Client) recordFloors(ctx context.Context, tx pgx.Tx, evts []*Event) error {
	var fresh []string
	c.floors.mu.Lock()
	for _, e := range evts {
		if !c.floors.seen[e.Type] && !contains(fresh, e.Type) {
			fresh = append(fresh, e.Type)
		}
	}
	c.floors.mu.Unlock()
	if len(fresh) == 0 {
		return nil
	}
	kept, err := c.floorsKept(ctx, tx)
	if err != nil || !kept {
		return err
	}
	for _, typ := range fresh {
		tag, err := tx.Exec(ctx, `
			INSERT INTO loom_event_floors (service, event_type, release)
			VALUES ($1,$2,$3)
			ON CONFLICT DO NOTHING`,
			c.reg.Service, typ, c.release)
		if err != nil {
			return err
		}
		// no row written: one is committed already, and this client need
		// not ask again. A row written here is not committed yet — the
		// unit may still roll back — so the next append asks once more.
		if tag.RowsAffected() == 0 {
			c.floors.mu.Lock()
			if c.floors.seen == nil {
				c.floors.seen = map[string]bool{}
			}
			c.floors.seen[typ] = true
			c.floors.mu.Unlock()
		}
	}
	return nil
}

// checkFloor refuses a build older than the store's floor: a *FloorError
// naming the newest release that first wrote a type this build predates.
// A build that states no release is not checked.
func (c *Client) checkFloor(ctx context.Context) error {
	if c.release == 0 {
		return nil
	}
	kept, err := c.floorsKept(ctx, c.db)
	if err != nil || !kept {
		return err
	}
	fe := &FloorError{Service: c.reg.Service, Release: c.release}
	var ahead int
	err = c.db.QueryRow(ctx, `
		SELECT event_type, release, count(*) OVER ()
		FROM loom_event_floors
		WHERE service=$1 AND release > $2
		ORDER BY release DESC, event_type
		LIMIT 1`,
		c.reg.Service, c.release).Scan(&fe.EventType, &fe.Floor, &ahead)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	fe.Others = ahead - 1
	return fe
}

// EventFloors lists the release that first wrote each event type in the
// service's log, by event type.
func (c *Client) EventFloors(ctx context.Context) ([]EventFloor, error) {
	rows, err := c.db.Query(ctx, `
		SELECT event_type, release, recorded_at FROM loom_event_floors
		WHERE service=$1 ORDER BY event_type`, c.reg.Service)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []EventFloor{}
	for rows.Next() {
		var f EventFloor
		if err := rows.Scan(&f.EventType, &f.Release, &f.At); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// seedFloors gives every event type already in the log a floor of zero,
// once: the first Migrate after floors arrive. What wrote those types
// first is not known, so they hold no build back; every type written
// from then on gets its floor with its first event.
func (c *Client) seedFloors(ctx context.Context) error {
	_, err := c.db.Exec(ctx, `
		INSERT INTO loom_event_floors (service, event_type, release)
		SELECT $1, t.type, 0
		FROM (SELECT DISTINCT type FROM loom_events WHERE service=$1) t
		WHERE NOT EXISTS (SELECT 1 FROM loom_event_floors WHERE service=$1)
		ON CONFLICT DO NOTHING`, c.reg.Service)
	if err != nil {
		return err
	}
	c.floors.mu.Lock()
	c.floors.kept = 1
	c.floors.mu.Unlock()
	return nil
}
