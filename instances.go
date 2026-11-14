package loom

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// A deploy leaves the previous revision's instances alive for a while:
// Cloud Run idles them with CPU allocated, keeps one for a traffic tag,
// or lets a deleted revision run on for minutes. The process lease
// already moves to the newest build (election.go). Timers did not: they
// fire on every instance, so an old instance went on firing them, and
// running the commands they hold, with the old code.
//
// So every instance with a generation says it is alive — a row in
// loom_instances, refreshed every beat — and an instance fires timers
// only while no live instance is of a newer generation than its own.
// Liveness, not a high-water mark: when traffic is shifted back to an
// older revision and the newer one's instances are gone, the older
// instances are the newest alive and fire again within a few beats.
//
// With no generation none of this happens: every instance fires timers,
// as it always did.

// instanceBeat is how often an instance refreshes its row and looks for
// a newer generation, for the poll interval it was started with: the
// poll itself, kept between a tenth of a second and five.
func instanceBeat(poll time.Duration) time.Duration {
	return min(max(poll, 100*time.Millisecond), 5*time.Second)
}

// instanceAlive is how long after its last beat an instance still
// counts: four beats, so one slow beat retires nobody's elders.
func instanceAlive(beat time.Duration) time.Duration { return 4 * beat }

// runInstances keeps this instance's row fresh and c.superseded true
// while a newer generation is alive. It closes ready after the first
// beat, which the timer runner waits for: an old instance woken after a
// deploy learns it is superseded before it fires anything.
func (c *Client) runInstances(ctx context.Context, poll time.Duration, ready chan<- struct{}) {
	c.resolveGeneration(ctx)
	if c.gen.n <= 0 {
		close(ready)
		return
	}
	id := uuid.New()
	every := instanceBeat(poll)
	alive := instanceAlive(every).Seconds()
	beat := func() {
		look, cancel := context.WithTimeout(ctx, every)
		defer cancel()
		var newest int64
		err := c.db.QueryRow(look, `
			WITH me AS (
				INSERT INTO loom_instances (service, instance, generation, revision, seen_at)
				VALUES ($1,$2,$3,$4, now())
				ON CONFLICT (service, instance) DO UPDATE SET seen_at = now()
			), gone AS (
				DELETE FROM loom_instances WHERE service=$1 AND seen_at < now() - make_interval(secs => 10 * $5::float8)
			)
			SELECT coalesce(max(generation), 0) FROM loom_instances
			WHERE service=$1 AND instance <> $2 AND seen_at > now() - make_interval(secs => $5::float8)`,
			c.reg.Service, id, c.gen.n, c.gen.revision, alive).Scan(&newest)
		if err != nil {
			if ctx.Err() == nil {
				// not knowing is not being superseded: keep the last answer
				c.log.WarnContext(ctx, "instance beat", "error", err)
			}
			return
		}
		was := c.superseded.Swap(newest > c.gen.n)
		switch now := newest > c.gen.n; {
		case now && !was:
			c.log.InfoContext(ctx, "a newer generation is alive: this instance stops firing timers",
				append(c.genAttrs(), "newer_generation", newest)...)
		case was && !now:
			c.log.InfoContext(ctx, "no newer generation is alive: this instance fires timers again", c.genAttrs()...)
		}
	}
	beat()
	close(ready)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			// say so on the way out, so the instances left do not wait
			// out the liveness window for this one
			bye, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, _ = c.db.Exec(bye, `DELETE FROM loom_instances WHERE service=$1 AND instance=$2`, c.reg.Service, id)
			cancel()
			return
		case <-t.C:
			beat()
		}
	}
}

// InstanceStatus is the live instances of one build.
type InstanceStatus struct {
	Generation int64     `json:"generation"`
	Revision   string    `json:"revision,omitempty"`
	Count      int       `json:"count"`
	SeenAt     time.Time `json:"seen_at"`
	// Newest is whether this is the newest generation alive: the one
	// whose instances fire timers.
	Newest bool `json:"newest"`
}

// instances is the builds with an instance seen in the last minute,
// newest first. Instances with no generation keep no row and are not
// listed.
func (c *Client) instances(ctx context.Context) ([]InstanceStatus, error) {
	rows, err := c.db.Query(ctx, `
		SELECT generation, revision, count(*), max(seen_at) FROM loom_instances
		WHERE service=$1 AND seen_at > now() - interval '1 minute'
		GROUP BY generation, revision ORDER BY generation DESC, revision`, c.reg.Service)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []InstanceStatus{}
	for rows.Next() {
		var s InstanceStatus
		if err := rows.Scan(&s.Generation, &s.Revision, &s.Count, &s.SeenAt); err != nil {
			return nil, err
		}
		s.Newest = len(out) == 0 || s.Generation == out[0].Generation
		out = append(out, s)
	}
	return out, rows.Err()
}
