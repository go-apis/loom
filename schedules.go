package loom

import (
	"context"
	"encoding/json"
	"strings"
	"time"
)

// A schedule is a command the service dispatches every so often for as
// long as it runs (schema `schedule`, ScheduleDef). It rides the timer
// table: one row per schedule, keyed by its name, that the timer runner
// claims and fires like any timer — on the newest generation alive, one
// instance at a time. What is different is what happens after it fires.
// A timer is spent. A schedule's row is moved on to its next time,
// whether the command succeeded or not: nothing the tick causes arms
// the next one, so a tick whose handler refuses, whose reaction fails
// or whose process is not deployed does not end the series. That is
// the difference from a command that re-arms itself with loom.After,
// which stops for good the first time its chain breaks.
//
// A schedule that was not running — the service was down, the timer
// runner was behind — fires once when it can and then every interval
// from there. Missed ticks are not made up.

// scheduleKeyPrefix marks a loom_timers row as a schedule's.
const scheduleKeyPrefix = "loom:schedule:"

// scheduleEnsure is how often the newest generation re-reads the
// registry's schedules into the timer table.
const scheduleEnsure = time.Minute

func (c *Client) scheduleDef(key string) *ScheduleDef {
	name := strings.TrimPrefix(key, scheduleKeyPrefix)
	for _, s := range c.reg.Schedules {
		if s.Name == name {
			return s
		}
	}
	return nil
}

// ensureSchedules makes the timer table hold exactly the registry's
// schedules. A new one gets a row due now: its first tick is the
// deploy's. One whose command changed gets the new command. One whose
// interval shrank is pulled in to no later than one interval from now
// (plus the claim's lease, so a row being fired is left alone). One the
// schema no longer declares loses its row. Only the newest generation
// alive does this: an older build's registry is not the schedule.
func (c *Client) ensureSchedules(ctx context.Context) error {
	keys := make([]string, 0, len(c.reg.Schedules))
	for _, s := range c.reg.Schedules {
		cmd := s.New()
		raw, err := json.Marshal(cmd)
		if err != nil {
			return err
		}
		ns, _ := cmd.CommandTarget()
		key := scheduleKeyPrefix + s.Name
		keys = append(keys, key)
		_, err = c.db.Exec(ctx, `
			INSERT INTO loom_timers (service, namespace, key, command_type, command, meta, fire_at)
			VALUES ($1,$2,$3,$4,$5,'{}', now())
			ON CONFLICT (service, key) DO UPDATE SET
				namespace = EXCLUDED.namespace,
				command_type = EXCLUDED.command_type,
				command = EXCLUDED.command,
				fire_at = LEAST(loom_timers.fire_at, now() + make_interval(secs => $6))`,
			c.reg.Service, ns, key, cmd.LoomCommand(), raw, (s.Every + timerLease).Seconds())
		if err != nil {
			return err
		}
	}
	_, err := c.db.Exec(ctx, `
		DELETE FROM loom_timers
		WHERE service=$1 AND key LIKE $2 AND NOT (key = ANY($3))`,
		c.reg.Service, scheduleKeyPrefix+"%", keys)
	return err
}

// fireSchedule fires one claimed schedule row and moves it on. The
// command's failure is logged and counted, not parked: the next tick is
// the retry, and a dead letter per failed tick would bury the ones that
// need a person.
func (c *Client) fireSchedule(ctx context.Context, d dueTimer) error {
	def := c.scheduleDef(d.key)
	if def == nil {
		// not in this build's schema: ensureSchedules removes it
		return nil
	}
	meta, _ := json.Marshal(Metadata{Actor: "schedule:" + def.Name})
	if err := c.fireTimer(ctx, d.key, d.cmdType, d.cmd, meta); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.log.ErrorContext(ctx, "scheduled command failed; the next tick tries again",
			"schedule", def.Name, "command", d.cmdType, "every", def.Every.String(), "error", err)
	}
	// only if it is still the row the claim leased: ensureSchedules may
	// have pulled it in meanwhile, and that time stands
	_, err := c.db.Exec(ctx, `
		UPDATE loom_timers SET fire_at = now() + make_interval(secs => $4)
		WHERE service=$1 AND key=$2 AND fire_at=$3`,
		c.reg.Service, d.key, d.leasedAt, def.Every.Seconds())
	return err
}

// ScheduleStatus is one schedule and when it next fires.
type ScheduleStatus struct {
	Name    string `json:"name"`
	Command string `json:"command"`
	// Every is the interval, in seconds.
	Every float64 `json:"every_seconds"`
	// NextAt is when it is next due; nil for a schedule with no row yet
	// (no instance of the newest generation has started). A row being
	// fired shows its claim's lease, a few minutes out, until it lands.
	NextAt *time.Time `json:"next_at,omitempty"`
}

func (c *Client) schedules(ctx context.Context) ([]ScheduleStatus, error) {
	out := []ScheduleStatus{}
	if len(c.reg.Schedules) == 0 {
		return out, nil
	}
	next := map[string]time.Time{}
	rows, err := c.db.Query(ctx, `SELECT key, fire_at FROM loom_timers WHERE service=$1 AND key LIKE $2`, c.reg.Service, scheduleKeyPrefix+"%")
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var key string
		var at time.Time
		if err := rows.Scan(&key, &at); err != nil {
			rows.Close()
			return nil, err
		}
		next[strings.TrimPrefix(key, scheduleKeyPrefix)] = at
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, s := range c.reg.Schedules {
		st := ScheduleStatus{Name: s.Name, Command: s.Command, Every: s.Every.Seconds()}
		if at, ok := next[s.Name]; ok {
			st.NextAt = &at
		}
		out = append(out, st)
	}
	return out, nil
}
