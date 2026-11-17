package loom

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// A command loom stores to run later — a timer (At/After), a batch item, a
// plan — rests with the schema version (@v) it was written under, in the
// command_version column beside it. When it is read to run, a version
// behind the command's current one is lifted through the command's
// declared upcasts first (`upcast X @from(n)`, as events are), so the
// handler only ever sees the current shape. Rows written before the column
// existed read as version 1: until @v arrived on commands, every command
// was version 1.

// commandShape is what a stored command's version is read against.
type commandShape struct {
	version int
	upcasts map[int]UpcastFunc
}

// lowest is the oldest version the declared upcasts lift to current.
func (s commandShape) lowest() int {
	low := s.version
	for v := s.version - 1; v >= 1 && s.upcasts[v] != nil; v-- {
		low = v
	}
	return low
}

func (c *Client) commandShape(cmdType string) (commandShape, bool) {
	norm := func(v int, ups map[int]UpcastFunc) commandShape {
		if v < 1 {
			v = 1
		}
		return commandShape{version: v, upcasts: ups}
	}
	for _, agg := range c.reg.Aggregates {
		for _, def := range agg.Commands {
			if def.Name == cmdType {
				return norm(def.SchemaVersion, def.Upcasts), true
			}
		}
	}
	for _, rec := range c.reg.Records {
		for _, def := range rec.Commands {
			if def.Name == cmdType {
				return norm(def.SchemaVersion, def.Upcasts), true
			}
		}
	}
	return commandShape{}, false
}

// commandVersion is the version a command is stored with now.
func (c *Client) commandVersion(cmdType string) int {
	if s, ok := c.commandShape(cmdType); ok {
		return s.version
	}
	return 1
}

// decodeStoredCommand decodes a command stored under version (0 reads as
// 1), lifting it through the declared upcasts when it is behind. raw is
// already opened (its @pii fields decrypted).
func (c *Client) decodeStoredCommand(cmdType string, version int, raw []byte) (Command, error) {
	shape, ok := c.commandShape(cmdType)
	if !ok {
		return c.decodeCommand(cmdType, raw)
	}
	if version < 1 {
		version = 1
	}
	if version > shape.version {
		return nil, &UpcastError{Type: cmdType, Stored: version, To: shape.version}
	}
	for v := version; v < shape.version; v++ {
		fn := shape.upcasts[v]
		if fn == nil {
			return nil, &UpcastError{Type: cmdType, Stored: version, At: v, To: shape.version}
		}
		out, err := fn(raw)
		if err != nil {
			return nil, &UpcastError{Type: cmdType, Stored: version, At: v, To: shape.version, Err: err}
		}
		raw = out
	}
	return c.decodeCommand(cmdType, raw)
}

// checkStoredCommands refuses a migration while any stored command — a
// timer, a pending batch item, an unexpired plan — holds a version of its
// command that no declared upcast brings forward: deployed, it would fail
// when it came due, days later. The remedy is to declare the hop. A
// stored version NEWER than this build's (a rollback) is left to fail
// loudly when read; refusing it here would block the rollback itself.
func (c *Client) checkStoredCommands(ctx context.Context) error {
	rows, err := c.db.Query(ctx, `
		SELECT command_type, command_version, count(*) FROM (
			-- a schedule's row is rewritten in the current shape at start
			SELECT command_type, command_version FROM loom_timers
			WHERE service=$1 AND key NOT LIKE $2
			UNION ALL
			SELECT command_type, command_version FROM loom_batch_items
			WHERE service=$1 AND status IN ('pending','working')
			UNION ALL
			SELECT command_type, command_version FROM loom_plans
			WHERE service=$1 AND expires_at > now()
		) stored
		GROUP BY command_type, command_version`, c.reg.Service, scheduleKeyPrefix+"%")
	if err != nil {
		return err
	}
	defer rows.Close()
	var stranded []string
	for rows.Next() {
		var (
			cmdType string
			version int
			n       int64
		)
		if err := rows.Scan(&cmdType, &version, &n); err != nil {
			return err
		}
		shape, ok := c.commandShape(cmdType)
		if !ok || version >= shape.lowest() {
			continue // a retry or a schedule's tick, or liftable
		}
		stranded = append(stranded, fmt.Sprintf("%s v%d (%d stored rows; the command is @v(%d) and no upcast @from(%d) brings it forward)",
			cmdType, version, n, shape.version, shape.lowest()-1))
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(stranded) == 0 {
		return nil
	}
	sort.Strings(stranded)
	return fmt.Errorf("loom: %s stores commands this build cannot run: %s — declare the upcast (`upcast X @from(n)`) and migrate again",
		c.reg.Service, strings.Join(stranded, "; "))
}
