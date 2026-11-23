package loom

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

// TestCheckpointProbeUsesTheIndex holds probeCheckpoint's statement to
// an index lookup on a log big enough for the planner to weigh a
// sequential scan. v0.132.0's EXISTS form planned a Seq Scan on
// loom_events with real parameters, and for a runner with nothing new
// that scan read the whole log on every look (3.6 GB a time in
// runsheet's production log, 23 Nov 2026). 200k events are enough to
// reproduce that plan on Postgres 17; the probe must plan, and answer,
// from the (service, global_seq) index whether the runner is caught up,
// behind, or has no row, in a custom plan and in a generic one.
func TestCheckpointProbeUsesTheIndex(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	pool := checkpointPG(t, nil)
	cli, err := New(Config{DB: pool, Registry: spreadRegistry(&spreadHandled{})})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	// most of the log is this service's, as in a single-service
	// database, with another service's events among it
	const events = 200_000
	if _, err := pool.Exec(ctx, `
		INSERT INTO loom_events (service, namespace, aggregate_type, aggregate_id, version, type, data)
		SELECT CASE WHEN g % 50 = 0 THEN 'other' ELSE 'spread' END, 'ns', 'Agg', gen_random_uuid(), 1,
			'T' || lpad((g % 20)::text, 2, '0'), '{}'
		FROM generate_series(1, $1::int) g`, events); err != nil {
		t.Fatal(err)
	}
	var head int64
	if err := pool.QueryRow(ctx, `SELECT max(global_seq) FROM loom_events WHERE service='spread'`).Scan(&head); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO loom_checkpoints (service, runner, global_seq)
		SELECT 'spread', 'process:R' || i, CASE WHEN i = 2 THEN $1::bigint - 1000 ELSE $1::bigint END
		FROM generate_series(1, 300) i`, head); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `ANALYZE loom_events; ANALYZE loom_checkpoints`); err != nil {
		t.Fatal(err)
	}

	// the statement prepared by name, as pgx's statement cache does in
	// production, explained under a custom plan (the values known, what
	// v0.132.0 seq-scanned under) and a generic one
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `PREPARE ckprobe(text, text, bigint, bigint, bool) AS `+probeCheckpointSQL); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name   string
		runner string
		known  bool  // the runner knows its row
		seq    int64 // its position in memory
		more   bool
	}{
		{"caught up, row unknown", "process:R1", false, 0, false},
		{"caught up, row known", "process:R1", true, head, false},
		{"behind", "process:R2", false, 0, true},
		{"no row", "process:none", false, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pos := newRunnerPos(tc.runner, nil)
			knew := int64(-1)
			if tc.known {
				pos.adopt(tc.seq, true)
				knew = tc.seq
			}
			for _, mode := range []string{"force_custom_plan", "force_generic_plan"} {
				if _, err := conn.Exec(ctx, `SET plan_cache_mode = `+mode); err != nil {
					t.Fatal(err)
				}
				var plan []byte
				// EXECUTE takes no bind parameters: the test's own values, inline
				explain := fmt.Sprintf(`EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE ckprobe('spread', '%s', %d, %d, true)`,
					tc.runner, pos.seq, knew)
				if err := conn.QueryRow(ctx, explain).Scan(&plan); err != nil {
					t.Fatal(err)
				}
				var doc []struct {
					Plan planNode `json:"Plan"`
				}
				if err := json.Unmarshal(plan, &doc); err != nil {
					t.Fatal(err)
				}
				if n := doc[0].Plan.seqScans("loom_events"); n > 0 {
					t.Fatalf("%s: the probe plans a Seq Scan on loom_events:\n%s", mode, plan)
				}
				// an index lookup touches a handful of pages; the log
				// is over a thousand
				if pages := doc[0].Plan.SharedHit + doc[0].Plan.SharedRead; pages > 50 {
					t.Fatalf("%s: the probe touched %d pages, want an index lookup:\n%s", mode, pages, plan)
				}
			}
			_, _, _, more, err := cli.probeCheckpoint(ctx, pool, pos, true)
			if err != nil {
				t.Fatal(err)
			}
			if more != tc.more {
				t.Fatalf("more = %v, want %v", more, tc.more)
			}
		})
	}
}

// planNode is the part of an EXPLAIN (FORMAT JSON) node the plan tests
// read.
type planNode struct {
	NodeType     string     `json:"Node Type"`
	RelationName string     `json:"Relation Name"`
	SharedHit    int        `json:"Shared Hit Blocks"`
	SharedRead   int        `json:"Shared Read Blocks"`
	Plans        []planNode `json:"Plans"`
}

// seqScans counts the Seq Scan nodes on rel in the plan.
func (n planNode) seqScans(rel string) int {
	c := 0
	if n.NodeType == "Seq Scan" && n.RelationName == rel {
		c++
	}
	for _, p := range n.Plans {
		c += p.seqScans(rel)
	}
	return c
}
