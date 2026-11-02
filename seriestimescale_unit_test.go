package loom

import (
	"strings"
	"testing"
	"time"
)

// pickRollup answers from the coarsest existing rollup that gives the raw
// table's exact answer, and from the raw table otherwise (ADR 0011).
func TestPickRollup(t *testing.T) {
	def := &SeriesDef{
		Table: "loom_s_shop_price", Time: "at", Dims: []string{"product", "grade"},
		Rollups: []string{"hour", "day"},
		Columns: []TableColumn{
			{Name: "product", Type: "text"}, {Name: "grade", Type: "text"},
			{Name: "at", Type: "timestamptz"}, {Name: "cents", Type: "bigint"},
			{Name: "note", Type: "text"},
		},
	}
	hour, day := "loom_r_shop_price_hour", "loom_r_shop_price_day"
	both := map[string]bool{hour: true, day: true}
	midnight := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	q := func(mod func(*SeriesBucketQuery)) SeriesBucketQuery {
		x := SeriesBucketQuery{Namespace: "n", Value: "cents", Bucket: "month"}
		if mod != nil {
			mod(&x)
		}
		return x
	}
	for name, c := range map[string]struct {
		q     SeriesBucketQuery
		views map[string]bool
		want  string
	}{
		"coarsest":            {q(nil), both, day},
		"hour bucket":         {q(func(x *SeriesBucketQuery) { x.Bucket = "hour" }), both, hour},
		"all":                 {q(func(x *SeriesBucketQuery) { x.Bucket = "all" }), both, day},
		"day view missing":    {q(nil), map[string]bool{hour: true}, hour},
		"none exist":          {q(nil), nil, ""},
		"utc off (no views)":  {q(nil), nil, ""},
		"percentiles":         {q(func(x *SeriesBucketQuery) { x.Percentiles = []float64{50} }), both, ""},
		"raw":                 {q(func(x *SeriesBucketQuery) { x.Raw = true }), both, ""},
		"dim filter":          {q(func(x *SeriesBucketQuery) { x.Filters = []Filter{{Field: "grade", Value: "psa10"}} }), both, day},
		"non-dim filter":      {q(func(x *SeriesBucketQuery) { x.Filters = []Filter{{Field: "cents", Op: "gt", Value: "5"}} }), both, ""},
		"since on midnight":   {q(func(x *SeriesBucketQuery) { x.Since = midnight }), both, day},
		"since mid-day":       {q(func(x *SeriesBucketQuery) { x.Since = midnight.Add(5 * time.Hour) }), both, hour},
		"until mid-hour":      {q(func(x *SeriesBucketQuery) { x.Until = midnight.Add(90 * time.Minute) }), both, ""},
		"value not rolled up": {q(func(x *SeriesBucketQuery) { x.Value = "note" }), both, ""},
	} {
		if got := pickRollup(def, c.q, c.views); got != c.want {
			t.Errorf("%s: pickRollup = %q, want %q", name, got, c.want)
		}
	}

	keyed := *def
	keyed.Keys = []string{"listing"}
	if got := pickRollup(&keyed, q(nil), both); got != "" {
		t.Errorf("@key series: pickRollup = %q, want raw", got)
	}
}

// The rollup's definition aggregates every numeric non-dim column and
// groups by every dim; its hash changes when the shape does.
func TestRollupSQL(t *testing.T) {
	def := &SeriesDef{
		Table: "loom_s_shop_price", Time: "at", Dims: []string{"product"},
		Columns: []TableColumn{{Name: "product", Type: "text"}, {Name: "at", Type: "timestamptz"}, {Name: "cents", Type: "bigint"}},
	}
	sql := rollupSQL(def, "day")
	for _, want := range []string{
		`time_bucket(INTERVAL '1 day', "at") AS loom_bt`,
		`last("cents", "at")::float8 AS "cents__last"`,
		`GROUP BY service, namespace, loom_bt, "product"`,
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("rollupSQL missing %s:\n%s", want, sql)
		}
	}
	h := rollupHash(def, "day")
	def.Columns = append(def.Columns, TableColumn{Name: "qty", Type: "double precision"})
	if rollupHash(def, "day") == h {
		t.Errorf("a new numeric column left the rollup hash unchanged")
	}
}
