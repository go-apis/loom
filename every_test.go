package loom

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
)

// listOf is a list read over rows held in memory, ordered as loom's own
// SQL orders: by updated_at (ascending, or descending for "-updated_at")
// and then id, paged past q.After, at most MaxPage rows. reads counts
// the queries, and between counts whatever write the test scripts.
type listOf struct {
	rows  map[string]Row
	reads int
	write map[int]func(l *listOf)
}

func (l *listOf) read(_ context.Context, q Query) ([]Row, error) {
	l.reads++
	if w := l.write[l.reads]; w != nil {
		w(l)
	}
	desc := strings.HasPrefix(q.OrderBy, "-")
	var all []Row
	for _, r := range l.rows {
		all = append(all, r)
	}
	less := func(a, b Row) bool {
		if !a.UpdatedAt.Equal(b.UpdatedAt) {
			return a.UpdatedAt.Before(b.UpdatedAt) != desc
		}
		return (a.ID < b.ID) != desc
	}
	sort.Slice(all, func(i, j int) bool { return less(all[i], all[j]) })
	var out []Row
	for _, r := range all {
		if q.After != nil {
			at, err := time.Parse(time.RFC3339Nano, q.After.Value)
			if err != nil {
				return nil, err
			}
			if !less(Row{ID: q.After.ID, UpdatedAt: at}, r) {
				continue
			}
		}
		if out = append(out, r); len(out) == clampLimit(q.Limit) {
			break
		}
	}
	return out, nil
}

func rowsAt(n int, at time.Time) *listOf {
	l := &listOf{rows: map[string]Row{}, write: map[int]func(*listOf){}}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("row-%04d", i)
		l.rows[id] = Row{ID: id, Namespace: "default", UpdatedAt: at}
	}
	return l
}

// Every reads a list longer than one page whole, each row once, though
// every row ties on the order column; a Limit over MaxPage does not end
// the walk at the first full page.
func TestEveryReadsPastOnePage(t *testing.T) {
	tie := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, limit := range []int{0, 100, MaxPage, 5000} {
		l := rowsAt(1230, tie)
		rows, err := Every(context.Background(), l.read, Query{Namespace: "default", Limit: limit})
		if err != nil {
			t.Fatal(err)
		}
		seen := map[string]int{}
		for _, r := range rows {
			seen[r.ID]++
		}
		if len(rows) != 1230 || len(seen) != 1230 {
			t.Fatalf("limit %d: %d rows, %d distinct, want 1230 of each", limit, len(rows), len(seen))
		}
		page := limit
		if page <= 0 || page > MaxPage {
			page = MaxPage
		}
		if want := 1230/page + 1; l.reads != want {
			t.Fatalf("limit %d: %d reads, want %d", limit, l.reads, want)
		}
	}
}

// A row written to while the pages are read moves ahead of the walk in
// Every's default order: it is met again, and answered once, as last
// read.
func TestEveryAnswersARowWrittenMidReadOnce(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	l := rowsAt(250, start)
	l.write[2] = func(l *listOf) { // before the second page: a row of the first page is written to
		r := l.rows["row-0003"]
		r.UpdatedAt = start.Add(time.Hour)
		r.Data = []byte(`{"n":2}`)
		l.rows["row-0003"] = r
	}
	rows, err := Every(context.Background(), l.read, Query{Namespace: "default", Limit: 100})
	if err != nil || len(rows) != 250 {
		t.Fatalf("%d rows, %v; want 250", len(rows), err)
	}
	if rows[3].ID != "row-0003" || string(rows[3].Data) != `{"n":2}` {
		t.Fatalf("the row written to is answered once, where it was first read, as last read: %+v", rows[3])
	}
}

// An error on any page answers no rows: never part of the list.
func TestEveryAnswersNothingOnAnError(t *testing.T) {
	l := rowsAt(250, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	boom := errors.New("the second page failed")
	read := func(ctx context.Context, q Query) ([]Row, error) {
		if q.After != nil {
			return nil, boom
		}
		return l.read(ctx, q)
	}
	rows, err := Every(context.Background(), read, Query{Limit: 100})
	if !errors.Is(err, boom) || rows != nil {
		t.Fatalf("%d rows, %v; want none and the page's error", len(rows), err)
	}
}

// Pages stops when its caller does, and names a cursor it cannot build:
// Fields that leave the OrderBy field out.
func TestPagesStopsAndNamesAMissingOrderField(t *testing.T) {
	l := rowsAt(250, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	for range Pages(context.Background(), l.read, Query{Limit: 100}) {
		break
	}
	if l.reads != 1 {
		t.Fatalf("%d reads after the caller stopped at the first page, want 1", l.reads)
	}
	for i := range 250 {
		id := fmt.Sprintf("row-%04d", i)
		r := l.rows[id]
		r.Data = []byte(`{"title":"x"}`)
		l.rows[id] = r
	}
	var last error
	for _, err := range Pages(context.Background(), l.read, Query{Limit: 100, OrderBy: "slug"}) {
		last = err
	}
	if last == nil || !strings.Contains(last.Error(), `field "slug" not in row`) {
		t.Fatalf("a page whose rows lack the order field: %v", last)
	}
}

// Newest first is read oldest first and answered in reverse, so a row
// written to mid-read is not lost behind the walk: it is answered once,
// first.
func TestEveryNewestFirstKeepsARowWrittenMidRead(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	l := &listOf{rows: map[string]Row{}, write: map[int]func(*listOf){}}
	for i := range 250 {
		id := fmt.Sprintf("row-%04d", i)
		l.rows[id] = Row{ID: id, Namespace: "default", UpdatedAt: start.Add(time.Duration(i) * time.Second)}
	}
	l.write[2] = func(l *listOf) { // a row the walk has not reached is written to
		r := l.rows["row-0200"]
		r.UpdatedAt = start.Add(time.Hour)
		l.rows["row-0200"] = r
	}
	rows, err := Every(context.Background(), l.read, Query{Namespace: "default", Limit: 100, OrderBy: "-updated_at"})
	if err != nil || len(rows) != 250 {
		t.Fatalf("%d rows, %v; want 250", len(rows), err)
	}
	if rows[0].ID != "row-0200" || rows[1].ID != "row-0249" || rows[249].ID != "row-0000" {
		t.Fatalf("newest first, the row written to at the top: %s, %s ... %s", rows[0].ID, rows[1].ID, rows[249].ID)
	}
}
