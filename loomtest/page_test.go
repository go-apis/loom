package loomtest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-apis/loom"
)

// A fake read that answers Page is walked whole by loom.Every, a page
// at a time, each row once; and an After naming no row ends the walk.
func TestPageIsWalkedByEvery(t *testing.T) {
	var held []loom.Row
	for i := range 1230 {
		held = append(held, loom.Row{ID: fmt.Sprintf("row-%04d", i), Namespace: "default", UpdatedAt: time.Unix(int64(i), 0)})
	}
	reads := 0
	read := func(_ context.Context, q loom.Query) ([]loom.Row, error) {
		reads++
		return Page(held, q), nil
	}
	rows, err := loom.Every(context.Background(), read, loom.Query{Limit: 100})
	if err != nil || len(rows) != len(held) || reads != 13 {
		t.Fatalf("%d rows in %d reads, %v; want %d rows in 13", len(rows), reads, err, len(held))
	}
	for i, r := range rows {
		if r.ID != held[i].ID {
			t.Fatalf("row %d is %s, want %s", i, r.ID, held[i].ID)
		}
	}
	if got := Page(held, loom.Query{}); len(got) != loom.MaxPage {
		t.Fatalf("no Limit answers %d rows, want loom.MaxPage", len(got))
	}
	if got := Page(held, loom.Query{After: &loom.Cursor{ID: "gone"}}); got != nil {
		t.Fatalf("an After that names no row answers %d rows, want none", len(got))
	}
}
