package loom

import (
	"context"
	"fmt"
	"iter"
)

// ListRead is one list read of an entity or a record: a query in, a
// page of rows out. Client.QueryEntities and Client.QueryRecords bound
// to a name (Client.ListEntities, Client.ListRecords) are each one, and
// so is whatever a service wraps them in.
type ListRead func(ctx context.Context, q Query) ([]Row, error)

// ListEntities is the list read of an entity, for Every and Pages.
func (c *Client) ListEntities(entity string) ListRead {
	return func(ctx context.Context, q Query) ([]Row, error) { return c.QueryEntities(ctx, entity, q) }
}

// ListRecords is the list read of a record, for Every and Pages.
func (c *Client) ListRecords(record string) ListRead {
	return func(ctx context.Context, q Query) ([]Row, error) { return c.QueryRecords(ctx, record, q) }
}

// MaxPage is the most rows one list read answers, whatever its Limit
// asks.
const MaxPage = 500

// Pages walks a list read to its end, a page at a time: q as given, then
// q again with After set to the cursor of the page's last row, until a
// page comes back short. A Limit of zero or less, or over MaxPage, reads
// MaxPage rows a page. q.After, when set, is where the walk starts.
//
// The order is q.OrderBy and then id, so rows that tie are each read
// once. A row that changes its place in the order while the walk runs
// is read where it stands when its page is read: one that moves ahead of
// the walk is read again, and one that moves behind it is not read at
// all. To read every row whatever is written meanwhile, order by a field
// that does not change, or ascending by updated_at (Every's default),
// where a row written to moves ahead.
//
// q.Fields, when set, must name the OrderBy field: the cursor is built
// from the row.
//
// The sequence ends at the first error, which it yields with a nil page.
func Pages(ctx context.Context, read ListRead, q Query) iter.Seq2[[]Row, error] {
	return func(yield func([]Row, error) bool) {
		if q.Limit <= 0 || q.Limit > MaxPage {
			q.Limit = MaxPage
		}
		for {
			rows, err := read(ctx, q)
			if err != nil {
				yield(nil, err)
				return
			}
			if len(rows) > 0 && !yield(rows, nil) {
				return
			}
			if len(rows) < q.Limit {
				return
			}
			after, err := rows[len(rows)-1].Cursor(q.OrderBy)
			if err != nil {
				yield(nil, fmt.Errorf("loom: pages: %w", err))
				return
			}
			q.After = &after
		}
	}
}

// Every is every row a list read answers for q, however many pages that
// takes, or an error and no rows: never part of the list. With no
// OrderBy it reads ascending by updated_at, so a row written to while
// the pages are read is met again further on, not lost; a row met twice
// is answered once, as it was last read, in the place it was first read.
func Every(ctx context.Context, read ListRead, q Query) ([]Row, error) {
	if q.OrderBy == "" {
		q.OrderBy = "updated_at"
	}
	var out []Row
	at := map[string]int{}
	for rows, err := range Pages(ctx, read, q) {
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			if i, seen := at[r.Namespace+"/"+r.ID]; seen {
				out[i] = r
				continue
			}
			at[r.Namespace+"/"+r.ID] = len(out)
			out = append(out, r)
		}
	}
	return out, nil
}
