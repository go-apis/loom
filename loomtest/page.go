package loomtest

import "github.com/go-apis/loom"

// Page is the page a fake list read answers for q. rows is every row
// the read matches, in the order the fake keeps them; the page is the
// rows after the one q.After names by id, from the top for a nil After,
// and at most q.Limit of them (loom.MaxPage when Limit is zero, less, or
// more). An After that names no row of the list answers no rows: the
// walk is past it. It does not filter or sort: a fake that honours
// q.Filters or q.OrderBy does so before it calls Page.
//
// It stands where a fake sliced by Query.Offset, so a test of code that
// walks a list with loom.Every or loom.Pages pages as the store does.
func Page(rows []loom.Row, q loom.Query) []loom.Row {
	if q.After != nil {
		at := -1
		for i, r := range rows {
			if r.ID == q.After.ID {
				at = i
				break
			}
		}
		if at < 0 {
			return nil
		}
		rows = rows[at+1:]
	}
	limit := q.Limit
	if limit <= 0 || limit > loom.MaxPage {
		limit = loom.MaxPage
	}
	if len(rows) > limit {
		rows = rows[:limit]
	}
	return append([]loom.Row(nil), rows...)
}
