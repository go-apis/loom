package graphql

import "testing"

func TestQueryDepth(t *testing.T) {
	cases := []struct {
		name  string
		query string
		want  int
	}{
		{"flat", `{ a b c }`, 1},
		{"nested", `{ a { b { c } } }`, 3},
		{"siblings take the max", `{ a { b } c { d { e { f } } } }`, 4},
		{"inline fragment adds no depth", `{ a { ... on T { b { c } } } }`, 3},
		{"fragment spread follows the fragment", `{ a { ...f } } fragment f on T { b { c } }`, 3},
		{"cyclic fragments do not hang", `{ a { ...f } } fragment f on T { b { ...g } } fragment g on T { c { ...f } }`, 3},
		{"multiple operations take the max", `query One { a } query Two { a { b } }`, 2},
		{"unparsable reports zero", `{ a `, 0},
	}
	for _, tc := range cases {
		if got := queryDepth(tc.query); got != tc.want {
			t.Errorf("%s: depth %d, want %d", tc.name, got, tc.want)
		}
	}
}
