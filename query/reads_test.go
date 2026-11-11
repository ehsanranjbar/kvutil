package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
)

// TestNormalisedReads checks that the conditions that the planner builds from the ones of a
// query read what those did, so that a normalised query is not decoded in full.
func TestNormalisedReads(t *testing.T) {
	// Reading changes the function it is called on, so each use makes one.
	odd := func() *cond.Predicate[rec] { return cond.Func("odd", func(r *rec) bool { return r.S == "x" }) }
	// A field of a function whose reads are not known, with the path of fn.
	hidden := field.Of("N", func(r *rec) int {
		if r.N == nil {
			return 0
		}
		return *r.N
	})
	declared := hidden.Reading("N", "M")

	for _, test := range []struct {
		name  string
		conds []cond.Cond[rec]
		count int // the conditions that are left
		want  []string
		known bool
	}{
		{"a comparison", conds(fn.Gte(1)), 1, []string{"N"}, true},
		{"comparisons of one field are merged", conds(fn.Gte(1), fn.Lt(5)), 1, []string{"N"}, true},
		{"equal values are merged", conds(fn.In(1, 2, 3), fn.In(2, 3, 4)), 1, []string{"N"}, true},
		{"fields", conds(fn.Gte(1), fs.Eq("a"), fn.Lt(5), fm.Gt(0)), 3, []string{"M", "N", "S"}, true},
		{"a conjunction", conds(cond.And(fn.Gte(1), fs.Eq("a"), fn.Lt(5))), 2, []string{"N", "S"}, true},
		{"a negation", conds(cond.Not(fn.Gte(1)), fs.Eq("a")), 2, []string{"N", "S"}, true},
		{"a disjunction", conds(cond.Or(fn.Eq(1), fs.Eq("a"))), 1, []string{"N", "S"}, true},
		{"a disjunction is an In", conds(cond.Or(fn.Eq(1), fn.Eq(2), fn.In(5, 6))), 1, []string{"N"}, true},
		{"a nested condition", conds(cond.Or(cond.And(fn.Gt(1), fn.Lt(5)), cond.Not(cond.Or(fs.Eq("a"), fm.Eq(2))))), 1,
			[]string{"M", "N", "S"}, true},
		{"a multiple field", conds(ftags.Eq("a"), ftags.Gt("b")), 2, []string{"Tags"}, true},
		{"a function that says", conds(odd().Reading("S"), fn.Gt(1)), 2, []string{"N", "S"}, true},
		{"a function that reads nothing", conds(odd().Reading(), fn.Gt(1)), 2, []string{"N"}, true},
		{"a field that says", conds(declared.Gt(1), declared.Lt(5)), 1, []string{"M", "N"}, true},

		{"a function", conds(odd(), fn.Gt(1)), 2, nil, false},
		{"a function in a negation", conds(cond.Not(odd()), fn.Gt(1)), 2, nil, false},
		{"a field that does not say", conds(hidden.Gt(1), hidden.Lt(5)), 1, nil, false},
		{"fields that do and that do not", conds(hidden.Gt(1), fn.Lt(5)), 2, nil, false},
		{"a disjunction of fields that do and that do not", conds(cond.Or(hidden.Eq(1), fn.Eq(2))), 1, nil, false},
		{"a disjunction of fields that do not", conds(cond.Or(hidden.Eq(1), hidden.Eq(2))), 1, nil, false},
		{"a condition that is not a reader", conds(fn.Gt(1), notReader{}), 2, nil, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			normal, empty := normalise(test.conds)
			require.False(t, empty)
			require.Len(t, normal, test.count)

			paths, ok := cond.Reads(normal...)
			require.Equal(t, test.known, ok)
			if test.known {
				require.Equal(t, test.want, paths)
			} else {
				require.Nil(t, paths)
			}
		})
	}

	// Comparisons that are not known to read the same are not merged: the merged one would read
	// what only one of them does.
	normal, _ := normalise(conds(hidden.Gt(1), declared.Lt(5)))
	require.Len(t, normal, 2)
	_, ok := cond.Reads(normal...)
	require.False(t, ok)
	normal, _ = normalise(conds(cond.Or(hidden.Eq(1), declared.Eq(2))))
	require.Len(t, normal, 1)
	_, ok = cond.Reads(normal...)
	require.False(t, ok)
	normal, _ = normalise(conds(fn.Gt(1), fn.Reading("N", "M").Lt(5)))
	require.Len(t, normal, 2)
}

// notReader is a condition that does not say what it reads.
type notReader struct{}

func (notReader) Match(*rec) (bool, error) { return true, nil }
func (notReader) String() string           { return "not a reader" }
