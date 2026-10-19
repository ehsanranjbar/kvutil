package query

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
)

type rec struct {
	N    *int
	M    *int
	S    string
	Tags []string
	Flag bool
}

var (
	fn    = field.Path[rec, int]("N")
	fm    = field.Path[rec, int]("M")
	fs    = field.Path[rec, string]("S")
	ftags = field.Path[rec, string]("Tags")
	fflag = field.Path[rec, bool]("Flag")
	// fn32 is the field N encoded as a 32-bit integer, unlike fn.
	fn32 = field.Of("N", func(r *rec) int32 {
		if r.N == nil {
			return 0
		}
		return int32(*r.N)
	})
)

func conds(cs ...cond.Cond[rec]) []cond.Cond[rec] { return cs }

func TestImplication(t *testing.T) {
	var (
		isTrue  = cond.And[rec]()
		isFalse = cond.Or[rec]()
		even    = cond.Func("even", func(r *rec) bool { return r.N != nil && *r.N%2 == 0 })
		odd     = cond.Func("odd", func(r *rec) bool { return r.N != nil && *r.N%2 == 1 })
	)

	for _, test := range []struct {
		name    string
		queries []cond.Cond[rec]
		index   cond.Cond[rec]
		want    bool
	}{
		// Equality and values.
		{"a value is itself", conds(fn.Eq(5)), fn.Eq(5), true},
		{"a value is another", conds(fn.Eq(5)), fn.Eq(6), false},
		{"a value is among values", conds(fn.Eq(5)), fn.In(4, 5, 6), true},
		{"a value is not among values", conds(fn.Eq(5)), fn.In(4, 6), false},
		{"values are among values", conds(fn.In(5, 6)), fn.In(4, 5, 6, 7), true},
		{"values are not all among values", conds(fn.In(5, 8)), fn.In(4, 5, 6, 7), false},
		{"values are not a value", conds(fn.In(5, 6)), fn.Eq(5), false},
		{"values of one", conds(fn.In(5)), fn.Eq(5), true},
		// The same path with another encoding: the encoded values cannot be compared.
		{"a value of another encoding", conds(fn32.Eq(5)), fn.Eq(5), false},
		{"a range of another encoding", conds(fn32.Gte(5)), fn.Gte(3), false},
		{"a negation of another encoding", conds(cond.Not[rec](fn32.Eq(5))), cond.Not[rec](fn.Eq(5)), false},

		// Negations, by the contrapositive.
		{"a negation is itself", conds(cond.Not[rec](fn.Eq(5))), cond.Not[rec](fn.Eq(5)), true},
		{"not a wider range is not a narrower one", conds(cond.Not[rec](fn.Gte(3))), cond.Not[rec](fn.Gte(5)), true},
		{"not a narrower range is not a wider one", conds(cond.Not[rec](fn.Gte(5))), cond.Not[rec](fn.Gte(3)), false},
		{"not values is not one of them", conds(cond.Not[rec](fn.In(4, 5))), cond.Not[rec](fn.Eq(5)), true},
		{"not a value is not values", conds(cond.Not[rec](fn.Eq(5))), cond.Not[rec](fn.In(4, 5)), false},
		{"not a function is itself", conds(cond.Not(even)), cond.Not(even), true},
		{"a negation is not what it negates", conds(cond.Not[rec](fn.Eq(5))), fn.Eq(5), false},

		// Values and ranges.
		{"a value is in a range", conds(fn.Eq(5)), fn.Gte(5), true},
		{"a value is in the range above", conds(fn.Eq(5)), fn.Gt(4), true},
		{"a value is not in an exclusive range", conds(fn.Eq(5)), fn.Gt(5), false},
		{"a value is in a range of two sides", conds(fn.Eq(5)), fn.Between(1, 5), true},
		{"a value is not in the range below", conds(fn.Eq(5)), fn.Lt(5), false},
		{"values are in a range", conds(fn.In(3, 4, 5)), fn.Between(3, 5), true},
		{"values are not all in a range", conds(fn.In(3, 4, 6)), fn.Between(3, 5), false},
		{"a range is not a value", conds(fn.Between(5, 5)), fn.Eq(5), false},

		// Ranges.
		{"a range is itself", conds(fn.Gte(3)), fn.Gte(3), true},
		{"a range is inside the range that starts sooner", conds(fn.Gte(5)), fn.Gte(3), true},
		{"a range is not inside the range that starts later", conds(fn.Gte(3)), fn.Gte(5), false},
		{"an exclusive start is inside an inclusive one", conds(fn.Gt(3)), fn.Gte(3), true},
		{"an inclusive start is not inside an exclusive one", conds(fn.Gte(3)), fn.Gt(3), false},
		{"an exclusive start is inside an exclusive one", conds(fn.Gt(3)), fn.Gt(3), true},
		{"an exclusive end is inside an inclusive one", conds(fn.Lt(5)), fn.Lte(5), true},
		{"an inclusive end is not inside an exclusive one", conds(fn.Lte(5)), fn.Lt(5), false},
		{"a range that ends is inside the range that ends later", conds(fn.Lte(5)), fn.Lt(9), true},
		{"a range that does not end is not inside one that does", conds(fn.Gte(5)), fn.Between(3, 9), false},
		{"a range that does not start is not inside one that does", conds(fn.Lte(5)), fn.Between(3, 9), false},
		{"a range of two sides is inside a wider one", conds(fn.Between(5, 8)), fn.Between(3, 9), true},
		{"a range of two sides is not inside a narrower one", conds(fn.Between(3, 9)), fn.Between(4, 8), false},
		{"a range is inside the one that has no side", conds(fn.Gte(5)), fn.Range(kvutil.All[int]()), true},
		{"ranges of one field are one", conds(fn.Gte(3), fn.Lte(5)), fn.Between(2, 6), true},
		{"ranges of one field are one, not a wider one", conds(fn.Gte(3), fn.Lte(7)), fn.Between(2, 6), false},
		{"a value and a range of one field", conds(fn.In(1, 5, 9), fn.Gte(4)), fn.Between(5, 9), true},
		{"a range with the wrong sides is nothing", conds(fn.Gt(5), fn.Lt(3)), fn.Eq(1), false},
		// The values of the comparisons are the encoded ones: the next integer is not seen.
		{"an exclusive start is not the next value", conds(fn.Gt(5)), fn.Gte(6), false},

		// Other fields.
		{"another field", conds(fm.Eq(5)), fn.Eq(5), false},
		{"another field of the conditions too", conds(fm.Eq(9), fn.Eq(5)), fn.Eq(5), true},
		{"text", conds(fs.Eq("a")), fs.In("a", "b"), true},
		{"text is not a number", conds(fs.Eq("a")), fn.Eq(5), false},
		{"booleans", conds(fflag.Eq(true)), fflag.Eq(true), true},
		{"booleans are not the others", conds(fflag.Eq(false)), fflag.Eq(true), false},

		// A field of several values is satisfied by one of its values.
		{"several values: a value among values", conds(ftags.Eq("a")), ftags.In("a", "b"), true},
		{"several values: a range inside a range", conds(ftags.Gte("b"), ftags.Lt("c")), ftags.Gte("a"), true},
		{"several values: not the other way", conds(ftags.Gte("a")), ftags.Gte("b"), false},
		{"several values: each condition is its own", conds(ftags.Eq("a"), ftags.Eq("b")), ftags.Eq("b"), true},

		// Conjunctions of the index.
		{"every part is implied", conds(fn.Eq(5), fs.Eq("a")), cond.And[rec](fn.Gte(3), fs.Eq("a")), true},
		{"a part that is not implied", conds(fn.Eq(5)), cond.And[rec](fn.Gte(3), fs.Eq("a")), false},
		{"parts that are implied by one condition each", conds(cond.And[rec](fn.Eq(5), fs.Eq("a"))),
			cond.And[rec](fn.Gte(3), fs.Eq("a")), true},
		{"ranges of the index are one", conds(fn.Between(4, 5)), cond.And[rec](fn.Gte(3), fn.Lte(6)), true},
		{"ranges of the index are one, not a wider one", conds(fn.Between(4, 7)), cond.And[rec](fn.Gte(3), fn.Lte(6)), false},
		{"values and ranges of the index", conds(fn.Eq(5)), cond.And[rec](fn.In(4, 5, 6), fn.Gte(5)), true},
		{"a condition that is true", nil, isTrue, true},
		{"a condition that is true, whatever the conditions", conds(fn.Eq(5)), isTrue, true},
		{"a part that is true", conds(fn.Eq(5)), cond.And[rec](fn.Eq(5), isTrue), true},
		{"a condition that is false", conds(fn.Eq(5)), isFalse, false},
		{"a condition that nothing satisfies", conds(fn.Eq(5)), cond.And[rec](fn.Eq(5), fn.Eq(6)), false},
		{"conditions that are true or false whatever the record", conds(fn.Eq(5), isTrue), fn.Eq(5), true},

		// Disjunctions of the index.
		{"a branch is implied", conds(fn.Eq(2)), cond.Or[rec](fn.Eq(1), fn.Eq(2)), true},
		{"no branch is implied", conds(fn.Eq(3)), cond.Or[rec](fn.Eq(1), fn.Eq(2)), false},
		{"values that are among the branches are not a branch", conds(fn.In(1, 2)), cond.Or[rec](fn.Eq(1), fn.Eq(2)), false},
		{"a branch of other fields", conds(fs.Eq("a")), cond.Or[rec](fn.Eq(1), fs.Eq("a")), true},
		{"a branch that is a conjunction", conds(fn.Eq(1), fs.Eq("a")), cond.Or[rec](cond.And[rec](fn.Eq(1), fs.Eq("a")), fm.Eq(3)), true},
		{"a branch that is a conjunction, partly", conds(fn.Eq(1)), cond.Or[rec](cond.And[rec](fn.Eq(1), fs.Eq("a")), fm.Eq(3)), false},
		{"a disjunction and a part", conds(fs.Eq("a")), cond.And[rec](cond.Or[rec](fn.Eq(1), fs.Eq("a")), fs.Gte("a")), true},

		// Disjunctions of the conditions.
		{"every branch implies it", conds(cond.Or[rec](fn.Eq(1), fn.Eq(2))), fn.In(1, 2, 3), true},
		{"a branch does not imply it", conds(cond.Or[rec](fn.Eq(1), fn.Eq(4))), fn.In(1, 2, 3), false},
		{"every branch says the same", conds(cond.Or[rec](cond.And[rec](fflag.Eq(true), fn.Eq(1)), cond.And[rec](fflag.Eq(true), fm.Eq(2)))),
			fflag.Eq(true), true},
		{"a branch says another", conds(cond.Or[rec](cond.And[rec](fflag.Eq(true), fn.Eq(1)), fm.Eq(2))), fflag.Eq(true), false},
		{"a branch implies it with the others", conds(fflag.Eq(true), cond.Or[rec](fn.Eq(1), fn.Eq(2))), cond.And[rec](fflag.Eq(true), fn.Lt(3)), true},
		{"a disjunction in a disjunction", conds(cond.Or[rec](cond.Or[rec](fn.Eq(1), fn.Eq(2)), fn.Eq(3))), fn.Between(1, 3), true},
		{"a disjunction does not imply a branch", conds(cond.Or[rec](fn.Eq(1), fn.Eq(2))), fn.Eq(1), false},
		{"nothing for no branch", conds(isFalse), fn.Eq(1), false},

		// Anything else is implied by the identical condition.
		{"a function is itself", conds(even), even, true},
		{"a function is not another", conds(even), odd, false},
		{"a function is not a comparison", conds(even), fn.Eq(2), false},
		{"a comparison is not a function", conds(fn.Eq(2)), even, false},
		{"a function among conditions", conds(fn.Eq(2), even, fs.Eq("a")), cond.And[rec](even, fs.Eq("a")), true},
		{"a negation is itself", conds(cond.Not[rec](fflag.Eq(true))), cond.Not[rec](fflag.Eq(true)), true},
		{"a negation is not its opposite", conds(fflag.Eq(false)), cond.Not[rec](fflag.Eq(true)), false},
		{"a negation is not the opposite", conds(cond.Not[rec](fflag.Eq(true))), fflag.Eq(false), false},
		{"a negation is not another", conds(cond.Not[rec](fn.Eq(1))), cond.Not[rec](fn.Eq(2)), false},
		{"a negation in a disjunction", conds(cond.Not[rec](fflag.Eq(true))), cond.Or[rec](fn.Eq(1), cond.Not[rec](fflag.Eq(true))), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			// The planner has the conditions of the query normalised.
			normal, empty := normalise(test.queries)
			require.Equal(t, test.want, !empty && implies(normal, test.index), "%v => %v", test.queries, test.index)
		})
	}
}

// TestImplicationOnTheNormalisedConditions checks that the conditions of a query are used as the
// planner has them, normalised.
func TestImplicationOnTheNormalisedConditions(t *testing.T) {
	queries, empty := normalise(conds(fn.Gte(3), fn.Lte(7), fs.Eq("a"), cond.And[rec](fm.In(1, 2), fm.Gt(1))))
	require.False(t, empty)

	require.True(t, implies(queries, cond.And[rec](fn.Between(2, 8), fm.Eq(2))))
	require.False(t, implies(queries, cond.And[rec](fn.Between(4, 8))))
}

// TestImplicationIsSound checks, on random conditions and every record there can be, that the
// conditions never imply one that some record satisfies them without satisfying.
func TestImplicationIsSound(t *testing.T) {
	var records []*rec
	records = append(records, &rec{})
	for n := range 8 {
		for m := range 3 {
			records = append(records, &rec{N: &n, M: &m, S: []string{"a", "b"}[n%2], Flag: n%3 == 0,
				Tags: [][]string{nil, {"a"}, {"a", "c"}, {"b"}}[n%4]})
		}
	}
	records = append(records, &rec{N: new(int)}, &rec{S: "a", Flag: true, Tags: []string{"b", "c"}})

	rng := rand.New(rand.NewPCG(7, 8))
	even := cond.Func("even", func(r *rec) bool { return r.N != nil && *r.N%2 == 0 })
	var leaf func() cond.Cond[rec]
	leaf = func() cond.Cond[rec] {
		v := func() int { return rng.IntN(9) - 1 }
		switch rng.IntN(14) {
		case 0, 1:
			return fn.Eq(v())
		case 2:
			return fn.In(v(), v(), v())
		case 3:
			return fn.Gt(v())
		case 4:
			return fn.Gte(v())
		case 5:
			return fn.Lt(v())
		case 6:
			return fn.Lte(v())
		case 7:
			return fn.Between(v(), v())
		case 8:
			return fm.Eq(rng.IntN(4))
		case 9:
			return fs.Eq([]string{"a", "b"}[rng.IntN(2)])
		case 10:
			return fflag.Eq(rng.IntN(2) == 0)
		case 11:
			return ftags.In([]string{"a", "b", "c"}[rng.IntN(3)], []string{"a", "b", "c"}[rng.IntN(3)])
		case 12:
			return even
		}
		return ftags.Gte([]string{"a", "b", "c"}[rng.IntN(3)])
	}
	var random func(depth int) cond.Cond[rec]
	random = func(depth int) cond.Cond[rec] {
		if depth == 0 || rng.IntN(3) > 0 {
			return leaf()
		}
		children := make([]cond.Cond[rec], 1+rng.IntN(3))
		for i := range children {
			children[i] = random(depth - 1)
		}
		switch rng.IntN(3) {
		case 0:
			return cond.And(children...)
		case 1:
			return cond.Or(children...)
		}
		return cond.Not(children[0])
	}

	implied, total := 0, 0
	for range 40000 {
		queries := make([]cond.Cond[rec], 1+rng.IntN(3))
		for i := range queries {
			queries[i] = random(2)
		}
		index := random(2)
		normal, empty := normalise(queries)
		if empty {
			continue
		}

		total++
		if !implies(normal, index) {
			continue
		}
		implied++
		for _, r := range records {
			asked := true
			for _, q := range queries {
				ok, err := q.Match(r)
				require.NoError(t, err)
				asked = asked && ok
			}
			if !asked {
				continue
			}
			ok, err := index.Match(r)
			require.NoError(t, err)
			require.True(t, ok, "%v is satisfied by %+v, and %v is not", queries, r, index)
		}
	}
	// The conditions are of the kind that imply each other often enough to check.
	require.Greater(t, implied, 500, fmt.Sprintf("%d of %d", implied, total))
}
