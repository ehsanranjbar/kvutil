package query_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/query"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

func explain(t *testing.T, q *query.Query[int64, Item]) string {
	t.Helper()

	text, err := q.Explain()
	require.NoError(t, err)
	return text
}

// TestPlans checks the plans that the planner chooses, as Explain shows them.
func TestPlans(t *testing.T) {
	items := defineItems()
	from := func() *query.Query[int64, Item] { return query.From(items) }

	for _, test := range []struct {
		name string
		q    *query.Query[int64, Item]
		want string
	}{
		// The cost of a plan: how much it reads.
		{"unique equality wins", from().Where(code.Eq("i0007"), score.Eq(2)),
			`IndexLookup by_code: 1 range, then check [Score = 2]`},
		{"unique equality, then the fewest conditions to check", from().Where(code.Eq("i0007"), group.Eq("a")),
			`IndexLookup by_group_code: 1 range`},
		{"unique with several values", from().Where(code.In("i0007", "i0001", "i0007")),
			`IndexLookup by_code: 2 ranges`},
		{"equality beats range", from().Where(group.Eq("a"), note.Gte("n1")),
			`IndexLookup by_group_rank: 1 range, then check [Note >= "n1"]`},
		{"equality and range of an index", from().Where(score.Eq(2), label.Gte("l1")),
			`IndexLookup by_score_label: 1 range`},
		{"a range of a side beats nothing", from().Where(code.Gte("i0390")),
			`IndexLookup by_code: 1 range`},
		{"composite equality prefix and range", from().Where(group.Eq("a"), rank.Gt(3)),
			`IndexLookup by_group_rank: 1 range`},
		{"ranges of a field are one", from().Where(group.Eq("a"), rank.Gte(3), rank.Lt(8)),
			`IndexLookup by_group_rank: 1 range`},
		{"the second field alone does not narrow", from().Where(rank.Eq(3)),
			`StoreScan, then check [Rank = 3]`},
		{"not is checked", from().Where(group.Eq("a"), cond.Not[Item](rank.Gt(5))),
			`IndexLookup by_group_rank: 1 range, then check [not (Rank > 5)]`},
		{"or is checked", from().Where(cond.Or[Item](group.Eq("a"), rank.Eq(3))),
			`StoreScan, then check [(Group = "a") or (Rank = 3)]`},
		{"nested conjunctions are one", from().Where(cond.And[Item](group.Eq("a"), cond.And[Item](rank.Eq(3)))),
			`IndexLookup by_group_rank: 1 range`},

		// The order that is asked for.
		{"order forward", from().Where(group.Eq("a")).OrderBy(rank.Desc()),
			`IndexLookup by_group_rank (in order of Rank desc): 1 range`},
		{"order forward by the rest of the fields", from().Where(score.Eq(2), label.Gte("l0")).OrderBy(label),
			`IndexLookup by_score_label (in order of Label): 1 range`},
		{"order of all the fields", from().Where(score.Gte(1), label.Gte("l0")).OrderBy(score, label),
			`IndexLookup by_score_label (in order of Score, Label): 1 range, then check [Label >= "l0"]`},
		{"order of all the fields, the other way", from().Where(score.Gte(1), label.Gte("l0")).OrderBy(score.Desc(), label.Desc()),
			"Sort by Score desc, Label desc\n└─ IndexLookup by_score_label: 1 range, then check [Label >= \"l0\"]"},
		{"an optional field the index needs is not read", from().Where(score.Eq(2)).OrderBy(label),
			"Sort by Label\n└─ StoreScan, then check [Score = 2]"},
		{"order reversed, of a unique index", from().Where(code.Gte("i0100")).OrderBy(code.Desc()).Limit(10),
			"Limit 10\n└─ IndexLookup by_code (reversed, in order of Code desc): 1 range"},
		{"order reversed, with fixed fields", from().Where(group.Eq("b"), code.Gt("i0100")).OrderBy(code),
			`IndexLookup by_group_code (reversed, in order of Code): 1 range`},
		{"order is not reversed where records can be equal", from().Where(group.Eq("a")).OrderBy(rank),
			"Sort by Rank\n└─ IndexLookup by_group_rank: 1 range"},
		{"order of a prefix of the fields is not an order", from().Where(group.Gte("a")).OrderBy(group),
			"Sort by Group\n└─ IndexLookup by_group_rank: 1 range"},
		{"order not delivered", from().Where(group.Eq("a")).OrderBy(score),
			"Sort by Score\n└─ IndexLookup by_group_rank: 1 range"},
		{"order not delivered, with a limit", from().Where(group.Eq("a")).OrderBy(score).Limit(5),
			"Limit 5\n└─ TopN by Score\n   └─ IndexLookup by_group_rank: 1 range"},
		{"order not delivered, with a page", from().Where(group.Eq("a")).OrderBy(score, label.Desc()).Offset(20).Limit(10),
			"Limit 10, offset 20\n└─ TopN by Score, Label desc\n   └─ IndexLookup by_group_rank: 1 range"},
		{"an offset alone", from().Where(group.Eq("a")).OrderBy(rank.Desc()).Offset(5),
			"Offset 5\n└─ IndexLookup by_group_rank (in order of Rank desc): 1 range"},
		{"in breaks the order", from().Where(group.In("a", "b")).OrderBy(rank.Desc()),
			"Sort by Rank desc\n└─ IndexLookup by_group_rank: 2 ranges"},
		{"in is in order if the field is", from().Where(group.In("b", "a")).OrderBy(group, rank.Desc()),
			`IndexLookup by_group_rank (in order of Group, Rank desc): 2 ranges`},
		{"in of one value is a value", from().Where(group.In("a")).OrderBy(rank.Desc()),
			`IndexLookup by_group_rank (in order of Rank desc): 1 range`},
		{"fields with a value are in any order", from().Where(group.Eq("a"), rank.Eq(3)).OrderBy(group, rank.Desc()),
			`IndexLookup by_group_rank: 1 range`},
		{"a field with a value is left out of the order", from().Where(group.Eq("a")).OrderBy(group, rank.Desc()),
			`IndexLookup by_group_rank (in order of Rank desc): 1 range`},
		{"a field twice", from().Where(group.Eq("a")).OrderBy(rank.Desc(), rank),
			`IndexLookup by_group_rank (in order of Rank desc): 1 range`},
		{"an index of several entries is not an order", from().Where(tags.Eq("t1")).OrderBy(rank),
			"Sort by Rank\n└─ IndexLookup by_tag_rank: 1 range"},
		// An index that nothing narrows has all the records if all of its fields are always there.
		{"an index that nothing narrows, for its order", from().OrderBy(code),
			`IndexLookup by_code (in order of Code): 1 range`},
		{"an index that nothing narrows, for its order, with a limit", from().OrderBy(code).Limit(10),
			"Limit 10\n└─ IndexLookup by_code (in order of Code): 1 range"},
		{"an index that nothing narrows, read from its end", from().OrderBy(code.Desc()).Offset(3).Limit(10),
			"Limit 10, offset 3\n└─ IndexLookup by_code (reversed, in order of Code desc): 1 range"},
		{"a composite index that nothing narrows", from().OrderBy(group, code.Desc()).Limit(10),
			"Limit 10\n└─ IndexLookup by_group_code (in order of Group, Code desc): 1 range"},
		{"an index that nothing narrows is not read for another order", from().OrderBy(extra).Limit(10),
			"Limit 10\n└─ TopN by Extra\n   └─ StoreScan"},
		{"an index of an optional field is not read for its order", from().OrderBy(note).Limit(10),
			"Limit 10\n└─ TopN by Note\n   └─ StoreScan"},
		{"an index of a field after others is not read for its order", from().OrderBy(rank).Limit(10),
			"Limit 10\n└─ TopN by Rank\n   └─ StoreScan"},
		{"an optional field", from().Where(note.Gte("n1")).OrderBy(note),
			`IndexLookup by_note (in order of Note): 1 range`},
		{"an optional field, the other way", from().Where(note.Gte("n1")).OrderBy(note.Desc()),
			"Sort by Note desc\n└─ IndexLookup by_note: 1 range"},

		// The store.
		{"no conditions", from(), `StoreScan`},
		{"the keys are in order", from().Limit(5), "Limit 5\n└─ StoreScan"},
		{"sorted without an index", from().OrderBy(extra.Desc(), label),
			"Sort by Extra desc, Label\n└─ StoreScan"},
		{"top without an index", from().Where(extra.Lt(3)).OrderBy(weight).Limit(7),
			"Limit 7\n└─ TopN by Weight\n   └─ StoreScan, then check [Extra < 3]"},
		{"an index of some records is not read", from().Where(rank.Gt(3), ready.Eq(true)),
			`StoreScan, then check [Rank > 3; Ready = true]`},
		{"an index of some records is not read for its order", from().Where(ready.Eq(true)).OrderBy(rank),
			"Sort by Rank\n└─ StoreScan, then check [Ready = true]"},

		// What no record can satisfy.
		{"two values", from().Where(code.Eq("i0001"), code.Eq("i0002")), `Empty`},
		{"a value and values", from().Where(group.Eq("a"), group.In("b", "c")), `Empty`},
		{"a value outside of a range", from().Where(rank.Eq(9), rank.Lt(3)), `Empty`},
		{"values outside of a range", from().Where(rank.In(1, 2), rank.Gt(5)), `Empty`},
		{"an empty range", from().Where(rank.Gt(5), rank.Lt(3), group.Eq("a")), `Empty`},
		{"a range of a point", from().Where(rank.Gt(5), rank.Lt(5)), `Empty`},
		{"a range the wrong way round", from().Where(rank.Between(8, 2)), `Empty`},
		{"no values", from().Where(rank.In()), `Empty`},
		{"false", from().Where(group.Eq("a"), cond.Or[Item]()), `Empty`},
		{"false inside", from().Where(cond.And[Item](group.Eq("a"), cond.Or[Item]())), `Empty`},
		{"not true", from().Where(cond.Not(cond.And[Item]())), `Empty`},
		{"limit of 0", from().Where(group.Eq("a")).Limit(0), `Empty`},
		{"true", from().Where(cond.And[Item]()), `StoreScan`},
		{"true inside", from().Where(cond.Or[Item](group.Eq("a"), cond.And[Item]())), `StoreScan`},
		{"not false", from().Where(cond.Not(cond.Or[Item]())), `StoreScan`},
		{"a value inside a range is the value", from().Where(rank.Eq(4), rank.Gte(3)).OrderBy(rank),
			`StoreScan, then check [Rank = 4 and Rank >= 3]`},
		{"values inside a range are the values", from().Where(group.Eq("a"), rank.In(1, 2, 3), rank.Gt(1)),
			`IndexLookup by_group_rank: 2 ranges`},

		// Several values of a field are not one.
		{"two ranges of a field of several values", from().Where(tags.Gt("t0"), tags.Lt("t1")),
			`IndexLookup by_tag_rank: 1 range, then check [Tags < "t1"]`},
		{"two values of a field of several values", from().Where(tags.Eq("t0"), tags.Eq("t1")),
			`IndexLookup by_tag_rank: 1 range, then check [Tags = "t1"]`},

		// The hints.
		{"scan", from().Where(code.Eq("i0007")).FullScan(), `StoreScan, then check [Code = "i0007"]`},
		{"scan with an order", from().Where(group.Eq("a")).OrderBy(rank.Desc()).FullScan(),
			"Sort by Rank desc\n└─ StoreScan, then check [Group = \"a\"]"},
		{"scan, then an index", from().Where(group.Eq("a")).FullScan().Using("by_group_code").OrderBy(code.Desc()),
			`IndexLookup by_group_code (in order of Code desc): 1 range`},
		{"an index, then scan", from().Where(group.Eq("a")).Using("by_group_code").FullScan(),
			`StoreScan, then check [Group = "a"]`},
		{"an index that is not the best", from().Where(code.Eq("i0007"), group.Eq("a")).Using("by_group_rank"),
			"IndexLookup by_group_rank: 1 range, then check [Code = \"i0007\"]"},
		{"an index that nothing narrows, as a hint", from().Where(group.Eq("a")).Using("by_code"),
			"IndexLookup by_code: 1 range, then check [Group = \"a\"]"},
		{"empty with a hint", from().Where(rank.Gt(5), rank.Lt(3)).Using("by_code"), `Empty`},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.q.Explain()
			require.NoError(t, err)
			require.Equal(t, test.want, got)
			// A query is planned once.
			again, err := test.q.Explain()
			require.NoError(t, err)
			require.Equal(t, got, again)
		})
	}
}

func TestHintsThatCannotBeFollowed(t *testing.T) {
	items := defineItems()
	from := func() *query.Query[int64, Item] { return query.From(items) }

	for name, q := range map[string]*query.Query[int64, Item]{
		"an index that there is not":    from().Where(group.Eq("a")).Using("nope"),
		"an optional first field":       from().Where(group.Eq("a")).Using("by_note"),
		"an optional second field":      from().Where(score.Eq(2)).Using("by_score_label"),
		"an optional field, ordered":    from().Where(score.Eq(2)).OrderBy(score, label).Using("by_score_label"),
		"a field of several values":     from().OrderBy(rank).Using("by_tag_rank"),
		"only for its order":            from().OrderBy(note).Using("by_note"),
		"a negation needs no value":     from().Where(cond.Not[Item](label.Eq("l1")), score.Eq(2)).Using("by_score_label"),
		"a disjunction needs no value":  from().Where(cond.Or[Item](label.Eq("l1"), label.Eq("l2")), score.Eq(2)).Using("by_score_label"),
		"an index of some records":      from().Where(rank.Eq(3)).Using("ready_by_rank"),
		"an index of some, and its own": from().Where(rank.Eq(3), ready.Eq(true)).Using("ready_by_rank"),
		"an unknown index with no rows": from().Where(rank.Gt(5), rank.Lt(3)).Using("nope"),
		"an unknown index, no limit":    from().Where(group.Eq("a")).Limit(0).Using("nope"),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := q.Explain()
			require.ErrorIs(t, err, query.ErrHint)

			// Running it fails the same, through the iterator.
			require.NoError(t, testkv.NewMem(t).View(func(tx kv.Tx) error {
				it := q.Run(tx)
				defer it.Close()
				require.False(t, it.Valid())
				require.ErrorIs(t, it.Err(), query.ErrHint)
				return nil
			}))
		})
	}
}

func TestInvalidQueries(t *testing.T) {
	items := defineItems()
	from := func() *query.Query[int64, Item] { return query.From(items) }

	for name, test := range map[string]struct {
		q   *query.Query[int64, Item]
		err error
	}{
		"several values":         {from().OrderBy(rank, tags), query.ErrOrder},
		"several values alone":   {from().Where(tags.Eq("t1")).OrderBy(tags), query.ErrOrder},
		"nothing":                {from().OrderBy(nil), query.ErrOrder},
		"negative offset":        {from().Offset(-1), query.ErrNegative},
		"negative limit":         {from().Limit(-1), query.ErrNegative},
		"an order that replaced": {from().OrderBy(tags).OrderBy(tags.Desc()), query.ErrOrder},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := test.q.Explain()
			require.ErrorIs(t, err, test.err)

			testkv.Each(t, func(t *testing.T, db kv.DB) {
				require.NoError(t, db.View(func(tx kv.Tx) error {
					it := test.q.Run(tx)
					defer it.Close()
					require.False(t, it.Valid())
					require.ErrorIs(t, it.Err(), test.err)
					return nil
				}))
			})
		})
	}

	// The last order is the one, so an order that was wrong can be replaced.
	_, err := from().OrderBy(tags).OrderBy(rank).Explain()
	require.NoError(t, err)
}

// TestMissingValuesSortFirst checks where the records that have no value for a field come, in
// every plan.
func TestMissingValuesSortFirst(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f := newFixture(t, db)

		for _, test := range []struct {
			name string
			q    *query.Query[int64, Item]
		}{
			{"scanned", query.From(f.store)},
			{"scanned and filtered", query.From(f.store).Where(extra.Gte(0))},
			{"read from an index", query.From(f.store).Where(note.Gte("n0"))},
		} {
			for _, desc := range []bool{false, true} {
				orderField := field.Component[Item](weight)
				if desc {
					orderField = weight.Desc()
				}
				keys := f.keys(t, db, test.q.OrderBy(orderField))
				require.NotEmpty(t, keys)

				var seen []int
				for _, key := range keys {
					w := f.items[key].Weight
					if w == nil {
						seen = append(seen, -1)
					} else {
						seen = append(seen, *w)
					}
				}
				for i := 1; i < len(seen); i++ {
					if desc {
						require.GreaterOrEqual(t, seen[i-1], seen[i], "%s: %v", test.name, seen)
					} else {
						require.LessOrEqual(t, seen[i-1], seen[i], "%s: %v", test.name, seen)
					}
				}
				// Without a value: first, or last when descending.
				if desc {
					require.Equal(t, -1, seen[len(seen)-1], test.name)
				} else {
					require.Equal(t, -1, seen[0], test.name)
				}
			}
		}

		// The records that are equal come in the order of their keys.
		for _, q := range []*query.Query[int64, Item]{
			query.From(f.store).Where(group.Eq("b")).OrderBy(score),
			query.From(f.store).Where(group.Eq("b")).OrderBy(score.Desc()),
			query.From(f.store).Where(group.Eq("b")).OrderBy(rank.Desc()), // read from an index
		} {
			keys := f.keys(t, db, q)
			require.NotEmpty(t, keys)
			for i := 1; i < len(keys); i++ {
				a, b := f.items[keys[i-1]], f.items[keys[i]]
				if a.Score == b.Score && a.Rank == b.Rank {
					require.Less(t, keys[i-1], keys[i])
				}
			}
		}
	})
}

// failing is a condition that cannot be evaluated on one of the items.
type failing struct{ code string }

var errEvaluation = errors.New("cannot evaluate")

func (f failing) Match(it *Item) (bool, error) {
	if it.Code == f.code {
		return false, errEvaluation
	}
	return true, nil
}

func (f failing) String() string { return "failing(" + f.code + ")" }

func TestEvaluationFailsWhileIterating(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f := newFixture(t, db)
		from := func() *query.Query[int64, Item] { return query.From(f.store) }

		// An item in the middle fails, so there are records before it.
		bad := f.items[200]
		for name, test := range map[string]struct {
			q       *query.Query[int64, Item]
			records bool
		}{
			"scan":                  {from().Where(failing{bad.Code}), true},
			"index and residual":    {from().Where(group.Eq(bad.Group), failing{bad.Code}).OrderBy(rank.Desc()), true},
			"index, sorted":         {from().Where(group.Eq(bad.Group), failing{bad.Code}).OrderBy(score), false},
			"index, not sorted":     {from().Where(group.Eq(bad.Group), failing{bad.Code}), true},
			"sort":                  {from().Where(failing{bad.Code}).OrderBy(extra), false},
			"top":                   {from().Where(failing{bad.Code}).OrderBy(extra).Limit(5), false},
			"page":                  {from().Where(failing{bad.Code}).Offset(3).Limit(1000), true},
			"unique and residual":   {from().Where(code.Gte("i0000"), failing{bad.Code}), true},
			"scan, hinted":          {from().Where(group.Eq(bad.Group), failing{bad.Code}).FullScan(), true},
			"index of a hint":       {from().Where(group.Eq(bad.Group), failing{bad.Code}).Using("by_group_code").OrderBy(rank), false},
			"offset past the error": {from().Where(failing{f.items[itemCount].Code}).Offset(itemCount - 1).Limit(5), false},
		} {
			t.Run(name, func(t *testing.T) {
				require.NoError(t, db.View(func(tx kv.Tx) error {
					it := test.q.Run(tx)
					defer it.Close()

					n := 0
					for ; it.Valid(); it.Next() {
						_, err := it.Key()
						require.NoError(t, err)
						n++
					}
					require.ErrorIs(t, it.Err(), errEvaluation)
					require.False(t, it.Valid())
					// The first error is the one that stays.
					require.Equal(t, it.Err(), it.Err())
					if test.records {
						require.Positive(t, n)
					} else {
						// A sort reads all the records before it gives one.
						require.Zero(t, n)
					}
					return nil
				}))
			})
		}

		// A condition that fails on a record that the plan does not reach is not a failure.
		require.NoError(t, db.View(func(tx kv.Tx) error {
			it := from().Where(code.Gte("i0001"), failing{f.items[itemCount].Code}).Limit(3).OrderBy(code).Run(tx)
			defer it.Close()
			keys, err := it.Keys()
			require.NoError(t, err)
			require.Equal(t, []int64{1, 2, 3}, keys)
			return nil
		}))
	})
}

// TestCloseReleases checks that every plan closes what it opens, whether it is read to the end
// or not. Badger fails a transaction that has an iterator open.
func TestCloseReleases(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f := newFixture(t, db)
		from := func() *query.Query[int64, Item] { return query.From(f.store) }

		queries := []*query.Query[int64, Item]{
			from(),
			from().Limit(3),
			from().Where(extra.Lt(3)).OrderBy(weight, code.Desc()).Limit(4),
			from().OrderBy(extra).Offset(5),
			from().Where(group.Eq("a")),
			from().Where(group.Eq("a")).OrderBy(rank.Desc()).Limit(2),
			from().Where(group.In("a", "b")).OrderBy(group, rank.Desc()),
			from().Where(code.Gte("i0100")).OrderBy(code.Desc()).Limit(10),
			from().Where(code.In("i0001", "i0002", "i0003")),
			from().Where(tags.Eq("t1")).OrderBy(rank).Limit(5),
			from().Where(note.Gte("n0"), cond.Not[Item](rank.Eq(1))),
			from().Where(group.Eq("a")).Limit(0),
			from().Where(rank.Gt(5), rank.Lt(3)),
			from().Where(group.Eq("a")).FullScan(),
			from().Where(group.Eq("a")).Using("by_group_code").OrderBy(code),
			from().OrderBy(tags),
			from().Where(failing{"i0003"}),
			from().Where(failing{"i0003"}).OrderBy(extra),
			from().Where(group.Eq("a"), failing{"i0003"}).OrderBy(rank.Desc()),
		}
		for _, q := range queries {
			// Not started, started, and read to the end, and closed twice.
			require.NoError(t, db.View(func(tx kv.Tx) error {
				q.Run(tx).Close()

				it := q.Run(tx)
				if it.Valid() {
					it.Next()
				}
				require.NoError(t, it.Close())
				require.NoError(t, it.Close())

				it = q.Run(tx)
				for ; it.Valid(); it.Next() {
				}
				_ = it.Err()
				return it.Close()
			}), explain2(q))
		}
	})
}

func explain2(q *query.Query[int64, Item]) string {
	text, err := q.Explain()
	if err != nil {
		return err.Error()
	}
	return text
}

func TestQueriesAreDefinitions(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f := newFixture(t, db)

		base := query.From(f.store).Where(group.Eq("a"))
		baseKeys := f.keys(t, db, base)
		baseExplain := explain(t, base)

		// Two queries from one.
		narrower := base.Where(rank.Gt(5))
		paged := base.OrderBy(rank.Desc()).Offset(2).Limit(3)
		hinted := base.FullScan()
		otherWhere := base.Where(group.Eq("b"))

		require.Equal(t, baseKeys, f.keys(t, db, base))
		require.Equal(t, baseExplain, explain(t, base))

		require.Equal(t, "IndexLookup by_group_rank: 1 range", baseExplain)
		require.Equal(t, "IndexLookup by_group_rank: 1 range", explain(t, narrower))
		require.Equal(t, "Limit 3, offset 2\n└─ IndexLookup by_group_rank (in order of Rank desc): 1 range", explain(t, paged))
		require.Equal(t, `StoreScan, then check [Group = "a"]`, explain(t, hinted))
		require.Equal(t, "Empty", explain(t, otherWhere))

		require.Less(t, len(f.keys(t, db, narrower)), len(baseKeys))
		require.Len(t, f.keys(t, db, paged), 3)
		// Without an order, the scan and the index give the same records in their own orders.
		require.ElementsMatch(t, baseKeys, f.keys(t, db, hinted))
		require.Empty(t, f.keys(t, db, otherWhere))

		// The arguments are not kept: a slice that is changed later does not change a query.
		conds := []cond.Cond[Item]{group.Eq("c")}
		order := []field.Component[Item]{rank}
		q := query.From(f.store).Where(conds...).OrderBy(order...)
		before := f.keys(t, db, q)
		conds[0], order[0] = group.Eq("d"), tags
		require.Equal(t, before, f.keys(t, db, q))

		// An error of a query is the error of that query only.
		bad := base.OrderBy(tags)
		_, err := bad.Explain()
		require.ErrorIs(t, err, query.ErrOrder)
		require.Equal(t, baseKeys, f.keys(t, db, base))
		_, err = base.OrderBy(tags).OrderBy(rank).Explain()
		require.NoError(t, err)
	})
}

func TestQueriesAreRunConcurrently(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f := newFixture(t, db)
		q := query.From(f.store).Where(group.Eq("a"), rank.Gte(2)).OrderBy(rank.Desc()).Limit(20)
		want := f.keys(t, db, q.FullScan())

		var wg sync.WaitGroup
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()

				// The first to ask plans it, and the others have the same plan.
				text, err := q.Explain()
				if err != nil || text != "Limit 20\n└─ IndexLookup by_group_rank (in order of Rank desc): 1 range" {
					t.Errorf("explain: %q %v", text, err)
				}
				for range 5 {
					keys := []int64{}
					err := db.View(func(tx kv.Tx) error {
						it := q.Run(tx)
						defer it.Close()
						for ; it.Valid(); it.Next() {
							key, err := it.Key()
							if err != nil {
								return err
							}
							keys = append(keys, key)
						}
						return it.Err()
					})
					if err != nil || fmt.Sprint(keys) != fmt.Sprint(want) {
						t.Errorf("run: %v %v", keys, err)
					}
				}
			}()
		}
		wg.Wait()
	})
}

// TestPlansFollowTheIndexesThatThereAre checks that a query is planned with the indexes that the
// store has when it is first planned.
func TestPlansFollowTheIndexesThatThereAre(t *testing.T) {
	items := defineItems()
	q := query.From(items).Where(group.Eq("a"))
	require.Equal(t, "IndexLookup by_group_rank: 1 range", explain(t, q))
	// A query has no index of the store that is bare of them.
	require.Equal(t, `StoreScan, then check [Group = "a"]`,
		explain(t, query.From(extstore.New[int64, Item]("bare")).Where(group.Eq("a"))))
}
