package query_test

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/query"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

// TestProjectionPlans checks the plans of projections, as Explain shows them.
func TestProjectionPlans(t *testing.T) {
	items := defineItems()
	from := func() *query.Query[int64, Item] { return query.From(items) }

	for _, test := range []struct {
		name string
		p    *query.Projection[int64, Item]
		want string
	}{
		// An index that holds what is selected and what is checked is read alone.
		{"index only, a field that is included", from().Where(code.Eq("i0007")).Select(group),
			`IndexOnlyScan by_code: 1 range`},
		{"index only, a field of the key and a cover", from().Where(group.Eq("a")).Select(rank, codeUpper),
			`IndexOnlyScan by_group_rank: 1 range`},
		{"index only, a check on a field of the key", from().Where(group.Eq("a"), cond.Not(rank.Gt(5))).Select(extra),
			`IndexOnlyScan by_group_rank: 1 range, then check [not (Rank > 5)]`},
		{"index only, a check on an included field", from().Where(group.Eq("a"), extra.Gte(3)).Select(rank),
			`IndexOnlyScan by_group_rank: 1 range, then check [Extra >= 3]`},
		{"index only, a check on an included field that is optional", from().Where(group.Eq("a"), cond.Or(weight.Eq(1), cond.Not(weight.Lt(3)))).Select(rank),
			`IndexOnlyScan by_group_rank: 1 range, then check [(Weight = 1) or (not (Weight < 3))]`},
		{"index only, in order", from().Where(code.Gte("i0100")).OrderBy(code.Desc()).Limit(10).Select(group),
			"Limit 10\n└─ IndexOnlyScan by_code (reversed, in order of Code desc): 1 range"},
		{"index only, an index of some of the records needs no check of its condition",
			from().Where(ready.Eq(true)).Select(score),
			`IndexOnlyScan ready_by_rank: 1 range`},
		{"index only, sorted by a field that is held", from().Where(group.Eq("a")).OrderBy(extra).Limit(5).Select(rank),
			"Limit 5\n└─ TopN by Extra\n   └─ IndexOnlyScan by_group_rank: 1 range"},

		// Anything that the index does not hold is read from the records.
		{"an item that is not held", from().Where(group.Eq("a")).Select(note),
			`IndexLookup by_group_rank: 1 range`},
		{"a condition on a field that is not held", from().Where(group.Eq("a"), note.Gte("n1")).Select(rank),
			`IndexLookup by_group_rank: 1 range, then check [Note >= "n1"]`},
		{"a condition that is a function", from().Where(group.Eq("a"), cond.Func("odd", func(it *Item) bool { return it.Rank%2 == 1 })).Select(),
			`IndexLookup by_group_rank: 1 range, then check [odd()]`},
		{"sorted by a field that is not held", from().Where(group.Eq("a")).OrderBy(score).Limit(5).Select(rank),
			"Limit 5\n└─ TopN by Score\n   └─ IndexLookup by_group_rank: 1 range"},
		{"a field of the key that has several values, selected", from().Where(tags.Eq("t1")).Select(tags),
			`IndexLookup by_tag_rank: 1 range`},
		{"a field of the key that has several values, not selected", from().Where(tags.Eq("t1")).Select(rank),
			`IndexOnlyScan by_tag_rank: 1 range`},
		{"a store scan reads the records", from().Where(extra.Lt(3)).Select(upperTag),
			`StoreScan, then check [Extra < 3]`},
		{"a hint to scan", from().Where(extra.Lt(3)).FullScan().Select(extra),
			`StoreScan, then check [Extra < 3]`},

		// Nothing is selected: only the keys.
		{"keys only", from().Where(code.Gte("i0390")).Select(),
			`IndexOnlyScan by_code: 1 range`},
		{"keys only, with a check that the index holds", from().Where(group.Eq("a"), rank.Gt(2), extra.Lt(4)).Select(),
			`IndexOnlyScan by_group_rank: 1 range, then check [Extra < 4]`},
		{"keys only, with a check that it does not", from().Where(group.Eq("a"), score.Eq(1)).Select(),
			`IndexLookup by_group_rank: 1 range, then check [Score = 1]`},
		{"keys only, of everything", from().Select(),
			`IndexOnlyScan by_group_rank: 1 range`},
		{"keys only, nothing", from().Where(group.Eq("a"), group.Eq("b")).Select(),
			`Empty`},
	} {
		got, err := test.p.Explain()
		require.NoError(t, err, test.name)
		require.Equal(t, test.want, got, test.name)
	}
}

// countingStore counts the records that rows get from the store.
type countingStore struct {
	*extstore.Store[int64, Item]
	gets *atomic.Int64
}

func (s countingStore) Getter(tx kv.Tx) extstore.Getter[int64, Item] {
	return countingGetter{Getter: s.Store.Getter(tx), gets: s.gets}
}

type countingGetter struct {
	extstore.Getter[int64, Item]
	gets *atomic.Int64
}

func (g countingGetter) Get(key int64) (*Item, error) {
	g.gets.Add(1)
	return g.Getter.Get(key)
}

func TestRowRecordIsGotWhenAskedFor(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f := newFixture(t, db)
		gets := &atomic.Int64{}
		store := countingStore{Store: f.store, gets: gets}
		from := func() *query.Query[int64, Item] { return query.From[int64, Item](store) }

		// An index serves the projection alone: the records are got when they are asked for, once.
		indexOnly := from().Where(group.Eq("a")).OrderBy(rank.Desc())
		text, err := indexOnly.Select(rank, codeUpper).Explain()
		require.NoError(t, err)
		require.Contains(t, text, "IndexOnlyScan by_group_rank")

		require.NoError(t, db.View(func(tx kv.Tx) error {
			plain, err := indexOnly.Run(tx).Values()
			require.NoError(t, err)
			require.NotEmpty(t, plain)

			gets.Store(0)
			it := indexOnly.Select(rank, codeUpper).Run(tx)
			defer it.Close()
			var rows []*query.Row[int64, Item]
			for ; it.Valid(); it.Next() {
				row, err := it.Value()
				require.NoError(t, err)
				rows = append(rows, row)
			}
			require.NoError(t, it.Err())
			require.Len(t, rows, len(plain))
			require.Zero(t, gets.Load(), "no record is got for the values of the rows")

			// Asked for after the iterator is done with, within the transaction.
			first, err := rows[0].Record()
			require.NoError(t, err)
			require.Equal(t, plain[0], first)
			again, err := rows[0].Record()
			require.NoError(t, err)
			require.Same(t, first, again)
			require.EqualValues(t, 1, gets.Load())

			last, err := rows[len(rows)-1].Record()
			require.NoError(t, err)
			require.Equal(t, plain[len(plain)-1], last)
			require.EqualValues(t, 2, gets.Load())
			return nil
		}))

		// A plan that reads the records gives them to the rows: no more are got.
		fetching := from().Where(extra.Lt(3)).OrderBy(code).FullScan()
		text, err = fetching.Select(extra).Explain()
		require.NoError(t, err)
		require.Contains(t, text, "StoreScan")
		require.NoError(t, db.View(func(tx kv.Tx) error {
			plain, err := fetching.Run(tx).Values()
			require.NoError(t, err)

			gets.Store(0)
			it := fetching.Select(extra).Run(tx)
			defer it.Close()
			n := 0
			for ; it.Valid(); it.Next() {
				row, err := it.Value()
				require.NoError(t, err)
				record, err := row.Record()
				require.NoError(t, err)
				require.Equal(t, plain[n], record)
				n++
			}
			require.NoError(t, it.Err())
			require.Equal(t, len(plain), n)
			require.Zero(t, gets.Load())
			return nil
		}))

		// An item that was not selected has no value in the row, whatever the plan is.
		require.NoError(t, db.View(func(tx kv.Tx) error {
			for _, q := range []*query.Query[int64, Item]{indexOnly, indexOnly.FullScan(), fetching} {
				it := q.Select(rank).Run(tx)
				require.True(t, it.Valid())
				row, err := it.Value()
				require.NoError(t, err)
				_, ok := row.Get(rank)
				require.True(t, ok)
				_, ok = row.Get(group)
				require.False(t, ok)
				_, ok = row.Get(codeUpper)
				require.False(t, ok)
				_, ok = row.GetAll(extra)
				require.False(t, ok)
				require.NoError(t, it.Close())
			}
			return nil
		}))
	})
}

func TestProjectionErrors(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f := newFixture(t, db)
		from := func() *query.Query[int64, Item] { return query.From(f.store) }

		// A hint that cannot be followed is an error of the projection and of the count.
		bad := from().Using("nowhere")
		_, err := bad.Select(rank).Explain()
		require.ErrorIs(t, err, query.ErrHint)
		require.NoError(t, db.View(func(tx kv.Tx) error {
			it := bad.Select(rank).Run(tx)
			require.False(t, it.Valid())
			require.ErrorIs(t, it.Err(), query.ErrHint)
			require.NoError(t, it.Close())

			_, err := bad.Count(tx)
			require.ErrorIs(t, err, query.ErrHint)
			return nil
		}))

		// Nothing is read for conditions that contradict each other.
		require.NoError(t, db.View(func(tx kv.Tx) error {
			n, err := from().Where(rank.Eq(1), rank.Eq(2)).Count(tx)
			require.NoError(t, err)
			require.Zero(t, n)
			return nil
		}))

		// An item that is nil is left out, and one that is given twice is one.
		require.NoError(t, db.View(func(tx kv.Tx) error {
			it := from().Where(code.Eq("i0007")).Select(nil, rank, rank).Run(tx)
			defer it.Close()
			require.True(t, it.Valid())
			row, err := it.Value()
			require.NoError(t, err)
			require.Equal(t, int64(7), row.Key())
			value, ok := row.Get(rank)
			require.True(t, ok)
			require.Equal(t, f.items[7].Rank, value)
			return nil
		}))
	})
}

// TestCountIsThePageOfTheQuery checks that the count is the number of the records that the query
// gives, and that it does not need the records where an index holds the conditions.
func TestCountIsThePageOfTheQuery(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f := newFixture(t, db)
		base := query.From(f.store).Where(group.Eq("a"), rank.Gte(3))

		var all int
		for _, it := range f.items {
			if it.Group == "a" && it.Rank >= 3 {
				all++
			}
		}
		require.Greater(t, all, 10)

		for _, test := range []struct {
			name string
			q    *query.Query[int64, Item]
			want int
		}{
			{"all", base, all},
			{"ordered", base.OrderBy(score.Desc(), code), all},
			{"a limit", base.OrderBy(code).Limit(7), 7},
			{"an offset", base.Offset(3), all - 3},
			{"a page", base.OrderBy(code).Offset(all - 2).Limit(5), 2},
			{"a limit of zero", base.Limit(0), 0},
			{"a scan", base.FullScan(), all},
			{"no condition", query.From(f.store), itemCount},
		} {
			require.NoError(t, db.View(func(tx kv.Tx) error {
				got, err := test.q.Count(tx)
				require.NoError(t, err, test.name)
				require.Equal(t, test.want, got, test.name)
				// Again, with the plan that was made.
				got, err = test.q.Count(tx)
				require.NoError(t, err, test.name)
				require.Equal(t, test.want, got, test.name)
				return nil
			}), test.name)
		}

		// The plan of a count is that of the projection of the keys.
		text, err := base.Select().Explain()
		require.NoError(t, err)
		require.Equal(t, "IndexOnlyScan by_group_rank: 1 range", text)
	})
}
