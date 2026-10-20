package query_test

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/query"
)

type itemRow = query.Row[int64, Item]

// selected is an item that a projection selects, with what checks a row against an item.
type selected struct {
	name string
	item field.Item[Item]
	// check checks the values of a row against those that the item has for the item.
	check func(t *testing.T, row *itemRow, it *Item, msg string)
	// absent checks that a row has nothing for the item, which was not selected.
	absent func(t *testing.T, row *itemRow, msg string)
}

func selectable[F any](s field.Selectable[Item, F]) selected {
	return selected{
		name: s.Path(),
		item: s,
		check: func(t *testing.T, row *itemRow, it *Item, msg string) {
			t.Helper()
			want := s.Values(it)
			all, ok := row.GetAll(s)
			first, firstOK := row.Get(s)
			if len(want) == 0 {
				require.False(t, ok, "%s: %s has no value", msg, s.Path())
				require.False(t, firstOK, "%s: %s has no value", msg, s.Path())
				return
			}
			require.True(t, ok, "%s: %s", msg, s.Path())
			require.Equal(t, want, all, "%s: %s", msg, s.Path())
			require.True(t, firstOK, "%s: %s", msg, s.Path())
			require.Equal(t, want[0], first, "%s: %s", msg, s.Path())
		},
		absent: func(t *testing.T, row *itemRow, msg string) {
			t.Helper()
			_, ok := row.Get(s)
			require.False(t, ok, "%s: %s was not selected", msg, s.Path())
			_, ok = row.GetAll(s)
			require.False(t, ok, "%s: %s was not selected", msg, s.Path())
		},
	}
}

// selectables are the items that a random projection selects from: fields that indexes have in
// their keys, fields that they include, fields that no index has, and covers.
var selectables = []selected{
	selectable(code), selectable(group), selectable(rank), selectable(score), selectable(label),
	selectable(extra), selectable(tags), selectable(note), selectable(weight), selectable(ready),
	selectable(total), selectable(upperTag),
	selectable(codeUpper), selectable(rankSquared), selectable(labelLen),
}

// randomSelection returns the items of a random projection, which may be none.
func randomSelection(rng *rand.Rand) []selected {
	if rng.IntN(5) == 0 {
		return nil
	}
	var sel []selected
	for range 1 + rng.IntN(3) {
		s := selectables[rng.IntN(len(selectables))]
		dup := false
		for _, o := range sel {
			dup = dup || o.name == s.name
		}
		if !dup {
			sel = append(sel, s)
		}
	}
	return sel
}

// heldBy are the items that each index holds, in its key, included, or covered.
var heldBy = map[string][]string{
	"by_group_rank":  {"Group", "Rank", "Extra", "Weight", "Ready", "CodeUpper"},
	"by_score_label": {"Score", "Label", "Note", "Tags", "RankSquared"},
	"by_code":        {"Code", "Group", "Rank", "Score", "Label", "Weight"},
	"by_group_code":  {"Group", "Code", "Rank", "Extra", "RankSquared", "LabelLen"},
	"by_tag_rank":    {"Tags", "Rank", "Group", "Extra", "Note", "CodeUpper"},
	"by_note":        {"Note", "Group", "Extra", "Weight", "RankSquared"},
	"ready_by_rank":  {"Rank", "Score", "Weight", "Tags", "CodeUpper"},
	"ab_by_score":    {"Score", "Rank", "Group", "Extra"},
	"by_total":       {"Rank + Score", "Group", "Tags", "Weight", "LabelLen"},
	"by_upper_tag":   {"upper(Tags)", "Rank", "Group", "Extra", "RankSquared"},
}

// selectionHeldBy returns a random selection of items that an index holds, which may be none.
func selectionHeldBy(rng *rand.Rand, index string) []selected {
	if rng.IntN(6) == 0 {
		return nil
	}
	var sel []selected
	for range 1 + rng.IntN(3) {
		name := heldBy[index][rng.IntN(len(heldBy[index]))]
		if contains(sel, name) {
			continue
		}
		for _, s := range selectables {
			if s.name == name {
				sel = append(sel, s)
			}
		}
	}
	return sel
}

func itemsOf(sel []selected) []field.Item[Item] {
	items := make([]field.Item[Item], len(sel))
	for i, s := range sel {
		items[i] = s.item
	}
	return items
}

// expectCount returns the number of records that a spec gives.
func (f *fixture) expectCount(t *testing.T, s spec) int {
	t.Helper()

	// The memo is made by the check of the keys, which comes before.
	if len(s.order) > 0 {
		return len(s.memo.expected)
	}
	n := max(0, len(s.memo.all)-s.offset)
	if s.limited {
		n = min(n, s.limit)
	}
	return n
}

// requireRows runs the projection of a query on a selection, and checks its rows against the
// spec: the keys, in the order if there is one, and the values of the items against those that
// the records have. It checks that the record of a row is the one of its key as well, and the
// count of the query. It returns the plan of the projection.
func (f *fixture) requireRows(t *testing.T, db kv.DB, s spec, sel []selected, q *query.Query[int64, Item], msg string) string {
	t.Helper()

	p := q.Select(itemsOf(sel)...)
	explain, err := p.Explain()
	require.NoError(t, err, msg)
	msg += "\n" + explain

	keys := []int64{}
	require.NoError(t, db.View(func(tx kv.Tx) error {
		it := p.Run(tx)
		defer it.Close()

		n := 0
		var rows []*itemRow
		for ; it.Valid(); it.Next() {
			key, err := it.Key()
			require.NoError(t, err, msg)
			row, err := it.Value()
			require.NoError(t, err, msg)
			require.Equal(t, key, row.Key(), msg)
			keys = append(keys, key)

			record := f.items[key]
			for _, item := range sel {
				item.check(t, row, record, msg)
			}
			if n < 3 {
				for _, item := range selectables {
					if !contains(sel, item.name) {
						item.absent(t, row, msg)
					}
				}
			}
			// Every third row is asked for its record, which is the record of its key; the
			// others are kept and asked after the iterator has moved on.
			if n%3 == 0 {
				got, err := row.Record()
				require.NoError(t, err, msg)
				require.Equal(t, record, got, msg)
				again, err := row.Record()
				require.NoError(t, err, msg)
				require.Same(t, got, again, msg)
			} else if len(rows) < 5 {
				rows = append(rows, row)
			}
			n++
		}
		require.NoError(t, it.Err(), msg)
		for _, row := range rows {
			got, err := row.Record()
			require.NoError(t, err, msg)
			require.Equal(t, f.items[row.Key()], got, msg)
		}
		return nil
	}))
	f.requireResult(t, s, keys, msg)

	require.NoError(t, db.View(func(tx kv.Tx) error {
		count, err := q.Count(tx)
		require.NoError(t, err, msg)
		require.Equal(t, f.expectCount(t, s), count, "count of the query: %s", msg)
		return nil
	}))
	return explain
}

func contains(sel []selected, name string) bool {
	for _, s := range sel {
		if s.name == name {
			return true
		}
	}
	return false
}

// TestProjectionAgreesWithBruteForce checks the rows of random projections, and the counts of
// random queries, with the plan that the planner chooses and with every plan that can be forced,
// against the values that the records have.
func TestProjectionAgreesWithBruteForce(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f := newFixture(t, db)
		indexes := []string{
			"by_group_rank", "by_score_label", "by_code", "by_group_code", "by_tag_rank", "by_note",
			"ready_by_rank", "ab_by_score", "by_total", "by_upper_tag",
		}

		var (
			rng     = rand.New(rand.NewPCG(3, 4))
			shapes  = map[string]int{}
			chosen  = map[string]int{}
			forced  = map[string]int{}
			lookups = map[string]int{}
		)
		tally := func(into map[string]int, explain string) {
			for _, shape := range []string{"IndexOnlyScan", "IndexLookup", "StoreScan", "Sort by", "TopN by", "then check", "reversed", "in order of"} {
				if strings.Contains(explain, shape) {
					into[shape]++
				}
			}
			if strings.Contains(explain, "IndexOnlyScan") {
				for _, shape := range []string{"Sort by", "TopN by", "then check", "reversed", "in order of"} {
					if strings.Contains(explain, shape) {
						into["IndexOnlyScan with "+shape]++
					}
				}
			}
		}

		for n := range 2 * queryCount {
			s := randomSpec(rng)
			sel := randomSelection(rng)
			base := query.From(f.store)
			desc := fmt.Sprintf("query %d: %v order %v offset %d limit %d/%v select %v",
				n, s.conds, orderTexts(s.order), s.offset, s.limit, s.limited, names(sel))

			// The plan of the planner.
			explain := f.requireRows(t, db, s, sel, s.query(base), desc)
			tally(shapes, explain)
			for _, name := range indexes {
				if strings.Contains(explain, "IndexOnlyScan "+name) {
					chosen[name]++
					f.requireImplied(t, s, name, desc+"\n"+explain)
				}
			}

			// A scan of the store.
			explain = f.requireRows(t, db, s, sel, s.query(base).FullScan(), desc)
			require.True(t, strings.Contains(explain, "StoreScan") || explain == "Empty", explain)

			// Every index.
			for _, name := range indexes {
				forcedQuery := s.query(base).Using(name)
				if _, err := forcedQuery.Select(itemsOf(sel)...).Explain(); err != nil {
					require.ErrorIs(t, err, query.ErrHint, desc)
					continue
				}
				// Half of the time what the index holds is what is selected.
				chosenSel := sel
				if rng.IntN(2) == 0 {
					chosenSel = selectionHeldBy(rng, name)
				}
				explain = f.requireRows(t, db, s, chosenSel, forcedQuery, fmt.Sprintf("%s (select %v)", desc, names(chosenSel)))
				if explain != "Empty" {
					require.Regexp(t, "Index(Only|Lookup)\\w* "+name+"\\b", explain, desc)
					f.requireImplied(t, s, name, desc+"\n"+explain)
				}
				tally(forced, explain)
				if strings.Contains(explain, "IndexOnlyScan") {
					forced[name+" index-only"]++
				} else if strings.Contains(explain, "IndexLookup") {
					lookups[name]++
				}
			}
		}

		t.Logf("chosen plans %v, by index %v, forced %v, lookups %v", shapes, chosen, forced, lookups)

		for _, shape := range []string{"IndexOnlyScan", "IndexLookup", "StoreScan", "Sort by", "TopN by", "then check"} {
			require.Greater(t, shapes[shape], 3, "%s in %v", shape, shapes)
		}
		require.Greater(t, shapes["IndexOnlyScan with then check"], 3, "%v", shapes)
		// Forced, every kind of the plans that make rows from an index is there.
		for _, shape := range []string{"IndexOnlyScan", "IndexLookup", "IndexOnlyScan with Sort by", "IndexOnlyScan with TopN by",
			"IndexOnlyScan with then check", "IndexOnlyScan with in order of"} {
			require.Greater(t, forced[shape], 10, "%s in %v", shape, forced)
		}
		// Reading from the end needs a unique index and an order that it gives backwards.
		require.Greater(t, forced["IndexOnlyScan with reversed"], 2, "%v", forced)
		for _, name := range indexes {
			require.Greater(t, forced[name+" index-only"], 1, "%s in %v", name, forced)
			require.Greater(t, lookups[name], 3, "%s in %v", name, lookups)
		}
	})
}

func names(sel []selected) []string {
	var names []string
	for _, s := range sel {
		names = append(names, s.name)
	}
	return names
}
