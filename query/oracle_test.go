package query_test

import (
	"bytes"
	"cmp"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/query"
)

// picker returns a value to compare a field with: one of the values that items have, or
// sometimes one that no item has.
type picker[F any] func(rng *rand.Rand) F

func pickInt(max int) picker[int] {
	return func(rng *rand.Rand) int { return rng.IntN(max+2) - 1 }
}

func pickText(prefix string, n int) picker[string] {
	return func(rng *rand.Rand) string {
		if rng.IntN(15) == 0 {
			return prefix + "z"
		}
		return fmt.Sprintf("%s%d", prefix, rng.IntN(n))
	}
}

// compare makes a random comparison of a field with values.
func compare[F any](rng *rand.Rand, f field.Field[Item, F], pick picker[F]) cond.Cond[Item] {
	switch rng.IntN(9) {
	case 0, 1:
		return f.Eq(pick(rng))
	case 2:
		values := make([]F, 1+rng.IntN(3))
		for i := range values {
			values[i] = pick(rng)
		}
		return f.In(values...)
	case 3:
		return f.Gt(pick(rng))
	case 4:
		return f.Gte(pick(rng))
	case 5:
		return f.Lt(pick(rng))
	case 6:
		return f.Lte(pick(rng))
	case 7:
		// The sides may be the wrong way round, which nothing satisfies.
		return f.Between(pick(rng), pick(rng))
	}
	return f.Range(kvutil.After(pick(rng)).Until(pick(rng)))
}

// randomLeaf returns a random condition that is not made of others.
func randomLeaf(rng *rand.Rand) cond.Cond[Item] {
	switch rng.IntN(17) {
	case 13:
		return compare(rng, total, pickInt(14))
	case 14:
		return compare(rng, upperTag, pickText("T", 4))
	case 0, 1:
		return compare(rng, group, func(rng *rand.Rand) string { return []string{"a", "b", "c", "d", "z"}[rng.IntN(5)] })
	case 2, 3:
		return compare(rng, rank, pickInt(9))
	case 4, 5:
		return compare(rng, score, pickInt(4))
	case 6:
		return compare(rng, label, pickText("l", 5))
	case 7:
		return compare(rng, code, func(rng *rand.Rand) string { return fmt.Sprintf("i%04d", 1+rng.IntN(itemCount+10)) })
	case 8, 9:
		return compare(rng, tags, pickText("t", 4))
	case 10:
		return compare(rng, note, pickText("n", 3))
	case 11:
		return compare(rng, weight, pickInt(4))
	case 12:
		return compare(rng, extra, pickInt(5))
	}
	switch rng.IntN(5) {
	case 0:
		return ready.Eq(rng.IntN(2) == 0)
	case 1:
		return cond.Func("rank is even", func(it *Item) bool { return it.Rank%2 == 0 })
	case 2:
		return cond.Or[Item]()
	case 3:
		return cond.And[Item]()
	}
	return ready.Eq(true)
}

// randomCond returns a random condition, which may be made of others.
func randomCond(rng *rand.Rand, depth int) cond.Cond[Item] {
	if depth == 0 || rng.IntN(3) > 0 {
		return randomLeaf(rng)
	}

	children := func() []cond.Cond[Item] {
		conds := make([]cond.Cond[Item], 1+rng.IntN(3))
		for i := range conds {
			conds[i] = randomCond(rng, depth-1)
		}
		return conds
	}
	switch rng.IntN(3) {
	case 0:
		return cond.And(children()...)
	case 1:
		return cond.Or(children()...)
	}
	return cond.Not(randomCond(rng, depth-1))
}

// orderFields are the fields that a random query is ordered by.
var orderFields = []field.Component[Item]{
	group, rank, rank.Desc(), score, score.Desc(), label, label.Desc(), code, code.Desc(),
	note, note.Desc(), weight, weight.Desc(), extra, extra.Desc(), total, total.Desc(),
}

// indexOrders are orders that the indexes of the store give, forward or in reverse.
var indexOrders = [][]field.Component[Item]{
	{rank}, {rank.Desc()}, {code}, {code.Desc()}, {note}, {note.Desc()}, {label},
	{group}, {group, rank.Desc()}, {group.Desc(), rank}, {group, code.Desc()}, {group.Desc(), code},
	{score, label}, {score.Desc(), label.Desc()}, {score.Desc(), label}, {rank, code}, {rank, score},
	{total}, {total.Desc()}, {score}, {score.Desc()},
}

// spec is a random query.
type spec struct {
	conds   []cond.Cond[Item]
	order   []field.Component[Item]
	offset  int
	limit   int
	limited bool
	// small is set if the query is paged with a small limit.
	small bool
	// memo keeps what the spec asks for, which is found once.
	memo *memo
}

type memo struct {
	expected, all []int64
}

// randomIndexSpec returns a query that an index can serve with its order.
func randomIndexSpec(rng *rand.Rand) spec {
	g := func() string { return []string{"a", "b", "c", "d"}[rng.IntN(4)] }
	c := func() string { return fmt.Sprintf("i%04d", 1+rng.IntN(itemCount)) }

	// The groups that the condition of the index of some of the items is about.
	ab := func() string { return []string{"a", "b"}[rng.IntN(2)] }

	var s spec
	// The cases for the indexes of some of the items and of expressions are three times as likely.
	which := rng.IntN(27)
	if which > 14 {
		which = 9 + (which-15)%6
	}
	switch which {
	case 9:
		// Only the items that are ready, which the index of them is read for.
		s.conds = []cond.Cond[Item]{ready.Eq(true)}
		if rng.IntN(2) == 0 {
			s.conds = append(s.conds, rank.Gte(rng.IntN(10)))
		}
		s.order = [][]field.Component[Item]{{rank}, {rank.Desc()}, {rank, code}}[rng.IntN(3)]
	case 10:
		// Conditions that imply the SQL condition of an index: a group of two and a rank.
		if rng.IntN(2) == 0 {
			s.conds = []cond.Cond[Item]{group.Eq(ab())}
		} else {
			s.conds = []cond.Cond[Item]{group.In("a", "b")}
		}
		s.conds = append(s.conds, []cond.Cond[Item]{rank.Gte(3 + rng.IntN(7)), rank.Gt(2 + rng.IntN(7)), rank.Eq(3 + rng.IntN(7))}[rng.IntN(3)])
		if rng.IntN(4) > 0 {
			s.conds = append(s.conds, score.Eq(rng.IntN(5)))
		}
		s.order = [][]field.Component[Item]{{score}, {score.Desc()}, {score, code}}[rng.IntN(3)]
	case 11:
		// Nearly: a group or a rank that the condition of the index does not allow.
		s.conds = []cond.Cond[Item]{group.In(ab(), "c"), rank.Gte(3 + rng.IntN(7)), score.Eq(rng.IntN(5))}
		if rng.IntN(2) == 0 {
			s.conds = []cond.Cond[Item]{group.Eq(ab()), rank.Gte(rng.IntN(4)), score.Eq(rng.IntN(5))}
		}
		s.order = [][]field.Component[Item]{{score}, {score.Desc()}}[rng.IntN(2)]
	case 12:
		// An expression of the item.
		switch rng.IntN(3) {
		case 0:
			s.conds = []cond.Cond[Item]{total.Eq(rng.IntN(14))}
		case 1:
			s.conds = []cond.Cond[Item]{total.Gte(rng.IntN(14)), total.Lt(rng.IntN(14))}
		default:
			s.conds = []cond.Cond[Item]{total.In(rng.IntN(14), rng.IntN(14), rng.IntN(14))}
		}
		s.order = [][]field.Component[Item]{{total}, {total.Desc()}, {rank}, {code}}[rng.IntN(4)]
	case 13:
		// An expression of the several values of an item, and the items that are ready.
		s.conds = []cond.Cond[Item]{upperTag.Eq(fmt.Sprintf("T%d", rng.IntN(4)))}
		if rng.IntN(3) == 0 {
			s.conds = append(s.conds, cond.Or(
				cond.And(ready.Eq(true), rank.Eq(rng.IntN(10))),
				cond.And(ready.Eq(true), rank.Eq(rng.IntN(10)))))
		}
		s.order = [][]field.Component[Item]{{rank}, {code}}[rng.IntN(2)]
	case 14:
		// A disjunction whose branches all say that the items are ready.
		s.conds = []cond.Cond[Item]{cond.Or(
			cond.And(ready.Eq(true), rank.Eq(rng.IntN(10))),
			cond.And(ready.Eq(true), score.Lt(rng.IntN(5))),
			ready.Eq(true))}
		s.order = [][]field.Component[Item]{{rank}, {rank.Desc()}}[rng.IntN(2)]
	case 0:
		s.conds = []cond.Cond[Item]{group.Eq(g())}
		s.order = [][]field.Component[Item]{{rank.Desc()}, {rank}, {code.Desc()}, {code}}[rng.IntN(4)]
	case 1:
		s.conds = []cond.Cond[Item]{group.In(g(), g())}
		s.order = [][]field.Component[Item]{{group, rank.Desc()}, {group.Desc(), rank}, {group, code.Desc()}}[rng.IntN(3)]
	case 2:
		s.conds = []cond.Cond[Item]{code.Gte(c()), code.Lt(c())}
		s.order = [][]field.Component[Item]{{code}, {code.Desc()}}[rng.IntN(2)]
	case 3:
		// The label is optional, so the index is read if the label is asked for.
		s.conds = []cond.Cond[Item]{score.Eq(rng.IntN(5)), label.Gte(fmt.Sprintf("l%d", rng.IntN(5)))}
		s.order = [][]field.Component[Item]{{label}, {label.Desc()}, {score.Desc(), label}}[rng.IntN(3)]
		if rng.IntN(3) == 0 {
			// The label is not asked for, so the index is not read.
			s.conds = s.conds[:1]
		}
	case 4:
		s.conds = []cond.Cond[Item]{note.Gte(fmt.Sprintf("n%d", rng.IntN(3)))}
		s.order = [][]field.Component[Item]{{note}, {note.Desc()}}[rng.IntN(2)]
	case 5:
		s.conds = []cond.Cond[Item]{group.Eq(g()), rank.Gt(rng.IntN(9))}
		s.order = [][]field.Component[Item]{{rank.Desc()}, {rank}}[rng.IntN(2)]
	case 6:
		s.conds = []cond.Cond[Item]{group.Eq(g()), code.Lte(c())}
		s.order = [][]field.Component[Item]{{code.Desc()}, {code}, {group, code.Desc()}}[rng.IntN(3)]
	case 7:
		// Nothing narrows an index, but the fields of some of them are all there is, so they
		// are read for their order when a limit makes that cheap.
		s.order = [][]field.Component[Item]{{code}, {code.Desc()}, {group, code.Desc()}, {group.Desc(), code}, {group}}[rng.IntN(5)]
		s.small = true
	default:
		s.conds = []cond.Cond[Item]{tags.Eq(fmt.Sprintf("t%d", rng.IntN(4)))}
		s.order = [][]field.Component[Item]{{rank}, {code}}[rng.IntN(2)]
	}
	if !s.small && rng.IntN(3) == 0 {
		s.conds = append(s.conds, randomLeaf(rng))
	}
	return s
}

func randomSpec(rng *rand.Rand) spec {
	var s spec
	switch {
	case rng.IntN(3) == 0:
		s = randomIndexSpec(rng)
	case rng.IntN(3) == 0:
		// A few comparisons, which indexes serve.
		for range 1 + rng.IntN(3) {
			s.conds = append(s.conds, randomLeaf(rng))
		}
	default:
		for range rng.IntN(5) {
			s.conds = append(s.conds, randomCond(rng, 2))
		}
	}
	if s.order == nil {
		if rng.IntN(3) == 0 {
			// An order that an index may have.
			s.order = indexOrders[rng.IntN(len(indexOrders))]
		} else {
			for range rng.IntN(3) {
				s.order = append(s.order, orderFields[rng.IntN(len(orderFields))])
			}
		}
	}
	s.offset = []int{0, 0, 0, 1, 5, 50, 1000}[rng.IntN(7)]
	if rng.IntN(2) == 0 {
		s.limited = true
		s.limit = []int{0, 1, 3, 10, 40, 1000}[rng.IntN(6)]
	}
	if s.small {
		s.limited, s.limit = true, []int{1, 3, 10}[rng.IntN(3)]
	}
	s.memo = &memo{}
	return s
}

// query makes the query of a spec, with the conditions given in two calls.
func (s spec) query(q *query.Query[int64, Item]) *query.Query[int64, Item] {
	half := len(s.conds) / 2
	q = q.Where(s.conds[:half]...).Where(s.conds[half:]...)
	if len(s.order) > 0 {
		q = q.OrderBy(s.order...)
	}
	if s.offset > 0 {
		q = q.Offset(s.offset)
	}
	if s.limited {
		q = q.Limit(s.limit)
	}
	return q
}

// expect returns the keys that the spec asks for, by checking and sorting all the items.
func (f *fixture) expect(t *testing.T, s spec) []int64 {
	t.Helper()

	type entry struct {
		id   int64
		keys []*[]byte
	}
	var found []entry
	for id := int64(1); id <= itemCount; id++ {
		it := f.items[id]
		ok := true
		for _, c := range s.conds {
			m, err := c.Match(it)
			require.NoError(t, err)
			ok = ok && m
		}
		if !ok {
			continue
		}

		e := entry{id: id}
		for _, o := range s.order {
			keys, err := o.Keys(it)
			require.NoError(t, err)
			if len(keys) == 0 {
				e.keys = append(e.keys, nil)
			} else {
				e.keys = append(e.keys, &keys[0])
			}
		}
		found = append(found, e)
	}

	slices.SortFunc(found, func(a, b entry) int {
		for i, o := range s.order {
			ka, kb := a.keys[i], b.keys[i]
			switch {
			case ka == nil && kb == nil:
			case ka == nil:
				// Without a value: first, or last for a descending field.
				if o.Descending() {
					return 1
				}
				return -1
			case kb == nil:
				if o.Descending() {
					return -1
				}
				return 1
			default:
				if c := bytes.Compare(*ka, *kb); c != 0 {
					return c
				}
			}
		}
		return cmp.Compare(a.id, b.id)
	})

	ids := make([]int64, 0, len(found))
	for _, e := range found {
		ids = append(ids, e.id)
	}
	if s.offset >= len(ids) {
		return []int64{}
	}
	ids = ids[s.offset:]
	if s.limited && s.limit < len(ids) {
		ids = ids[:s.limit]
	}
	return ids
}

// requireResult checks the keys that a query gave against the spec. With an order they are the
// keys, in the order. Without one they are in no order that is promised, so what is checked is
// that they are distinct, that every one satisfies the conditions, and how many there are: as
// many as the page asks for and the records that satisfy the conditions allow.
func (f *fixture) requireResult(t *testing.T, s spec, got []int64, msg string) {
	t.Helper()

	if s.memo.expected == nil {
		s.memo.expected = f.expect(t, s)
		s.memo.all = f.expect(t, spec{conds: s.conds})
	}
	if len(s.order) > 0 {
		require.Equal(t, s.memo.expected, got, msg)
		return
	}

	all := s.memo.all
	n := max(0, len(all)-s.offset)
	if s.limited {
		n = min(n, s.limit)
	}
	require.Len(t, got, n, msg)

	seen := map[int64]bool{}
	for _, key := range got {
		require.False(t, seen[key], "%s: %d twice", msg, key)
		seen[key] = true
		require.True(t, slices.Contains(all, key), "%s: %d does not satisfy the conditions", msg, key)
	}
}

// keys runs a query and returns the keys that it finds, and that the records it gives are theirs.
func (f *fixture) keys(t *testing.T, db kv.DB, q *query.Query[int64, Item]) []int64 {
	t.Helper()

	keys := []int64{}
	require.NoError(t, db.View(func(tx kv.Tx) error {
		it := q.Run(tx)
		defer it.Close()

		for ; it.Valid(); it.Next() {
			key, err := it.Key()
			require.NoError(t, err)
			record, err := it.Value()
			require.NoError(t, err)
			require.Equal(t, f.items[key].Code, record.Code)
			keys = append(keys, key)
		}
		return it.Err()
	}))
	return keys
}

// queryCount is the number of random queries.
const queryCount = 300

// conditionalIndexes are the indexes of some of the items, with what it takes for an item to be
// in them.
var conditionalIndexes = map[string]func(it *Item) bool{
	"ready_by_rank": func(it *Item) bool { return it.Ready },
	"ab_by_score":   func(it *Item) bool { return (it.Group == "a" || it.Group == "b") && it.Rank >= 3 },
}

// requireImplied checks that every item that satisfies the conditions of the spec is one that
// an index of some of the items has, which is what makes it right to read the index.
func (f *fixture) requireImplied(t *testing.T, s spec, index, msg string) {
	t.Helper()

	in, ok := conditionalIndexes[index]
	if !ok {
		return
	}
	for _, id := range f.expect(t, spec{conds: s.conds}) {
		require.True(t, in(f.items[id]), "%s: item %d satisfies the conditions and is not in %s", msg, id, index)
	}
}

// TestAgreesWithBruteForce checks the results of random queries, with the plan that the planner
// chooses and with every plan that can be forced, against checking and sorting all the items.
func TestAgreesWithBruteForce(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f := newFixture(t, db)
		indexes := []string{
			"by_group_rank", "by_score_label", "by_code", "by_group_code", "by_tag_rank", "by_note",
			"ready_by_rank", "ab_by_score", "by_total", "by_upper_tag",
		}

		var (
			rng      = rand.New(rand.NewPCG(1, 2))
			shapes   = map[string]int{}
			forced   = map[string]int{}
			chosen   = map[string]int{}
			nonEmpty int
		)
		for n := range queryCount {
			s := randomSpec(rng)
			want := f.expect(t, s)
			if len(want) > 0 {
				nonEmpty++
			}
			base := query.From(f.store)
			desc := fmt.Sprintf("query %d: %v order %v offset %d limit %d/%v", n, s.conds, orderTexts(s.order), s.offset, s.limit, s.limited)

			// The plan of the planner.
			q := s.query(base)
			explain, err := q.Explain()
			require.NoError(t, err, desc)
			for _, name := range indexes {
				if strings.Contains(explain, "IndexLookup "+name) {
					chosen[name]++
					f.requireImplied(t, s, name, desc+"\n"+explain)
				}
			}
			if strings.HasPrefix(explain, "Limit") && strings.Contains(explain, "└─ IndexLookup") &&
				strings.Contains(explain, "(in order of") {
				shapes["a limit over an index in order"]++
			}
			for _, shape := range []string{"Empty", "StoreScan", "IndexLookup", "Sort by", "TopN by", "reversed", "in order of", "Limit", "Offset", "then check"} {
				if strings.Contains(explain, shape) {
					shapes[shape]++
				}
			}
			f.requireResult(t, s, f.keys(t, db, q), desc+"\n"+explain)
			// A query is planned once, and gives the same again.
			again, err := q.Explain()
			require.NoError(t, err)
			require.Equal(t, explain, again)
			f.requireResult(t, s, f.keys(t, db, q), desc+"\n"+explain)

			// Every plan that can be forced.
			scan := s.query(base).FullScan()
			explain, err = scan.Explain()
			require.NoError(t, err, desc)
			require.True(t, strings.Contains(explain, "StoreScan") || explain == "Empty", explain)
			f.requireResult(t, s, f.keys(t, db, scan), desc+"\n"+explain)

			for _, name := range indexes {
				forcedQuery := s.query(base).Using(name)
				explain, err := forcedQuery.Explain()
				if err != nil {
					require.ErrorIs(t, err, query.ErrHint, desc)
					forced[name+" refused"]++
					continue
				}
				if explain != "Empty" {
					require.Contains(t, explain, "IndexLookup "+name, desc)
					f.requireImplied(t, s, name, desc+"\n"+explain)
				}
				forced[name]++
				for _, shape := range []string{"reversed", "in order of"} {
					if strings.Contains(explain, shape) {
						forced[shape]++
					}
				}
				f.requireResult(t, s, f.keys(t, db, forcedQuery), desc+"\n"+explain)
			}
		}

		t.Logf("plans %v, chosen %v, forced %v", shapes, chosen, forced)

		// The queries ask for a good deal of everything.
		require.Greater(t, nonEmpty, queryCount/3)
		for _, shape := range []string{"Empty", "StoreScan", "IndexLookup", "Sort by", "TopN by", "in order of", "Limit", "Offset", "then check"} {
			require.Greater(t, shapes[shape], 5, "%s in %v", shape, shapes)
		}
		require.Greater(t, shapes["a limit over an index in order"], 5, "%v", shapes)
		// The label of an item is sometimes missing: the index of it is refused where the query
		// does not need a label, and read where it does.
		require.Greater(t, forced["by_score_label refused"], 50, "%v", forced)
		// Reading an index from its end is chosen less, as only a unique index can be read so,
		// but forced it is checked as well.
		require.Greater(t, forced["reversed"], 10, "%v", forced)
		require.Greater(t, forced["in order of"], 30, "%v", forced)
		for _, name := range indexes[:6] {
			require.Greater(t, forced[name], 10, "%s in %v", name, forced)
		}
		// The indexes of some of the items are read when the conditions imply theirs, forced or
		// chosen, and refused when they do not; the indexes of expressions are read when a
		// condition needs a value of the expression.
		for _, name := range indexes[6:] {
			require.Greater(t, forced[name], 10, "%s in %v", name, forced)
			require.Greater(t, forced[name+" refused"], 10, "%s in %v", name, forced)
			require.Greater(t, chosen[name], 5, "%s in %v", name, chosen)
		}
	})
}

func orderTexts(order []field.Component[Item]) []string {
	var texts []string
	for _, o := range order {
		text := o.Path()
		if o.Descending() {
			text += " desc"
		}
		texts = append(texts, text)
	}
	return texts
}
