package query_test

import (
	"slices"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/internal/decoding"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/query"
	"github.com/ehsanranjbar/kvutil/store/record"
)

// decodeCounts counts how the records are decoded.
type decodeCounts struct {
	full    atomic.Int64
	partial atomic.Int64
}

func (c *decodeCounts) reset() {
	c.full.Store(0)
	c.partial.Store(0)
}

// countingCodec is a value codec that counts the records decoded in full and those decoded in part.
type countingCodec[V any] struct {
	codec.ValueCodec[V]
	counts *decodeCounts
}

// Decode implements the codec.ValueCodec interface.
func (c countingCodec[V]) Decode(bz []byte) (V, error) {
	c.counts.full.Add(1)
	return c.ValueCodec.Decode(bz)
}

// PartialDecode implements the codec.PartialDecoder interface.
func (c countingCodec[V]) PartialDecode(fields []string) (func(bz []byte, v *V) error, bool) {
	decode, ok := codec.PartialOf(c.ValueCodec, fields)
	if !ok {
		return nil, false
	}
	return func(bz []byte, v *V) error {
		c.counts.partial.Add(1)
		return decode(bz, v)
	}, true
}

// countingItems fills a fixture whose items are decoded as the counts say. wrap, if it is not nil,
// changes the codec after counting.
func countingItems(
	t *testing.T,
	db kv.DB,
	wrap func(codec.ValueCodec[Item]) codec.ValueCodec[Item],
) (*fixture, *decodeCounts) {
	t.Helper()

	counts := &decodeCounts{}
	var c codec.ValueCodec[Item] = countingCodec[Item]{ValueCodec: codec.ValueFor[Item](nil), counts: counts}
	if wrap != nil {
		c = wrap(c)
	}
	f := newFixtureWith(t, db, c)
	counts.reset()
	return f, counts
}

// run runs a query and returns the ids and the records that it gives, in order.
func run(t *testing.T, db kv.DB, q *query.Query[int64, Item]) (ids []int64, items []*Item) {
	t.Helper()

	require.NoError(t, db.View(func(tx kv.Tx) error {
		it := q.Run(tx)
		defer it.Close()
		for ; it.Valid(); it.Next() {
			id, err := it.Key()
			require.NoError(t, err)
			item, err := it.Value()
			require.NoError(t, err)
			ids, items = append(ids, id), append(items, item)
		}
		return it.Err()
	}))
	return ids, items
}

// matching returns the ids of the items that satisfy a condition, in order.
func (f *fixture) matching(match func(*Item) bool) []int64 {
	var ids []int64
	for id, it := range f.items {
		if match(it) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

// requireItems checks that the query gives the items with the ids, in any order, as they are
// stored in full.
func (f *fixture) requireItems(t *testing.T, db kv.DB, q *query.Query[int64, Item], want []int64) {
	t.Helper()

	ids, items := run(t, db, q)
	require.Len(t, ids, len(want))
	for i, id := range ids {
		require.Equal(t, *f.items[id], *items[i], "item %d", id)
	}
	slices.Sort(ids)
	require.Equal(t, want, ids)
}

// twoSteps makes the filters decode the records in two steps, the fields that the conditions
// read and then the records that satisfy them, whatever the costs of the decodes are: for a test
// that counts what two steps decode, which would otherwise depend on how fast the machine is.
func twoSteps(t testing.TB) { t.Cleanup(decoding.SetAdaptive(false)) }

func TestResidualCheckDecodesPartially(t *testing.T) {
	twoSteps(t)
	scoreIn := func(it *Item) bool { return it.Score >= 1 && it.Score <= 2 }

	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f, counts := countingItems(t, db, nil)
		want := f.matching(scoreIn)
		require.Greater(t, len(want), 20)
		require.Less(t, len(want), itemCount-20)

		// A scan reads the fields of the condition of every record, and the whole of the ones that
		// satisfy it.
		q := query.From(f.store).Where(score.Between(1, 2)).FullScan()
		text, err := q.Explain()
		require.NoError(t, err)
		require.Equal(t, "StoreScan, then check [Score >= 1 and Score <= 2]", text)
		f.requireItems(t, db, q, want)
		require.EqualValues(t, itemCount, counts.partial.Load())
		require.EqualValues(t, len(want), counts.full.Load())

		// Sorting and paging the records read them in full, once.
		counts.reset()
		ids, _ := run(t, db, q.OrderBy(extra, code).Limit(7))
		require.Len(t, ids, 7)
		require.EqualValues(t, itemCount, counts.partial.Load())
		require.EqualValues(t, len(want), counts.full.Load())

		// An index leaves the condition that it does not serve to be checked on the records.
		counts.reset()
		q = query.From(f.store).Where(group.Eq("a"), extra.Eq(2)).Using("by_group_rank")
		text, err = q.Explain()
		require.NoError(t, err)
		require.Equal(t, "IndexLookup by_group_rank: 1 range, then check [Extra = 2]", text)
		inGroup := f.matching(func(it *Item) bool { return it.Group == "a" })
		want = f.matching(func(it *Item) bool { return it.Group == "a" && it.Extra == 2 })
		require.NotEmpty(t, want)
		f.requireItems(t, db, q, want)
		require.EqualValues(t, len(inGroup), counts.partial.Load())
		require.EqualValues(t, len(want), counts.full.Load())

		// A condition on a field that is not a field of the struct, like one of SQL, reads the
		// fields that it says.
		counts.reset()
		q = query.From(f.store).Where(total.Gt(9)).FullScan()
		f.requireItems(t, db, q, f.matching(func(it *Item) bool { return it.Rank+it.Score > 9 }))
		require.EqualValues(t, itemCount, counts.partial.Load())
	})
}

func TestFunctionsAreDecodedInFullUnlessTheyDeclareTheirReads(t *testing.T) {
	twoSteps(t)
	scoreIn := func(it *Item) bool { return it.Score >= 1 && it.Score <= 2 }

	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f, counts := countingItems(t, db, nil)
		want := f.matching(scoreIn)

		// What a function reads is not known: every record is decoded in full.
		undeclared := cond.Func("score in 1..2", scoreIn)
		f.requireItems(t, db, query.From(f.store).Where(undeclared).FullScan(), want)
		require.Zero(t, counts.partial.Load())
		require.EqualValues(t, itemCount, counts.full.Load())

		// A function that says what it reads is checked on the fields that it reads.
		counts.reset()
		declared := cond.Func("score in 1..2", scoreIn).Reading("Score")
		f.requireItems(t, db, query.From(f.store).Where(declared).FullScan(), want)
		require.EqualValues(t, itemCount, counts.partial.Load())
		require.EqualValues(t, len(want), counts.full.Load())

		// One of the conditions that is not known makes all of the check decoded in full.
		counts.reset()
		f.requireItems(t, db, query.From(f.store).Where(score.Between(1, 2), cond.Func("any", func(*Item) bool { return true })).FullScan(), want)
		require.Zero(t, counts.partial.Load())
		require.EqualValues(t, itemCount, counts.full.Load())

		// A function that reads nothing of the record sees the zero value of it, and nothing is
		// decoded to check it (the codec is not asked for a decoder), so every record is decoded
		// in full only because all of them satisfy the function.
		counts.reset()
		nothing := cond.Func("zero score", func(it *Item) bool { return it.Score == 0 && it.Code == "" }).Reading()
		ids, _ := run(t, db, query.From(f.store).Where(nothing).FullScan())
		require.Len(t, ids, itemCount)
		require.Zero(t, counts.partial.Load())
		require.EqualValues(t, itemCount, counts.full.Load())
	})
}

func TestCodecWithoutPartialDecoding(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f, counts := countingItems(t, db, codec.WithoutPartial[Item])
		want := f.matching(func(it *Item) bool { return it.Score >= 1 && it.Score <= 2 })

		f.requireItems(t, db, query.From(f.store).Where(score.Between(1, 2)).FullScan(), want)
		require.Zero(t, counts.partial.Load())
		require.EqualValues(t, itemCount, counts.full.Load())
	})
}

func TestCombinedPlansDecodePartially(t *testing.T) {
	twoSteps(t)
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f, counts := countingItems(t, db, nil)

		// A union reads each branch with the check of the query.
		q := query.From(f.store).Where(cond.Or(group.Eq("a"), code.Eq("i0005")), extra.Eq(3))
		text, err := q.Explain()
		require.NoError(t, err)
		require.Contains(t, text, "Union")
		require.Contains(t, text, "then check [Extra = 3]")
		want := f.matching(func(it *Item) bool { return (it.Group == "a" || it.Code == "i0005") && it.Extra == 3 })
		require.NotEmpty(t, want)
		f.requireItems(t, db, q, want)
		require.Greater(t, counts.partial.Load(), int64(len(want)))
		require.EqualValues(t, len(want), counts.full.Load())

		// An intersection streams the first index and checks what is left of the conditions.
		counts.reset()
		q = query.From(f.store).Where(group.Eq("a"), tags.Eq("t1"), extra.Lt(3)).UsingAll("by_group_rank", "by_tag_rank")
		text, err = q.Explain()
		require.NoError(t, err)
		require.Contains(t, text, "Intersect")
		require.Contains(t, text, "then check")
		want = f.matching(func(it *Item) bool {
			return it.Group == "a" && slices.Contains(it.Tags, "t1") && it.Extra < 3
		})
		require.NotEmpty(t, want)
		f.requireItems(t, db, q, want)
		require.GreaterOrEqual(t, counts.partial.Load(), int64(len(want)))
		require.EqualValues(t, len(want), counts.full.Load())
	})
}

// Pet is a record that knows its id, which is not part of what is stored.
type Pet struct {
	id   int64
	Name string
	Age  int
	Kind string
}

func (p *Pet) GetId() int64           { return p.id }
func (p *Pet) SetId(i int64)          { p.id = i }
func (p *Pet) isOdd() bool            { return p.id%2 == 1 }
func (p *Pet) idAtLeast(n int64) bool { return p.id >= n }

func TestRecordStoreSetsIdsOnPartialRecords(t *testing.T) {
	twoSteps(t)
	var (
		age  = field.Path[Pet, int]("Age")
		kind = field.Path[Pet, string]("Kind")
	)
	const count = 200

	testkv.Each(t, func(t *testing.T, db kv.DB) {
		counts := &decodeCounts{}
		pets := record.NewWithCodecs[int64, Pet]("pets", nil,
			countingCodec[Pet]{ValueCodec: codec.ValueFor[Pet](nil), counts: counts})
		index.On(pets, "by_kind", kind)
		require.NoError(t, pets.Check(db))

		want := map[int64]Pet{}
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			ins := pets.Instantiate(tx)
			for id := int64(1); id <= count; id++ {
				p := &Pet{id: id, Name: "pet", Age: int(id % 7), Kind: []string{"cat", "dog", "owl"}[id%3]}
				if err := ins.Set(p); err != nil {
					return err
				}
				want[id] = *p
			}
			return nil
		}))
		counts.reset()

		// The condition reads the id, which is not decoded but set.
		oddAndOld := cond.Func("odd id", (*Pet).isOdd).Reading()
		atLeast := cond.Func("id >= 50", func(p *Pet) bool { return p.idAtLeast(50) }).Reading()

		for _, test := range []struct {
			name  string
			q     *query.Query[int64, Pet]
			match func(id int64, p Pet) bool
			// candidates are the records that are checked.
			candidates func(id int64, p Pet) bool
		}{
			{
				"scan", query.From(pets).Where(oddAndOld, atLeast, age.Gt(2)).FullScan(),
				func(id int64, p Pet) bool { return id%2 == 1 && id >= 50 && p.Age > 2 },
				func(int64, Pet) bool { return true },
			},
			{
				"index", query.From(pets).Where(kind.Eq("dog"), oddAndOld, age.Gt(2)).Using("by_kind"),
				func(id int64, p Pet) bool { return p.Kind == "dog" && id%2 == 1 && p.Age > 2 },
				func(_ int64, p Pet) bool { return p.Kind == "dog" },
			},
		} {
			counts.reset()
			var ids []int64
			require.NoError(t, db.View(func(tx kv.Tx) error {
				it := test.q.Run(tx)
				defer it.Close()
				for ; it.Valid(); it.Next() {
					id, err := it.Key()
					require.NoError(t, err)
					p, err := it.Value()
					require.NoError(t, err)
					// The record that comes out is whole, with its id.
					require.Equal(t, id, p.GetId(), test.name)
					require.Equal(t, want[id], *p, test.name)
					ids = append(ids, id)
				}
				return it.Err()
			}), test.name)

			var wantIDs []int64
			candidates := 0
			for id := int64(1); id <= count; id++ {
				if test.candidates(id, want[id]) {
					candidates++
				}
				if test.match(id, want[id]) {
					wantIDs = append(wantIDs, id)
				}
			}
			require.NotEmpty(t, wantIDs, test.name)
			slices.Sort(ids)
			require.Equal(t, wantIDs, ids, test.name)
			require.EqualValues(t, candidates, counts.partial.Load(), test.name)
			require.EqualValues(t, len(wantIDs), counts.full.Load(), test.name)
		}
	})
}

func TestFiltersChooseHowToDecode(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f, counts := countingItems(t, db, nil)
		all := cond.Func("all", func(*Item) bool { return true }).Reading("Score")
		none := cond.Func("none", func(*Item) bool { return false }).Reading("Score")

		// When every record satisfies the conditions, decoding their fields first is a waste:
		// after the first records the filter decodes them in full at once, each once.
		q := query.From(f.store).Where(all).FullScan()
		f.requireItems(t, db, q, f.matching(func(*Item) bool { return true }))
		require.EqualValues(t, itemCount, counts.full.Load())
		require.Less(t, counts.partial.Load(), int64(itemCount/2))

		// When none does, the fields are all that is decoded.
		counts.reset()
		f.requireItems(t, db, query.From(f.store).Where(none).FullScan(), nil)
		require.EqualValues(t, itemCount, counts.partial.Load())
		require.Zero(t, counts.full.Load())

		// Without the choice, the records are decoded in two steps whatever they cost.
		counts.reset()
		restore := decoding.SetAdaptive(false)
		f.requireItems(t, db, q, f.matching(func(*Item) bool { return true }))
		restore()
		require.EqualValues(t, itemCount, counts.partial.Load())
		require.EqualValues(t, itemCount, counts.full.Load())
	})
}
