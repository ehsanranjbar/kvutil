package query_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/query"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/ehsanranjbar/kvutil/store/record"
)

// The tests of the projections that decode part of the records use types of their own: the
// partial decoders of the codecs are limited in number for each type, and the oracles of the
// other tests use up the ones of Item.

// Ticket is a record of a few fields that a projection selects, checks and sorts by.
type Ticket struct {
	Code  string
	Group string
	Rank  int
	Score int
	Extra int
}

var (
	tCode  = field.Path[Ticket, string]("Code")
	tGroup = field.Path[Ticket, string]("Group")
	tRank  = field.Path[Ticket, int]("Rank")
	tScore = field.Path[Ticket, int]("Score")
	tExtra = field.Path[Ticket, int]("Extra")
)

const ticketCount = 300

type ticketRow = query.Row[int64, Ticket]

// ticketFixture is a store of tickets, and the tickets that are in it.
type ticketFixture struct {
	store  *extstore.Store[int64, Ticket]
	items  map[int64]*Ticket
	counts *decodeCounts
}

// newTickets fills a database with tickets, whose decoding is counted.
func newTickets(t *testing.T, db kv.DB) *ticketFixture {
	t.Helper()

	f := &ticketFixture{items: map[int64]*Ticket{}, counts: &decodeCounts{}}
	f.store = extstore.NewWithCodecs[int64, Ticket]("tickets", nil,
		countingCodec[Ticket]{ValueCodec: codec.ValueFor[Ticket](nil), counts: f.counts})
	index.Composite(f.store, "by_group_rank", field.List(tGroup, tRank.Desc()))
	require.NoError(t, f.store.Check(db))

	require.NoError(t, db.Update(func(tx kv.Tx) error {
		ins := f.store.Instantiate(tx)
		for id := int64(1); id <= ticketCount; id++ {
			tk := &Ticket{
				Code:  fmt.Sprintf("t%04d", id),
				Group: string(rune('a' + id%4)),
				Rank:  int(id % 10),
				Score: int(id/3) % 5,
				Extra: int(id % 6),
			}
			f.items[id] = tk
			if err := ins.Set(id, tk); err != nil {
				return err
			}
		}
		return nil
	}))
	f.counts.reset()
	return f
}

// matching returns the ids of the tickets that satisfy a condition, in order of id.
func (f *ticketFixture) matching(match func(*Ticket) bool) []int64 {
	var ids []int64
	for id, tk := range f.items {
		if match(tk) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

// sorted sorts ids by a comparison of their tickets.
func (f *ticketFixture) sorted(ids []int64, cmp func(a, b *Ticket) int) []int64 {
	slices.SortFunc(ids, func(a, b int64) int { return cmp(f.items[a], f.items[b]) })
	return ids
}

// viewRows runs a projection, calls each for its rows, and then done with all of them, within the
// transaction. It returns the keys of the rows in order.
func viewRows(
	t *testing.T,
	db kv.DB,
	p *query.Projection[int64, Ticket],
	each func(n int, row *ticketRow),
	done func(rows []*ticketRow),
) []int64 {
	t.Helper()

	keys := []int64{}
	require.NoError(t, db.View(func(tx kv.Tx) error {
		it := p.Run(tx)
		defer it.Close()
		var rows []*ticketRow
		for n := 0; it.Valid(); it.Next() {
			key, err := it.Key()
			require.NoError(t, err)
			row, err := it.Value()
			require.NoError(t, err)
			require.Equal(t, key, row.Key())
			if each != nil {
				each(n, row)
			}
			keys, rows = append(keys, key), append(rows, row)
			n++
		}
		if err := it.Err(); err != nil {
			return err
		}
		if done != nil {
			done(rows)
		}
		return nil
	}))
	return keys
}

func TestProjectionDecodesNoFullRecord(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f := newTickets(t, db)
		from := func() *query.Query[int64, Ticket] { return query.From(f.store) }

		inGroup := func(g string) int {
			return len(f.matching(func(tk *Ticket) bool { return tk.Group == g }))
		}
		extraIs2 := f.matching(func(tk *Ticket) bool { return tk.Extra == 2 })
		require.NotEmpty(t, extraIs2)

		scoreAndCode := func(row *ticketRow, tk *Ticket) {
			s, ok := row.Get(tScore)
			require.True(t, ok)
			require.Equal(t, tk.Score, s)
			c, ok := row.Get(tCode)
			require.True(t, ok)
			require.Equal(t, tk.Code, c)
			_, ok = row.Get(tExtra)
			require.False(t, ok)
		}
		extraOnly := func(row *ticketRow, tk *Ticket) {
			e, ok := row.Get(tExtra)
			require.True(t, ok)
			require.Equal(t, tk.Extra, e)
			_, ok = row.Get(tScore)
			require.False(t, ok)
		}
		rankOnly := func(row *ticketRow, tk *Ticket) {
			r, ok := row.Get(tRank)
			require.True(t, ok)
			require.Equal(t, tk.Rank, r)
		}

		byScoreDescCode := func(a, b *Ticket) int {
			if a.Score != b.Score {
				return b.Score - a.Score
			}
			return strings.Compare(a.Code, b.Code)
		}
		byScoreCode := func(a, b *Ticket) int {
			if a.Score != b.Score {
				return a.Score - b.Score
			}
			return strings.Compare(a.Code, b.Code)
		}
		extraLt3 := f.matching(func(tk *Ticket) bool { return tk.Extra < 3 })
		page := f.sorted(slices.Clone(extraLt3), byScoreDescCode)[3:13]
		inB := f.matching(func(tk *Ticket) bool { return tk.Group == "b" })
		topB := f.sorted(slices.Clone(inB), byScoreCode)[:5]

		for _, test := range []struct {
			name string
			p    *query.Projection[int64, Ticket]
			// text is what the plan shows.
			text string
			// want are the keys of the rows, in order if ordered is set.
			want    []int64
			ordered bool
			// candidates are the records that are decoded in part.
			candidates int
			check      func(row *ticketRow, tk *Ticket)
		}{
			{
				name: "a scan with a check",
				p:    from().Where(tExtra.Eq(2)).FullScan().Select(tScore, tCode),
				text: "StoreScan, then check [Extra = 2]",
				want: extraIs2, candidates: ticketCount, check: scoreAndCode,
			},
			{
				name: "an index with a check that it leaves",
				p:    from().Where(tGroup.Eq("a"), tExtra.Eq(2)).Using("by_group_rank").Select(tScore, tCode),
				text: "IndexLookup by_group_rank: 1 range, then check [Extra = 2]",
				want: f.matching(func(tk *Ticket) bool { return tk.Group == "a" && tk.Extra == 2 }), candidates: inGroup("a"),
				check: scoreAndCode,
			},
			{
				name: "an order that the store does not give, on fields that are not selected",
				p:    from().Where(tExtra.Lt(3)).FullScan().OrderBy(tScore.Desc(), tCode).Offset(3).Limit(10).Select(tExtra),
				text: "Limit 10, offset 3\n└─ TopN by Score desc, Code\n   └─ StoreScan, then check [Extra < 3]",
				want: page, ordered: true, candidates: ticketCount, check: extraOnly,
			},
			{
				name: "an order that an index does not give, without a check",
				p:    from().Where(tGroup.Eq("b")).Using("by_group_rank").OrderBy(tScore, tCode).Limit(5).Select(tRank),
				text: "Limit 5\n└─ TopN by Score, Code\n   └─ IndexLookup by_group_rank: 1 range",
				want: topB, ordered: true, candidates: inGroup("b"), check: rankOnly,
			},
		} {
			text, err := test.p.Explain()
			require.NoError(t, err, test.name)
			require.Equal(t, test.text, text, test.name)

			f.counts.reset()
			var asking []int64
			keys := viewRows(t, db, test.p, func(n int, row *ticketRow) {
				test.check(row, f.items[row.Key()])
				if n%3 == 0 && len(asking) < 4 {
					asking = append(asking, row.Key())
				}
			}, func(rows []*ticketRow) {
				// The rows have been read, none in full.
				require.Zero(t, f.counts.full.Load(), test.name)
				require.EqualValues(t, test.candidates, f.counts.partial.Load(), test.name)

				// The record of a row is got from the store when it is asked for, whole, once.
				asked := 0
				for _, row := range rows {
					if !slices.Contains(asking, row.Key()) {
						continue
					}
					record, err := row.Record()
					require.NoError(t, err, test.name)
					require.Equal(t, *f.items[row.Key()], *record, test.name)
					again, err := row.Record()
					require.NoError(t, err, test.name)
					require.Same(t, record, again, test.name)
					asked++
					require.EqualValues(t, asked, f.counts.full.Load(), test.name)
				}
				require.Len(t, asking, asked, test.name)
				require.NotZero(t, asked, test.name)
			})

			if !test.ordered {
				slices.Sort(keys)
			}
			require.Equal(t, test.want, keys, test.name)
		}
	})
}

func TestCountDecodesPartially(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f := newTickets(t, db)
		count := func(q *query.Query[int64, Ticket]) int {
			t.Helper()
			f.counts.reset()
			var n int
			require.NoError(t, db.View(func(tx kv.Tx) (err error) {
				n, err = q.Count(tx)
				return err
			}))
			return n
		}

		// A residual condition reads the fields that it needs of each record, and nothing is
		// decoded in full.
		extraIs2 := f.matching(func(tk *Ticket) bool { return tk.Extra == 2 })
		q := query.From(f.store).Where(tExtra.Eq(2)).FullScan()
		require.Equal(t, len(extraIs2), count(q))
		require.Zero(t, f.counts.full.Load())
		require.EqualValues(t, ticketCount, f.counts.partial.Load())

		inGroup := len(f.matching(func(tk *Ticket) bool { return tk.Group == "a" }))
		want := len(f.matching(func(tk *Ticket) bool { return tk.Group == "a" && tk.Score == 1 }))
		require.NotZero(t, want)
		q = query.From(f.store).Where(tGroup.Eq("a"), tScore.Eq(1)).Using("by_group_rank")
		text, err := q.Select().Explain()
		require.NoError(t, err)
		require.Equal(t, "IndexLookup by_group_rank: 1 range, then check [Score = 1]", text)
		require.Equal(t, want, count(q))
		require.Zero(t, f.counts.full.Load())
		require.EqualValues(t, inGroup, f.counts.partial.Load())

		// A scan with nothing to check reads nothing of a record: the decoder decodes nothing, and
		// the codec is not even asked for one.
		q = query.From(f.store).FullScan()
		text, err = q.Select().Explain()
		require.NoError(t, err)
		require.Equal(t, "StoreScan", text)
		require.Equal(t, ticketCount, count(q))
		require.Zero(t, f.counts.full.Load())
		require.Zero(t, f.counts.partial.Load())

		// What a function reads is not known: the records are decoded in full to be checked.
		undeclared := cond.Func("extra is 2", func(tk *Ticket) bool { return tk.Extra == 2 })
		q = query.From(f.store).Where(undeclared).FullScan()
		require.Equal(t, len(extraIs2), count(q))
		require.EqualValues(t, ticketCount, f.counts.full.Load())
		require.Zero(t, f.counts.partial.Load())

		declared := cond.Func("extra is 2", func(tk *Ticket) bool { return tk.Extra == 2 }).Reading("Extra")
		q = query.From(f.store).Where(declared).FullScan()
		require.Equal(t, len(extraIs2), count(q))
		require.Zero(t, f.counts.full.Load())
		require.EqualValues(t, ticketCount, f.counts.partial.Load())

		// The offset and the limit apply to the records that are found.
		q = query.From(f.store).Where(tExtra.Eq(2)).FullScan().OrderBy(tCode).Offset(2).Limit(5)
		require.Equal(t, 5, count(q))
		require.Zero(t, f.counts.full.Load())
	})
}

func TestProjectionOfWhatIsNotKnownToBeReadDecodesInFull(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f := newTickets(t, db)
		from := func() *query.Query[int64, Ticket] { return query.From(f.store) }

		doubled := field.Of("Doubled", func(tk *Ticket) int { return tk.Score * 2 })
		doubledDeclared := field.Of("Doubled", func(tk *Ticket) int { return tk.Score * 2 }).Reading("Score")
		byRank := field.Of("ByRank", func(tk *Ticket) int { return tk.Rank })
		accepted := f.matching(func(tk *Ticket) bool { return tk.Extra == 2 })
		require.NotEmpty(t, accepted)

		// A field of a function that does not say what it reads: the check reads the fields of the
		// condition only, and the records that satisfy it are decoded in full, as before, and held
		// by the rows.
		f.counts.reset()
		viewRows(t, db, from().Where(tExtra.Eq(2)).FullScan().Select(doubled),
			func(n int, row *ticketRow) {
				d, ok := row.Get(doubled)
				require.True(t, ok)
				require.Equal(t, f.items[row.Key()].Score*2, d)
			}, func(rows []*ticketRow) {
				require.EqualValues(t, len(accepted), f.counts.full.Load())
				require.EqualValues(t, ticketCount, f.counts.partial.Load())
				// The rows hold the records: none is got.
				record, err := rows[0].Record()
				require.NoError(t, err)
				require.Equal(t, *f.items[rows[0].Key()], *record)
				require.EqualValues(t, len(accepted), f.counts.full.Load())
			})

		// Declared, it is decoded in part.
		f.counts.reset()
		viewRows(t, db, from().Where(tExtra.Eq(2)).FullScan().Select(doubledDeclared),
			func(n int, row *ticketRow) {
				d, ok := row.Get(doubledDeclared)
				require.True(t, ok)
				require.Equal(t, f.items[row.Key()].Score*2, d)
			}, func(rows []*ticketRow) {
				require.Zero(t, f.counts.full.Load())
				require.EqualValues(t, ticketCount, f.counts.partial.Load())
			})

		// An order by such a field makes the records be decoded in full as well.
		f.counts.reset()
		sorted := from().Where(tExtra.Eq(2)).FullScan().OrderBy(byRank, tCode).Select(tScore)
		text, err := sorted.Explain()
		require.NoError(t, err)
		require.Contains(t, text, "Sort by ByRank, Code")
		keys := viewRows(t, db, sorted, func(n int, row *ticketRow) {
			s, ok := row.Get(tScore)
			require.True(t, ok)
			require.Equal(t, f.items[row.Key()].Score, s)
		}, nil)
		require.Len(t, keys, len(accepted))
		require.EqualValues(t, len(accepted), f.counts.full.Load())
		require.EqualValues(t, ticketCount, f.counts.partial.Load())
	})
}

func TestProjectionOfARecordStoreSetsIds(t *testing.T) {
	var (
		age  = field.Path[Pet, int]("Age")
		kind = field.Path[Pet, string]("Kind")
		// An item that is the id of the record, which is set and not decoded.
		id = field.Of("ID", func(p *Pet) int64 { return p.GetId() }).Reading()
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
			for i := int64(1); i <= count; i++ {
				p := &Pet{id: i, Name: "pet", Age: int(i % 7), Kind: []string{"cat", "dog", "owl"}[i%3]}
				if err := ins.Set(p); err != nil {
					return err
				}
				want[i] = *p
			}
			return nil
		}))

		odd := cond.Func("odd id", (*Pet).isOdd).Reading()
		for _, test := range []struct {
			name       string
			p          *query.Projection[int64, Pet]
			match      func(id int64, p Pet) bool
			candidates func(id int64, p Pet) bool
		}{
			{
				"scan", query.From(pets).Where(odd, age.Gt(2)).FullScan().Select(id, age),
				func(id int64, p Pet) bool { return id%2 == 1 && p.Age > 2 },
				func(int64, Pet) bool { return true },
			},
			{
				"index", query.From(pets).Where(kind.Eq("dog"), odd).Using("by_kind").Select(id, age),
				func(id int64, p Pet) bool { return p.Kind == "dog" && id%2 == 1 },
				func(_ int64, p Pet) bool { return p.Kind == "dog" },
			},
			{
				"sorted by a field", query.From(pets).Where(odd).FullScan().OrderBy(age.Desc()).Select(id),
				func(id int64, p Pet) bool { return id%2 == 1 },
				func(int64, Pet) bool { return true },
			},
		} {
			counts.reset()
			var keys []int64
			var rows []*query.Row[int64, Pet]
			require.NoError(t, db.View(func(tx kv.Tx) error {
				it := test.p.Run(tx)
				defer it.Close()
				for ; it.Valid(); it.Next() {
					key, err := it.Key()
					require.NoError(t, err)
					row, err := it.Value()
					require.NoError(t, err)
					require.Equal(t, key, row.Key(), test.name)
					got, ok := row.Get(id)
					require.True(t, ok, test.name)
					require.Equal(t, key, got, test.name)
					keys, rows = append(keys, key), append(rows, row)
				}
				if err := it.Err(); err != nil {
					return err
				}

				var wantKeys []int64
				candidates := 0
				for i := int64(1); i <= count; i++ {
					if test.candidates(i, want[i]) {
						candidates++
					}
					if test.match(i, want[i]) {
						wantKeys = append(wantKeys, i)
					}
				}
				slices.Sort(keys)
				require.Equal(t, wantKeys, keys, test.name)
				require.Zero(t, counts.full.Load(), test.name)
				require.EqualValues(t, candidates, counts.partial.Load(), test.name)

				// The record of a row is the whole one, with its id.
				for _, row := range rows[:3] {
					record, err := row.Record()
					require.NoError(t, err, test.name)
					require.Equal(t, row.Key(), record.GetId(), test.name)
					require.Equal(t, want[row.Key()], *record, test.name)
				}
				require.EqualValues(t, 3, counts.full.Load(), test.name)
				return nil
			}), test.name)
		}
	})
}
