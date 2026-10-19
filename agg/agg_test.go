package agg_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/agg"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/codec/lex"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/badgerkv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/ehsanranjbar/kvutil/kv/prefix"
	"github.com/ehsanranjbar/kvutil/migrate"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/stretchr/testify/require"
)

type order struct {
	Customer string
	Amount   int64
	At       time.Time
	Draft    bool // a draft is not counted
}

// amount is what an order contributes to the aggregations of amounts, by its customer.
func amount(o *order) (string, int64, bool) {
	return o.Customer, o.Amount, !o.Draft
}

func customer(o *order) (string, bool) {
	return o.Customer, !o.Draft
}

func at(o *order) time.Time {
	return o.At
}

// median returns the middle one of the amounts.
func median(amounts iterutil.AdvancedIterator[time.Time, int64]) (int64, error) {
	values, err := amounts.Values()
	if err != nil {
		return 0, err
	}
	slices.Sort(values)
	return values[len(values)/2], nil
}

func day(d int) time.Time {
	return time.Date(2026, 3, d, 0, 0, 0, 0, time.UTC)
}

// fixture is a store of orders with aggregations of both kinds.
type fixture struct {
	orders *extstore.Store[int64, order]

	total  *agg.Aggregation[int64, order, string, int64, int64]
	count  *agg.Aggregation[int64, order, string, int64, int64]
	avg    *agg.Aggregation[int64, order, string, int64, agg.Average[int64]]
	max    *agg.Aggregation[int64, order, string, int64, int64]
	min    *agg.Aggregation[int64, order, string, int64, int64]
	median *agg.Aggregation[int64, order, string, int64, int64]
}

func newFixture() *fixture {
	f := &fixture{orders: extstore.New[int64, order]("orders")}

	// Folded from what the orders add and remove.
	f.total = agg.Sum(f.orders, "total", amount, agg.ByTime(at, agg.Daily, agg.Monthly, agg.AllTime))
	f.count = agg.Count(f.orders, "count", customer)
	f.avg = agg.Avg(f.orders, "avg", amount, agg.ByTime(at, agg.Weekly))

	// Computed from the orders of a span. The days are stored and the rest is put together from them.
	f.max = agg.Max(f.orders, "max", amount, agg.ByTime(at, agg.Daily, agg.Weekly, agg.Monthly, agg.AllTime))
	f.min = agg.Min(f.orders, "min", amount)
	// Each bucket is stored, as two medians cannot be put together.
	f.median = agg.Recompute(f.orders, "median", amount, median, agg.ByTime(at, agg.Daily, agg.Weekly, agg.AllTime))
	return f
}

// span is a group and a span of a bucket.
type span struct {
	group string
	start time.Time
}

// amountsBy returns the amounts of the orders that count, by their groups and their spans of a bucket.
func amountsBy(orders map[int64]order, b agg.Bucket) map[span][]int64 {
	amounts := map[span][]int64{}
	for _, id := range slices.Sorted(func(yield func(int64) bool) {
		for id := range orders {
			if !yield(id) {
				return
			}
		}
	}) {
		o := orders[id]
		if o.Draft {
			continue
		}
		s := span{group: o.Customer, start: b.Start(o.At)}
		amounts[s] = append(amounts[s], o.Amount)
	}
	return amounts
}

func sum(amounts []int64) (total int64) {
	for _, a := range amounts {
		total += a
	}
	return total
}

func middle(amounts []int64) int64 {
	sorted := slices.Sorted(slices.Values(amounts))
	return sorted[len(sorted)/2]
}

var (
	groups = []string{"", "ann", "bob", "nobody"}
	from   = day(1).AddDate(0, -1, 0)
	to     = day(1).AddDate(0, 2, 0)
)

// verify checks an aggregation against what its states must be for the given orders.
func verify[S any](
	t *testing.T,
	tx kv.Tx,
	a *agg.Aggregation[int64, order, string, int64, S],
	orders map[int64]order,
	want func(amounts []int64) S,
	collected bool,
) {
	t.Helper()

	ins := a.Instantiate(tx)
	for _, b := range a.Buckets() {
		amounts := amountsBy(orders, b)

		for _, group := range groups {
			var wantSeries []agg.Point[S]
			for s, as := range amounts {
				if s.group == group {
					wantSeries = append(wantSeries, agg.Point[S]{Start: s.start, State: want(as)})
				}
			}
			slices.SortFunc(wantSeries, func(a, b agg.Point[S]) int { return a.Start.Compare(b.Start) })

			series := ins.Series
			if collected {
				series = ins.SeriesCollected
			}
			gotSeries, err := series(b, group, from, to)
			require.NoError(t, err)
			require.Len(t, gotSeries, len(wantSeries), "%s %s of %q", a.Name(), b.Name(), group)
			for i, p := range wantSeries {
				require.True(t, p.Start.Equal(gotSeries[i].Start), "%s %s of %q: start %s, want %s",
					a.Name(), b.Name(), group, gotSeries[i].Start, p.Start)
				require.Equal(t, p.State, gotSeries[i].State, "%s %s of %q at %s", a.Name(), b.Name(), group, p.Start)
			}

			// Every day of the month on its own, with the days that have nothing.
			for d := 1; d <= 31; d += 3 {
				probe := day(d).Add(13 * time.Hour)
				as, wantOK := amounts[span{group: group, start: b.Start(probe)}]

				at := ins.At
				if collected {
					at = ins.AtCollected
				}
				got, ok, err := at(b, group, probe)
				require.NoError(t, err)
				require.Equal(t, wantOK, ok, "%s %s of %q at %s", a.Name(), b.Name(), group, probe)
				if wantOK {
					require.Equal(t, want(as), got, "%s %s of %q at %s", a.Name(), b.Name(), group, probe)
				}
			}
		}
	}
}

func (f *fixture) verify(t *testing.T, db kv.DB, orders map[int64]order, collected bool) {
	t.Helper()

	err := db.View(func(tx kv.Tx) error {
		verify(t, tx, f.total, orders, sum, collected)
		verify(t, tx, f.count, orders, func(as []int64) int64 { return int64(len(as)) }, collected)
		verify(t, tx, f.avg, orders, func(as []int64) agg.Average[int64] {
			return agg.Average[int64]{Sum: sum(as), Count: int64(len(as))}
		}, collected)
		verify(t, tx, f.max, orders, func(as []int64) int64 { return slices.Max(as) }, collected)
		verify(t, tx, f.min, orders, func(as []int64) int64 { return slices.Min(as) }, collected)
		verify(t, tx, f.median, orders, middle, collected)
		return nil
	})
	require.NoError(t, err)
}

// TestAggregationsEqualRecomputation writes, changes and deletes orders at random, collects at
// random points, and checks that the aggregations are what they are when computed from the
// orders that are there, whether what was written is collected or not.
func TestAggregationsEqualRecomputation(t *testing.T) {
	run := func(t *testing.T, db kv.DB, ops int) {
		f := newFixture()
		require.NoError(t, f.orders.Check(db))
		collector := agg.NewCollector(db, []agg.Source{f.orders}, agg.WithBatchSize(7), agg.WithErrorHandler(func(err error) {
			t.Errorf("collector: %v", err)
		}))

		rng := rand.New(rand.NewPCG(21, 22))
		orders := map[int64]order{}
		for op := 1; op <= ops; op++ {
			id := int64(1 + rng.IntN(30))
			err := db.Update(func(tx kv.Tx) error {
				store := f.orders.Instantiate(tx)
				if rng.IntN(10) < 3 {
					delete(orders, id)
					return store.Delete(id)
				}

				o := order{
					Customer: groups[rng.IntN(3)],
					Amount:   rng.Int64N(9) - 2,
					// Over a few weeks that cross a month, at any time of the day, in any order.
					At:    day(20 + rng.IntN(25)).Add(time.Duration(rng.IntN(86400)) * time.Second),
					Draft: rng.IntN(8) == 0,
				}
				if old, ok := orders[id]; ok && rng.IntN(4) == 0 {
					// The same order again, which must change nothing.
					o = old
				}
				orders[id] = o
				return store.Set(id, &o)
			})
			require.NoError(t, err)

			if rng.IntN(15) == 0 {
				require.NoError(t, collector.Collect(context.Background()))
			}
			if op%20 == 0 || op == ops {
				f.verify(t, db, orders, false)
			}
		}

		// With everything collected, the stored states alone are right too.
		require.NoError(t, collector.Collect(context.Background()))
		f.verify(t, db, orders, false)
		f.verify(t, db, orders, true)
		require.NotEmpty(t, orders)

		// And with every order gone, nothing is left of the aggregations.
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			for id := range orders {
				if err := f.orders.Instantiate(tx).Delete(id); err != nil {
					return err
				}
			}
			return nil
		}))
		f.verify(t, db, nil, false)
		require.NoError(t, collector.Collect(context.Background()))
		f.verify(t, db, nil, true)

		require.NoError(t, db.View(func(tx kv.Tx) error {
			for _, p := range [][]byte{
				f.total.Prefix(), f.count.Prefix(), f.avg.Prefix(),
				f.max.Prefix(), f.min.Prefix(), f.median.Prefix(),
			} {
				it, err := prefix.New(tx, p).Iterator(nil, nil)
				require.NoError(t, err)
				require.False(t, it.Valid(), "%s still has the key %x", migrate.Label(p), keyOf(it))
				it.Close()
			}
			return nil
		}))
	}

	t.Run("Mem", func(t *testing.T) { run(t, memkv.New(), 600) })
	t.Run("Badger", func(t *testing.T) { run(t, testkv.NewBadger(t), 120) })
}

func keyOf(it kv.Iterator) []byte {
	if it.Valid() {
		return it.Key()
	}
	return nil
}

// count returns the number of the keys under a prefix.
func count(t *testing.T, db kv.DB, p []byte) (n int) {
	t.Helper()

	require.NoError(t, db.View(func(tx kv.Tx) error {
		it, err := prefix.New(tx, p).Iterator(nil, nil)
		require.NoError(t, err)
		defer it.Close()

		for n = 0; it.Valid(); it.Next() {
			n++
		}
		return nil
	}))
	return n
}

func set(t *testing.T, db kv.DB, orders *extstore.Store[int64, order], id int64, o order) error {
	t.Helper()
	return db.Update(func(tx kv.Tx) error {
		return orders.Instantiate(tx).Set(id, &o)
	})
}

func TestUnchangedRecordNotesNothing(t *testing.T) {
	db := memkv.New()
	f := newFixture()
	o := order{Customer: "ann", Amount: 5, At: day(3)}
	require.NoError(t, set(t, db, f.orders, 1, o))

	prefixes := [][]byte{f.total.Prefix(), f.max.Prefix(), f.median.Prefix()}
	var before []int
	for _, p := range prefixes {
		before = append(before, count(t, db, p))
	}

	// The same order, and a draft that is set and deleted, which never counted.
	require.NoError(t, set(t, db, f.orders, 1, o))
	require.NoError(t, set(t, db, f.orders, 2, order{Customer: "ann", Amount: 9, Draft: true}))
	require.NoError(t, db.Update(func(tx kv.Tx) error { return f.orders.Instantiate(tx).Delete(2) }))

	for i, p := range prefixes {
		require.Equal(t, before[i], count(t, db, p), migrate.Label(p))
	}

	// A change of what an order contributes is noted.
	o.Amount = 6
	require.NoError(t, set(t, db, f.orders, 1, o))
	require.Greater(t, count(t, db, f.total.Prefix()), before[0])
}

func TestReadsInTheSameTransaction(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f := newFixture()

		err := db.Update(func(tx kv.Tx) error {
			orders := f.orders.Instantiate(tx)
			total, top := f.total.Instantiate(tx), f.max.Instantiate(tx)

			_, ok, err := total.Get("ann")
			require.NoError(t, err)
			require.False(t, ok)

			// What a transaction has written is in what it reads.
			require.NoError(t, orders.Set(1, &order{Customer: "ann", Amount: 5, At: day(3)}))
			require.NoError(t, orders.Set(2, &order{Customer: "ann", Amount: 7, At: day(4)}))
			got, ok, err := total.Get("ann")
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, int64(12), got)

			got, ok, err = top.At(agg.Monthly, "ann", day(15))
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, int64(7), got)

			require.NoError(t, orders.Delete(2))
			got, _, err = top.Get("ann")
			require.NoError(t, err)
			require.Equal(t, int64(5), got)

			// Nothing of it is collected yet.
			_, ok, err = total.GetCollected("ann")
			require.NoError(t, err)
			require.False(t, ok)
			_, ok, err = top.GetCollected("ann")
			require.NoError(t, err)
			require.False(t, ok)
			return nil
		})
		require.NoError(t, err)
	})
}

func TestSeries(t *testing.T) {
	db := memkv.New()
	f := newFixture()
	for id, o := range map[int64]order{
		1: {Customer: "ann", Amount: 1, At: day(2).Add(time.Hour)},
		2: {Customer: "ann", Amount: 2, At: day(2).Add(5 * time.Hour)},
		3: {Customer: "ann", Amount: 4, At: day(5)},
		4: {Customer: "bob", Amount: 8, At: day(3)},
	} {
		require.NoError(t, set(t, db, f.orders, id, o))
	}

	require.NoError(t, db.View(func(tx kv.Tx) error {
		total := f.total.Instantiate(tx)

		points, err := total.Series(agg.Daily, "ann", day(1), day(10))
		require.NoError(t, err)
		require.Equal(t, []agg.Point[int64]{{Start: day(2), State: 3}, {Start: day(5), State: 4}}, points)

		// From the span that the start is in, up to the spans that start before the end.
		points, err = total.Series(agg.Daily, "ann", day(2).Add(23*time.Hour), day(5))
		require.NoError(t, err)
		require.Equal(t, []agg.Point[int64]{{Start: day(2), State: 3}}, points)
		points, err = total.Series(agg.Daily, "ann", day(2), day(5).Add(time.Second))
		require.NoError(t, err)
		require.Len(t, points, 2)

		points, err = total.Series(agg.Daily, "ann", day(2), day(6), agg.FillGaps())
		require.NoError(t, err)
		require.Equal(t, []agg.Point[int64]{
			{Start: day(2), State: 3},
			{Start: day(3)},
			{Start: day(4)},
			{Start: day(5), State: 4},
		}, points)

		points, err = total.Series(agg.Daily, "ann", day(9), day(2))
		require.NoError(t, err)
		require.Empty(t, points)

		// All of time is one span.
		points, err = total.Series(agg.AllTime, "bob", day(1), day(2))
		require.NoError(t, err)
		require.Len(t, points, 1)
		require.Equal(t, int64(8), points[0].State)
		require.True(t, points[0].Start.IsZero())

		// A bucket that the aggregation is not kept for.
		_, _, err = total.At(agg.Weekly, "ann", day(2))
		require.ErrorIs(t, err, agg.ErrNoBucket)
		_, err = total.Series(agg.Hourly, "ann", day(1), day(2))
		require.ErrorIs(t, err, agg.ErrNoBucket)
		ny, err := time.LoadLocation("America/New_York")
		require.NoError(t, err)
		_, _, err = total.At(agg.Daily.In(ny), "ann", day(2))
		require.ErrorIs(t, err, agg.ErrNoBucket)
		return nil
	}))
}

func TestAppendOnly(t *testing.T) {
	db := memkv.New()
	orders := extstore.New[int64, order]("orders")
	adds := agg.Delta(orders, "adds", amount,
		agg.Fold[int64, int64]{Add: func(s, x int64) int64 { return s + x }},
		agg.AppendOnly())

	o := order{Customer: "ann", Amount: 5}
	require.NoError(t, set(t, db, orders, 1, o))
	// The same order again changes nothing, so it is allowed.
	require.NoError(t, set(t, db, orders, 1, o))

	o.Amount = 6
	require.ErrorIs(t, set(t, db, orders, 1, o), agg.ErrAppendOnly)
	err := db.Update(func(tx kv.Tx) error { return orders.Instantiate(tx).Delete(1) })
	require.ErrorIs(t, err, agg.ErrAppendOnly)

	// A draft was never counted, so it can be deleted, and counted later.
	require.NoError(t, set(t, db, orders, 2, order{Customer: "ann", Amount: 1, Draft: true}))
	require.NoError(t, set(t, db, orders, 2, order{Customer: "ann", Amount: 1}))

	require.NoError(t, db.View(func(tx kv.Tx) error {
		got, ok, err := adds.Instantiate(tx).Get("ann")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, int64(6), got)
		return nil
	}))
}

func TestDefinitionProblems(t *testing.T) {
	add := func(s, x int64) int64 { return s + x }
	newOrders := func() *extstore.Store[int64, order] {
		return extstore.New[int64, order]("orders")
	}

	// A fold that cannot remove must say that it is append-only.
	require.Panics(t, func() {
		agg.Delta(newOrders(), "a", amount, agg.Fold[int64, int64]{Add: add})
	})
	require.Panics(t, func() {
		agg.Delta(newOrders(), "a", amount, agg.Fold[int64, int64]{Remove: add})
	})
	// The options of the other kind.
	require.Panics(t, func() {
		agg.Delta(newOrders(), "a", amount, agg.Fold[int64, int64]{Add: add, Remove: add}, agg.WithMerge(add))
	})
	require.Panics(t, func() {
		agg.Recompute(newOrders(), "a", amount, median, agg.AppendOnly())
	})
	// An option for another type.
	require.Panics(t, func() {
		agg.Sum(newOrders(), "a", amount, agg.ByTime(func(*string) time.Time { return time.Time{} }, agg.Daily))
	})
	require.Panics(t, func() {
		agg.Recompute(newOrders(), "a", amount, median, agg.WithMerge(func(a, b string) string { return a }))
	})
	// Two buckets that cannot be told apart in the keys.
	require.Panics(t, func() {
		agg.Sum(newOrders(), "a", amount, agg.ByTime(at, agg.Daily, agg.Daily.In(time.FixedZone("x", 3600))))
	})
	require.Panics(t, func() {
		agg.Sum(newOrders(), "a", amount, agg.ByTime(at, agg.Every(time.Minute), agg.Every(time.Hour)))
	})
	// Weeks cannot be put together into months.
	require.Panics(t, func() {
		agg.Max(newOrders(), "a", amount, agg.ByTime(at, agg.Weekly, agg.Monthly))
	})

	// The same name twice is a mistake.
	orders := newOrders()
	agg.Count(orders, "same", customer)
	require.Panics(t, func() { agg.Count(orders, "same", customer) })
}

func TestCatalog(t *testing.T) {
	define := func(buckets ...agg.Bucket) (*extstore.Store[int64, order], *agg.Aggregation[int64, order, string, int64, int64]) {
		orders := extstore.New[int64, order]("orders")
		return orders, agg.Sum(orders, "total", amount, agg.ByTime(at, buckets...),
			agg.Describe("sum", "Amount", "At", "Customer"))
	}

	orders, total := define(agg.Daily, agg.AllTime)
	require.Equal(t, "total", total.Name())
	// The data is under the prefix of the store, and under the name of the aggregation.
	require.True(t, bytes.HasPrefix(total.Prefix(), orders.Prefix()))
	require.Equal(t, append(append(bytes.Clone(orders.Prefix()), 'x'), lex.AppendTerminated(nil, []byte("total"))...),
		total.Prefix())

	parts := orders.Unit().Parts()
	require.Len(t, parts, 1)
	require.Equal(t, "total", parts[0].Name)
	require.Equal(t, "agg", parts[0].Kind)
	require.Equal(t, total.Prefix(), parts[0].Prefix)
	require.NotNil(t, parts[0].Rebuild)
	require.Equal(t, map[string]string{
		"kind":     "delta",
		"buckets":  "daily,all",
		"merged":   "false",
		"encoding": "string,msgpack,msgpack",
	}, parts[0].Attributes())
	require.Equal(t, agg.Descriptor{
		Kind:     "delta",
		Function: "sum",
		Value:    "Amount",
		GroupBy:  []string{"Customer"},
		Time:     "At",
		Buckets:  []string{"daily", "all"},
		Encoding: "string,msgpack,msgpack",
	}, total.Descriptor())

	db := memkv.New()
	require.NoError(t, orders.Check(db))
	same, _ := define(agg.Daily, agg.AllTime)
	require.NoError(t, same.Check(db))

	// An aggregation with other buckets has to be built again.
	changed, _ := define(agg.Daily, agg.Weekly, agg.AllTime)
	require.ErrorIs(t, changed.Check(db), migrate.ErrNeedsMigration)
	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	changed, _ = define(agg.Daily.In(ny), agg.AllTime)
	require.ErrorIs(t, changed.Check(db), migrate.ErrNeedsMigration)
}

func TestRebuild(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		// Orders that were written when the store had no aggregation.
		plain := extstore.New[int64, order]("orders")
		rng := rand.New(rand.NewPCG(5, 6))
		orders := map[int64]order{}
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			for id := int64(1); id <= 40; id++ {
				o := order{
					Customer: groups[rng.IntN(3)],
					Amount:   rng.Int64N(9) - 2,
					At:       day(20 + rng.IntN(25)).Add(time.Duration(rng.IntN(86400)) * time.Second),
					Draft:    rng.IntN(8) == 0,
				}
				orders[id] = o
				if err := plain.Instantiate(tx).Set(id, &o); err != nil {
					return err
				}
			}
			return nil
		}))

		f := newFixture()
		ctx := context.Background()
		require.NoError(t, agg.Rebuild(ctx, db, f.total, agg.WithBatchSize(7)))
		require.NoError(t, agg.Rebuild(ctx, db, f.count, agg.WithBatchSize(7)))
		require.NoError(t, agg.Rebuild(ctx, db, f.avg))
		require.NoError(t, agg.Rebuild(ctx, db, f.max, agg.WithBatchSize(7)))
		require.NoError(t, agg.Rebuild(ctx, db, f.min))
		require.NoError(t, agg.Rebuild(ctx, db, f.median, agg.WithBatchSize(7)))
		f.verify(t, db, orders, true)

		// Building an aggregation again starts from nothing.
		require.NoError(t, agg.Rebuild(ctx, db, f.total))
		require.NoError(t, agg.Rebuild(ctx, db, f.max))
		f.verify(t, db, orders, true)

		// And it is kept up to date from there.
		o := order{Customer: "ann", Amount: 100, At: day(21)}
		orders[41] = o
		require.NoError(t, set(t, db, f.orders, 41, o))
		f.verify(t, db, orders, false)
	})
}

func TestCollectorPutsFailuresAside(t *testing.T) {
	db := memkv.New()
	orders := extstore.New[int64, order]("orders")

	// A fold and a function that cannot take the amount 13.
	unlucky := agg.Delta(orders, "unlucky", amount, agg.Fold[int64, int64]{
		Add: func(s, x int64) int64 {
			if x == 13 {
				panic("thirteen")
			}
			return s + x
		},
		Remove: func(s, x int64) int64 { return s - x },
	}, agg.ByTime(at, agg.Daily, agg.AllTime))
	failing := agg.Recompute(orders, "failing", amount, func(amounts iterutil.AdvancedIterator[time.Time, int64]) (int64, error) {
		values, err := amounts.Values()
		if err != nil {
			return 0, err
		}
		if slices.Contains(values, 13) {
			return 0, errors.New("thirteen")
		}
		return int64(len(values)), nil
	}, agg.ByTime(at, agg.Daily))
	total := agg.Sum(orders, "total", amount)

	require.NoError(t, set(t, db, orders, 1, order{Customer: "ann", Amount: 5, At: day(1)}))
	require.NoError(t, set(t, db, orders, 2, order{Customer: "ann", Amount: 13, At: day(2)}))
	require.NoError(t, set(t, db, orders, 3, order{Customer: "ann", Amount: 7, At: day(3)}))

	var reported []string
	collector := agg.NewCollector(db, []agg.Source{orders}, agg.WithBatchSize(1), agg.WithErrorHandler(func(err error) {
		reported = append(reported, err.Error())
	}))
	require.NoError(t, collector.Collect(context.Background()))
	require.Len(t, reported, 2)
	require.Contains(t, reported[0]+reported[1], `agg "unlucky"`)
	require.Contains(t, reported[0]+reported[1], `agg "failing"`)

	require.NoError(t, db.View(func(tx kv.Tx) error {
		// The record that the fold could not take is put aside, with none of its states changed,
		// and the others are collected.
		got, ok, err := unlucky.Instantiate(tx).GetCollected("ann")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, int64(12), got)
		_, ok, err = unlucky.Instantiate(tx).AtCollected(agg.Daily, "ann", day(2))
		require.NoError(t, err)
		require.False(t, ok)
		got, _, err = unlucky.Instantiate(tx).Get("ann")
		require.NoError(t, err)
		require.Equal(t, int64(12), got)

		// The span that the function fails for stays marked, and the others are computed.
		points, err := failing.Instantiate(tx).SeriesCollected(agg.Daily, "ann", day(1), day(9))
		require.NoError(t, err)
		require.Equal(t, []agg.Point[int64]{{Start: day(1), State: 1}, {Start: day(3), State: 1}}, points)
		_, _, err = failing.Instantiate(tx).At(agg.Daily, "ann", day(2))
		require.ErrorContains(t, err, "thirteen")

		// The aggregation that has nothing wrong is not held back.
		got, _, err = total.Instantiate(tx).GetCollected("ann")
		require.NoError(t, err)
		require.Equal(t, int64(25), got)
		return nil
	}))

	// A round later the record that was put aside does not come up again, and the span that
	// could not be computed is tried again.
	reported = nil
	require.NoError(t, collector.Collect(context.Background()))
	require.Len(t, reported, 1)
	require.Contains(t, reported[0], `agg "failing"`)

	// Once the order is changed, the span can be computed.
	require.NoError(t, set(t, db, orders, 2, order{Customer: "ann", Amount: 14, At: day(2)}))
	reported = nil
	require.NoError(t, collector.Collect(context.Background()))
	require.Empty(t, reported)
	require.NoError(t, db.View(func(tx kv.Tx) error {
		got, ok, err := failing.Instantiate(tx).AtCollected(agg.Daily, "ann", day(2))
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, int64(1), got)
		return nil
	}))
}

func TestCollectorRun(t *testing.T) {
	db := memkv.New()
	orders := extstore.New[int64, order]("orders")
	total := agg.Sum(orders, "total", amount)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- agg.NewCollector(db, []agg.Source{orders}, agg.WithInterval(5*time.Millisecond)).Run(ctx)
	}()

	require.NoError(t, set(t, db, orders, 1, order{Customer: "ann", Amount: 5}))
	require.Eventually(t, func() bool {
		var ok bool
		require.NoError(t, db.View(func(tx kv.Tx) error {
			var err error
			_, ok, err = total.Instantiate(tx).GetCollected("ann")
			return err
		}))
		return ok
	}, 5*time.Second, 5*time.Millisecond)

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)

	// A collector that is asked to collect with a context that is done does not go on.
	require.NoError(t, set(t, db, orders, 2, order{Customer: "ann", Amount: 5}))
	require.ErrorIs(t, agg.NewCollector(db, []agg.Source{orders}).Collect(ctx), context.Canceled)
}

// slowDB is a database whose writing transactions take a while.
type slowDB struct {
	kv.DB
	delay time.Duration
}

func (s slowDB) Update(fn func(kv.Tx) error) error {
	time.Sleep(s.delay)
	return s.DB.Update(fn)
}

func TestCollectorSpawn(t *testing.T) {
	db := memkv.New()
	orders := extstore.New[int64, order]("orders")
	total := agg.Sum(orders, "total", amount)
	require.NoError(t, orders.Check(db))

	collected := func() (sum int64) {
		require.NoError(t, db.View(func(tx kv.Tx) error {
			var err error
			sum, _, err = total.Instantiate(tx).GetCollected("ann")
			return err
		}))
		return sum
	}
	collects := func(want int64) {
		t.Helper()
		require.Eventually(t, func() bool { return collected() == want }, 5*time.Second, 2*time.Millisecond)
	}
	ctx := context.Background()

	// Shutting down a collector that was never spawned does nothing.
	c := agg.NewCollector(db, []agg.Source{orders}, agg.WithInterval(2*time.Millisecond))
	require.NoError(t, c.Shutdown(ctx))

	// A collector that is spawned collects, and spawning it again leaves it as it is.
	c.Spawn(ctx)
	c.Spawn(ctx)
	require.NoError(t, set(t, db, orders, 1, order{Customer: "ann", Amount: 5}))
	collects(5)

	// Once it is shut down it collects no more, however often it is shut down.
	require.NoError(t, c.Shutdown(ctx))
	require.NoError(t, c.Shutdown(ctx))
	require.NoError(t, set(t, db, orders, 2, order{Customer: "ann", Amount: 7}))
	time.Sleep(30 * time.Millisecond)
	require.Equal(t, int64(5), collected())

	// What was not collected is still noted, for the collector when it is spawned again.
	c.Spawn(ctx)
	collects(12)

	// It stops with its context too, and can be spawned again after that.
	parent, cancel := context.WithCancel(ctx)
	require.NoError(t, c.Shutdown(ctx))
	c.Spawn(parent)
	require.NoError(t, set(t, db, orders, 3, order{Customer: "ann", Amount: 1}))
	collects(13)
	cancel()
	time.Sleep(10 * time.Millisecond)
	require.NoError(t, set(t, db, orders, 4, order{Customer: "ann", Amount: 1}))
	time.Sleep(30 * time.Millisecond)
	require.Equal(t, int64(13), collected())
	c.Spawn(ctx)
	collects(14)
	require.NoError(t, c.Shutdown(ctx))

	t.Run("WhileCollecting", func(t *testing.T) {
		// A collector that is in the middle of a batch finishes it. Whoever shuts it down
		// can stop waiting for that, and the collector stops all the same.
		slow := agg.NewCollector(slowDB{DB: db, delay: 150 * time.Millisecond}, []agg.Source{orders}, agg.WithInterval(time.Millisecond))
		for id := int64(10); id < 20; id++ {
			require.NoError(t, set(t, db, orders, id, order{Customer: "ann", Amount: 1}))
		}
		slow.Spawn(ctx)
		time.Sleep(20 * time.Millisecond)

		impatient, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
		defer cancel()
		require.ErrorIs(t, slow.Shutdown(impatient), context.DeadlineExceeded)
		require.NoError(t, slow.Shutdown(ctx))

		// The batch was written whole, and the aggregation is what the orders give.
		require.NoError(t, db.View(func(tx kv.Tx) error {
			sum, _, err := total.Instantiate(tx).Get("ann")
			require.NoError(t, err)
			require.Equal(t, int64(24), sum)
			return nil
		}))
		require.NoError(t, agg.NewCollector(db, []agg.Source{orders}).Collect(ctx))
		require.Equal(t, int64(24), collected())
	})
}

// TestConcurrentWriters has many transactions write into one group at the same time, with the
// retries of conflicts turned off: writing to an aggregation must not make them conflict.
func TestConcurrentWriters(t *testing.T) {
	const writers, perWriter = 8, 40

	run := func(t *testing.T, withCollector bool) {
		raw, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLoggingLevel(badger.ERROR))
		require.NoError(t, err)
		defer raw.Close()
		strict := badgerkv.Wrap(raw, badgerkv.WithMaxRetries(0))
		db := badgerkv.Wrap(raw)

		orders := extstore.New[int64, order]("orders")
		total := agg.Sum(orders, "total", amount, agg.ByTime(at, agg.Daily, agg.AllTime))
		top := agg.Max(orders, "max", amount, agg.ByTime(at, agg.Daily, agg.AllTime))
		counted := agg.Count(orders, "count", customer)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var background sync.WaitGroup
		if withCollector {
			collector := agg.NewCollector(db, []agg.Source{orders}, agg.WithInterval(time.Millisecond), agg.WithBatchSize(16),
				agg.WithErrorHandler(func(err error) { t.Errorf("collector: %v", err) }))
			background.Go(func() { collector.Run(ctx) })

			// What is read is exact all along: it never goes down, and never past what was written.
			background.Go(func() {
				var last int64
				for ctx.Err() == nil {
					err := db.View(func(tx kv.Tx) error {
						n, _, err := counted.Instantiate(tx).Get("ann")
						if err != nil {
							return err
						}
						sum, _, err := total.Instantiate(tx).Get("ann")
						if err != nil {
							return err
						}
						if n < last || n > writers*perWriter || sum != 2*n {
							t.Errorf("read a count of %d after %d, with a total of %d", n, last, sum)
						}
						last = n
						return nil
					})
					if err != nil {
						t.Errorf("read: %v", err)
					}
				}
			})
		}

		var failures atomic.Int64
		var wg sync.WaitGroup
		for w := range writers {
			wg.Go(func() {
				for i := range perWriter {
					id := int64(w*perWriter + i + 1)
					err := strict.Update(func(tx kv.Tx) error {
						return orders.Instantiate(tx).Set(id, &order{Customer: "ann", Amount: 2, At: day(1 + i%3)})
					})
					if err != nil {
						failures.Add(1)
						t.Errorf("write %d: %v", id, err)
					}
				}
			})
		}
		wg.Wait()
		cancel()
		background.Wait()
		require.Zero(t, failures.Load())

		check := func(collected bool) {
			require.NoError(t, db.View(func(tx kv.Tx) error {
				get := func(a *agg.Aggregation[int64, order, string, int64, int64]) int64 {
					ins := a.Instantiate(tx)
					read := ins.Get
					if collected {
						read = ins.GetCollected
					}
					got, ok, err := read("ann")
					require.NoError(t, err)
					require.True(t, ok)
					return got
				}
				require.Equal(t, int64(writers*perWriter), get(counted))
				require.Equal(t, int64(2*writers*perWriter), get(total))
				require.Equal(t, int64(2), get(top))
				return nil
			}))
		}
		check(false)
		require.NoError(t, agg.NewCollector(db, []agg.Source{orders}).Collect(context.Background()))
		check(true)
	}

	t.Run("WithoutCollector", func(t *testing.T) { run(t, false) })
	t.Run("WithCollector", func(t *testing.T) { run(t, true) })
}

// BenchmarkWrites measures writing records into a store with aggregations of both kinds,
// with and without a collector running, to show what the notes that are left to be read and
// deleted cost.
func BenchmarkWrites(b *testing.B) {
	for _, withCollector := range []bool{false, true} {
		b.Run(fmt.Sprintf("Collector=%v", withCollector), func(b *testing.B) {
			db := testkv.NewBadger(b)
			orders := extstore.New[int64, order]("orders")
			total := agg.Sum(orders, "total", amount, agg.ByTime(at, agg.Daily, agg.AllTime))
			agg.Max(orders, "max", amount, agg.ByTime(at, agg.Daily, agg.AllTime))

			ctx, cancel := context.WithCancel(context.Background())
			var wg sync.WaitGroup
			if withCollector {
				wg.Go(func() { agg.NewCollector(db, []agg.Source{orders}, agg.WithInterval(10*time.Millisecond)).Run(ctx) })
			}

			b.ResetTimer()
			for i := range b.N {
				err := db.Update(func(tx kv.Tx) error {
					return orders.Instantiate(tx).Set(int64(i%1000), &order{Customer: "ann", Amount: int64(i), At: day(1 + i%28)})
				})
				if err != nil {
					b.Fatal(err)
				}
				if i%100 == 0 {
					// A read that takes what is not collected into account.
					err := db.View(func(tx kv.Tx) error {
						_, _, err := total.Instantiate(tx).Get("ann")
						return err
					})
					if err != nil {
						b.Fatal(err)
					}
				}
			}
			b.StopTimer()
			cancel()
			wg.Wait()
		})
	}
}

type purchase struct {
	Shop       string
	CustomerID int64
	Amount     int64
}

func TestTypedGroups(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		purchases := extstore.New[int64, purchase]("purchases")

		byShop := agg.Sum(purchases, "by_shop", func(p *purchase) (string, int64, bool) {
			return p.Shop, p.Amount, true
		})
		byCustomer := agg.Sum(purchases, "by_customer", func(p *purchase) (int64, int64, bool) {
			return p.CustomerID, p.Amount, true
		})
		// A group of two fields is a pair of them.
		byBoth := agg.Max(purchases, "by_both", func(p *purchase) (codec.Pair[string, int64], int64, bool) {
			return codec.NewPair(p.Shop, p.CustomerID), p.Amount, true
		})
		overall := agg.Count(purchases, "overall", func(*purchase) (agg.NoGroup, bool) {
			return agg.NoGroup{}, true
		})
		require.NoError(t, purchases.Check(db))
		require.Equal(t, "pair(string,int64),msgpack,msgpack", byBoth.Descriptor().Encoding)
		require.Equal(t, "none,msgpack,msgpack", overall.Descriptor().Encoding)

		write := func() {
			require.NoError(t, db.Update(func(tx kv.Tx) error {
				store := purchases.Instantiate(tx)
				for id, p := range map[int64]purchase{
					1: {Shop: "north", CustomerID: 7, Amount: 30},
					2: {Shop: "north", CustomerID: 8, Amount: 50},
					3: {Shop: "south", CustomerID: 7, Amount: 20},
					// A shop whose name begins like the name of another one, and customers
					// whose numbers have bytes in common.
					4: {Shop: "north-east", CustomerID: -7, Amount: 1},
					5: {Shop: "north", CustomerID: 7 + 256, Amount: 5},
				} {
					if err := store.Set(id, &p); err != nil {
						return err
					}
				}
				return nil
			}))
		}
		check := func(collected bool) {
			require.NoError(t, db.View(func(tx kv.Tx) error {
				get := func(read func() (int64, bool, error)) int64 {
					got, ok, err := read()
					require.NoError(t, err)
					require.True(t, ok)
					return got
				}
				shops, customers, both, all := byShop.Instantiate(tx), byCustomer.Instantiate(tx), byBoth.Instantiate(tx), overall.Instantiate(tx)
				if collected {
					require.Equal(t, int64(85), get(func() (int64, bool, error) { return shops.GetCollected("north") }))
					require.Equal(t, int64(50), get(func() (int64, bool, error) { return customers.GetCollected(7) }))
					require.Equal(t, int64(30), get(func() (int64, bool, error) { return both.GetCollected(codec.NewPair("north", int64(7))) }))
					require.Equal(t, int64(5), get(func() (int64, bool, error) { return all.GetCollected(agg.NoGroup{}) }))
					return nil
				}

				require.Equal(t, int64(85), get(func() (int64, bool, error) { return shops.Get("north") }))
				require.Equal(t, int64(1), get(func() (int64, bool, error) { return shops.Get("north-east") }))
				require.Equal(t, int64(50), get(func() (int64, bool, error) { return customers.Get(7) }))
				require.Equal(t, int64(1), get(func() (int64, bool, error) { return customers.Get(-7) }))
				require.Equal(t, int64(5), get(func() (int64, bool, error) { return customers.Get(263) }))
				require.Equal(t, int64(30), get(func() (int64, bool, error) { return both.Get(codec.NewPair("north", int64(7))) }))
				require.Equal(t, int64(20), get(func() (int64, bool, error) { return both.Get(codec.NewPair("south", int64(7))) }))
				require.Equal(t, int64(5), get(func() (int64, bool, error) { return all.Get(agg.NoGroup{}) }))

				_, ok, err := shops.Get("nort")
				require.NoError(t, err)
				require.False(t, ok)
				_, ok, err = both.Get(codec.NewPair("north", int64(9)))
				require.NoError(t, err)
				require.False(t, ok)
				return nil
			}))
		}

		write()
		check(false)
		require.NoError(t, agg.NewCollector(db, []agg.Source{purchases}, agg.WithBatchSize(2)).Collect(context.Background()))
		check(false)
		check(true)
	})
}

func TestGroupCodec(t *testing.T) {
	type shop struct{ Name string }
	newPurchases := func() *extstore.Store[int64, purchase] {
		return extstore.New[int64, purchase]("purchases")
	}
	byShop := func(p *purchase) (shop, int64, bool) { return shop{p.Shop}, p.Amount, true }

	// A type of groups that has no key codec is a mistake in the definition.
	require.Panics(t, func() { agg.Sum(newPurchases(), "a", byShop) })
	require.Panics(t, func() { agg.Sum(newPurchases(), "a", byShop, agg.WithGroupCodec(codec.StringKey[string]())) })

	// A key codec that is given is used.
	total := agg.Sum(newPurchases(), "a",
		func(p *purchase) (string, int64, bool) { return p.Shop, p.Amount, true },
		agg.WithGroupCodec(codec.StringKey[string]()))
	require.Equal(t, "string,msgpack,msgpack", total.Descriptor().Encoding)
}
