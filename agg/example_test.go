package agg_test

import (
	"context"
	"fmt"
	"time"

	"github.com/ehsanranjbar/kvutil/agg"
	"github.com/ehsanranjbar/kvutil/catalog"
	"github.com/ehsanranjbar/kvutil/iters"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

type Sale struct {
	Shop   string
	Amount int64
	At     time.Time
}

// Example keeps the sales of each shop summed up by day, by month and for all of time, and the
// largest sale of each day, and reads them before and after they are collected.
func Example() {
	schema := catalog.NewSchema()
	sales := extstore.New[int64, Sale](schema.Namespace("sales"))

	byShop := func(s *Sale) ([]byte, int64, bool) { return []byte(s.Shop), s.Amount, true }
	when := func(s *Sale) time.Time { return s.At }

	// A sum can take a sale back out, so it is folded from what the sales add and remove.
	revenue := agg.Sum(sales, "revenue", byShop, agg.ByTime(when, agg.Daily, agg.Monthly, agg.AllTime))

	// The largest sale cannot be found by taking a sale out, so it is computed from the sales of
	// a day. The months are put together from the days.
	largest := agg.Max(sales, "largest", byShop, agg.ByTime(when, agg.Daily, agg.Monthly))

	// Any function of the sales of a span can be an aggregation.
	spread := agg.Recompute(sales, "spread", byShop,
		func(amounts iters.Seq[time.Time, int64]) (int64, error) {
			values, err := amounts.Values()
			if err != nil {
				return 0, err
			}
			lo, hi := values[0], values[0]
			for _, v := range values {
				lo, hi = min(lo, v), max(hi, v)
			}
			return hi - lo, nil
		},
		agg.ByTime(when, agg.Monthly))

	db := memkv.New()
	if err := schema.Check(db); err != nil {
		panic(err)
	}

	date := func(month time.Month, day int) time.Time {
		return time.Date(2026, month, day, 12, 0, 0, 0, time.UTC)
	}
	err := db.Update(func(tx kv.Tx) error {
		store := sales.Instantiate(tx)
		for id, sale := range map[int64]Sale{
			1: {Shop: "north", Amount: 30, At: date(time.March, 1)},
			2: {Shop: "north", Amount: 50, At: date(time.March, 1)},
			3: {Shop: "north", Amount: 20, At: date(time.March, 2)},
			4: {Shop: "north", Amount: 70, At: date(time.April, 5)},
			5: {Shop: "south", Amount: 10, At: date(time.March, 1)},
		} {
			if err := store.Set(id, &sale); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	read := func(title string) {
		err := db.View(func(tx kv.Tx) error {
			north := []byte("north")

			days, err := revenue.Instantiate(tx).Series(agg.Daily, north, date(time.March, 1), date(time.March, 3), agg.FillGaps())
			if err != nil {
				return err
			}
			fmt.Print(title, ": by day")
			for _, p := range days {
				fmt.Printf(" %s=%d", p.Start.Format("Jan 2"), p.State)
			}

			march, _, err := revenue.Instantiate(tx).At(agg.Monthly, north, date(time.March, 15))
			if err != nil {
				return err
			}
			total, _, err := revenue.Instantiate(tx).Get(north)
			if err != nil {
				return err
			}
			top, _, err := largest.Instantiate(tx).At(agg.Monthly, north, date(time.March, 15))
			if err != nil {
				return err
			}
			gap, _, err := spread.Instantiate(tx).At(agg.Monthly, north, date(time.March, 15))
			if err != nil {
				return err
			}
			fmt.Printf("; March=%d, all=%d, largest in March=%d, spread in March=%d\n", march, total, top, gap)
			return nil
		})
		if err != nil {
			panic(err)
		}
	}

	// What is read is right before anything is collected.
	read("written")

	// A collector usually runs in the background, with Run. Here it is asked to collect once.
	collector := agg.NewCollector(db, schema)
	if err := collector.Collect(context.Background()); err != nil {
		panic(err)
	}
	read("collected")

	// A sale that is changed moves from one day to another, and one that is deleted is gone.
	err = db.Update(func(tx kv.Tx) error {
		store := sales.Instantiate(tx)
		if err := store.Set(2, &Sale{Shop: "north", Amount: 50, At: date(time.March, 3)}); err != nil {
			return err
		}
		return store.Delete(1)
	})
	if err != nil {
		panic(err)
	}
	read("changed")

	// Output:
	// written: by day Mar 1=80 Mar 2=20 Mar 3=0; March=100, all=170, largest in March=50, spread in March=30
	// collected: by day Mar 1=80 Mar 2=20 Mar 3=0; March=100, all=170, largest in March=50, spread in March=30
	// changed: by day Mar 1=0 Mar 2=20 Mar 3=50; March=70, all=140, largest in March=50, spread in March=30
}
