package upkeep_test

import (
	"context"
	"fmt"

	"github.com/ehsanranjbar/kvutil/agg"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/ehsanranjbar/kvutil/upkeep"
)

type Order struct {
	Customer string
	Amount   int64
}

// Example has an upkeep runner do the background work of the extensions of a store, which
// is turning what the writes noted for an aggregation into its stored state.
func Example() {
	orders := extstore.New[int64, Order]("orders")
	revenue := agg.Sum(orders, "revenue", func(o *Order) (string, int64, bool) {
		return o.Customer, o.Amount, true
	})

	db := memkv.New()
	if err := orders.Check(db); err != nil {
		panic(err)
	}
	err := db.Update(func(tx kv.Tx) error {
		store := orders.Instantiate(tx)
		for id, o := range []Order{{"ann", 10}, {"bob", 5}, {"ann", 20}} {
			if err := store.Set(int64(id+1), &o); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	// read prints what the aggregation says of a customer, with the notes that are not folded into
	// the state yet and without them.
	read := func(label string) {
		err := db.View(func(tx kv.Tx) error {
			ins := revenue.Instantiate(tx)
			exact, _, err := ins.Get("ann")
			if err != nil {
				return err
			}
			stored, _, err := ins.GetCollected("ann")
			if err != nil {
				return err
			}
			fmt.Printf("%s: exact %d, stored %d\n", label, exact, stored)
			return nil
		})
		if err != nil {
			panic(err)
		}
	}
	read("written")

	// The runner finds the jobs among the extensions of the stores that are added to it. A
	// program runs it in the background with Spawn, and stops it with Shutdown. Here it is asked
	// to do what there is to do once, and to return.
	upkeeps := upkeep.New(db).Add(orders)
	if err := upkeeps.Drain(context.Background()); err != nil {
		panic(err)
	}
	read("drained")

	for _, s := range upkeeps.Stats() {
		fmt.Printf("%s: %d pass, failed %d times\n", s.Name, s.Passes, s.Failures)
	}

	// Output:
	// written: exact 30, stored 0
	// drained: exact 30, stored 30
	// aggregation "revenue": 1 pass, failed 0 times
}
