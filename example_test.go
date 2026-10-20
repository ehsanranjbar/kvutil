package kvutil_test

import (
	"context"
	"fmt"
	"time"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/agg"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/ehsanranjbar/kvutil/kv/prefix"
	"github.com/ehsanranjbar/kvutil/query/sql"
	"github.com/ehsanranjbar/kvutil/store/record"
	"github.com/ehsanranjbar/kvutil/store/typed"
)

// The lowest layer is bytes in transactions, the same for every engine.
func Example_bytes() {
	db := memkv.New() // or badgerkv.Open(badger.DefaultOptions(dir))

	err := db.Update(func(tx kv.Tx) error {
		// A part of the keys as a keyspace of its own.
		users := prefix.New(tx, []byte("users/"))
		if err := users.Set([]byte("ann"), []byte("admin")); err != nil {
			return err
		}
		return users.Set([]byte("bob"), []byte("guest"))
	})
	if err != nil {
		panic(err)
	}

	err = db.View(func(tx kv.Tx) error {
		it, err := tx.Iterator([]byte("users/"), kv.PrefixEnd([]byte("users/")))
		if err != nil {
			return err
		}
		defer it.Close()

		for ; it.Valid(); it.Next() {
			value, err := it.Value()
			if err != nil {
				return err
			}
			fmt.Printf("%s = %s\n", it.Key(), value)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Output:
	// users/ann = admin
	// users/bob = guest
}

type City struct {
	Country    string
	Population int
}

// A typed store has keys and values of Go types. It is defined once, under a prefix, and is
// instantiated for each transaction.
func Example_typedStore() {
	cities := typed.New[string, City]("cities")

	db := memkv.New()
	// Once, when the program starts: the database is brought to what is defined, and a
	// definition that is not the one that its data was written with is an error.
	if _, err := cities.Migrate(context.Background(), db); err != nil {
		panic(err)
	}

	err := db.Update(func(tx kv.Tx) error {
		store := cities.Instantiate(tx)
		for name, city := range map[string]City{
			"lyon":  {Country: "FR", Population: 520_000},
			"paris": {Country: "FR", Population: 2_100_000},
			"rome":  {Country: "IT", Population: 2_800_000},
		} {
			if err := store.Set(name, &city); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	err = db.View(func(tx kv.Tx) error {
		// The keys sort as their values do, so a range of keys is a range of the store.
		it, err := cities.Instantiate(tx).Iterator(kvutil.From("m"))
		if err != nil {
			return err
		}
		for name, city := range iterutil.Of(it).All(&err) {
			fmt.Println(name, city.Country, city.Population)
		}
		return err
	})
	if err != nil {
		panic(err)
	}

	// Output:
	// paris FR 2100000
	// rome IT 2800000
}

type Order struct {
	ID       int64
	Customer string
	Total    int64
	At       time.Time
}

func (o *Order) GetId() int64   { return o.ID }
func (o *Order) SetId(id int64) { o.ID = id }

// A record store keeps values that know their ids. Indexes and aggregations are defined on
// it, and are kept up to date in the transactions that write the records.
func Example_records() {
	var last int64
	orders := record.New[int64, Order]("orders").
		WithIDFunc(func(*Order) (int64, error) { last++; return last, nil })

	customer := field.Path[Order, string]("Customer")
	byCustomer := index.On(orders, "by_customer", customer)

	revenue := agg.Sum(orders, "revenue",
		func(o *Order) (string, int64, bool) { return o.Customer, o.Total, true },
		agg.ByTime(func(o *Order) time.Time { return o.At }, agg.Daily, agg.AllTime))

	db := memkv.New()
	if err := orders.Check(db); err != nil {
		panic(err)
	}

	day := func(d int) time.Time { return time.Date(2026, time.March, d, 9, 0, 0, 0, time.UTC) }
	err := db.Update(func(tx kv.Tx) error {
		store := orders.Instantiate(tx)
		for _, o := range []*Order{
			{Customer: "ann", Total: 30, At: day(1)},
			{Customer: "bob", Total: 20, At: day(1)},
			{Customer: "ann", Total: 70, At: day(2)},
		} {
			if err := store.Set(o); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	err = db.View(func(tx kv.Tx) error {
		// By the index.
		it, err := byCustomer.Instantiate(tx).Match("ann")
		if err != nil {
			return err
		}
		ids, err := iterutil.Of(it).Keys()
		if err != nil {
			return err
		}
		fmt.Println("orders of ann:", ids)

		// By the WHERE clause of a query, which uses an index where one serves it.
		parser := sql.For(orders)
		where := `Customer = 'ann' AND Total >= 50`
		plan, err := parser.Explain(where)
		if err != nil {
			return err
		}
		found, err := parser.Query(tx, where)
		if err != nil {
			return err
		}
		ids, err = found.Keys()
		if err != nil {
			return err
		}
		fmt.Println(where, "->", ids, "by", plan)

		// The aggregation, for a day and for all of time.
		first, _, err := revenue.Instantiate(tx).At(agg.Daily, "ann", day(1))
		if err != nil {
			return err
		}
		total, _, err := revenue.Instantiate(tx).Get("ann")
		if err != nil {
			return err
		}
		fmt.Println("revenue of ann:", first, "on the first day,", total, "in all")
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Output:
	// orders of ann: [1 3]
	// Customer = 'ann' AND Total >= 50 -> [3] by IndexLookup by_customer: 1 range, then check [Total >= 50]
	// revenue of ann: 30 on the first day, 100 in all
}
