# kvutil

A toolkit of layered, optional abstractions over ordered key-value stores, for Go.

It starts at bytes in transactions and goes up to typed stores, records, secondary indexes,
stored aggregations, relations and conditions written as text. Every layer is built on the one
below it and can be used without the ones above it, so a project takes what it needs and
replaces what it does not like. [Badger](https://github.com/dgraph-io/badger) is the engine that
it is written for; the engine is behind a small interface, and an in-memory one comes with it.

> The API is not stable yet: there is no tagged release, and the layers above the indexes are
> still being built.

```sh
go get github.com/ehsanranjbar/kvutil
```

It needs Go 1.27.

## The layers

```text
query/ql          text of a condition  ->  cond
field, cond       fields of records, and conditions on them
index  agg  relation
                  secondary indexes, stored aggregations, relations between stores
store/record      records that know their ids
store/ext         a typed store that extensions are told of every write of
store/typed       keys and values of Go types
schema            namespaces, definitions, and a check of them against the database
codec             order-preserving keys, values in a default format
kv                bytes in transactions: the interface of an engine
kv/badgerkv  kv/memkv  kv/prefix
                  Badger, an in-memory engine, and a part of the keys as a keyspace
```

`iterutil` has the iterator that all of them return, with `Filter`, `Map`, `Limit`, `Lookup`,
`FlatMap` and the like.

## Bytes

`kv.DB` opens transactions and `kv.Tx` reads and writes bytes in one. `Update` runs its
function again if the transaction conflicts with another one.

```go
db := memkv.New() // or badgerkv.Open(badger.DefaultOptions(dir))

err := db.Update(func(tx kv.Tx) error {
    users := prefix.New(tx, []byte("users/")) // a part of the keys as a keyspace of its own
    if err := users.Set([]byte("ann"), []byte("admin")); err != nil {
        return err
    }
    return users.Set([]byte("bob"), []byte("guest"))
})

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
```

## Typed stores

A store is defined once, in a namespace of a schema, and is instantiated for each transaction.
It is given no codecs: keys are encoded by their type so that they sort as their values do, and
values are in the default format of the schema (msgpack, unless the type encodes itself).

```go
sch := schema.New()
cities := typed.New[string, City](sch.Namespace("cities"))

// Once, when the program starts: the definitions are checked against the ones that the
// database was written with, so a changed codec cannot corrupt it silently.
if err := sch.Check(db); err != nil {
    panic(err)
}

err := db.Update(func(tx kv.Tx) error {
    return cities.Instantiate(tx).Set("paris", &City{Country: "FR", Population: 2_100_000})
})

err = db.View(func(tx kv.Tx) error {
    it, err := cities.Instantiate(tx).Iterator(kvutil.From("m")) // the keys from "m" on
    if err != nil {
        return err
    }
    for name, city := range iterutil.Of[string, *City](it).All(&err) {
        fmt.Println(name, city.Country, city.Population)
    }
    return err
})
```

## Records, indexes and aggregations

A record store keeps values that know their ids. Indexes and aggregations are defined on it and
are kept up to date in the transactions that write the records.

```go
type Order struct {
    ID       int64
    Customer string
    Total    int64
    At       time.Time
}

func (o *Order) GetId() int64   { return o.ID }
func (o *Order) SetId(id int64) { o.ID = id }

orders := record.New[int64, Order](sch.Namespace("orders")).
    WithIDFunc(func(*Order) (int64, error) { last++; return last, nil })

customer := field.Path[Order, string]("Customer")
byCustomer := index.On(orders, "by_customer", customer)

revenue := agg.Sum(orders, "revenue",
    func(o *Order) (string, int64, bool) { return o.Customer, o.Total, true },
    agg.ByTime(func(o *Order) time.Time { return o.At }, agg.Daily, agg.AllTime))
```

```go
it, err := byCustomer.Instantiate(tx).Match("ann")        // the orders of a customer

first, _, err := revenue.Instantiate(tx).At(agg.Daily, "ann", day) // of a day
total, _, err := revenue.Instantiate(tx).Get("ann")                 // of all time
```

**Indexes** are on one field (`index.On`, `index.Unique`), on several (`index.Composite`), on a
part of the records (`index.Where`), and can carry values to read without the records
(`index.Covering`). `Lookup` takes conditions, uses the index for the ones that it can serve and
checks the others on what it finds. The results of several indexes are combined with
`index.Intersect`, `Union` and `Except`, or as bitmaps.

**Aggregations** never make two writers conflict: a write leaves a small note, and
`agg.NewCollector(db, sch).Run(ctx)` folds the notes into the states in the background. Reads
are exact all the same, as they take the notes into account. There are two kinds: `agg.Delta`
for what a record can be taken back out of (`Count`, `Sum`, `Avg`), and `agg.Recompute` for any
function of the records of a span (`Min`, `Max`, a median). Both can be kept by spans of time,
like days, weeks and months.

## Relations

```go
// Many to many, with data for every pair.
memberships := relation.New[Role](sch.Namespace("memberships"), users, groups)
err := memberships.Instantiate(tx).Set(userID, groupID, &Role{Name: "lead"})
groupsOf, err := memberships.Instantiate(tx).Rights(userID)

// One to many. Deleting a parent deletes its children.
ownership := relation.Associate(sch.Namespace("ownership"), owners, pets)
err = pets.Instantiate(tx).Set(&Pet{Name: "kit"}, ownership.Under(ownerID))
children, err := ownership.Instantiate(tx).Children(ownerID)
```

## Conditions as text

`query/ql` turns text into the conditions that the indexes take. Comparisons of a field with
values are served by indexes; functions and everything else are evaluated on each record by
[qlbridge](https://github.com/araddon/qlbridge).

```go
c, err := ql.For(orders).Parse(`Customer = "ann" AND Total >= 50`)
it, err := byCustomer.Instantiate(tx).Lookup(c)
```

A text that names a field or a function that there is not, or that compares a field with a
value that it cannot have, is an error when it is parsed. The lexer of qlbridge does not return
for some texts, which the documentation of the package lists: check a text that comes from
someone who is not trusted before parsing it.

## What is specific to Badger

The general interfaces have what every ordered engine has. The rest of Badger stays within
reach:

| Feature of Badger | Where it is |
| --- | --- |
| Encryption, compression, in-memory and read-only modes, conflict detection | The options that the database is opened with |
| Keys that expire | `kv.SetWithTTL`, and `SetWithTTL` of a typed store. Not for a store with indexes or aggregations, which would not be told |
| `DropPrefix` | `kv.DropRange` |
| `WriteBatch` | `kv.NewBulkWriter` |
| Sequences | `db.Badger().GetSequence`, and `badgerkv.SequenceID` to take the ids of records from one |
| Garbage collection of the value log, backup and restore, `Flatten`, `Subscribe`, `Stream`, merge operators | `db.Badger()` |
| The versions of a key, the metadata of an item, managed timestamps | `badgerkv.Unwrap(tx)`, which returns the transaction of Badger |
| The user metadata byte | Not used |

An engine that does not have a capability returns `kv.ErrUnsupported` for it.

## What is not there yet

- A planner that chooses the indexes of a query, and queries as text beyond conditions.
- Building an index for the records that were written before it was defined. An aggregation
  has `agg.Rebuild` for that.
- Records that expire in stores with indexes or aggregations.
- A second engine besides Badger and the in-memory one.

## Example

[examples/petstore](examples/petstore) is a small HTTP server on Badger with record stores,
relations that cascade, an index, two aggregations and a running collector.

```sh
cd examples/petstore && go run .
```

The examples of the packages, which are run as tests, show each of them in more detail:
[index](index/example_test.go), [agg](agg/example_test.go), [relation](relation/example_test.go),
[store/record](store/record/example_test.go), [query/ql](query/ql/example_test.go).

## Tests

```sh
go test -race ./...
```

The engines are checked by one conformance suite (`kv/kvtest`), which is there for other
engines too.
