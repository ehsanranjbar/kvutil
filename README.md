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
query/sql         the WHERE clause of a query  ->  cond, and a query on a store
query             queries: conditions, order, offset and limit, planned with the indexes
field, cond       fields of records, and conditions on them
index  agg  relation
                  secondary indexes, stored aggregations, relations between stores
store/record      records that know their ids
store/ext         a typed store that extensions are told of every write of
store/typed       keys and values of Go types
keyspace          short prefixes by name, and the retirement of stores that are gone
migrate           the unit that a store records of itself, its version, and what brings a
                  database to them: migrations, builds of what is derived
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

A store is defined once, under a prefix, and is instantiated for each transaction. It is given
no codecs: keys are encoded by their type so that they sort as their values do, and values are
in msgpack, unless the type encodes itself.

A store keeps everything that is its own under its prefix: the records, its version and the
names of its codecs, and the data of its extensions. The prefixes of different stores must not
be the beginning of one another.

```go
cities := typed.New[string, City]("cities")

// Once, when the program starts: the database is brought to what is defined, and a definition
// that is not the one that its data was written with is an error, so a changed codec cannot
// corrupt it silently.
if _, err := cities.Migrate(ctx, db); err != nil {
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
    for name, city := range iterutil.Of(it).All(&err) {
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

orders := record.New[int64, Order]("orders").
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
part of the records (`index.Where`, or `index.WhereFunc` for a Go function), and can carry
fields and values to read without the records (`index.Including`, `index.Covering`; see
[Selecting values](#selecting-values)). `Lookup` takes conditions, uses the index for the ones that it can serve and
checks the others on what it finds. The results of several indexes are combined with
`index.Intersect`, `Union` and `Except`, or as bitmaps.

**Aggregations** never make two writers conflict: a write leaves a small note, and a collector
folds the notes into the states in the background. Reads are exact all the same, as they take
the notes into account.

```go
collector := agg.NewCollector(db, []agg.Source{orders}) // the stores whose aggregations it collects
collector.Spawn(ctx)          // runs in the background
defer collector.Shutdown(ctx) // stops after the batch that it is in, so nothing is half written
```

There are two kinds: `agg.Delta`
for what a record can be taken back out of (`Count`, `Sum`, `Avg`), and `agg.Recompute` for any
function of the records of a span (`Min`, `Max`, a median). Both can be kept by spans of time,
like days, weeks and months.

## Relations

```go
// Many to many, with data for every pair. A relation has a prefix of its own.
memberships := relation.New[Role]("memberships", users, groups)
err := memberships.Instantiate(tx).Set(userID, groupID, &Role{Name: "lead"})
groupsOf, err := memberships.Instantiate(tx).Rights(userID)

// One to many. Deleting a parent deletes its children.
ownership := relation.Associate("owned", owners, pets)
err = pets.Instantiate(tx).Set(&Pet{Name: "kit"}, ownership.Under(ownerID))
children, err := ownership.Instantiate(tx).Children(ownerID)
```

## Versions and migrations

The data of a store has a version, which the database records. When the records must be
converted for the next version of a program to read them, the version is raised and a migration
says how, as a part of that program. The type that the records had stays in the code next to it.

```go
// UserV1 is a user as version 1 wrote it.
type UserV1 struct {
    Name string
}

users := typed.New[int64, User]("users").WithVersion(2).
    MigrateFrom(1, func(id int64, old *UserV1) (*User, error) {
        var u User
        fmt.Sscan(old.Name, &u.First, &u.Last)
        return &u, nil // nil deletes the record
    })

report, err := users.Migrate(ctx, db) // when the program starts, before it uses the database
fmt.Println(report)                    // migrated typed "users" from version 1 to 2
```

A store that went through several versions has a migration for each of them, and the types of
the versions in between stay in the code next to them. Every step is given the type that the
version before wrote, and the last one gives the type of the store. A database of any version
before the last one goes through the steps from its own:

```go
users := typed.New[int64, User]("users").WithVersion(3).
    MigrateFrom(1, func(id int64, old *UserV1) (*UserV2, error) { ... }).
    MigrateFrom(2, func(id int64, old *UserV2) (*User, error) { ... })
```

The records of a type in between are read and written with the default codec of the type. A
program of that version that gave the store another codec for it passes the codec to the step
that writes the type, with `migrate.WithNewCodec(c)`, and to the step that reads it, with
`migrate.WithOldCodec(c)`. A last step that does not give the type of the store fails when the
migration runs, before it converts a record.

A program with several stores migrates them together, in one call, which compares all of them
with the database before it changes anything:

```go
report, err := migrate.Run(ctx, db, users, orders, memberships)
```

`Migrate` does what the database needs and nothing else:

| The database has | What `Migrate` does |
| --- | --- |
| Nothing of a store | Records it, with its version |
| An older version | Runs the migrations, a version at a time. One that is missing is an error |
| A newer version | Fails: the program is older than its data |
| The same version and another definition | Fails for a store: something changed without a version |
| No index or aggregation that is defined, or another definition of it | Builds it from the records |
| An index or aggregation that is not defined any more | Deletes it, as nothing keeps it up to date; it is built again if it comes back |
| An extension that keeps data and is not defined any more | Fails, unless `store.RetireExtension("audit")`, with the name that it was added with, says that its data is to be deleted |

Records are converted in batches, and the place that a migration has come to is saved with
every batch, so one that is interrupted goes on and converts no record twice. The indexes and
the aggregations of a store are built again after its migration, so they need none of their own:
adding, changing or removing an index is only a change of the code. `migrate.Custom` is for what
is not one record becoming one record. A value that only gets new fields needs no version in the
formats that leave out what they do not know, like the default one.

`users.Check(db)` or `migrate.Check(db, ...)` is for the programs that must not change a
database and for the databases that are always new, like the ones of tests: it records what is
new and fails where `Migrate` would have work to do.

## Short prefixes by name

A prefix can be any bytes, written by hand. `keyspace` hands them out by name instead, one byte
long by default, and keeps the names in the database, so the stores have readable names and the
keys stay short. It also deletes the stores that the program has dropped, which `migrate.Run`
alone reports as unknown.

```go
ks, err := keyspace.Open(db)
users := typed.New[int64, User](ks.Prefix("users"))
orders := record.New[int64, Order](ks.Prefix("orders"))
ks.Retire("toys") // the store of the toys is gone from the program: its data is deleted

report, err := ks.Migrate(ctx, users, orders) // like migrate.Run, and the units go by their names
```

## Queries

`query` builds a query on the records of a store from conditions, an order and a page, and
plans it: it chooses between scanning the store and reading one of its indexes, and between
sorting the records and reading them in the order of an index. A query is a definition that
does not change, so every method returns another one, and one query can be run in any number of
transactions.

```go
total := field.Path[Order, int64]("Total")

q := query.From(orders).
    Where(customer.Eq("ann"), total.Gte(50)). // ANDed with the conditions so far
    OrderBy(total.Desc()).                    // replaces the order; ties go by key
    Offset(20).Limit(10)                      // there is no limit unless one is set

fmt.Println(q.Explain())
// Limit 10, offset 20
// └─ TopN by Total desc
//    └─ IndexLookup by_customer: 1 range, then check [Total >= 50]

it := q.Run(tx) // an iterator that must be closed; errors come through its Err()
```

With an order, the records come in it, with the ones that have no value for a field first
(last, for a field that is `Desc()`), and records that are equal in it by key. So the result
does not depend on the plan. Without an order the records come in no order that is promised: a
scan gives the order of the keys and an index the order of its entries, so `Offset` and `Limit`
of a query without `OrderBy` page whatever order the plan gives, which can change when the
indexes do.

An index has no entry for a record that has no value for one of its fields, so the planner
reads one only if none of the records that the query asks for lacks one: each field of the index
is either `Always()` there (`field.Of`, or a `field.Path` with no pointer on the way), or a
comparison of the query needs a value of it. `index.Lookup` fails with `index.ErrIncomplete` in
the same case. An index that nothing narrows can still be read in full for its order under a
`Limit`.

An index of some of the records (`index.Where`) is read when the conditions of the query imply
its condition, so that every record the query asks for is in it. The planner decides that
conservatively: a comparison is implied by one of the same field that allows only values that it
allows (`Year > 1980` implies `Year >= 1970`, `Kind = 'a'` implies `Kind IN ('a', 'b')`), a
disjunction by one of its branches, and anything else, like a negation or a function, by the
identical condition only. The query is still checked against all its conditions, and `Using` an
index whose condition is not implied is an error.

`Explain` shows the plan as a tree of operators: `StoreScan`, `IndexLookup` (with the order
that it gives, and `reversed` if it is read from its end), `IndexOnlyScan` (the same, for a
projection that the index answers alone), `Sort` and `TopN` (when the order is
not given), `Limit` and `Empty` (when the conditions contradict each other, or the limit is 0).
The planner merges the comparisons of a field, turns an `OR` of values of one field into one
`IN`, keeps the other `NOT` and `OR` to be checked on the records, estimates the cost of each
index without any statistics (the numbers are in `query/plan.go`), and takes the cheapest.
`Using("by_customer")` and `FullScan()` are hints, and an index that cannot serve the query is
an error.

### Several indexes for one query

A query can read more than one index. A disjunction among the conditions whose branches are each
served by an index is a union of them, and a conjunction whose conditions are served by
different indexes can be an intersection of them, when that costs less than reading one index
and checking the rest on its records:

```go
q := query.From(orders).Where(cond.Or(customer.Eq("ann"), status.Eq("late")))
fmt.Println(q.Explain())
// Union
// ├─ IndexLookup by_customer: 1 range
// └─ IndexLookup by_status: 1 range

q = query.From(orders).Where(customer.Eq("ann"), status.Eq("late"), total.Gte(50))
fmt.Println(q.Explain())
// Intersect
// ├─ IndexLookup by_customer: 1 range, then check [Status = "late"; Total >= 50]  <- streamed
// └─ IndexKeys by_status: 1 range                         <- its primary keys are remembered
```

Each branch of a union is read with its own conditions and the other conditions of the query,
and the records are returned once each, in no order, so a requested order is a `Sort` or a
`TopN` on top of it, and a `Limit` without an order stops it early. An intersection streams one
index, remembers the primary keys of the others, and fetches only the records that all of them
have; the streamed index gives its order, if it is the one asked for. The results are the same
as with any other plan. The keys that are remembered are held in memory, so a plan that would
hold more than `MaxKeys(n)` of them (100,000 by default; 0 allows none) is not made. Keys that
are numbers of the same size, of 8 bytes at most, are kept exactly in a bitmap; other keys
(strings, arrays) are kept as hashes, which `Explain` marks `(hashed)`, and a record whose hash
is known is checked against the index. `UsingAll("by_customer", "by_status")` forces the
combination of exactly those indexes, and is an error (`ErrHint`) if they cannot serve the query
as an intersection or as a union, or hold more keys than `MaxKeys` allows. The last of `Using`,
`UsingAll` and `FullScan` is the hint.

## Selecting values

A query gives records. `Select` makes of it a projection, which gives rows with the values of the
items that are selected instead, and `Count` gives how many records a query has. An index can
answer them without reading the records, if it holds what they need. An index holds the fields of
its key, and it can hold more with every entry:

```go
// A field that is not in the key, with every entry: all its values, if it has several.
// A value computed from the record, with a name, which is selected like a field.
shout := index.Cover("Shout", func(o *Order) string { return strings.ToUpper(o.Customer) })
byCustomer := index.On(orders, "by_customer", customer,
    index.Including(total),    // conditions on it can be checked on the entries as well
    index.Covering(shout))     // a different codec: index.Cover(name, get, codec)

p := query.From(orders).Where(customer.Eq("ann"), total.Gte(50)).OrderBy(total.Desc()).Limit(10).
    Select(total, shout)       // no items: the keys only

fmt.Println(p.Explain())
// Limit 10
// └─ TopN by Total desc
//    └─ IndexOnlyScan by_customer: 1 range, then check [Total >= 50]

for key, row := range iterutil.Of(p.Run(tx)).All(&err) { // the rows come in the order of the query
    t, ok := row.Get(total)       // typed by the item: an int64; false if it has no value
    s, _ := row.Get(shout)
    all, ok := row.GetAll(tags)   // every value of an item that may have several
    o, err := row.Record()        // the record: read when it is asked for, if the plan did not
}

n, err := query.From(orders).Where(customer.Eq("ann")).Count(tx) // the records that Run would give
```

`Item` is anything that can be selected, a field or a cover, so one `Select` takes items of
different types, and `Get` knows the type from the item it is given. A row has the values of the
items that were selected only: `Get` on any other item is false.

An index answers a projection alone (an `IndexOnlyScan` in the plan, which reads no record) when
it holds: every selected item, as a field of its key that has one value for a record at most, as a
field that it includes, or as a cover; every condition it does not serve with its ranges, as a
comparison of a field that it holds, or a conjunction, a disjunction or a negation of such
(a function needs the record, and so does a field of the key that may have several values, as an
entry has one of them only); and, if the rows have to be sorted, every field of the order. Anything
else makes the plan an `IndexLookup`, with the records got. The values of a row are the same
whatever the plan is, and so are the keys and their order. `Count` is a projection of the keys
only, without the order: it is index-only whenever the conditions can be checked on an index.

`Iterator.Get(item)` of an index iterator reads a field of the key, an included field or a cover
from the entry, and fails with `index.ErrNotHeld` for anything else. What an index holds is part
of what it is identified by: the names and codecs of its included fields and covers, so a change
of them is a migration (`Migrate` builds the index again). The format of an entry is described in
[index/hold.go](index/hold.go).

## Conditions as text

`query/sql` reads the WHERE clause of a query, in the SQL of SQLite
([rqlite/sql](https://github.com/rqlite/sql) parses it), into the conditions that the indexes
take. Comparisons of a field with values are served by indexes; `LIKE`, functions, arithmetic
and everything else are evaluated on each record. `Where` returns the `query.Query` of a clause,
to give an order and a page to, and `Query` runs a clause with the index that serves it, or by
scanning the store; `Explain` says which.

```go
p := sql.For(orders)
it, err := p.Query(tx, `Customer = 'ann' AND Total >= 50 AND lower(Customer) LIKE 'a%'`)
plan, err := p.Explain(`Customer = 'ann' AND Total >= 50`)
// IndexLookup by_customer: 1 range, then check [Total >= 50]

q, err := p.Where(`Customer = 'ann'`) // ORDER BY and LIMIT are never in the text:
it = q.OrderBy(total.Desc()).Limit(10).Run(tx) // they are methods of the query

c, err := p.Parse(`lower(Customer) = 'ann'`) // a cond.Cond, for anything that has records
```

As in SQLite, single quotes make a string and double quotes make a name: `Customer = "ann"`
compares two fields, and is an error when there is no field `ann`. A text that names a field or
a function that there is not, that compares a field with a value that it cannot have, or that
is not true or false where a condition is expected (`Total + 1`), is an error when it is parsed.
Conditions are true or false, not true, false or unknown as in SQL: a comparison with a value that a
record does not have is false, and `NOT` makes it true.

**Functions of your own** are given to a parser, and a text calls them like `lower` and `length`:
`Func1`, `Func2` and `Func3` make one from a Go function, and `FuncN` from the types of its
arguments, for the ones that fail or that take any number. A call is checked when the text is
parsed (an unknown name, the wrong number or kinds of arguments, a result that is not true or
false where a condition is expected), and an error of a function fails the evaluation with
`sql.ErrEvaluation`.

```go
fns := sql.Functions{}.
    Add(sql.Func1("initial", func(s string) string { return s[:1] }).WithVersion("1")).
    Add(sql.Func2("age", func(year, now int) int { return now - year }))
p := sql.For(orders, sql.WithFunctions(fns)) // other parsers do not see them
it, err := p.Query(tx, `initial(Customer) = 'a' AND age(Year, 2000) > 30`)
```

A function must be deterministic. Its version says what it computes, and an index on an
expression or a condition that calls it is built again by `Migrate` when the version changes.

**Indexes of an SQL expression and of an SQL condition** are the same as the Go ones, written as
text. `sql.Expr[V, F]` is a field whose values are those of an expression of the record, and
`sql.Where[V]` the option that keeps the records that satisfy a condition. A query is served by
the index when it compares the same expression with values, in whatever spelling (the path of the
field is the expression as the parser prints it, with the names of functions in lower case), or
when its conditions imply the condition of the index.

```go
index.On(orders, "by_customer_lower", sql.Expr[Order, string](`lower(Customer)`))
index.On(orders, "big_by_customer", customer, sql.Where[Order](`Total >= 1000`))

p.Explain(`LOWER(Customer) = 'ann'`)           // IndexLookup by_customer_lower: 1 range
p.Explain(`Total > 5000 AND Customer = 'ann'`) // IndexLookup big_by_customer: 1 range, then check [Total > 5000]
p.Explain(`Total > 500 AND Customer = 'ann'`)  // not implied by the condition of big_by_customer
```

A record whose expression cannot be evaluated or converted to `F` is not left out of the index:
writing it fails with the error (`field.Computed` is the field behind it). The text of an
expression and of a condition, and the versions of the functions they call, identify the index,
so a change in either is a migration.

## What is specific to Badger

The general interfaces have what every ordered engine has. The rest of Badger stays within
reach:

| Feature of Badger | Where it is |
| --- | --- |
| Encryption, compression, in-memory and read-only modes, conflict detection | The options that the database is opened with |
| Keys that expire | `kv.SetWithTTL`, and `SetWithTTL` of a typed store. Not for a store with indexes or aggregations, which would not be told |
| `DropPrefix` | `kv.DropRange` |
| `WriteBatch` | `kv.NewBulkWriter` |
| Garbage collection of the value log | Done by the DB that `badgerkv.Open` opens, every 5 minutes; `badgerkv.WithValueLogGC` and `WithoutValueLogGC` change that, and `db.RunValueLogGC()` runs it now |
| Sequences | `db.Sequence`, which the DB releases when it is closed, and `badgerkv.SequenceID` to take the ids of records from one |
| Backup and restore, `Flatten`, `Subscribe`, `Stream`, merge operators | `db.Badger()` |
| The versions of a key, the metadata of an item, managed timestamps | `badgerkv.Unwrap(tx)`, which returns the transaction of Badger |
| The user metadata byte | Not used |

An engine that does not have a capability returns `kv.ErrUnsupported` for it.

## What is not there yet

- A planner that combines indexes (a query reads one, and checks the rest on the records) or
  keeps statistics; and queries as text beyond the WHERE clause.
- Building an index while the program writes to its store: `Migrate` is alone with the data.
- A migration that changes the keys of the records, other than by hand in `migrate.Custom`.
- Records that expire in stores with indexes or aggregations.
- A second engine besides Badger and the in-memory one.

## Example

[examples/petstore](examples/petstore) is a small HTTP server on Badger with record stores,
relations that cascade, an index, two aggregations, a running collector, and a migration from
the version of its pets before.

```sh
cd examples/petstore && go run .
```

The examples of the packages, which are run as tests, show each of them in more detail:
[index](index/example_test.go), [agg](agg/example_test.go), [relation](relation/example_test.go),
[store/record](store/record/example_test.go), [migrate](migrate/example_test.go),
[query](query/example_test.go), [query/sql](query/sql/example_test.go).

## Tests

```sh
go test -race ./...
```

The engines are checked by one conformance suite (`kv/kvtest`), which is there for other
engines too.
