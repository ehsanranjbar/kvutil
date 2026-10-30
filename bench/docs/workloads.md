# The workloads

What each workload of the suite does: its variants, its cases and how `kvbench verify` checks it.
`kvbench list` has them with their variants. How to run the suite and read its report is in the
[README](../README.md).

## kv-basic (group `kv`)

Get, Set (update of a record that is there, and insert of a new one), Delete, and scans by the
primary key, short (100 rows) and long (10,000 rows, at most half of the scale), at 1 worker and at
`-concurrency`. Cases: `load`, `get`, `scan-short`, `scan-long`, `set-update`, `delete`, `set-insert`.
The ids are `0..N-1`; a `get` and an update take a random id, a scan starts at a random id, and a
delete takes the records of a pool that is loaded right before, past the scale, each once
(`pool`, 20,000 to 300,000 of them, a tenth of which are the warm-up).

* kvutil, three layers over the same Badger, and values that are the same bytes:
  * `raw`: `kv.Tx` with the id as 8 bytes (big endian) and the bytes of the record, encoded by
    msgpack outside of the measured time. This is the engine and the thin adapter.
  * `typed-bytes`: `typed.Store[int64, blob]`, whose values are bytes that encode themselves
    (`MarshalBinary`). The difference with `raw` is the typed layer alone (the prefix, the key codec,
    the instances, the checks).
  * `typed`: `typed.Store[int64, Order]` with its default codecs, which encodes and decodes the
    record (msgpack) inside of the measured time. This is how the store is used.
* PostgreSQL, `bytea`: `kv(k bigint PRIMARY KEY, v bytea)`, with the same bytes as `raw`. The key is
  a `bigint`, which is the natural key of an integer id (a `bytea` key would only add the cost of
  comparing bytes), and the value is `bytea`, which is the shape of a key-value use of a table, so
  that what is compared is the same data and the same access. `Get` is `SELECT v ... WHERE k = $1`,
  `Set` an upsert (`INSERT ... ON CONFLICT DO UPDATE`), scans `WHERE k >= $1 ORDER BY k LIMIT $2`; all
  prepared by pgx.

`raw` and `typed-bytes` return bytes without decoding them, and so does PostgreSQL; `typed` decodes
a record: compare it with the others knowing that.

## bulk-load (group `load`)

Loading N records into an empty database, each strategy into a database of its own that is made and
removed for it, at 1 worker and at `-concurrency` (the workers load contiguous parts of the ids).
The time includes the last flush or commit, and the case also has the size on disk.

| System | Variant | Strategies (cases) |
| --- | --- | --- |
| kvutil | `typed` | `writebatch` (`kv.NewBulkWriter`: Badger's `WriteBatch`), `txn-b1000`, `txn-b100`, `txn-b1` (transactions of that many records, with `typed.Store.Set`) |
| PostgreSQL | `bytea` (`kv` table as in kv-basic), `rows` (a table of columns) | `copy`, `insert-b1000`, `insert-b100` (multi-row `INSERT` of that many rows in one statement), `insert-b1` (one row per statement) |

The tables have their primary key when they are loaded; kvutil's key is the primary key. The loads of
single records are capped at 5,000 records (`params.case.capped`), as each is a commit that waits
for the disk in `durable` mode. After each load the records are counted.

## record-codecs (group `records`)

`record.Store` (`record.New`) with three value codecs, against three shapes of a PostgreSQL row:

| System | Variant | |
| --- | --- | --- |
| kvutil | `msgpack` | the default codec |
| | `json` | `codec.Formatted[Order](codec.JSON)` |
| | `binary` | a record type with `MarshalBinary`/`UnmarshalBinary` written by hand (a format of varints and lengths), which a record store takes as its codec by itself |
| PostgreSQL | `columns` | a row of typed columns, the tags in a `text[]` |
| | `jsonb` | `(id, doc jsonb)`, the record marshalled by `encoding/json` in the client |
| | `bytea-msgpack` | `(id, v bytea)` with the msgpack of kvutil's default |

Cases: `load`, `size` (average bytes of a stored record: the length of the codec's output, or
`pg_column_size`), `get`, `set-update`, `set-insert`. Reads decode into the same Go struct in all of
them. The ids start at 1, as a record store takes the zero id for a record that has none. The id is
the key, not a part of the value.

## index-maintenance (group `index`)

What an index costs a write, and what it costs to build. A variant is a configuration of indexes,
on a database of its own: the records are loaded first, without the indexes, and then the indexes are
built (`build`: kvutil `Migrate` of a record store that has the indexes added, which builds them
over the records that are there; PostgreSQL `CREATE INDEX`), and then the writes are measured with
`update`, `delete` and `insert`. Updates give a record every field again, so every index entry
changes; deletes take records of a pool that is loaded right before (through the indexes); inserts
are new records.

| Variant | kvutil | PostgreSQL |
| --- | --- | --- |
| `none` | no index | no index (the primary key is there) |
| `n1`, `n3`, `n5` | `index.On` of `Customer`; and `Status`, `Total`; and `At`, `Name` | B-tree on `customer`; `status`, `total`; `created_at`, `name` |
| `unique` | `index.Unique` on `Code` | `CREATE UNIQUE INDEX` on `code` |
| `composite` | `index.Composite` of `Customer`, `At` | B-tree on `(customer, created_at)` |
| `multi` | `index.On` of the slice field `Tags`: an entry per tag | GIN on the `text[]` column `tags` (a join table would be the other way; GIN is what is used for this) |
| `expr` | `sql.Expr[Order, string]("lower(Name)")` | an expression index on `lower(name)` |
| `conditional` | `index.Where(Total >= 90000)` on `Customer`: a tenth of the records | a partial index `WHERE total >= 90000` |
| `covering` | `index.Including(Total)` on `Customer` | `INCLUDE (total)` |

PostgreSQL's table is the columns of `record-codecs`; kvutil's is the record store with msgpack. The
`build` of PostgreSQL is its server's work, so the CPU of the harness in that case is nothing: see
the `docker stats` file for it. kvutil builds one index after the other on one thread, and
PostgreSQL may use parallel workers for a large build.

## queries (group `query`)

The queries that a catalogue of books answers, on kvutil and PostgreSQL with the same indexes, each
asked in the ways that each system has. The records are books (`dataset.Book`, ids 1 to N):

| Field | Values |
| --- | --- |
| `code` | unique, 8 hex digits of a scramble of the id (`BK-9a3f21c7`), so the order of the codes is not that of the ids |
| `author` | `author-000000` and so on, N/20 of them (50 at least), drawn from a Zipf law of exponent 1: the author of rank k has about 1/(k+1) of the books of the first, who has ln 2 / ln(A+1) of all (6% at a million books) |
| `year` | 1450 to 2025, uniform: 576 years |
| `price` | 100 to 9999 (cents), uniform |
| `in_print` | true for 10% |
| `tags` | 0 to 4 different ones of 50 (`tag-07`), uniform: 4% of the books have a given tag |
| `title` | an adjective, a noun and a number, `Blue Widget 12`: about four books have each; 256 adjective-noun prefixes |
| `subtitle` | optional: absent for half |
| `score` | uniform, 0 to 2^30 - 1, with no index here: what the top-N case sorts by |
| `payload` | 4096 random bytes with `-values large` |

PostgreSQL has `books(id bigint PRIMARY KEY, code text, author text, year bigint, price bigint,
in_print boolean, tags text[], title text, subtitle text, score bigint, payload bytea)`, and kvutil
a record store with msgpack values, with the same eight indexes. The books are loaded first and the
indexes built after (kvutil `Migrate`, PostgreSQL `CREATE INDEX`), both as phases (`load` and
`build`), which the first way of each system reports; then PostgreSQL runs `VACUUM (ANALYZE)` and a
checkpoint, so that it has statistics and a visibility map, and index-only scans are possible.

| Index | kvutil | PostgreSQL |
| --- | --- | --- |
| unique code | `index.Unique(…, Code)` | `CREATE UNIQUE INDEX ON books (code)` |
| author | `index.On(…, Author)` | `(author)` |
| author and year | `index.Composite(…, Author, Year.Desc())` | `(author, year DESC, id)` |
| year | `index.On(…, Year)` | `(year, id)` |
| tags | `index.On(…, Tags)`, an entry per tag | `USING gin (tags)` |
| lower(title) | `index.On(…, sql.Expr[Book, string]("lower(Title)"))` | `(lower(title))` |
| in print | `index.On(…, Price, sql.Where[Book]("InPrint"))` | `(price) WHERE in_print` |
| covering | `index.On(…, Author, index.Including(Title, Price))` | `(author) INCLUDE (title, price)` |

Every entry of an index of kvutil ends with the primary key, which is why the two indexes of
PostgreSQL that serve an order have `id` as their last column: the order that kvutil promises
(the fields, then the key) comes from them without a sort.

**The ways of asking** are the variants. All run on one database that they share, which the first
loads and the last removes:

| System | Variant | |
| --- | --- | --- |
| kvutil | `typed` | the query is built from typed conditions (`query.From(store).Where(Year.Between(…)).OrderBy(…)`) for each operation, and planned when it runs |
| | `sql` | the text of the WHERE clause is parsed for each operation (`sql.For(store).Where(text)`, the parser is made once), then ordered, paged, planned and run: parse, plan and run are measured |
| | `prepared` | the query is built and planned beforehand, for each of the arguments of the pool, and only run |
| PostgreSQL | `prepared` | the statement is prepared once on each connection (pgx), and run with parameters; the server may use a generic plan after five runs |
| | `simple` | the statement is sent as text with the values in it, by the simple protocol (`QueryExecModeSimpleProtocol`): the server parses and plans it for each operation |

kvutil has no bind parameters, so "prepared" there is one query for each set of arguments: every case
has a pool of 1,024 sets of arguments (random, from the seed; the same pool for every way and both
systems), an operation picks one at random, and `prepared` has the 1,024 queries built and planned
before the measurement. A query reads and decodes everything it returns (kvutil decodes the books,
pgx scans the rows into a `Book`), and the rows are counted (`units`, rows/s).

**The cases**, each at 1 worker and at `-concurrency`. `$1`… are the parameters of the prepared
statement; the select list is every column (`id, code, … payload`) unless the case says otherwise.
Where a case has no order, the order of the result is not a part of it.

| Case | Asks for | kvutil (typed) | kvutil (text of WHERE) | PostgreSQL (`SELECT … FROM books WHERE`) |
| --- | --- | --- | --- | --- |
| `point-lookup` | a book by its code | `Code.Eq(c)` | `Code = 'c'` | `code = $1` |
| `author-few` | the books of an author with 1 to 4 | `Author.Eq(a)` | `Author = 'a'` | `author = $1` |
| `author-many` | those of an author of the ranks 20 to 39 (0.2% to 0.4% of the books: 2,300 to 4,400 at a million) | the same | the same | the same |
| `year-narrow` | two years: 0.35% | `Year.Between(y, y+1)` | `Year BETWEEN y AND y+1` | `year BETWEEN $1 AND $2` |
| `year-wide` | 25 years: 4.3% | `Year.Between(y, y+24)` | the same | the same |
| `author-year-order` | the 10 latest books of an author of the ranks 20 to 39 in 100 years: the composite index in the order asked for | `Author.Eq(a)`, `Year.Between(…)`, `OrderBy(Year.Desc()).Limit(10)` | `Author = 'a' AND Year BETWEEN lo AND hi` and the same order | `author = $1 AND year BETWEEN $2 AND $3 ORDER BY year DESC, id LIMIT 10` |
| `top-n-sort` | the 10 books of the highest score among those of 5 years: no index has the order | `Year.Between(y, y+4)`, `OrderBy(Score.Desc()).Limit(10)` | `Year BETWEEN y AND y+4` and the same order | `year BETWEEN $1 AND $2 ORDER BY score DESC, id LIMIT 10` |
| `deep-paging` | a page of 20 books in the order of years after an offset of 10,000 + 20 × (0 to 9) (less in a table of less than 20,300 books: (N-300)/2) | `OrderBy(Year).Offset(o).Limit(20)` | `TRUE` and the same order | `ORDER BY year, id OFFSET $1 LIMIT 20` |
| `in-list-20` | 20 books by their codes | `Code.In(c1, …, c20)` | `Code IN ('c1', …)` | `code = ANY($1::text[])` |
| `or-two-indexes` | the books of an author with a few, or of a year: kvutil union, PostgreSQL BitmapOr | `cond.Or(Author.Eq(a), Year.Eq(y))` | `Author = 'a' OR Year = y` | `author = $1 OR year = $2` |
| `and-two-indexes` | the books of a year that have a tag: kvutil intersection, PostgreSQL BitmapAnd | `Year.Eq(y)`, `Tags.Eq(t)` | `Year = y AND Tags = 't'` | `year = $1 AND tags @> ARRAY[$2]::text[]` |
| `tag-membership` | the books that have a tag: 4% | `Tags.Eq(t)` | `Tags = 't'` | `tags @> ARRAY[$1]::text[]` |
| `like-prefix` | the books whose title starts with an adjective and a noun: 0.4% | `cond.Func("Title LIKE 'p%'", strings.HasPrefix)` (no typed LIKE) | `Title LIKE 'p%'` | `title LIKE $1` (`'p%'`) |
| `lower-title` | the books of a title, as lower case: the expression index | `sql.Expr("lower(Title)").Eq(t)` | `lower(Title) = 't'` | `lower(title) = $1` |
| `conditional-index` | the books in print with a price in a range of 300 cents: 0.3% | `InPrint.Eq(true)`, `Price.Between(lo, lo+300)` | `InPrint AND Price BETWEEN lo AND hi` | `in_print AND price BETWEEN $1 AND $2` |
| `index-only-select` | the title and price of the books of an author of the ranks 20 to 39: held in an index | `Select(Title, Price)` of `Author.Eq(a)` | `Author = 'a'` | `SELECT title, price … WHERE author = $1` |
| `count` | how many books there are in 25 years | `Count(tx)` of `Year.Between(y, y+24)` | the same text | `SELECT count(*) … WHERE year BETWEEN $1 AND $2` |

The arguments are books that exist (a code, a title, a year with a tag) where a case asks for
one, so that the answers are not empty. `like-prefix` is evaluated on each record by kvutil
(`StoreScan`) and is a `Seq Scan` in PostgreSQL, whose default collation could not use a B-tree
for a prefix; both are what a system does without an index meant for it.

**Ties.** The answers of `author-year-order`, `top-n-sort` and `deep-paging` must be the same books
in the same order. kvutil breaks a tie by the key (the id, which sorts as a number), so every
`ORDER BY` of PostgreSQL ends with `, id`; and the indexes that serve an order have `id` last.
`score` has 2^30 values for the books, so ties are rare, but they would be settled the same way.
`year` has many ties, which is why those orders have the key.

**Reading the plans.** See [Reading the plans](#reading-the-plans).

**Verification.** The first four sets of arguments of every case are answered by every way of
both systems: for the books, the number of rows and a digest of the canonical text of every book
(all of its fields), in the order of the case if it has one and sorted otherwise, and the ids if
there are 40 or fewer; for `index-only-select` the sorted lines `title|price`; for `count` the
count. All must be the same.

## selectivity-sweep (group `query`)

A range on an indexed field, `score BETWEEN lo AND hi`, whose selectivity goes from 10 rows to half
of the table, with and without `ORDER BY` the primary key, and what each system does about it.
Only `score` is indexed (besides the key). The steps are 0.001%, 0.01%, 0.1%, 1%, 5%, 10%, 25% and 50%
of the table, but never fewer than 10 rows: in a small table the first steps are the same, and run
once (10,000 books: 10, 100, 500, 1,000, 2,500 and 5,000 rows). A case is named for its percentage
and its order: `0.1pct-unordered`, `0.1pct-by-pk`; `params.case` has `selectivity_pct`,
`target_rows` and `order`. The ranges are 256 random ones of the right width (a range finds
about the rows of the step: scores are uniform), and the queries of kvutil are built and planned
before the measurement; PostgreSQL runs a prepared statement.

| | kvutil | PostgreSQL |
| --- | --- | --- |
| Query | `Score.Between(lo, hi)`, and `OrderBy(ID)` for the key | `SELECT … FROM books WHERE score BETWEEN $1 AND $2 [ORDER BY id]` |
| Variants | `planner` (as it plans), `index` (`Using("by_score")`), `scan` (`FullScan()`) | `planner` |

The three variants of kvutil let the report find where reading the index stops paying whatever the
planner picks, which has no statistics. kvutil has no way to ask for the order of the primary key:
`OrderBy(ID)` sorts by a field that no index has, so the plan is a `Sort` on top of the read
(a scan of the store does not know that it is in the order of the keys).

## key-order-fetch (group `query`, kvutil only)

Does a "bitmap heap scan" pay on Badger? K records are fetched by primary key, the keys coming in
random order, as an index gives them; is it faster to read them in that order, or to sort the keys
first and read them in the order of the keys? Records are msgpack values under 8 byte keys in raw
Badger (ids 0 to N-1); the variants are `small` (about 200 bytes) and `large` (4 KB; at most 250,000
records), whatever `-values` says. Each K (10, 100, 1,000, 10,000, 100,000 and half of the records,
500,000 at most, but not more than half) is read in four ways, all from the unsorted ids to the last
value read, in a read transaction of its own:

| Way | |
| --- | --- |
| `random-get` | `tx.Get` for each key in the order they came in |
| `sorted-get` | the keys sorted first (that is in the time), then `tx.Get` for each: only the order changes |
| `seek` | sorted, then one iterator and `Seek` to each key in turn |
| `scan` | sorted, then one iterator from the least to the greatest key, which steps over the keys that are not wanted; run when K is 1% of the records or more, as it reads them all |

The cache is `warm` (the whole database is read before) or `cold` (the database is closed and
opened again before each trial, which empties the block cache of Badger; the page cache of the
operating system stays, as it cannot be dropped from a container). The iterators do not read values
ahead, as they would read those of the keys that are skipped. A trial picks a new set of keys (the
same for every way and both cache states: the trials are numbered), and there are as many trials as
fit in the duration of the run, from 3 to 25 (5 at most when cold). The case is `way/kK/cache`
(`seek/k1000/cold`); the result has no latencies but `extra`: `ns_per_record_median`, `_min`, `_mean`
and `_max` over the trials, `trials` and `percent_of_the_table`, and `params.case` has `way`, `k`,
`cache`, `records`, `values` and `value_bytes`. The ways must read the same values, which they check
(the number of values, their bytes, and a checksum that depends on the key of each).

## The aggregations (group `agg`)

Stored aggregations of time-series events, on kvutil (`agg.Sum`, `agg.Count`, `agg.Max`, with a
collector), on TimescaleDB (a hypertable and continuous aggregates with refresh policies) and, as
the baselines, on PostgreSQL (computed at read time, and a summary table that a trigger keeps). It
is the one group that runs on a third system, `timescale`: `kvbench run -system timescale`, with
`KVB_TS_DSN` for its DSN. `run.sh` starts the `timescaledb` service (the compose profile `timescale`)
only if a selected workload runs on it (`kvbench needs -workload …` says so), and runs the
workloads of the group on kvutil, PostgreSQL and TimescaleDB one after the other, as for the rest.
The version of the extension is in each result (`system_version`: `PostgreSQL 18.6 …; timescaledb 2.31.0`,
and `settings.timescaledb`).

```sh
./run.sh --smoke -w agg                          # about 10 minutes
./run.sh -w agg -s 1000000 --agg-interval 1s     # a run: a million events over a year
./run.sh -w agg-freshness --agg-interval 5s      # the collector and the policies run every 5 seconds
```

### The events

`dataset.Event` is `(id, customer, amount, at, kind)`: the amount is in cents (an integer, so that
sums and maxima are exact on every system and cannot drift), `at` has the resolution of a second
(UTC), and `kind` is one of four words that no aggregate looks at.

| | |
| --- | --- |
| Events | `-scale` of them, ids 1 to N |
| Customers | `max(10, N/100)`, drawn from a Zipf law of exponent 1 (as the authors of the books): the first has ln 2 / ln(C+1) of the events (13% of 20,000 events), the one of rank k about 1/(k+1) of the first's. The reads draw their customer from the same law, so that the popular ones are read more |
| Times | spread uniformly over the `KVB_AGG_SPAN_DAYS` (default 365, `--agg-span-days`) days that end at midnight (UTC) of `KVB_AGG_END` (exclusive; `run.sh` sets it to the day it started, once for the whole run, so that every system gets the same events even if the run goes past midnight) |
| The stream | The new events of the write workloads: ids past N, a customer drawn as above, and `at` = now, which is in the day that `KVB_AGG_END` is (a run on a day other than it is also fine) |
| Versions | The version of an event changes its amount only: the customer, the time and the kind are those of version 0. The updates of the workloads are new versions; the verification also moves events (another customer and time) |

### What each system keeps

| System | Variant | What it keeps | What a read does |
| --- | --- | --- | --- |
| kvutil | `collector` (writes, rebuild), `exact`, `collected` (reads, freshness) | Three aggregations on a record store of the events (below). The collector runs with `agg.WithInterval(KVB_AGG_INTERVAL)` (1 s), `Spawn`ed while writes go on | `exact`: `Series`, `At`, `Get`, which fold the notes that are not collected yet; `collected`: `SeriesCollected`, `AtCollected`, `GetCollected`, the stored states only |
| PostgreSQL | `computed` | Nothing: `events` with an index on `(customer, at)` | `GROUP BY date_trunc(…, 'UTC')` at read time |
| | `trigger` | `daily_summary(customer, day, sum_amount, cnt, max_amount)` kept by a row trigger for every insert, update and delete | The summary (`date_trunc` over its days for weeks and months) |
| TimescaleDB | `realtime` | Hypertable `events` and the continuous aggregates `daily`, `weekly` and `monthly`, with `materialized_only = false`; the policies refresh up to an hour ago | The aggregate: its materialization up to the watermark, and the hypertable after it |
| | `materialized` | The same, with `materialized_only = true`; the policies refresh up to the end of the data | The materialization only |
| | `caggs` (rebuild) | The same, built by the workload | The materialization only |

**kvutil** (`internal/workloads/aggs/kvutil.go`), on `record.New[int64, Event]("events")` (msgpack):

```go
at := func(e *Event) time.Time { return e.At }
amount := func(e *Event) (string, int64, bool) { return e.Customer, e.Amount, true }
sum   := agg.Sum(events, "amount_sum", amount, agg.ByTime(at, agg.Daily, agg.Weekly, agg.Monthly, agg.AllTime))
count := agg.Count(events, "event_count", func(e *Event) (string, bool) { return e.Customer, true },
                   agg.ByTime(at, agg.Daily, agg.Weekly, agg.Monthly, agg.AllTime))
max   := agg.Max(events, "amount_max", amount, agg.ByTime(at, agg.Daily, agg.Weekly, agg.Monthly))
collector := agg.NewCollector(db, []agg.Source{events}, agg.WithInterval(interval))
```

`Sum` and `Count` are delta aggregations (a note for each write, folded by the collector into the
four states of the customer). `Max` is a recomputed one: `agg.Max` gives it `WithMerge`, so only the
days are stored and the weeks and months are put together from them when they are read. The
workload asks for the days and the months; the weeks cost nothing more (they are merged), and give
the read of the weeks a maximum too. Calendar buckets are in UTC, and the weeks start on Monday.

**TimescaleDB 2.31** (`sqltext.go`; `S` is the schema of the variant):

```sql
CREATE TABLE S.events (id bigint NOT NULL, customer text NOT NULL, amount bigint NOT NULL,
                       at timestamptz NOT NULL, kind text NOT NULL, PRIMARY KEY (at, id));
SELECT create_hypertable('S.events', by_range('at', INTERVAL '7 days'), create_default_indexes => false);
CREATE INDEX events_customer_at ON S.events (customer, at);

CREATE MATERIALIZED VIEW S.daily WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', at) AS bucket, customer,
       sum(amount)::bigint AS sum_amount, count(*) AS cnt, max(amount) AS max_amount
FROM S.events GROUP BY 1, 2 WITH NO DATA;

-- hierarchical: the weeks and the months are made from the days (the months likewise, with '1 month')
CREATE MATERIALIZED VIEW S.weekly WITH (timescaledb.continuous) AS
SELECT time_bucket('1 week', bucket) AS bucket, customer,
       sum(sum_amount)::bigint AS sum_amount, sum(cnt)::bigint AS cnt, max(max_amount) AS max_amount
FROM S.daily GROUP BY 1, 2 WITH NO DATA;

-- the variant `realtime` only (the three of them):
ALTER MATERIALIZED VIEW S.daily SET (timescaledb.materialized_only = false);

-- the policies of the workloads that write (KVB_AGG_INTERVAL is the schedule, 1000 ms):
SELECT add_continuous_aggregate_policy('S.daily', start_offset => NULL,
         end_offset => NULL,                 -- `materialized`; `realtime`: INTERVAL '1 hour'
         schedule_interval => INTERVAL '1000 milliseconds');   -- and likewise for weekly and monthly

-- the whole range, by hand (the catch-up, and the rebuild):
CALL refresh_continuous_aggregate('S.daily', NULL, NULL);      -- `realtime`: up to the end of the loaded events
```

* A unique index of a hypertable must have the column of the time, so the key is `(at, id)`, and the
  statements that find an event by its id (update, delete) give its time too. The hypertable
  has the same `(customer, at)` index as the table of PostgreSQL; its chunks are the default 7 days;
  nothing is compressed (a compressed chunk would make the size of the events smaller than the
  PostgreSQL's, and the aggregates are not what compression is for).
* `time_bucket('1 week', …)` is stored by TimescaleDB as `7 days`; its origin is a Monday. An `UPDATE`
  that moves a row to another chunk fails with a check constraint, so the update of the time of an
  event (only the verification makes one) is a `DELETE` and an `INSERT` in one transaction.
* **Defaults found in 2.31.** A continuous aggregate created with no option is **materialized only**
  (`materialized_only = t` in `timescaledb_information.continuous_aggregates`; real-time
  aggregation is off by default since 2.13), so the real-time variant is made by the `ALTER`. A
  continuous aggregate on another one (**hierarchical**) is supported: the weeks and the months are
  defined on `daily`, not on the hypertable. They are refreshed after the days
  (`daily`, `weekly`, `monthly`: `caggUnits`), which is the order they depend on one another in.
* **The window of a refresh.** The end of a window is rounded down to the start of a bucket, so a
  bucket that is not complete is not materialized, unless the end is `NULL`: then the last bucket
  with data is, partial or not, and the watermark of the aggregate moves to the end of it. A
  real-time aggregate reads the materialization up to its watermark and the hypertable after it, so it
  does not see what is written to a partial bucket that was materialized, until the next refresh. That
  is why the two variants have different refreshes: `materialized` refreshes up to `NULL`
  (the materialization is as current as the schedule allows, and it is what its lag is
  measured for), and `realtime` up to an hour ago (the policy) and up to the end of the loaded
  events (the catch-up), which leaves the buckets of the new events to the real-time part. The
  window starts at the beginning of time (`start_offset => NULL`), so that a change to an
  old event is refreshed too; a policy with `start_offset => '3 days'` would never refresh it.
* **What a real-time aggregate is exact for.** The events that are newer than its watermark (the new
  events, by the design above). A change to an old event (an update or a delete) is in a region that
  is materialized: until the next refresh the real-time aggregate has the old answer, which
  `agg-freshness` measures (`lag-change-old`) and the verification keeps out of the comparison
  (below). kvutil's exact read and PostgreSQL's are exact the moment a write commits.
* The container has `max_worker_processes=32` and `timescaledb.max_background_workers=8` besides
  the settings of PostgreSQL (see docker-compose.yml): the 8 worker processes of the default
  would be shared with the parallel queries, and the policies would not get workers.

**PostgreSQL** (`computed`: `events(id bigint PRIMARY KEY, customer, amount, at timestamptz, kind)` with
`(customer, at)`). `trigger` adds:

```sql
CREATE TABLE S.daily_summary (customer text NOT NULL, day timestamptz NOT NULL, sum_amount bigint NOT NULL,
                              cnt bigint NOT NULL, max_amount bigint NOT NULL, PRIMARY KEY (customer, day));
-- AFTER INSERT OR UPDATE OR DELETE ON events FOR EACH ROW, in plpgsql:
--   OLD (update, delete): sum_amount -= OLD.amount, cnt -= 1 for (OLD.customer, date_trunc('day', OLD.at, 'UTC'));
--     the maximum of that day is computed again from the events of the day if it was OLD.amount or less;
--     the row is deleted at cnt = 0
--   NEW (insert, update): INSERT ... ON CONFLICT (customer, day) DO UPDATE
--     SET sum_amount = s.sum_amount + EXCLUDED.sum_amount, cnt = s.cnt + 1, max_amount = greatest(...)
```

The trigger is off while the events are loaded; the summary is then built by one
`INSERT … SELECT … GROUP BY` and the trigger turned on. Writers of the same customer and day take
turns on the row of the summary: what `agg-hot-group` shows.

### The reads

All the reads are answered by every variant from the same questions (`model.go`); a series returns a
row for each span with events in it, with the sum, the count and the maximum, and a window is made of
whole spans. The statements (`S` the schema) of the three sources:

| Read | Computed (events) | Summary table | Continuous aggregate |
| --- | --- | --- | --- |
| series of a unit `u` (`day`, `week`, `month`) | `SELECT date_trunc('u', at, 'UTC'), sum(amount)::bigint, count(*), max(amount) FROM S.events WHERE customer = $1 AND at >= $2 AND at < $3 GROUP BY 1 ORDER BY 1` | `SELECT day, sum_amount, cnt, max_amount FROM S.daily_summary WHERE customer = $1 AND day >= $2 AND day < $3 ORDER BY day`; for weeks and months `date_trunc('u', day, 'UTC')` with `sum`, `sum`, `max` and `GROUP BY 1` | `SELECT bucket, sum_amount, cnt, max_amount FROM S.daily` (or `weekly`, `monthly`) `WHERE customer = $1 AND bucket >= $2 AND bucket < $3 ORDER BY bucket` |
| total of a customer | `SELECT coalesce(sum(amount), 0)::bigint, count(*) FROM S.events WHERE customer = $1` | the same `sum(sum_amount)`, `sum(cnt)` of the summary | the same of `S.monthly` |
| top 10 customers of a month | `SELECT customer, sum(amount)::bigint s FROM S.events WHERE at >= $1 AND at < $2 GROUP BY customer ORDER BY s DESC, customer LIMIT 10` | the same of `sum_amount` of the summary, `day >= $1 AND day < $2` | `SELECT customer, sum_amount FROM S.monthly WHERE bucket = $1 ORDER BY sum_amount DESC, customer LIMIT 10` |

kvutil reads the series with three calls in one read transaction, one for each aggregation
(`Series(bucket, customer, from, to)` of `amount_sum`, `event_count` and `amount_max`), merged by the
start of the span; the total with `Get` of the sum and the count; and the top 10 has no read of its
own: **an aggregation has no way to read all its groups**, so the top 10 reads the month of every
customer (`At(Monthly, customer, month)`, C of them in one transaction) and sorts. That is the cost
of the case `top10-monthly` on kvutil, and it grows with the number of customers.

### The aggregation workloads

All of them load the events as the phases `load` (with the aggregates noted, summarized or
invalidated, as the system does it: kvutil in transactions of 500 events with their notes,
PostgreSQL by `COPY` and one `INSERT … SELECT` for the summary, TimescaleDB by `COPY` into a
hypertable that has its continuous aggregates) and `catch-up` (kvutil's `Collect` of every
note; a refresh of every continuous aggregate; nothing for PostgreSQL), then settle (a checkpoint, and
`ANALYZE`). `load` + `catch-up` is what it costs to have N events and their aggregates.

* **agg-write.** Cases `insert` (new events of the stream), `update` (a new amount for an old
  event, picked at random) and `delete` (a pool of old events, each once, of the upper half of the ids:
  a case of operations, the first tenth the warm-up), each at 1 worker and at `-concurrency`, with the
  background work running: kvutil's collector, the refresh policies of TimescaleDB. Between the
  cases everything is brought up to date, so that a case does not start with the debt of the one
  before. A change to an old event forces kvutil to note the removal and the addition (and mark the
  day to compute the maximum of again), and TimescaleDB to invalidate the range, which the next
  refresh recomputes. The kvutil variant also reports `<case>-background` with the retries
  (conflicts) of the collector's transactions and the errors it reported (`collector_retries`,
  `collector_errors`); `retries` of the case itself are the writers'.
* **agg-hot-group.** The writers insert events of one customer (rank 0) at the same time of the day
  (now): the same group, the same day and the same month in every aggregation. `insert-hot` and, for
  comparison, `insert-spread` (the same writers with the customers of the dataset), at 1 worker
  and at `-concurrency`. Look at `errors`, `retries` and the latencies: writers of kvutil do not
  conflict on an aggregation (they note), and the trigger's writers wait for one another on the row
  of the summary.
* **agg-read.** `daily-30` (a window of 30 days at random), `daily-365` (the whole span),
  `weekly-year`, `monthly-year`, `total` and `top10-monthly` (a month of the span at random), each at 1
  worker and at `-concurrency`, on all the events, caught up. The customer is drawn from the Zipf law
  of the events. The ways: kvutil `exact` and `collected`; PostgreSQL `computed` and `trigger`;
  TimescaleDB `realtime` and `materialized`. The first query of every case is checked against the
  exact answer before it is measured. `units` are the rows returned.
* **agg-freshness.** On all variants. *Lag* (`lag-insert`, `lag-change-old`): the trials wait until
  the aggregates have everything, wait a random part of the interval (so that the trials do not
  all start at the same point of the cycle of the background work), and write: a burst of 200 new
  events of the customer of rank 0 by the workers at once, or (`lag-change-old`) 20 updates and 5
  deletes of its events of the last 60 days. The clock starts when the last write returns. The exact
  answer is read from the events (for kvutil from its exact read), and then the variant's read is
  polled every 5 ms (`poll_ms`) until it is the same, for the series of days, weeks and months of
  the customer over the last 60 days and the stream; `lag_daily_*` is the time until the series of
  days was right and `lag_all_*` until all three were (so, for TimescaleDB, until the hierarchy has
  gone through). The result has the minimum, mean, median, 95th percentile and maximum in milliseconds
  (`extra`: `lag_daily_ms_p50` and so on, `trials`, `timeouts`), and the lag of every trial in
  `info` (`lag_daily_ms`, `lag_all_ms`). The number of trials is twice the number of intervals in
  `-duration` (4 to 40). A trial that is not right in 60 s is a timeout. The precision is the
  poll (5 ms) and the time to read the exact answer (a few ms) after the writes: the lags of the
  exact variants are that, not zero. *Cost of an exact read while data is pending*
  (`daily30-pending-N`, `total-pending-N`; kvutil and TimescaleDB): the background work is stopped
  (the collector shut down, the policies unscheduled), everything is brought up to date, and N events of
  that customer, today, are left pending (N = 0, N/100, N/10 of the scale, at least 0, 100,
  1000): a series of the last 30 days and a total are read at 1 worker for each N. kvutil's
  `exact` folds the notes of the customer (and computes the marked days of the maximum from
  their events), its `collected` does not look at them; TimescaleDB's `realtime` unions the
  materialization with the hypertable beyond the watermark, `materialized` does not. The exact variants
  check that the read is the exact answer at each N.
* **agg-rebuild.** The events are loaded with no aggregate defined (`load-events`; kvutil by a write
  batch of the records without the aggregations), and the aggregates are built over them, each step a
  phase of N events: kvutil `migrate` (`store.Migrate` of the store with the three aggregations
  defined, which builds the three; the report says three were built), then `rebuild-sum`,
  `rebuild-count`, `rebuild-max` (`agg.Rebuild` of each alone: it deletes the aggregation and notes
  every record again, and collects); TimescaleDB `create-with-data` (the three `CREATE MATERIALIZED
  VIEW … WITH DATA`: the initial materialization), after which they are dropped and made `WITH NO
  DATA`, then `refresh-day`, `refresh-week` and `refresh-month` (`refresh_continuous_aggregate(…,
  NULL, NULL)` over the whole range); PostgreSQL `build-summary` (`INSERT … SELECT … GROUP BY`
  into the summary; the trigger is created after). The built aggregates are checked against the exact
  answers.
* **Storage** is the last case of `agg-rebuild`, `storage`: `extra` has `events_bytes`,
  `aggregate_bytes`, `aggregate_pct` (the aggregates as a share of the events) and the bytes per
  event of each. TimescaleDB: `hypertable_size` of the events and of the materialization hypertables
  of the three aggregates (with their indexes; no compression). PostgreSQL: `pg_total_relation_size` of
  the table and of the summary. kvutil: the sum of the lengths of the keys and values under the
  prefix of the records and under those of the three aggregations (`params.case.logical`:
  true): **before** Badger compresses them, so it is not the size on disk of the same kind as the
  others; the size of the whole directory on disk (`disk_bytes`) is in the result as always. The
  state of an aggregation of kvutil has the temporary records of the notes that were collected
  deleted, but Badger removes them from its files when it compacts.

### The time zone

The buckets are UTC everywhere. kvutil's `agg.Daily`, `Weekly` (Monday) and `Monthly` are in UTC unless
`In(loc)` is given; PostgreSQL's queries use the three-argument `date_trunc(unit, ts, 'UTC')`, which
does not depend on the `TimeZone` of the session, and the trigger likewise; TimescaleDB's
`time_bucket(interval, timestamptz)` without a time zone is in UTC too (`time_bucket('1 week', …)` has a Monday
origin), whatever the session has. The windows of the queries are instants (`timestamptz`
parameters). The verification makes this a test: events one second before and at the start of
a month, a week, a day, a year, and of the end of the span, and the reads again in a session of
`Pacific/Auckland` (12 or 13 hours from UTC) on PostgreSQL and TimescaleDB, which must give the
answers of the session in UTC (kvutil has no session, and reads again).

### Verification of the aggregations

The events are the same from the same seed on every system and the end of the span is fixed (a
Monday, the first of a month: 2026-06-01, so that the last day, week and month are complete).
`kvbench verify -workload agg` makes each variant of each system load the events, add the events of the
boundaries, and answer 28 reads (four customers: the series by days, weeks and months over the span,
and the total; a window of 30 days; the top 10 of every month), at stages. The answers are lines
of the number of rows, the sum, the count and a digest of every row (sums, counts and maxima are
integers, so they must be exact), and the groups of outputs are compared line by line:

| Group | Stages | Variants |
| --- | --- | --- |
| `caught-up` | S1 the events loaded and caught up (collected, refreshed); S4 after the changes below and caught up again; S5 the same in a session of another time zone | all six, which includes `collected` and `materialized` |
| `pending` | S0 the events loaded, nothing collected or refreshed; S2 after 40 new events (at and after the end of the span, singly and in a batch) | the variants that are exact before they catch up: kvutil `exact`, PostgreSQL both, TimescaleDB `realtime` |
| `pending-old` | S3 after the changes of old events: a new amount for a ninth of them, a move to another customer and time for a thirteenth, a deletion of a seventeenth | the same without TimescaleDB `realtime`, whose answer is the old one until the refresh (see above) |

`agg-write` and `agg-read` do all of it; `agg-hot-group` and `agg-freshness` have four writers insert
events of one customer at the same time and compare the reads (pending and caught up) with the
exact answer of the events; `agg-rebuild` builds the aggregates over events with none and
compares their reads. The cases of the timed workloads check their first query against the exact
answer too.

## The relations (group `relations`)

Parents and children, and a many-to-many relation with data on its pairs. One variant,
`relations`, with the cases of both; the data is a function of the seed and the scale
(`relations/model.go`). N is `-scale`:

| | Count | |
| --- | --- | --- |
| Children | N | each belongs to one parent; `title`, `qty`, `status`, a payload of 4 KB with `-values large` |
| Parents | max(20, N/10) | the parent of a child follows a Zipf law of exponent 1: the first has ln 2 / ln(P+1) of the children (13% of 10,000, 7% of a million), the one of rank k about 1/(k+1) of the first's. Most have a few |
| Left and right records | max(20, N/20) and max(20, N/10) | accounts and teams |
| Pairs | about N | the left record of rank k has 1/(k+1) of the pairs of the first (at most as many as there are right records), the right records of a left record are spread evenly; each pair has a role and a date |

**kvutil** (`relations/kvutil.go`), the stores are `record.New` with msgpack:

```go
owned   := relation.Associate("owned", parents, children)         // a child belongs to a parent
members := relation.New[pairData]("members", lefts, rights)       // pairs with data
// a child, with the check of the parent and the references of both directions:
children.Instantiate(tx).Set(&c, owned.Under(parentID))
owned.Instantiate(tx).Children(parentID)   // iterator of keys; Value() reads the record of each
owned.Instantiate(tx).Parent(childID)
parents.Instantiate(tx).Delete(parentID)    // deletes the children, through the child store
members.Instantiate(tx).Set(left, right, &data); .Get(left, right); .Rights(left); .Lefts(right)
lefts.Instantiate(tx).Delete(id)            // deletes the pairs
```

**PostgreSQL** (`relations/postgres.go`): `children.parent_id REFERENCES parents (id) ON DELETE
CASCADE`, an index on `children (parent_id, id)` (the order of kvutil's references, so that
`ORDER BY id` needs no sort), and a join table `memberships (left_id → lefts, right_id → rights,
role, since)` with both foreign keys `ON DELETE CASCADE`, the primary key `(left_id, right_id)`
and an index `(right_id, left_id)`. Loads are `COPY` (with the checks and the indexes in place).

| Case | kvutil | PostgreSQL |
| --- | --- | --- |
| `load-parents`, `load-children`, `load-entities`, `load-pairs` | transactions of 500 (`Set` with the parent checked, `Set` of the pair), on up to 4 workers | `COPY` of the same on up to 4 connections |
| `children-T`, `children-keys-T` (T is the tier: `few` 1 to 5 children, `typical` 10 to 100, `many` 200 to 5,000; at 1 and `-concurrency` workers) | `Children(parent)` with the record of each / the key of each | `SELECT id, title, qty, status, payload FROM children WHERE parent_id = $1 ORDER BY id` / `SELECT id …` |
| `parent-of` | `Parent(child)` | `SELECT p.* FROM children c JOIN parents p ON p.id = c.parent_id WHERE c.id = $1` |
| `insert-child` (a parent drawn from the law of the data) | `Set(child, Under(parent))` in `Update`: checks the parent, looks for the current parent of the child, writes the record and both references | `INSERT INTO children …`: the foreign key is checked |
| `rights-M-T` (M is `keys`, `data` or `records`; T is `typical` or `many` pairs of the left record) | `Rights(left)`: keys; with `Value()` (the pair data); with a `Get` of each right record | `SELECT right_id …`; `SELECT right_id, role, since …`; a join with `rights` (all `WHERE left_id = $1 ORDER BY right_id`) |
| `lefts-M` (a right record at random) | `Lefts(right)` the same way | the same with `WHERE right_id = $1`, joined with `lefts` |
| `pair-get`, `pair-set` | `Get(left, right)`; `Set` (both records are checked: an upsert) | `SELECT role, since …`; `INSERT … ON CONFLICT (left_id, right_id) DO UPDATE` |
| `delete-parent-T`, `delete-left-T` (T is `few` or `many`) | `Delete` of a parent or a left record in `Update`: its children or pairs go with it, in the same transaction | `DELETE FROM parents WHERE id = $1` (cascade) |
| `cascade-limit` (kvutil only) | parents of 1,024, 2,048… children are deleted until one fails; `largest_cascade_that_fit_children`, `first_cascade_that_failed_children` and `delete_ms_<size>` | |

The cases that delete use up their records: a pool of the tier is shuffled (the same way for the
same seed, as the pool is in the order of the ids, which is the order of the sizes) and split between
the two levels of workers, each record once (cases of operations; a tenth is the warm-up), so that
the levels delete the same mix of sizes and their times can be compared. The pools are 4,096
records at most, taken evenly from the tier. `units` of the cases are children or pairs.
`params.case` has `tier_mean_children` / `tier_mean_pairs`, the mean size of the tier. The tiers
that have no record at a small scale have no case (10,000 children have all of them).

kvutil deletes the children of a parent in the transaction of the delete, one by one, and a
transaction holds a limited number of writes (`kv.ErrTxnTooBig`): `cascade-limit` finds how large a
parent can be, and the time of a cascade grows with the square of the number of children there
(see the findings in the report). A run walks up to 32,768 children (a smoke run to 8,192, a tiny
one to 2,048); the `many` tier stays below the limit. PostgreSQL has no such limit.

## The lifecycle (group `lifecycle`)

Changing a database that has records in it. N records are loaded, then each variant changes
them, as phases (the time, `records_per_sec`, and the sizes after):

| Variant | Cases | kvutil | PostgreSQL |
| --- | --- | --- | --- |
| `index-single`, `index-unique`, `index-composite`, `index-multi` | `add-index`, `drop-index`, `add-index-concurrently` (PostgreSQL only) | the store is defined again with the index and `Migrate` builds it (checked: one `Built` action); defined without it, `Migrate` deletes its entries (one `Dropped`) | `CREATE INDEX` on `customer` / `CREATE UNIQUE INDEX` on `code` / `(customer, created_at)` / `USING gin (tags)`; `DROP INDEX`; `CREATE INDEX CONCURRENTLY` |
| `migrate` | `convert` (the sum), `convert-add-column` and `convert-update` (PostgreSQL), `add-field` | `record.New[int64, orderV2]("rec").WithVersion(2).MigrateFrom(1, func(id, old *Order) (*orderV2, error) {…})`: `Migrate` converts every record in batches of 256 records (a priority from the total); the second case is a store whose record has one more field and the same version: `Migrate` finds nothing to do | `ALTER TABLE ADD COLUMN priority integer` and `UPDATE … SET priority = total / 10000`; `ALTER TABLE ADD COLUMN extra text NOT NULL DEFAULT 'x'`, which PostgreSQL does in the catalog and not in the rows |
| `migrate-indexed` | `build-indexes` (customer, status, total), then the same as `migrate` | `Migrate` converts the records and builds the three indexes again from the new ones (three `Built`) | the `UPDATE` writes a new version of every row, and of every index entry (the update is not HOT: it writes the page full) |
| `open` | `open-new`, `open-loaded`, `restart-loaded` | a database with nothing in it: `Open`, define the stores, `Migrate` (records them), a first `Get`; reopened with records and three indexes: `Open`, `Migrate` (nothing to do: an empty report is checked), a first `Get`; restarted: the close and the open of the database in the process | a new connection and a first query (`SELECT count(*)` of an empty table; a primary key lookup); a restart: `docker restart` of the container through the Docker API, then a new connection until the first answer |

The cases of `open` have 7 trials (3 for the restart) and say, in `extra`, the minimum, the median
(`ready_ms_median`, which is also the `elapsed_sec`), the mean and the maximum in milliseconds,
and for `open-loaded` the parts (`close_ms_*`, `open_ms_*`, `first_ms_*`, the first answer). On
kvutil `open-loaded` leaves out the close, and `restart-loaded` has it. `restart-loaded` has
`info.method`: without the Docker socket PostgreSQL is not restarted, and the case says that it is
only a new connection to the running server.

A database is alone while it is changed, as `Migrate` needs: nothing writes during the cases. The
data of the `index-*` cases is `dataset.Order`, the one of `index-maintenance`, in the same
table.

## The concurrency (group `concurrency`)

Contention. Every case runs at 1, 2, 4, 8 and 16 workers (whatever `-concurrency` is) as a closed
loop, which does not correct for coordinated omission (see [Limits](../README.md#limits-of-what-is-measured)).
The pool of the connections has room for them. A variant is a scenario on a database of its own:

| Variant | System | Cases | What a worker does |
| --- | --- | --- | --- |
| `rmw-update` | kvutil | `rmw-k1`, `rmw-k10`, `rmw-k100`, `rmw-k10000` | a counter of a set of that many keys, chosen at random, is incremented in `db.Update` (`Get`, add one, `Set`), run again on a conflict up to 10 times (`badgerkv.DefaultMaxRetries`): `retries` of the result are the times it was run again, `errors` the commits that still conflicted after the limit |
| `rmw-update-r100` | kvutil | the same for 1, 10 and 100 keys | the same with `badgerkv.WithMaxRetries(100)` |
| `rmw-for-update` | PostgreSQL | the same | in a transaction: `SELECT n FROM counters WHERE k = $1 FOR UPDATE`, `UPDATE counters SET n = $2 WHERE k = $1`, commit |
| `rmw-atomic` | PostgreSQL | the same | `UPDATE counters SET n = n + 1 WHERE k = $1` |
| `blind-write` | both | `blind-write` | a write of a new key (a record of 200 bytes) in a transaction of its own: no conflicts, so what is shown is what the commits cost when many ask for the disk at once (group commit). Both durability modes are the modes of the run (`-d both`) |
| `mixed` | both | `mixed-uniform`, `mixed-zipf` (`load` is the phase before) | 90% reads of a record (`Get` of the record store; `SELECT` of the columns) and 10% writes that add one to its `Qty` (kvutil: `Get` and `Set` in `Update`; PostgreSQL `UPDATE orders SET qty = qty + 1`); the key is drawn uniformly or by a Zipf law of exponent 1 (`harness.NewKeys`: rank k has 1/(k+1) of the draws of the first, and the ranks are scrambled over the ids so that the hot records are not next to one another) |
| `long-read` | both | `writers-alone`, `writers-with-reader`, `reader` | writers increment random counters of 10,000 (PostgreSQL atomically). In `writers-with-reader` a transaction that took its snapshot before them reads all the counters, again and again, until the writers finish (kvutil `db.View`; PostgreSQL `REPEATABLE READ READ ONLY`, `SELECT sum(n), count(*)`). `reader` has the reader's side as `extra`: `scans`, `unstable_scans` (a scan that saw another sum than the first: the run fails if there is one), `scan_first_ms`, `scan_last_ms`, `scan_p50_ms`, `scan_p99_ms`, `scan_max_ms`, `snapshot_seconds`, `disk_growth_bytes`, and for PostgreSQL `dead_tuples_before/after` and `table_bytes_before/after` (what the open snapshot keeps the vacuum from cleaning) |

Each case checks its result, and fails the variant if it is wrong: the counters add up to the
increments that succeeded (counted in the warm-up too), the rows of `blind-write` to the writes,
and the quantities of `mixed` to the writes that succeeded. A set of one key is the worst case
for every system; of 10,000 keys, nobody conflicts.

## The storage (group `storage`)

What the data takes on disk. N records with the indexes `customer`, `code` (unique), `composite`
(customer, time) and `tags` (an entry per tag), the same as `index-maintenance`. Every step ends
with the writes made durable (kvutil closes the database and opens it, which flushes the memtable;
PostgreSQL `CHECKPOINT`), and its result has `extra`:

| Key | |
| --- | --- |
| `disk_bytes_before`, `disk_bytes_after`, `disk_growth_bytes` | the size of the database (the files of Badger by the blocks they occupy, `pg_total_relation_size` of the tables) |
| `user_bytes` | the bytes that the user wrote in the step: the records, encoded (msgpack), by the mean of 200 of them; 0 for a delete |
| `container_write_bytes`, `write_amp`, `write_bytes_per_dataset_byte` | the bytes that the container wrote to block devices during the step (from the cgroup `io.stat` of the harness' container for kvutil, from the Docker API for PostgreSQL's container); `write_amp` is those over `user_bytes`. Absent when the figure is unavailable, and then `info.io_source` is `unavailable`; it is `cgroup` or `docker-api` otherwise |
| `wal_bytes`, `pg_stat_io_write_bytes`, `wal_per_user_byte` | PostgreSQL only: the WAL the server made (`pg_stat_wal`), and the bytes it wrote to relations and the WAL according to `pg_stat_io` |

| Variant | Cases |
| --- | --- |
| `footprint` (both) | `load` (no index), `add-index-customer`, `-code`, `-composite`, `-tags` (one at a time), `footprint` (the sizes: `table_bytes`, `index_<kind>_bytes`, `total_bytes`, `bytes_per_record`, the same per record), `overwrite-1`, `-2`, `-3` (every record written again with new values for every indexed field: transactions of 500 on kvutil, batches of 500 statements on PostgreSQL), `delete-half` (the odd ids), and what gives the space back: kvutil `value-log-gc` (`RunValueLogGC`), `flatten` (`Badger.Flatten(4)`), `value-log-gc-after-flatten`; PostgreSQL `vacuum`, `vacuum-full` |
| `footprint-vlog` (kvutil) | the same on a database whose values are kept in the value log (threshold 64 bytes, files of 16 MiB): Badger keeps a value under 1 MiB in the tree, so in the default database the collection of the value log has nothing to do |
| `expiry` (both) | `insert-plain`, `insert-ttl` (and `…-io`: the bytes written for them), `after-expiry`, then the same reclaiming |

* In `footprint` the vacuum of PostgreSQL is off for the table (`autovacuum_enabled = false`), so that
  the size between the steps is what the writes left, and the vacuums are run by the workload.
  In `expiry` it is on, as in production.
* kvutil's sizes of the table and of each index are the disk after the load, and what each
  `Migrate` that built an index added to it: approximate, as Badger compacts in the background.
  `table_logical_bytes` and `index_<kind>_logical_bytes` are the keys and values under the prefix of
  each, before Badger compresses them (the sum of the lengths). PostgreSQL's are `pg_table_size`
  (with TOAST) and `pg_relation_size` of each index.
* The disk that Badger keeps for the records that were written again is not given back until the
  tables that hold them are compacted: the results show `flatten` as the step that does it.
* `expiry`: the records of kvutil are `SetWithTTL` of a typed store, `store.Instantiate(tx).SetWithTTL`;
  a record store with indexes cannot set a TTL (`ErrTTLWithExtensions`), so there is no index.
  PostgreSQL's table has `expires_at` with a partial index on it, and a job deletes `WHERE
  expires_at < now()` every second while the writers run (`job_runs`, `job_rows_deleted`,
  `job_busy_ms_total`, `job_longest_run_ms` of `after-expiry`). The life of a record is 4 s.
  Badger does not delete an expired key until it compacts the tables that hold it, but a read does
  not see it from the moment it expired. `after-expiry` has `records_not_expired_after` (0), the
  disk at the end of the writes and after the expiry. A run fails if a record that has expired is
  still visible.

**What is unavailable.** The container figures need the Docker API or a cgroup that counts block
I/O. On Linux with cgroup v2 the `io.stat` of the harness' container is read, and the daemon's
statistics give PostgreSQL's. On Docker Desktop for macOS (29.9, when this was written) both
worked: the containers are Linux ones in a VM, and the figures are the bytes that reach the
virtual disk of the VM, not the SSD of the Mac. A daemon or a cgroup that has no block I/O
figures (the I/O controller is off) makes `info.io_source` `unavailable` and leaves the keys
out; `docker-stats-<durability>.csv` (block I/O, sampled by `run.sh`) and, for PostgreSQL,
`pg_stat_io` and `pg_stat_wal` (always in the result) are what is left. When the harness is run
outside compose (see [Running it](../README.md#running-it)), there is no socket and no cgroup either. A restart of PostgreSQL
needs the socket as well; without it `restart-loaded` is a new connection and says so.

The bytes written include everything that the container does, not only the step: the first steps
on a fresh PostgreSQL server write the 16 MB WAL segments that it creates (zero-filled) as the
log grows, which `min_wal_size` then keeps, so the early cases show more `container_write_bytes`
than the later ones for the same work; `pg_stat_io_write_bytes` counts the same. The statistics
of the processes that write in the background reach `pg_stat_io` up to a second late, which is
why a PostgreSQL step waits 1.2 s after its checkpoint (`duration_incl_flush_sec` has it).

## Reading the plans

The result of a case of the query workloads has `metrics.info`:

| Key | |
| --- | --- |
| `plan_nodes` | the operators, one line, node then child with `>`, and children in parentheses joined by `+`: `Limit > Sort > Index Scan[books_year]`, `Bitmap Heap Scan > BitmapAnd > (Bitmap Index Scan[books_year] + Bitmap Index Scan[books_tags])`, `Limit > TopN > IndexLookup[by_year]`, `Union > (IndexLookup[by_author] + IndexLookup[by_year])`. The name of the index is in brackets |
| `plan` | the plan as a tree of lines. kvutil: the text of `Explain()` (`IndexLookup by_author_year (in order of Year desc): 1 range, then check […]`). PostgreSQL: the node types of `EXPLAIN (FORMAT JSON)`, with the index and table and the rows the planner expects (`Index Scan using books_code on books (rows=1)`) |
| `plans_seen` | only if the sets of arguments did not all get the same plan: each plan with how many of the first 20 sets got it (`A x17; B x3`); `plan` and `plan_nodes` are the most common |

kvutil has the plans of the first 20 sets of arguments of the pool, from `Explain` of the way that is
run (its text is parsed in `sql`). PostgreSQL's are `EXPLAIN (FORMAT JSON)` without ANALYZE: for
`simple`, of the text with the values in it; for `prepared`, of the prepared statement after it was
run five times, so that it is the plan that the server uses from then on, which may be a generic plan
that is not the one of the values (`EXECUTE` of a statement prepared on a connection of its own).
So the plans of the two ways of PostgreSQL can differ, and that is a finding. `Limit`, `Sort`,
`Aggregate`, `Gather` are nodes of the plan like the scans.

## Verification

`kvbench verify` has each workload say what its queries return, in a form that both systems can
produce, and compares them:

* `kv-basic`: all four variants load the dataset, change it (updates, deletes, inserts) and say the
  id and a digest of the value of every record, `Get` results, and the rows of scans.
* `bulk-load`: every strategy, at 1 and 3 workers, must leave a digest of all records that is the
  same as the others; the digest of the three variants of both systems is compared.
* `record-codecs`: every record read back through each of the six variants, canonically.
* `index-maintenance`: after the writes, each configuration answers a set of probes with ids
  through its indexes (kvutil `Lookup`, `Match`, `Range`) or in SQL; the lists of ids, sorted, must
  be the same. The configurations are different groups, so each is compared with its counterpart.
* `queries`: the first four sets of arguments of the 17 cases, answered by the three ways of kvutil and
  the two of PostgreSQL, which must all say the same (see the workload).
* `selectivity-sweep`: the first two ranges of every step in both orders, answered by the three
  variants of kvutil and by PostgreSQL: the number of rows and a digest of the ids, in the order of the
  key for `by-pk` and sorted otherwise.
* `key-order-fetch`: kvutil only. The four ways read the same sets of keys (from 1 to all the
  records, with the database opened again in between) and say the number of values, their bytes and a
  checksum; the ways of a size of the values must say the same.

* `relations`: the children of 6 parents, the parent of 4 children, the lists of a few left and right
  records (keys only, with the data of the pairs, with the records), a pair or the lack of it, after
  a child and some pairs were set (and a child under a parent that does not exist was refused), and
  again after the deletion of two parents and of a left and a right record with their children and
  pairs: the number of records of each kind and a digest of every list must be the same.
* `lifecycle`: the ids that each new index finds (again after it is dropped and built again), the
  ids of each priority after the value change of every record, the ids that the three indexes find
  after it (kvutil has built them again; PostgreSQL has updated them), that the record is still
  there after a field is added, and the ids that an index finds after the database was opened again.
* `concurrency`: after 4 workers did 150 operations of each scenario, the counters add up to the
  increments that succeeded, the rows to the writes, the quantities to the writes, the snapshot of
  the long reader did not change; the numbers themselves depend on the conflicts, so the lines say
  yes or no and every variant of a scenario must say yes.
* `storage`: the number of records after the load, two rewrites, the deletion of half and the
  reclaiming, and the records of a table with an expiry that are there just after they are written
  and after they have expired. There is no comparison of the sizes.

At the scale of a verification (2,000 books) the answers are not empty, and the deep paging offset is
smaller (850): see the cases.
