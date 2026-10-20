# kvutil benchmarks

A suite that measures kvutil (on Badger) against PostgreSQL, and against TimescaleDB for stored
aggregations only. It is a Go module of its own (`github.com/ehsanranjbar/kvutil/bench`, with
`replace github.com/ehsanranjbar/kvutil => ../`), so that pgx and the histogram library never enter
the module of the library, and `go test ./...` at the root does not go into it. The databases run
in Docker, under the same limits as the harness, on macOS and on Linux.

This is stage B2: the harness, the first workloads (B1) and the workloads of queries. The workloads
of later stages (aggregations, relations, lifecycle, concurrency, storage) are packages that
register themselves with the harness; see [Adding a workload](#adding-a-workload).

| Stage | Workloads |
| --- | --- |
| B1 | `kv-basic`, `bulk-load`, `record-codecs`, `index-maintenance` |
| B2 (here) | `queries`, `selectivity-sweep`, `key-order-fetch` (group `query`) |
| B3 | stored aggregations (against TimescaleDB too) |
| B4 | relations, lifecycle, concurrency, storage |
| B5 | the report |

## Running it

Needs Docker with compose v2 and bash; nothing else (Go is in the image).

```sh
cd bench

# A smoke run: every workload on both systems, 10,000 small records, 1 second per case, and
# `kvbench verify` first. It takes about 20 minutes (the three workloads of the queries take 7 of them).
./run.sh --smoke

# The same for one workload (or a group: kv, load, records, index, query), which is quicker:
./run.sh --smoke -w kv-basic
./run.sh --smoke -w queries,selectivity-sweep,key-order-fetch   # the same as -w query

# A run: a million small records, both durability modes, 4 CPUs and 4 GB for each container.
./run.sh -s 1000000 -d both

# Large values (4 KB), another machine size, a few workloads only:
./run.sh -s 500000 -V large --cpus 8 --mem 8g -w kv-basic,bulk-load

./run.sh --help
```

`run.sh` builds the image of the harness, starts what each step needs, runs, collects the results
in `results/<host>-<timestamp>/` (see [results/README.md](results/README.md)) and removes the
containers and the volumes (`--keep` leaves them). It runs kvutil first, with PostgreSQL not
started, and then PostgreSQL, for each durability mode.

By hand, with the services of the compose file:

```sh
docker compose up -d --wait postgres
docker compose run --rm --no-deps bench list
docker compose run --rm --no-deps bench verify -workload all -scale 2000
docker compose run --rm --no-deps bench run -system postgres -workload kv-basic -scale 100000 \
    -duration 10s -warmup 2s -concurrency 4 -durability durable -out /results/by-hand.jsonl
docker compose run --rm --no-deps bench run -system kvutil -workload kv-basic -scale 100000 -out /results/by-hand.jsonl
docker compose down -v
```

Without Docker, for development, the harness runs on the host against any PostgreSQL:

```sh
go build -o /tmp/kvbench ./cmd/kvbench
KVB_PG_DSN='postgres://postgres:bench@127.0.0.1:5432/bench?sslmode=disable' /tmp/kvbench verify
/tmp/kvbench run -system kvutil -workload index -scale 20000 -duration 3s
```

### The command

```text
kvbench run    -system kvutil|postgres -workload NAME|GROUP|all[,...] -scale N -values small|large
               -durability durable|relaxed -concurrency C -duration D -warmup W -out file.jsonl
kvbench verify -workload NAME|GROUP|all -scale N [-values ...] [-show N]
kvbench list
```

| Flag | Default | |
| --- | --- | --- |
| `-system` | | `kvutil` (Badger, in the process) or `postgres` (DSN) |
| `-workload` | `all` | names and groups separated by commas; `kvbench list` has them |
| `-scale` | 100000 (2000 for verify) | records in the dataset |
| `-values` | `small` | `small` is a record of about 200 bytes, `large` of about 4 KB |
| `-durability` | `durable` (`relaxed` for verify) | see [Durability](#durability) |
| `-concurrency` | 4 | workers of the cases that run concurrently; a case runs at 1 worker and at this many |
| `-duration`, `-warmup` | 10s, 2s | the measured interval of a timed case, and what runs before it and is not measured |
| `-seed` | 1 | the seed of the dataset and of the choices of the workers |
| `-out` | | the file that results are appended to, as JSON lines |
| `-pg-dsn`, `-data-dir`, `-cache-mb`, `-prefetch` | `$KVB_PG_DSN`, `$KVB_DATA_DIR` (`/data`), a quarter of the memory limit, 64 | |

A workload that fails does not stop the others: the command exits with 1 at the end.

`verify` runs the queries of the workloads on both systems and compares the results, line by
line: a variant of kvutil and a variant of PostgreSQL that are in the same group must return
exactly the same. It exits with 1 on any difference. It uses a small scale; the results of
`kvbench verify -show 5` can be looked at.

## What is the same on both sides

* **Limits.** The database and the harness (which has kvutil in its process) get the same CPUs,
  memory and no swap (`BENCH_CPUS`, `BENCH_MEM`; 4 and 4g by default), and their data is on named
  volumes of Docker, not on a bind mount of the host.
* **Images.** `postgres:18` (18.6 when this was written; `postgres:latest` was 18),
  `timescale/timescaledb:2.31.0-pg18` (the same major) for B3, `golang:1.27` for the build and
  `debian:trixie-slim` to run. `results/<run>/images.txt` has their digests.
* **Records.** Generated from the seed, the id and a version only, by `internal/dataset`, so both
  systems get the same ones and a run can be repeated. A record has ints, strings, a time (second
  resolution, UTC), an optional string, a list of up to four tags of 50, and, for large values, a
  payload of 4096 random bytes, which compression cannot shrink (on either side).
* **Durability.** See below.
* **Memory.** The block cache of Badger is a quarter of the memory limit, as `shared_buffers` of
  PostgreSQL is; `GOMEMLIMIT` is three quarters of it, so that the Go runtime collects before the
  container is killed.

### Durability

| Mode | kvutil (Badger) | PostgreSQL |
| --- | --- | --- |
| `durable` | `SyncWrites` on: a commit returns when its write is synced to disk | `fsync=on`, `synchronous_commit=on` |
| `relaxed` | `SyncWrites` off: the write is in the page cache when the commit returns | `fsync=on`, `synchronous_commit=off` |

`fsync` stays on in `relaxed` mode, as turning it off can corrupt a database, which no one runs in
production. `synchronous_commit=off` loses the last commits of a crash but never the consistency;
that is what Badger without `SyncWrites` does too. Both are set where they are used: Badger by the
options it is opened with, and PostgreSQL by the flags of its container (`run.sh` sets them from the
mode) and by `synchronous_commit` of every connection of the harness, so that `SHOW` and the
settings in each result agree with the mode.

On Docker Desktop for macOS the containers run in a Linux VM, and an `fsync` there reaches the
virtual disk of the VM; whether the Mac flushes that to the SSD is up to Docker Desktop. Numbers of
`durable` mode from a Mac are comparable between the two systems, not with a server.

### Settings of PostgreSQL

All are flags of the command of the container in [docker-compose.yml](docker-compose.yml); those
that depend on the memory are computed by `run.sh` from `--mem` (the defaults are those of 4 GB).

| Setting | Value | Why |
| --- | --- | --- |
| `shared_buffers` | 25% of the memory (1GB) | The usual rule, and the same share that Badger's block cache gets |
| `effective_cache_size` | 75% of the memory (3GB) | What the planner may assume the cache to be |
| `work_mem` | 32MB | Sorts and hashes of the queries of B2 |
| `maintenance_work_mem` | 12.5% of the memory, 1GB at most (512MB) | `CREATE INDEX`, `VACUUM` |
| `max_connections` | 200 | More than the pool of the harness ever opens |
| `wal_level` | `replica` | The default, which what is run in production usually has |
| `wal_buffers` | 64MB | A write-heavy workload; the default of 16MB is the most that auto-tuning picks |
| `max_wal_size`, `min_wal_size` | 4GB, 1GB | So that the checkpoints do not come every few seconds in a load |
| `checkpoint_timeout`, `checkpoint_completion_target` | 15min, 0.9 | The same, and spread the I/O of a checkpoint |
| `wal_compression` | off | The default, stated |
| `full_page_writes` | on | The default, stated: what the safe configuration pays |
| `fsync` | on | See [Durability](#durability) |
| `synchronous_commit` | `on` or `off` by the mode | See [Durability](#durability) |
| `autovacuum` | on, `autovacuum_naptime=15s` | Left on, as in production, but with a naptime that fits runs of minutes, not days |
| `random_page_cost` | 1.1 | An SSD |
| `effective_io_concurrency` | 64 | An SSD |
| `track_io_timing`, `log_checkpoints` | on | For the log of the container, saved with the results |
| `shm_size` of the container | 1g | The default of 64 MB breaks parallel queries |

Parallelism is left as PostgreSQL has it (`max_parallel_workers_per_gather=2`, parallel index
builds): it is what a user gets, and the CPU limit applies to it. TOAST is left as it is: a 4 KB
value is compressed or moved out of line by PostgreSQL, as it would be by anyone who stored it.

The settings that the server had when a run was made are in each result (`settings`).

### Settings of Badger

`badger.DefaultOptions` with `SyncWrites` by the mode, the logger off and the block cache of a
quarter of the memory limit. The default compression (Snappy), memtable size, number of compactors
and value threshold are kept. Iterators of kvutil read 64 values ahead (`-prefetch`), which is what
makes a scan of values fast; the setting is in the results.

## What is measured

* **Timed cases** run an operation on 1 worker and on `-concurrency` workers, in a closed loop
  (a worker starts its next operation when the last one returned; there is no correction for
  coordinated omission), for `-warmup` and then for `-duration`. Only the operations that start in
  the measured interval count. A case that uses up what it works on (`delete`) is a case of
  operations: `Case.Ops` operations, the first tenth the warm-up, then the rest for the duration at
  most.
* **Latency** of each operation that succeeded, in an HDR histogram per worker (1 ns to 120 s, 3
  significant digits), merged at the end: min, mean, p50, p95, p99, p99.9 and max, in microseconds.
  Whatever an operation prepares (the record that it writes, its encoding for the systems that
  take bytes) is outside of the time: it calls `Worker.Begin` when it is ready. What is inside is
  what the system does, which is the point: the encoding of kvutil's typed stores and record stores
  is inside, the encoding of the bytes of `kv-basic` for the raw store and for PostgreSQL is not.
* **Throughput** is operations per second over the measured interval, and for operations that do
  many things (a scan) the units per second too (rows/s).
* **Errors** are operations that returned an error; **retries** are the times that a transaction of
  kvutil conflicted and `Update` ran it again (counted by the system); neither is in the latencies.
  If every operation of a case fails the run fails after saving it.
* **Phases** (loads, builds) are measured once: the time, the records per second, and the resources
  and sizes after them.
* **Resources.** See below. **Sizes** after each case: for kvutil the blocks that the files of the
  Badger directory occupy on disk (`st_blocks`, not the length: Badger sets the length of its log
  files in advance), after a `Sync`; for PostgreSQL `pg_total_relation_size` of the tables of the
  schema, with indexes and TOAST, and the size of `pg_wal` separately. Badger's log is in the former.

### Resources

| What | How | macOS | Linux |
| --- | --- | --- | --- |
| CPU time of the harness (kvutil is in it) | `getrusage` before and after a case: `cpu_user_sec`, `cpu_sys_sec`, `cpu_cores` | the same | the same |
| Peak memory of the harness | `getrusage` `ru_maxrss` (bytes on macOS, kilobytes on Linux, normalised) and `/proc/self/statm` | the container is Linux: both | both |
| The harness container as a whole | cgroup v2 files read in the container (`cpu.stat`, `memory.current`, `memory.peak`): `cgroup_*` | works, as the containers are Linux | works |
| PostgreSQL's CPU and memory | `run.sh` samples `docker stats --no-stream` of every container with the time and writes `docker-stats-<durability>.csv` (CPU % of a core, memory with the page cache the cgroup counts, network, block I/O) | the Docker API of Docker Desktop | the Docker API |
| Database size | as above | the same | the same |

`docker stats` takes a second or two per sample, so the file has a row every 2 to 3 seconds. Each
result has the `start` and `end` of its case, so the rows of the interval can be selected; PostgreSQL's
share of the work of a case is its rows, as the CPU of the harness in a PostgreSQL case is only the
client's (pgx, the generation of records). A case with `ops` in `Case.Ops` has the resources of the
whole case.

A run outside Docker (development) has the CPU time and the peak memory of the process, not the cgroup.

### Results format

One JSON object per line (`internal/metrics`, `schema` 1):

```json
{
  "schema": 1, "start": "2026-10-20T10:00:00Z", "end": "2026-10-20T10:00:13Z",
  "system": "kvutil", "system_version": "kvutil <commit> (badger v4.9.6)",
  "settings": {"sync_writes": "true", "block_cache_size": "1073741824", "prefetch": "64", "...": "..."},
  "workload": "kv-basic", "variant": "typed", "case": "scan-short",
  "params": {"scale": 100000, "values": "small", "durability": "durable", "concurrency": 4,
             "duration_sec": 10, "warmup_sec": 2, "seed": 1, "case": {"rows": 100}},
  "host": {"label": "mac", "os": "linux", "arch": "arm64", "num_cpu": 18, "cpu_limit": 4,
           "mem_limit_bytes": 4294967296, "docker": {"server_version": "29.7.2", "...": "..."}},
  "metrics": {
    "ops": 123456, "errors": 0, "retries": 0, "elapsed_sec": 10.0, "ops_per_sec": 12345.6,
    "units": 12345600, "unit_name": "rows", "units_per_sec": 1234560,
    "latency": {"count": 123456, "min_us": 1.2, "mean_us": 3.4, "p50_us": 3.0, "p95_us": 5.0,
                "p99_us": 8.0, "p999_us": 20.0, "max_us": 900.0},
    "resources": {"wall_sec": 10.0, "cpu_user_sec": 20.1, "cpu_sys_sec": 2.0, "cpu_cores": 2.2,
                  "max_rss_bytes": 0, "rss_bytes": 0, "go_sys_bytes": 0, "cgroup_cpu_cores": 2.3,
                  "cgroup_mem_bytes": 0, "cgroup_mem_peak_bytes": 0},
    "disk_bytes": 0, "wal_bytes": 0
  }
}
```

A phase has `records` and `records_per_sec` instead of `ops` and `latency`; a measurement of
something else has `extra` (a map of numbers, like `avg_record_bytes`). The `variant` is the
variant of a workload (a codec, a configuration of indexes, a way of asking), the `case` the thing
that was measured. A result may have `info`, a map of text; the query workloads put the plan that
the system chose in it (see [Reading the plans](#reading-the-plans)).

## The workloads

`kvbench list` has them with their variants.

### kv-basic (group `kv`)

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

### bulk-load (group `load`)

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

### record-codecs (group `records`)

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

### index-maintenance (group `index`)

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

### queries (group `query`)

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

### selectivity-sweep (group `query`)

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

### key-order-fetch (group `query`, kvutil only)

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

### Reading the plans

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

### Verification

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

At the scale of a verification (2,000 books) the answers are not empty, and the deep paging offset is
smaller (850): see the cases.

## Adding a workload

A workload is a package under `internal/workloads` that implements `harness.Workload` and registers
itself with `harness.Register` in `init`; `cmd/kvbench/main.go` imports it for that.

```go
type Workload interface {
    Name() string
    Group() string
    Description() string
    Variants(sys System) []Variant    // by the type of the system; none if it does not run on it
    Verify(ctx, sys System, p Params) ([]Output, error)
}
type Variant interface {
    Name() string
    Run(ctx context.Context, rc *RunContext) error
}
```

`Variants` may only look at the type of `sys` (it is called with a nil pointer for `list`). A variant
creates what it needs (a fresh Badger database with `*kvsys.System.Open`, a schema with
`*pgsys.System.Schema`), and removes it. Through the `RunContext` it measures:

* `rc.Measure(ctx, Case{Name, Concurrency, Params, Unit, Init, Info}, op)`: a timed case. `op` takes
  the `Worker` (its `Rand`, `State`, `Begin()`, `Add(n)`) and returns an error, or `harness.ErrDone`.
  `Info` is text that goes into the result (`metrics.info`), like a plan.
* `rc.Phase(ctx, Case{...}, fn)`: a load or a build, with its time, records per second, resources.
* `rc.Report(ctx, Case{...}, start, end, metrics.Metrics{Extra: ...})`: anything else.
* `rc.SizeFn` says how to measure the size of the database after each of them.

Shared pieces: `internal/dataset` (records), `internal/wl` (the columns of a record in PostgreSQL,
loaders, the cursor over a pool of ids), `internal/testutil` (a kvutil system and a run for tests).
Scans for B2 or the aggregations for B3 add variants, and the harness, the CLI, the scripts and
the results format do not change.

## Limits of what is measured

* The harness is a closed loop at a fixed concurrency; it shows the latency at that load, not
  the response time of an open system.
* PostgreSQL is reached by pgx over the compose network, with a pool, and kvutil is a library call:
  the network and the protocol are a part of what a client of PostgreSQL pays.
* The ids of reads and writes are uniform. There is no skew (B4), but for the authors of the books
  of the query workloads, which follow a Zipf law.
* The query workloads draw the arguments of a case from a pool of 1,024 sets (256 ranges in the
  sweep), because a prepared query of kvutil has its values in it; a case does not touch more
  distinct records than the pool does, which is the same for both systems.
* The plan of PostgreSQL that is recorded is of the first 20 sets of arguments, from `EXPLAIN` on a
  connection of its own; the plan of a statement that is run by the pool can differ for an
  argument that is not among them, and `plans_seen` says when the first 20 differ.
* `kvutil` rows are decoded books, and PostgreSQL's are scanned into the same struct. The network
  and the protocol of PostgreSQL are in its numbers, as above; with the simple protocol it also
  parses and plans every statement, as kvutil's `sql` way does.
* kvutil's `Update` retries conflicts up to ten times; the retries are counted, and a transaction
  that conflicts more is an error.
* Badger compacts and collects garbage in the background and PostgreSQL vacuums: the cases run one
  after the other on the same database, so a case sees the debt of the one before it. The order of the
  cases (reads before writes, deletes before inserts) keeps that the same on both.
* The size of Badger's directory is of files that may have data that is not compacted yet, and the
  one of PostgreSQL does not have the dead tuples that a vacuum has not reclaimed.
* A Mac is not a server: Docker Desktop shares the CPUs and memory of the Mac with its VM.

## Tests

```sh
cd bench && go vet ./... && go test ./...
```

They run without Docker: the dataset (determinism, the distributions of the books: the Zipf skew of
the authors, the years, the tags, the titles), the histogram and the results format, the runner
(warm-up, errors, retries, cases of operations, the `info` of a case, the comparison used by
`verify`), the readers of plans, the loaders, the binary codec, and every workload on kvutil (its
variants against one another, the indexes and the queries against the records computed in Go, the
statements of PostgreSQL that the cases make and their parameters, a run of every case with the plans
that kvutil is expected to choose). The comparison with PostgreSQL is `kvbench verify`.
CI vets and builds the module without running Docker.
