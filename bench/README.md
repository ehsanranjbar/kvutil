# kvutil benchmarks

A suite that measures kvutil (on Badger) against PostgreSQL, and against TimescaleDB for stored
aggregations only. It is a Go module of its own (`github.com/ehsanranjbar/kvutil/bench`, with
`replace github.com/ehsanranjbar/kvutil => ../`), so that pgx and the histogram library never enter
the module of the library, and `go test ./...` at the root does not go into it. The databases run
in Docker, under the same limits as the harness, on macOS and on Linux.

It has a harness (`kvbench`), the workloads (see [The workloads](#the-workloads) and
[docs/workloads.md](docs/workloads.md)) and a report (`kvbench report`: the results as tables,
charts and CSV files). The workloads are packages that register themselves with the harness; see
[Adding a workload](#adding-a-workload).

## Running and reading

The short way, for someone who has the repository and Docker and wants numbers to read.

### What a machine needs

* Docker with compose v2, and bash. Nothing else: Go is in the image of the harness, and the
  databases are images. The first run builds the harness and pulls `postgres:18` (about 0.7 GB)
  and, for the stored aggregations, `timescale/timescaledb` (about 1.5 GB).
* CPUs and memory for two containers at once, the database and the harness, each with the limits
  of `--cpus` and `--mem` (4 CPUs and 4 GB by default). The machine, or the VM of Docker Desktop,
  needs the sum of the two and room for the system: 8 CPUs and 8 GB for the defaults (a full run was
  made with that; 10 GB leaves room).
* Disk, in the Docker volumes (the data of both systems, which is removed at the end of the run):
  see [How long and how much disk](#how-long-and-how-much-disk).

### On a Mac

```sh
# Docker Desktop: Settings > Resources, at least 8 CPUs and 8 GB of memory (10 GB leaves room),
# and 80 GB of disk for the runs below. Then:
open -a Docker
cd bench

./run.sh --smoke -w kv-basic            # a few minutes (the first run builds the image): does it all work?
./run.sh -s 1000000                     # the run: about 3.5 hours, durable only
./run.sh -s 1000000 -d both             # both durability modes: about 7 hours
./run.sh -s 100000                      # a smaller one: 3 hours, durable only
open results/<host>-<timestamp>/report.html
```

The numbers of a Mac are for comparing the two systems on that Mac: its CPUs are shared with the VM
and an `fsync` stops at the virtual disk of the VM (see [Caveats](#caveats-to-keep-in-mind)). The
report says so at its top.

### On a Linux server

```sh
# Docker Engine with the compose plugin, and a user that can run docker. Then:
git clone <the repository> && cd kvutil/bench
./run.sh --smoke -w kv-basic
./run.sh -s 1000000 -d both --cpus 8 --mem 16g   # whatever the machine can give each container
# a larger run, with the large values, in the background:
nohup ./run.sh -s 500000 -V large -d both > run.log 2>&1 &
```

Give the containers fewer CPUs and less memory than the machine has, so that the harness and the
database do not take from each other and from the system: a server of 16 cores and 64 GB can give
8 and 16g each. Keep the other work on the machine quiet during a run; a run does not try to
isolate itself from it.

### How long and how much disk

**Time.** What was measured: a smoke run of every workload (10,000 records, a second and a half per
timed case) on a MacBook, in Docker Desktop with 4 CPUs and 4 GB for each container, in three pieces
that took 7.5, 8.7 and 8 minutes (24 in all, the build of the image included). It had 841 timed
results (one case, at one level of concurrency, of one variant of one system) and 20 seconds of
loading and building; everything else was starting containers, preparing data and the fixed waits
of a few workloads, about 4 minutes. A timed result takes the warm-up and the duration, so:

```text
time = 841 x (warm-up + duration)  x  (2 for -d both)  +  20 s x (scale / 10,000)  +  about 5 min
```

| Run | Estimate |
| --- | --- |
| `./run.sh --smoke` (all workloads, with `verify`) | 25 minutes |
| `./run.sh -s 100000` (10 s and 2 s per case, durable) | 3 hours |
| `./run.sh -s 1000000 --verify` (small values, durable) | 3.5 hours (measured, below) |
| `./run.sh -s 1000000 -d both` | 7 hours |
| `./run.sh -s 1000000 -d both --duration 5s --warmup 1s` | 3.5 hours |

A full run was measured on a Mac, in Docker Desktop with 8 CPUs and 8 GB, and 4 CPUs and 4 GB for
each container: `-s 1000000`, durable, small values. kvutil with all the workloads took about 2
hours, `--verify` included, and PostgreSQL and TimescaleDB together about 1.5 hours: 3.5 hours.

The loading and building (indexes, aggregations, the storage steps) of the formula were measured at
10,000 records only and extended linearly; at a million, an index build is slower than linear, and
the cold reads and the rebuilds scale with the data. The full run above agrees with the estimate for
a million records; for larger scales take the estimates as a floor, and look at the log of the first
hour. The timed results of each workload at the defaults
(12 s each; the other cases of the workloads add to this) are: `kv-basic` 48 (10 minutes), `queries`
170 (34), `index-maintenance` 120 (24), `concurrency` 125 (25), `selectivity-sweep` 96 (19),
`relations` 92 (18), `agg-read` 72 (14), `record-codecs` 36 (7), `agg-write` 30 (6),
`agg-freshness` 24 (5), `agg-hot-group` 20 (4), `storage` 8 (2); `bulk-load`, `key-order-fetch`,
`agg-rebuild` and `lifecycle` have none (their cases are single measurements). A part of the suite
is a run of its own: `./run.sh -s 1000000 -w kv,query`.

**Disk.** The data of both systems is in Docker volumes, removed at the end of a run (`--keep` does
not). At 10,000 small records the largest database of a workload was 44 MiB (PostgreSQL's
`concurrency`, with 96 MiB of WAL, which grows to `max_wal_size` of 4 GB in a long run) and kvutil's
largest 42 MiB (`index-maintenance` and `key-order-fetch`, which keep several copies of the data and
the large values), so a million small records need about 5 GB for each system at the most, and at
most one database of each is alive at a time. With the 2.4 GB of images (`postgres:18` 0.7 GB,
TimescaleDB 1.5 GB, the harness 0.2 GB), the cache of the build and the report, plan on **20 GB free
for `-s 1000000`** (small values), and **60 GB for `-s 500000 -V large`**, where a record is 20
times larger. On Docker Desktop the limit is the size of its disk image (Settings > Resources).
The results of a run are 3 to 10 MB, the report about 1 MB.

### The Docker socket

The harness container has `/var/run/docker.sock` mounted. That gives the process in it control of
the Docker daemon of the machine (it could start containers, read their files, and so on), and
on a Linux server it is a control of the host: run the suite only on a machine where that is
acceptable. The harness uses the socket for two things only: the workloads of `storage` read the
bytes that a container wrote, and the workloads of `lifecycle` restart the database. To run
without it, remove the line `- /var/run/docker.sock:/var/run/docker.sock` from the service `bench`
in [docker-compose.yml](docker-compose.yml). Without it `storage` has no written-bytes figures
(its write amplification is not in the results, and the report shows n/a, not zero) and the
restart of `lifecycle` is only a new connection to the running server, which its results say.
The other workloads are not affected. Or leave the two out:
`-w kv,load,records,index,query,agg,relations,concurrency`.

### The report

`run.sh` ends by writing the report into the directory of the run, `results/<host>-<timestamp>/`
(a report that fails does not fail the run; the results are all there). To make it again, or of
other directories, with Docker or without it:

```sh
docker compose run --rm --no-deps bench report -out /results/NAME /results/NAME       # in the image
go run ./cmd/kvbench report -out DIR results/NAME [results/OTHER ...]                  # with Go
```

It writes `report.html` (one file: inline CSS and SVG, no scripts, light and dark), `report.md` (the
same, with the charts in `charts/*.svg`) and CSV files with the data of the tables: `runs.csv`, one
`<run>-<durability>-<workload>.csv` for each workload (a line for each result, with the parameters,
the extras and the info of the case as columns; a figure that was not recorded is an empty cell,
never zero) and one for each table of a special section or comparison.

What it has, in this order: a header for each run (host, versions, limits, scale, durations,
whether `verify` passed) with the warnings that matter (a failed verification, cases with errors,
Docker Desktop, a smoke scale, uncommitted changes in kvutil); a **Summary** of each run, the part
to read first (below); for each workload the tables of every case (a column for each system and variant, the ratio against the fastest, the
percentiles, errors and retries, plans, extras) with bars of throughput and p99; charts of its
own for `selectivity-sweep` (throughput against selectivity and the plan at each step),
`key-order-fetch` (ns per record against K), `agg-freshness` (the lag, and the cost of pending
events), `concurrency` (throughput and retries against workers) and `storage` (bytes after each
step, write amplification, the size before and after the steps that give space back); and the
resources of each case: the harness's own (CPU and peak memory, from `getrusage`) and the
database's (docker stats, joined to the case by its start and end). It draws no conclusions.

**The summary.** A full report is thousands of lines, so after the header it has a table for each
workload with a line for each case that kvutil and another system both ran: at 4 workers (the
largest number of workers the case has, if it has no such level), the best variant of kvutil, of
PostgreSQL and of TimescaleDB, where it has one, each with the name of the variant, and the ratio of
kvutil against the best of the others. The figure is operations per second for a timed case, records
per second for a load or a build, and seconds for the other phases. A ratio above 1x always means
that kvutil is faster: for seconds it is theirs over kvutil's. Cases that are a size, and cases of
one system, are not in it; the tables below it have everything.

| case | workers | unit | kvutil | variant | PostgreSQL | variant | kvutil / best other |
| --- | --- | --- | --- | --- | --- | --- | --- |
| get | 4 | ops/s | 1,000 | typed | 500 | bytea | 2.00x |
| load | 1 | records/s | 20,000 | typed | 12,500 | bytea | 1.60x |

**A run made in pieces.** A run can be added to: running `run.sh` again with the same `--out`
appends to its results files and `docker-stats-*.csv`, and the report is made again from everything.
`meta.json` is that of the first piece (it is written only if it is not there), and `verify.txt` is
written only by `--verify`. Where a case (the same durability, system, workload, variant, case and
workers) is in the results more than once, the report shows the last and says how many results it
left out, so a case that failed or was run with other flags can be made again alone:

```sh
./run.sh -s 1000000 -w relations --systems kvutil --out results/x
```

Give the same `-s`, `-V`, `-d`, `-c` and `--seed` as the first piece: the report does not check them
(it lists the scale and the values of a run as they are in the results). The systems and the
workloads in the table of the run are those of the results, not of `meta.json`.

### Comparing two runs

Give the report both directories (the first is the base of the ratios):

```sh
cd bench
go run ./cmd/kvbench report -out results/compare-mac-linux results/mac-20261030T100000Z results/srv-20261031T090000Z
open results/compare-mac-linux/report.html
```

The section "Comparison of runs" puts the same case of each run side by side with throughput and
p99 and the ratio against the first run. Only cases whose parameters match are compared (scale,
values, durability, workers, durations, seed and the parameters of the case); the others are listed
with the parameters that differ, so a Mac run at 100,000 records is not compared with a server run
at a million. Run both at the same flags to get the comparison. The tables of each run follow.

### Caveats to keep in mind

* **Embedded against client/server.** kvutil is a library in the process of the harness; PostgreSQL
  is a server that the harness reaches over a network connection with a pool. The round trip, the
  protocol, parsing and planning of a statement are in PostgreSQL's numbers, and its CPU is in its
  container, not the harness's. The report keeps harness and server figures apart for that reason.
* **Closed loop.** A worker starts its next operation when the last one returned, at a fixed number of
  workers. The latency is the latency at that load, and nothing corrects coordinated omission, so a
  stall shows less in the percentiles than it would to a client that arrives at a fixed rate.
* **kvutil's default configuration.** Badger with `DefaultOptions` (the block cache of a quarter of
  the memory, Snappy, the default memtable and compactors), no tuning for any workload.
* **PostgreSQL's fixed tuning.** The settings of [Settings of PostgreSQL](#settings-of-postgresql),
  chosen once and scaled with the memory limit; another tuning, or another version, would move the
  numbers, and the settings are in every result.
* **macOS.** The containers run in a VM and `fsync` reaches the disk of the VM, not the SSD: durable
  numbers from a Mac are not those of a server, and are comparable only between the two systems on
  that Mac. A smoke run (the scale of 10,000 and a second per case) shows that the suite works.
* **Sizes.** Badger's size is of its blocks on disk after a sync, with data that is not compacted
  yet; PostgreSQL's is of the relations without the dead tuples that a vacuum has not reclaimed,
  and the WAL apart. See [Limits of what is measured](#limits-of-what-is-measured).
* **Resources.** The CPU and memory of the server are samples of `docker stats` every two or three
  seconds, so a case shorter than that has few or none (n/a), and the memory counts the page cache.

## Running it

Needs Docker with compose v2 and bash; nothing else (Go is in the image).

```sh
cd bench

# A smoke run: every workload on all the systems, 10,000 small records, 1 second per case, and
# `kvbench verify` first, and the report at the end. It takes about 25 minutes (measured in three
# pieces of 7.5, 8.7 and 8 minutes; see "How long and how much disk" above).
./run.sh --smoke

# The same for one workload (or a group: kv, load, records, index, query, agg, relations,
# lifecycle, concurrency, storage), which is quicker:
./run.sh --smoke -w kv-basic
./run.sh --smoke -w queries,selectivity-sweep,key-order-fetch   # the same as -w query
./run.sh --smoke -w relations,lifecycle,concurrency,storage     # a group each

# A run: a million small records, both durability modes, 4 CPUs and 4 GB for each container.
./run.sh -s 1000000 -d both

# Large values (4 KB), another machine size, a few workloads only:
./run.sh -s 500000 -V large --cpus 8 --mem 8g -w kv-basic,bulk-load

./run.sh --help
```

`run.sh` builds the image of the harness, starts what each step needs, runs, collects the results
in `results/<host>-<timestamp>/` (see [results/README.md](results/README.md)) and removes the
containers and the volumes (`--keep` leaves them). It runs kvutil first, with PostgreSQL not
started, and then PostgreSQL, for each durability mode. With `--out DIR` of an existing run it adds
to that run (see "A run made in pieces" in [The report](#the-report)).

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
kvbench report -out DIR RESULTDIR...
kvbench list
```

`report` reads result directories (`results-*.jsonl`, `docker-stats-*.csv`, `meta.json`,
`verify.txt`, `images.txt`) and writes the report; see [The report](#the-report).

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

The workloads of storage and lifecycle also read `KVB_PG_CONTAINER` (the container of the database
to read the written bytes of and to restart; compose sets it), `KVB_SELF_CONTAINER` (the harness'
own, `kvbench-bench`; the host name if not set) and `KVB_DOCKER_SOCKET` (`/var/run/docker.sock`).

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
  `timescale/timescaledb:2.31.0-pg18` (the same major) for the aggregations, `golang:1.27` for the build and
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
| `work_mem` | 32MB | Sorts and hashes of the queries |
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
the system chose in it (see [Reading the plans](docs/workloads.md#reading-the-plans)).

## The workloads

`kvbench list` has them with their variants. What each one does, its variants and its cases, and
how `kvbench verify` checks it, is in [docs/workloads.md](docs/workloads.md).

| Workload | Group | Measures |
| --- | --- | --- |
| [`kv-basic`](docs/workloads.md#kv-basic-group-kv) | `kv` | get, set, delete and scans by the primary key, in three layers of kvutil |
| [`bulk-load`](docs/workloads.md#bulk-load-group-load) | `load` | loading N records: `WriteBatch` and transactions against `COPY` and multi-row `INSERT` |
| [`record-codecs`](docs/workloads.md#record-codecs-group-records) | `records` | records with msgpack, JSON and binary codecs against columns, `jsonb` and `bytea` |
| [`index-maintenance`](docs/workloads.md#index-maintenance-group-index) | `index` | what an index costs a write, and what it costs to build |
| [`queries`](docs/workloads.md#queries-group-query) | `query` | 17 queries on a catalogue of books with 8 indexes, in the ways each system has, with the plans |
| [`selectivity-sweep`](docs/workloads.md#selectivity-sweep-group-query) | `query` | a range on an indexed field from 10 rows to half of the table |
| [`key-order-fetch`](docs/workloads.md#key-order-fetch-group-query-kvutil-only) | `query` | K records by key in random order, or sorted first (kvutil only) |
| [`agg-write`, `agg-hot-group`, `agg-read`, `agg-freshness`, `agg-rebuild`](docs/workloads.md#the-aggregations-group-agg) | `agg` | stored aggregations of events; also on TimescaleDB |
| [`relations`](docs/workloads.md#the-relations-group-relations) | `relations` | parents and children, and a many-to-many relation with data on its pairs |
| [`lifecycle`](docs/workloads.md#the-lifecycle-group-lifecycle) | `lifecycle` | adding and dropping indexes, converting records, opening and restarting |
| [`concurrency`](docs/workloads.md#the-concurrency-group-concurrency) | `concurrency` | contention at 1 to 16 workers |
| [`storage`](docs/workloads.md#the-storage-group-storage) | `storage` | bytes on disk, write amplification, giving space back, expiry |

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
loaders, the cursor over a pool of ids), `internal/testutil` (a kvutil system and a run for tests),
`harness.NewKeys` (keys drawn uniformly or by a Zipf law), `kvsys.System.OpenWith` (Badger with
options changed: the retries of a conflict, the value log), `internal/dockerapi` (the bytes a
container wrote, and the restart of one).
A new workload, or a new variant of one, changes nothing in the harness, the CLI, the scripts or
the results format. Describe it in [docs/workloads.md](docs/workloads.md).

## Limits of what is measured

* The harness is a closed loop at a fixed concurrency; it shows the latency at that load, not
  the response time of an open system.
* PostgreSQL is reached by pgx over the compose network, with a pool, and kvutil is a library call:
  the network and the protocol are a part of what a client of PostgreSQL pays.
* The ids of reads and writes are uniform, but for the authors of the books of the query
  workloads, the customers of the aggregations, the children of the relations, and the Zipf cases of
  `concurrency` (`harness.NewKeys`).
* The workloads of `concurrency` are a closed loop, like the rest: a worker does not start an
  operation before the last one returned, so a system that stalls makes the workers wait with it,
  and the latencies do not show the time that the operations that were not started would have
  waited (coordinated omission). The model that corrects it (a schedule of arrivals) was not used.
* `storage` reads the bytes written by a container from its cgroup or from the Docker API, and
  `lifecycle` restarts the database through the Docker API; both need the socket of the daemon
  in the harness' container, which compose mounts. They say when they could not (see
  [The storage](docs/workloads.md#the-storage-group-storage)).
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
that kvutil is expected to choose), the data of the relations against the model, the
answers of the lifecycle against the records computed in Go, the consistency checks of the
concurrency scenarios, the counts of the storage steps, the Zipf keys, the parsers of the cgroup and
of the Docker API (with a fake daemon on a unix socket) and the SQL of every case. For the report
(`internal/report`, with two small result directories in `testdata`): the parsing, the join of the
samples of docker stats to the cases, the ratios, the figures that are not available (n/a and
empty in the CSV, never zero), the parameters that do not match, the case that was run again and
replaces the first, the summary, the special sections, a workload that the report has never heard
of, the charts and the files. The comparison with PostgreSQL is
`kvbench verify`.
CI vets and builds the module without running Docker.
