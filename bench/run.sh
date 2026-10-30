#!/usr/bin/env bash
#
# Builds the harness, starts what a run needs, runs workloads at a scale, collects the results in
# results/<host>-<timestamp>/ and tears everything down. It needs bash and docker compose v2, and
# works unchanged on macOS and Linux.
#
#   ./run.sh --smoke                         # about 25 minutes: every workload, all systems, small
#   ./run.sh -w kv-basic,bulk-load -s 1000000
#   ./run.sh -s 1000000 -V large -d both     # the full run, for both durability modes
#
# At the end it writes the report (report.html, report.md, charts/ and CSV files) into the run's
# directory; a report that fails does not fail the run.
#
# A run can be made in pieces, or a part of it made again: running with the same --out adds to
# that directory. The results, the docker stats and the logs of the databases are appended to
# their files; meta.json (the arguments of the first piece) is written only if it is not there
# yet; verify.txt is written only by --verify; and the report is made again from everything.
# Where a case is in the results more than once, the report shows the last:
#
#   ./run.sh -s 1000000 -w relations --systems kvutil --out results/x
#
set -euo pipefail
cd "$(dirname "$0")"

usage() {
  cat <<'EOF'
usage: ./run.sh [options]

  -w, --workload LIST     workloads, groups (kv, load, records, index, query, agg, relations,
                          lifecycle, concurrency, storage) or all                 (all)
  -s, --scale N           records (events, for the aggregations)                 (100000)
  -V, --values SIZE       small (~200 B) or large (~4 KB)                        (small)
  -d, --durability MODE   durable, relaxed or both                               (durable)
  -c, --concurrency C     workers of the cases that run concurrently             (4)
      --duration D        measured interval of a timed case                      (10s)
      --warmup D          warm-up of a timed case                                (2s)
      --systems LIST      any of kvutil, postgres, timescale                     (kvutil,postgres,timescale)
                          (TimescaleDB is started only if a selected workload runs on it)
      --agg-span-days N   days that the events of the aggregations spread over   (365)
      --agg-interval D    interval of kvutil's collector and of the refresh
                          policies of TimescaleDB                                (1s)
      --seed N            seed of the dataset and the workers                    (1)
      --cpus N            CPUs for the database and for the harness each         (4, or $BENCH_CPUS)
      --mem SIZE          memory for each of them, like 4g or 6144m              (4g, or $BENCH_MEM)
      --pg-service NAME   postgres or timescaledb                                (postgres)
      --verify            run "kvbench verify" first, and stop if it fails
      --smoke             -s 10000 --duration 1s --warmup 500ms --verify
      --out DIR           results directory   (results/<host>-<timestamp>)
                          an existing one is added to, not overwritten
      --keep              do not remove the containers and volumes at the end
      --no-build          do not build the image of the harness
  -h, --help
EOF
}

WORKLOAD=all SCALE=100000 VALUES=small DURABILITY=durable CONCURRENCY=4
DURATION=10s WARMUP=2s SYSTEMS=kvutil,postgres,timescale SEED=1 VERIFY=0 KEEP=0 BUILD=1 OUT=""
AGG_SPAN_DAYS=${KVB_AGG_SPAN_DAYS:-365} AGG_INTERVAL=${KVB_AGG_INTERVAL:-1s}
PG_SERVICE=${PG_SERVICE:-postgres}
BENCH_CPUS=${BENCH_CPUS:-4}
BENCH_MEM=${BENCH_MEM:-4g}

while [ $# -gt 0 ]; do
  case "$1" in
    -w|--workload) WORKLOAD=$2; shift 2 ;;
    -s|--scale) SCALE=$2; shift 2 ;;
    -V|--values) VALUES=$2; shift 2 ;;
    -d|--durability) DURABILITY=$2; shift 2 ;;
    -c|--concurrency) CONCURRENCY=$2; shift 2 ;;
    --duration) DURATION=$2; shift 2 ;;
    --warmup) WARMUP=$2; shift 2 ;;
    --systems) SYSTEMS=$2; shift 2 ;;
    --agg-span-days) AGG_SPAN_DAYS=$2; shift 2 ;;
    --agg-interval) AGG_INTERVAL=$2; shift 2 ;;
    --seed) SEED=$2; shift 2 ;;
    --cpus) BENCH_CPUS=$2; shift 2 ;;
    --mem) BENCH_MEM=$2; shift 2 ;;
    --pg-service) PG_SERVICE=$2; shift 2 ;;
    --verify) VERIFY=1; shift ;;
    --smoke) SCALE=10000 DURATION=1s WARMUP=500ms VERIFY=1; shift ;;
    --out) OUT=$2; shift 2 ;;
    --keep) KEEP=1; shift ;;
    --no-build) BUILD=0; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

case "$DURABILITY" in
  both) DURABILITIES="durable relaxed" ;;
  durable|relaxed) DURABILITIES=$DURABILITY ;;
  *) echo "durability must be durable, relaxed or both" >&2; exit 2 ;;
esac
case "$PG_SERVICE" in postgres|timescaledb) ;; *) echo "pg-service must be postgres or timescaledb" >&2; exit 2 ;; esac

# --- the limits, and the settings of PostgreSQL that scale with them

mem_mb() {
  case "$1" in
    *[gG]) echo $(( ${1%[gG]} * 1024 )) ;;
    *[mM]) echo "${1%[mM]}" ;;
    *) echo "memory must be like 4g or 4096m, not $1" >&2; exit 2 ;;
  esac
}
MEM_MB=$(mem_mb "$BENCH_MEM")
MAINT_MB=$(( MEM_MB / 8 )); [ "$MAINT_MB" -gt 1024 ] && MAINT_MB=1024
export BENCH_CPUS BENCH_MEM PG_SERVICE
export PG_SHARED_BUFFERS="$(( MEM_MB / 4 ))MB"            # a quarter of the memory
export PG_EFFECTIVE_CACHE_SIZE="$(( MEM_MB * 3 / 4 ))MB"  # three quarters
export PG_MAINTENANCE_WORK_MEM="${MAINT_MB}MB"
export PG_WORK_MEM=32MB
export PG_FSYNC=${PG_FSYNC:-on}
export BENCH_GOMEMLIMIT="$(( MEM_MB * 3 / 4 ))MiB"        # the Go runtime collects before the limit

# --- what is benchmarked, and where

if git rev-parse --git-dir >/dev/null 2>&1; then
  KVUTIL_COMMIT=$(git rev-parse HEAD)
  if [ -n "$(git -C .. status --porcelain -- . ':!bench' 2>/dev/null)" ]; then
    KVUTIL_COMMIT="$KVUTIL_COMMIT-dirty"
  fi
else
  KVUTIL_COMMIT=unknown
fi
export KVUTIL_COMMIT

BENCH_HOST=$(hostname -s 2>/dev/null || hostname)
export BENCH_HOST
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
[ -n "$OUT" ] || OUT="results/${BENCH_HOST}-${STAMP}"
mkdir -p "$OUT"
OUT_NAME=$(basename "$OUT")

# The day after the last day of the events of the aggregations, fixed for the whole run so that
# every system gets the same events even if the run goes past midnight (UTC). A run that is added
# to keeps the day of its first piece.
if [ -z "${KVB_AGG_END:-}" ] && [ -f "$OUT/meta.json" ]; then
  KVB_AGG_END=$(sed -n 's/.*"aggregations": {"end": "\([0-9-]*\)".*/\1/p' "$OUT/meta.json")
fi
KVB_AGG_END=${KVB_AGG_END:-$(date -u +%Y-%m-%d)}
export KVB_AGG_END KVB_AGG_SPAN_DAYS=$AGG_SPAN_DAYS KVB_AGG_INTERVAL=$AGG_INTERVAL

BENCH_DOCKER_SERVER_VERSION=$(docker info --format '{{.ServerVersion}}')
BENCH_DOCKER_OS=$(docker info --format '{{.OperatingSystem}}')
BENCH_DOCKER_NCPU=$(docker info --format '{{.NCPU}}')
BENCH_DOCKER_MEM_TOTAL=$(docker info --format '{{.MemTotal}}')
BENCH_DOCKER_KERNEL=$(docker info --format '{{.KernelVersion}}')
export BENCH_DOCKER_SERVER_VERSION BENCH_DOCKER_OS BENCH_DOCKER_NCPU BENCH_DOCKER_MEM_TOTAL BENCH_DOCKER_KERNEL

COMPOSE=(docker compose --profile bench --profile timescale)

log() { printf '\n==> %s\n' "$*"; }

STATS_PID=""
start_stats() {
  # docker stats of the containers, once in a while: the CPU and memory of the database, which
  # the harness cannot see from its own container. Each line has the time it was taken at.
  local file=$1
  if [ ! -e "$file" ]; then
    echo "ts,name,cpu_perc,mem_usage,mem_perc,net_io,block_io,pids" > "$file"
  fi
  (
    while true; do
      ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
      docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}},{{.NetIO}},{{.BlockIO}},{{.PIDs}}' 2>/dev/null \
        | grep '^kvbench-' | sed "s/^/$ts,/" >> "$file" || true
      sleep 1
    done
  ) &
  STATS_PID=$!
}
stop_stats() {
  if [ -n "$STATS_PID" ]; then
    kill "$STATS_PID" 2>/dev/null || true
    wait "$STATS_PID" 2>/dev/null || true
    STATS_PID=""
  fi
}

cleanup() {
  stop_stats
  if [ "$KEEP" = 0 ]; then
    log "tearing down"
    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM

harness() {
  "${COMPOSE[@]}" run --rm --no-deps -T --name kvbench-bench bench "$@"
}

has_system() { case ",$SYSTEMS," in *",$1,"*) return 0 ;; *) return 1 ;; esac; }

# --- the record of the run

"${COMPOSE[@]}" config > "$OUT/compose.resolved.yml"
# The record of the run is the one of its first piece: a run that is added to keeps its meta.json.
if [ ! -e "$OUT/meta.json" ]; then
  {
    printf '{\n'
    printf '  "host": "%s",\n' "$BENCH_HOST"
    printf '  "started": "%s",\n' "$STAMP"
    printf '  "kvutil_commit": "%s",\n' "$KVUTIL_COMMIT"
    printf '  "args": {"workload": "%s", "scale": %s, "values": "%s", "durability": "%s", "concurrency": %s, "duration": "%s", "warmup": "%s", "systems": "%s", "seed": %s, "pg_service": "%s"},\n' \
      "$WORKLOAD" "$SCALE" "$VALUES" "$DURABILITY" "$CONCURRENCY" "$DURATION" "$WARMUP" "$SYSTEMS" "$SEED" "$PG_SERVICE"
    printf '  "aggregations": {"end": "%s", "span_days": %s, "interval": "%s"},\n' \
      "$KVB_AGG_END" "$AGG_SPAN_DAYS" "$AGG_INTERVAL"
    printf '  "limits": {"cpus": "%s", "mem": "%s", "shared_buffers": "%s", "effective_cache_size": "%s"},\n' \
      "$BENCH_CPUS" "$BENCH_MEM" "$PG_SHARED_BUFFERS" "$PG_EFFECTIVE_CACHE_SIZE"
    printf '  "docker": {"server": "%s", "os": "%s", "ncpu": %s, "mem_total": %s, "kernel": "%s"}\n' \
      "$BENCH_DOCKER_SERVER_VERSION" "$BENCH_DOCKER_OS" "$BENCH_DOCKER_NCPU" "$BENCH_DOCKER_MEM_TOTAL" "$BENCH_DOCKER_KERNEL"
    printf '}\n'
  } > "$OUT/meta.json"
fi

# --- build

if [ "$BUILD" = 1 ]; then
  log "building the harness (kvutil $KVUTIL_COMMIT)"
  "${COMPOSE[@]}" build bench
fi

COMMON=(-scale "$SCALE" -values "$VALUES" -concurrency "$CONCURRENCY" -duration "$DURATION" -warmup "$WARMUP" -seed "$SEED" -workload "$WORKLOAD")

start_service() {
  "${COMPOSE[@]}" up -d --wait --force-recreate "$1" >/dev/null
}
stop_service() {
  "${COMPOSE[@]}" stop "$1" >/dev/null
}
start_pg() { start_service "$PG_SERVICE"; }

# TimescaleDB is started only if a selected workload runs on it (the aggregations).
NEED_TS=0
if has_system timescale && [ -n "$(harness needs -workload "$WORKLOAD" | tr -d '[:space:]')" ]; then
  NEED_TS=1
fi

if [ "$VERIFY" = 1 ]; then
  log "verifying that the systems return the same"
  export PG_SYNCHRONOUS_COMMIT=on
  start_pg
  if [ "$NEED_TS" = 1 ] && [ "$PG_SERVICE" != timescaledb ]; then
    start_service timescaledb
  fi
  harness verify -workload "$WORKLOAD" -seed "$SEED" -values "$VALUES" | tee "$OUT/verify.txt"
  stop_service "$PG_SERVICE"
  if [ "$NEED_TS" = 1 ] && [ "$PG_SERVICE" != timescaledb ]; then
    stop_service timescaledb
  fi
fi

# A workload that fails must not cost the other systems their run or the run its report: the failure
# is remembered, and the script fails at its end.
FAILED=""
run_system() {
  if ! harness run -system "$1" -durability "$2" -out "/results/$OUT_NAME/results-$2.jsonl" "${COMMON[@]}"; then
    FAILED="$FAILED $1/$2"
    echo "warning: $1 ($2) had workloads that failed; see the FAILED lines above" >&2
  fi
}

for d in $DURABILITIES; do
  if [ "$d" = durable ]; then export PG_SYNCHRONOUS_COMMIT=on; else export PG_SYNCHRONOUS_COMMIT=off; fi
  start_stats "$OUT/docker-stats-$d.csv"

  if has_system kvutil; then
    log "kvutil, $d"
    run_system kvutil "$d"
  fi
  if has_system postgres; then
    log "$PG_SERVICE, $d (fsync=$PG_FSYNC synchronous_commit=$PG_SYNCHRONOUS_COMMIT)"
    start_pg
    run_system postgres "$d"
    docker logs "kvbench-$PG_SERVICE" >> "$OUT/$PG_SERVICE-$d.log" 2>&1 || true
    stop_service "$PG_SERVICE"
  fi
  if has_system timescale && [ "$NEED_TS" = 1 ]; then
    log "timescaledb, $d (fsync=$PG_FSYNC synchronous_commit=$PG_SYNCHRONOUS_COMMIT)"
    start_service timescaledb
    run_system timescale "$d"
    docker logs kvbench-timescaledb >> "$OUT/timescaledb-$d.log" 2>&1 || true
    stop_service timescaledb
  fi
  stop_stats
done

for image in postgres:18 timescale/timescaledb:2.31.0-pg18; do
  docker image inspect "$image" --format "$image {{index .RepoDigests 0}}" 2>/dev/null || true
done > "$OUT/images.txt"

# The report: report.md, report.html, charts/ and the CSV files, in the directory of the run. A
# report that fails does not fail the run, whose results are all there; "kvbench report" makes it again.
log "report"
if harness report -out "/results/$OUT_NAME" "/results/$OUT_NAME"; then
  echo "report: bench/$OUT/report.html"
else
  echo "warning: the report could not be made; the results are in bench/$OUT. Try again with:" >&2
  echo "  docker compose run --rm --no-deps bench report -out /results/$OUT_NAME /results/$OUT_NAME" >&2
fi

log "results in bench/$OUT"
ls -l "$OUT"
if [ -n "$FAILED" ]; then
  echo "error: workloads failed on:$FAILED" >&2
  exit 1
fi
