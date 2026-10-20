#!/usr/bin/env bash
#
# Builds the harness, starts what a run needs, runs workloads at a scale, collects the results in
# results/<host>-<timestamp>/ and tears everything down. It needs bash and docker compose v2, and
# works unchanged on macOS and Linux.
#
#   ./run.sh --smoke                         # a few minutes: every workload, both systems, small
#   ./run.sh -w kv-basic,bulk-load -s 1000000
#   ./run.sh -s 1000000 -V large -d both     # the full run, for both durability modes
#
set -euo pipefail
cd "$(dirname "$0")"

usage() {
  cat <<'EOF'
usage: ./run.sh [options]

  -w, --workload LIST     workloads, groups (kv, load, records, index, query) or all  (all)
  -s, --scale N           records                                                (100000)
  -V, --values SIZE       small (~200 B) or large (~4 KB)                        (small)
  -d, --durability MODE   durable, relaxed or both                               (durable)
  -c, --concurrency C     workers of the cases that run concurrently             (4)
      --duration D        measured interval of a timed case                      (10s)
      --warmup D          warm-up of a timed case                                (2s)
      --systems LIST      kvutil, postgres or kvutil,postgres                    (kvutil,postgres)
      --seed N            seed of the dataset and the workers                    (1)
      --cpus N            CPUs for the database and for the harness each         (4, or $BENCH_CPUS)
      --mem SIZE          memory for each of them, like 4g or 6144m              (4g, or $BENCH_MEM)
      --pg-service NAME   postgres or timescaledb                                (postgres)
      --verify            run "kvbench verify" first, and stop if it fails
      --smoke             -s 10000 --duration 1s --warmup 500ms --verify
      --out DIR           results directory   (results/<host>-<timestamp>)
      --keep              do not remove the containers and volumes at the end
      --no-build          do not build the image of the harness
  -h, --help
EOF
}

WORKLOAD=all SCALE=100000 VALUES=small DURABILITY=durable CONCURRENCY=4
DURATION=10s WARMUP=2s SYSTEMS=kvutil,postgres SEED=1 VERIFY=0 KEEP=0 BUILD=1 OUT=""
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

BENCH_DOCKER_SERVER_VERSION=$(docker info --format '{{.ServerVersion}}')
BENCH_DOCKER_OS=$(docker info --format '{{.OperatingSystem}}')
BENCH_DOCKER_NCPU=$(docker info --format '{{.NCPU}}')
BENCH_DOCKER_MEM_TOTAL=$(docker info --format '{{.MemTotal}}')
BENCH_DOCKER_KERNEL=$(docker info --format '{{.KernelVersion}}')
export BENCH_DOCKER_SERVER_VERSION BENCH_DOCKER_OS BENCH_DOCKER_NCPU BENCH_DOCKER_MEM_TOTAL BENCH_DOCKER_KERNEL

COMPOSE=(docker compose --profile bench)
[ "$PG_SERVICE" = timescaledb ] && COMPOSE+=(--profile timescale)

log() { printf '\n==> %s\n' "$*"; }

STATS_PID=""
start_stats() {
  # docker stats of the containers, once in a while: the CPU and memory of the database, which
  # the harness cannot see from its own container. Each line has the time it was taken at.
  local file=$1
  echo "ts,name,cpu_perc,mem_usage,mem_perc,net_io,block_io,pids" > "$file"
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
    "${COMPOSE[@]}" --profile timescale down -v --remove-orphans >/dev/null 2>&1 || true
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
{
  printf '{\n'
  printf '  "host": "%s",\n' "$BENCH_HOST"
  printf '  "started": "%s",\n' "$STAMP"
  printf '  "kvutil_commit": "%s",\n' "$KVUTIL_COMMIT"
  printf '  "args": {"workload": "%s", "scale": %s, "values": "%s", "durability": "%s", "concurrency": %s, "duration": "%s", "warmup": "%s", "systems": "%s", "seed": %s, "pg_service": "%s"},\n' \
    "$WORKLOAD" "$SCALE" "$VALUES" "$DURABILITY" "$CONCURRENCY" "$DURATION" "$WARMUP" "$SYSTEMS" "$SEED" "$PG_SERVICE"
  printf '  "limits": {"cpus": "%s", "mem": "%s", "shared_buffers": "%s", "effective_cache_size": "%s"},\n' \
    "$BENCH_CPUS" "$BENCH_MEM" "$PG_SHARED_BUFFERS" "$PG_EFFECTIVE_CACHE_SIZE"
  printf '  "docker": {"server": "%s", "os": "%s", "ncpu": %s, "mem_total": %s, "kernel": "%s"}\n' \
    "$BENCH_DOCKER_SERVER_VERSION" "$BENCH_DOCKER_OS" "$BENCH_DOCKER_NCPU" "$BENCH_DOCKER_MEM_TOTAL" "$BENCH_DOCKER_KERNEL"
  printf '}\n'
} > "$OUT/meta.json"

# --- build

if [ "$BUILD" = 1 ]; then
  log "building the harness (kvutil $KVUTIL_COMMIT)"
  "${COMPOSE[@]}" build bench
fi

COMMON=(-scale "$SCALE" -values "$VALUES" -concurrency "$CONCURRENCY" -duration "$DURATION" -warmup "$WARMUP" -seed "$SEED" -workload "$WORKLOAD")

start_pg() {
  "${COMPOSE[@]}" up -d --wait --force-recreate "$PG_SERVICE" >/dev/null
}

if [ "$VERIFY" = 1 ]; then
  log "verifying that both systems return the same"
  export PG_SYNCHRONOUS_COMMIT=on
  start_pg
  harness verify -workload "$WORKLOAD" -seed "$SEED" -values "$VALUES" | tee "$OUT/verify.txt"
  "${COMPOSE[@]}" stop "$PG_SERVICE" >/dev/null
fi

for d in $DURABILITIES; do
  if [ "$d" = durable ]; then export PG_SYNCHRONOUS_COMMIT=on; else export PG_SYNCHRONOUS_COMMIT=off; fi
  RESULTS="/results/$OUT_NAME/results-$d.jsonl"
  start_stats "$OUT/docker-stats-$d.csv"

  if has_system kvutil; then
    log "kvutil, $d"
    harness run -system kvutil -durability "$d" -out "$RESULTS" "${COMMON[@]}"
  fi
  if has_system postgres; then
    log "$PG_SERVICE, $d (fsync=$PG_FSYNC synchronous_commit=$PG_SYNCHRONOUS_COMMIT)"
    start_pg
    harness run -system postgres -durability "$d" -out "$RESULTS" "${COMMON[@]}"
    docker logs "kvbench-$PG_SERVICE" > "$OUT/$PG_SERVICE-$d.log" 2>&1 || true
    "${COMPOSE[@]}" stop "$PG_SERVICE" >/dev/null
  fi
  stop_stats
done

for image in postgres:18 timescale/timescaledb:2.31.0-pg18; do
  docker image inspect "$image" --format "$image {{index .RepoDigests 0}}" 2>/dev/null || true
done > "$OUT/images.txt"

log "results in bench/$OUT"
ls -l "$OUT"
