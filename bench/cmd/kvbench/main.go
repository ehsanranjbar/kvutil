// Command kvbench runs the benchmarks of kvutil against PostgreSQL.
//
//	kvbench run    -system kvutil|postgres|timescale -workload NAME|GROUP|all -scale N ...
//	kvbench verify -workload NAME|GROUP|all -scale N
//	kvbench report -out DIR RESULTDIR...
//	kvbench list
//	kvbench needs  -workload NAME|GROUP|all
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/metrics"
	"github.com/ehsanranjbar/kvutil/bench/internal/report"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/kvsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/pgsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/tssys"

	_ "github.com/ehsanranjbar/kvutil/bench/internal/workloads/aggs"
	_ "github.com/ehsanranjbar/kvutil/bench/internal/workloads/bulkload"
	_ "github.com/ehsanranjbar/kvutil/bench/internal/workloads/codecs"
	_ "github.com/ehsanranjbar/kvutil/bench/internal/workloads/concurrency"
	_ "github.com/ehsanranjbar/kvutil/bench/internal/workloads/indexmaint"
	_ "github.com/ehsanranjbar/kvutil/bench/internal/workloads/keyfetch"
	_ "github.com/ehsanranjbar/kvutil/bench/internal/workloads/kvbasic"
	_ "github.com/ehsanranjbar/kvutil/bench/internal/workloads/lifecycle"
	_ "github.com/ehsanranjbar/kvutil/bench/internal/workloads/queries"
	_ "github.com/ehsanranjbar/kvutil/bench/internal/workloads/relations"
	_ "github.com/ehsanranjbar/kvutil/bench/internal/workloads/storage"
	_ "github.com/ehsanranjbar/kvutil/bench/internal/workloads/sweep"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "run":
		err = runCmd(ctx, os.Args[2:])
	case "verify":
		err = verifyCmd(ctx, os.Args[2:])
	case "report":
		err = reportCmd(os.Args[2:])
	case "list":
		err = listCmd()
	case "needs":
		err = needsCmd(os.Args[2:])
	case "-h", "-help", "--help", "help":
		usage()
		return
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "kvbench:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage:
  kvbench run    -system kvutil|postgres|timescale -workload <name|group|all> [-scale N] [-values small|large]
                 [-durability durable|relaxed] [-concurrency C] [-duration D] [-warmup W] [-out file.jsonl]
  kvbench verify -workload <name|group|all> [-scale N] [-values small|large]
  kvbench report -out DIR RESULTDIR...       writes DIR/report.md, report.html, charts/ and the CSV files
  kvbench list
  kvbench needs  -workload <name|group|all>   prints the systems besides kvutil and postgres that the workloads run on

The environment: KVB_PG_DSN (the DSN of PostgreSQL), KVB_TS_DSN (the DSN of TimescaleDB),
KVB_DATA_DIR (where Badger keeps its databases), KVB_BADGER_CACHE_MB, KVB_PREFETCH; KVB_AGG_END,
KVB_AGG_SPAN_DAYS and KVB_AGG_INTERVAL for the aggregations; KVB_PG_CONTAINER, KVB_SELF_CONTAINER
and KVB_DOCKER_SOCKET for the bytes written by a container and the restart of the database (storage,
lifecycle). "kvbench run -h" has the flags.
`)
}

// common are the flags of run and verify.
type common struct {
	workload   string
	scale      int
	values     string
	durability string
	seed       uint64
	dataDir    string
	pgDSN      string
	tsDSN      string
	cacheMB    int
	prefetch   int
}

func (c *common) register(fs *flag.FlagSet, defScale int, defDurability string) {
	fs.StringVar(&c.workload, "workload", "all", "workloads: names and groups separated by commas, or all")
	fs.IntVar(&c.scale, "scale", defScale, "number of records")
	fs.StringVar(&c.values, "values", "small", "size of the values: small (~200 B) or large (~4 KB)")
	fs.StringVar(&c.durability, "durability", defDurability, "durable (wait for the disk on commit) or relaxed")
	fs.Uint64Var(&c.seed, "seed", 1, "seed of the dataset and of the choices of the workers")
	fs.StringVar(&c.dataDir, "data-dir", envOr("KVB_DATA_DIR", ""), "directory of the Badger databases (default: /data if it exists, else a temporary one)")
	fs.StringVar(&c.pgDSN, "pg-dsn", os.Getenv("KVB_PG_DSN"), "DSN of PostgreSQL")
	fs.StringVar(&c.tsDSN, "ts-dsn", os.Getenv("KVB_TS_DSN"), "DSN of TimescaleDB")
	fs.IntVar(&c.cacheMB, "cache-mb", envInt("KVB_BADGER_CACHE_MB", 0), "block cache of Badger in MiB (default: a quarter of the memory limit of the container, or 256)")
	fs.IntVar(&c.prefetch, "prefetch", envInt("KVB_PREFETCH", 64), "values that the iterators of kvutil read ahead")
}

func (c *common) params() (harness.Params, error) {
	p := harness.Params{
		Scale:       c.scale,
		Values:      dataset.ValueSize(c.values),
		Durability:  harness.Durability(c.durability),
		Concurrency: 1,
		Seed:        c.seed,
	}
	if p.Scale < 100 {
		return p, errors.New("the scale must be 100 at least")
	}
	if !p.Values.Valid() {
		return p, fmt.Errorf("unknown value size %q", c.values)
	}
	if !p.Durability.Valid() {
		return p, fmt.Errorf("unknown durability %q", c.durability)
	}
	return p, nil
}

func (c *common) kvutil(host metrics.Host, d harness.Durability) (*kvsys.System, func(), error) {
	dir := c.dataDir
	cleanup := func() {}
	if dir == "" {
		if st, err := os.Stat("/data"); err == nil && st.IsDir() {
			dir = "/data"
		} else {
			tmp, err := os.MkdirTemp("", "kvbench-")
			if err != nil {
				return nil, nil, err
			}
			dir = tmp
			cleanup = func() { os.RemoveAll(tmp) }
		}
	}
	cache := int64(c.cacheMB) << 20
	if cache == 0 && host.MemLimitBytes > 0 {
		cache = host.MemLimitBytes / 4
	}
	sys, err := kvsys.New(kvsys.Config{DataDir: dir, Durability: d, BlockCacheBytes: cache, Prefetch: c.prefetch})
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return sys, cleanup, nil
}

func (c *common) postgres(ctx context.Context, d harness.Durability, conns int) (*pgsys.System, error) {
	if c.pgDSN == "" {
		return nil, errors.New("PostgreSQL needs a DSN: -pg-dsn or KVB_PG_DSN")
	}
	return pgsys.Open(ctx, pgsys.Config{DSN: c.pgDSN, Durability: d, MaxConns: int32(conns)})
}

func (c *common) timescale(ctx context.Context, d harness.Durability, conns int) (*tssys.System, error) {
	if c.tsDSN == "" {
		return nil, errors.New("TimescaleDB needs a DSN: -ts-dsn or KVB_TS_DSN")
	}
	return tssys.Open(ctx, pgsys.Config{DSN: c.tsDSN, Durability: d, MaxConns: int32(conns)})
}

func runCmd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	var c common
	c.register(fs, 100_000, "durable")
	system := fs.String("system", "", "kvutil, postgres or timescale")
	concurrency := fs.Int("concurrency", 4, "workers of the cases that run concurrently")
	duration := fs.Duration("duration", 10*time.Second, "length of the measured interval of a timed case")
	warmup := fs.Duration("warmup", 2*time.Second, "warm-up of a timed case, which is not measured")
	out := fs.String("out", "", "file that the results are appended to, as JSON lines")
	fs.Parse(args)

	p, err := c.params()
	if err != nil {
		return err
	}
	p.Concurrency, p.Duration, p.Warmup = *concurrency, *duration, *warmup
	if p.Concurrency < 1 {
		return errors.New("the concurrency must be 1 at least")
	}
	workloads, err := harness.Select(c.workload)
	if err != nil {
		return err
	}

	host := metrics.ReadHost()
	var sys harness.System
	switch *system {
	case "kvutil":
		s, cleanup, err := c.kvutil(host, p.Durability)
		if err != nil {
			return err
		}
		defer cleanup()
		sys = s
	case "postgres":
		s, err := c.postgres(ctx, p.Durability, poolSize(p.Concurrency))
		if err != nil {
			return err
		}
		sys = s
	case "timescale":
		s, err := c.timescale(ctx, p.Durability, poolSize(p.Concurrency))
		if err != nil {
			return err
		}
		sys = s
	default:
		return fmt.Errorf("-system must be kvutil, postgres or timescale, not %q", *system)
	}
	defer sys.Close()

	sink, err := metrics.NewSink(*out, os.Stderr)
	if err != nil {
		return err
	}
	defer sink.Close()

	fmt.Fprintf(os.Stderr, "%s: scale=%d values=%s durability=%s concurrency=%d duration=%s warmup=%s\n",
		sys.Version(), p.Scale, p.Values, p.Durability, p.Concurrency, p.Duration, p.Warmup)

	var errs []error
	for _, w := range workloads {
		if len(w.Variants(sys)) == 0 {
			fmt.Fprintf(os.Stderr, "-- %s does not run on %s\n", w.Name(), sys.Name())
			continue
		}
		if err := harness.Execute(ctx, w, sys, p, sink, host, os.Stderr); err != nil {
			fmt.Fprintf(os.Stderr, "FAILED: %v\n", err)
			errs = append(errs, err)
		}
		if ctx.Err() != nil {
			break
		}
	}
	return errors.Join(errs...)
}

// poolSize is the number of connections of the pool: the workers of a case, a few more for what
// runs beside them, and room for the concurrency workload, which runs 16 workers whatever the
// run's concurrency is, with a long read transaction beside them.
func poolSize(concurrency int) int {
	return max(concurrency, 16) + 6
}

func verifyCmd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	var c common
	c.register(fs, 2000, "relaxed")
	show := fs.Int("show", 0, "print this many lines of what each variant returned")
	fs.Parse(args)

	p, err := c.params()
	if err != nil {
		return err
	}
	workloads, err := harness.Select(c.workload)
	if err != nil {
		return err
	}

	host := metrics.ReadHost()
	kvs, cleanup, err := c.kvutil(host, p.Durability)
	if err != nil {
		return err
	}
	defer cleanup()
	pgs, err := c.postgres(ctx, p.Durability, 12)
	if err != nil {
		return err
	}
	defer pgs.Close()
	systems := []harness.System{kvs, pgs}
	if needsTimescale(workloads) {
		tss, err := c.timescale(ctx, p.Durability, 12)
		if err != nil {
			return err
		}
		defer tss.Close()
		systems = append(systems, tss)
	}

	failed := false
	for _, w := range workloads {
		outs, diffs, err := harness.Verify(ctx, w, systems, p)
		if err != nil {
			fmt.Printf("ERROR  %-16s %v\n", w.Name(), err)
			failed = true
			continue
		}
		var names []string
		lines := 0
		for _, o := range outs {
			for i, line := range o.Lines {
				if i >= *show {
					break
				}
				fmt.Printf("  %-28s %s\n", o.Label(), line)
			}
			names = append(names, o.Label())
			lines = max(lines, len(o.Lines))
		}
		if len(diffs) > 0 {
			failed = true
			fmt.Printf("DIFFER %-16s %s\n", w.Name(), strings.Join(names, ", "))
			for _, d := range diffs {
				fmt.Printf("         %s\n", d)
			}
			continue
		}
		fmt.Printf("OK     %-16s %d lines, the same from %s\n", w.Name(), lines, strings.Join(names, ", "))
	}
	if failed {
		return errors.New("verification failed")
	}
	return nil
}

// reportCmd writes the report of one or more result directories.
func reportCmd(args []string) error {
	fs := flag.NewFlagSet("report", flag.ExitOnError)
	out := fs.String("out", "", "directory that the report is written to (report.md, report.html, charts/, *.csv)")
	fs.Parse(args)
	if *out == "" || fs.NArg() == 0 {
		return errors.New("usage: kvbench report -out DIR RESULTDIR...")
	}
	return report.Generate(*out, fs.Args())
}

func listCmd() error {
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "WORKLOAD\tGROUP\tKVUTIL VARIANTS\tPOSTGRES VARIANTS\tTIMESCALE VARIANTS\tDESCRIPTION")
	for _, w := range harness.All() {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", w.Name(), w.Group(),
			names(w.Variants((*kvsys.System)(nil))), names(w.Variants((*pgsys.System)(nil))),
			names(w.Variants((*tssys.System)(nil))), w.Description())
	}
	return tw.Flush()
}

// needsTimescale reports whether any of the workloads runs on TimescaleDB.
func needsTimescale(workloads []harness.Workload) bool {
	for _, w := range workloads {
		if len(w.Variants((*tssys.System)(nil))) > 0 {
			return true
		}
	}
	return false
}

// needsCmd prints "timescale" if a workload of the selection runs on TimescaleDB, which is how the
// scripts know to start it.
func needsCmd(args []string) error {
	fs := flag.NewFlagSet("needs", flag.ExitOnError)
	workload := fs.String("workload", "all", "workloads: names and groups separated by commas, or all")
	fs.Parse(args)
	workloads, err := harness.Select(*workload)
	if err != nil {
		return err
	}
	if needsTimescale(workloads) {
		fmt.Println("timescale")
	}
	return nil
}

func names(vs []harness.Variant) string {
	if len(vs) == 0 {
		return "-"
	}
	ns := make([]string, len(vs))
	for i, v := range vs {
		ns[i] = v.Name()
	}
	return strings.Join(ns, ",")
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	var n int
	if _, err := fmt.Sscan(os.Getenv(key), &n); err != nil {
		return def
	}
	return n
}
