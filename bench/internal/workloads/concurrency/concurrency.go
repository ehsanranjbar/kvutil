// Package concurrency is the workload of contention: what the two systems do when many workers
// write the same keys, different keys, or read while others write.
//
// A variant is a scenario on a fresh database; the cases are the concurrency levels (1, 2, 4, 8
// and 16 workers, whatever -concurrency says) and the sizes of the sets of keys:
//
//	rmw-update          kvutil, a counter incremented in db.Update (read, add one, write) on a hot
//	                    set of 1, 10, 100 and 10,000 keys, the conflicts retried up to 10 times
//	rmw-update-r100     the same with the limit of retries at 100 (the sets of 1, 10, 100 keys)
//	rmw-for-update      PostgreSQL, SELECT ... FOR UPDATE and UPDATE in a transaction
//	rmw-atomic          PostgreSQL, UPDATE counters SET n = n + 1, the statement that is atomic
//	blind-write         inserts of distinct keys, which never conflict: what group commit gives
//	mixed               90% reads and 10% read-modify-writes of records, keys drawn uniformly
//	                    and from a Zipf law
//	long-read           writers incrementing counters while a read transaction takes the same
//	                    snapshot of all of them again and again
//
// The cases are a closed loop: a worker starts its next operation when the last one returned.
package concurrency

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/metrics"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/kvsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/pgsys"
)

func init() { harness.Register(workload{}) }

// levels are the numbers of workers of a case.
var levels = []int{1, 2, 4, 8, 16}

// hotKeys are the sizes of the sets of keys that the workers increment counters in.
var hotKeys = []int64{1, 10, 100, 10_000}

// retryLimit is the number of times that Update runs a conflicting transaction again: the
// default of kvutil's Badger adapter.
const retryLimit = 10

type workload struct{}

func (workload) Name() string  { return "concurrency" }
func (workload) Group() string { return "concurrency" }
func (workload) Description() string {
	return "contention at 1, 2, 4, 8, 16 workers: counters on a hot set of keys, blind writes, 90/10 read/write (uniform and Zipf), a long read transaction beside writers"
}

// scenario names what a variant does.
type scenario string

const (
	scenarioRMW   scenario = "rmw"
	scenarioBlind scenario = "blind"
	scenarioMixed scenario = "mixed"
	scenarioLong  scenario = "long-read"
)

// incMode is how a counter is incremented.
type incMode string

const (
	modeUpdate    incMode = "update"     // kvutil: read and write in db.Update
	modeForUpdate incMode = "for-update" // PostgreSQL: SELECT ... FOR UPDATE, UPDATE
	modeAtomic    incMode = "atomic"     // PostgreSQL: UPDATE ... SET n = n + 1
)

type spec struct {
	name     string
	scenario scenario
	mode     incMode
	// retries is the limit of retries of kvutil's Update; 0 is the default.
	retries int
	// keys are the sizes of the sets of the scenario rmw.
	keys []int64
}

func kvutilSpecs() []spec {
	return []spec{
		{name: "rmw-update", scenario: scenarioRMW, mode: modeUpdate, keys: hotKeys},
		{name: "rmw-update-r100", scenario: scenarioRMW, mode: modeUpdate, retries: 100, keys: hotKeys[:3]},
		{name: "blind-write", scenario: scenarioBlind},
		{name: "mixed", scenario: scenarioMixed, mode: modeUpdate},
		{name: "long-read", scenario: scenarioLong, mode: modeUpdate},
	}
}

func postgresSpecs() []spec {
	return []spec{
		{name: "rmw-for-update", scenario: scenarioRMW, mode: modeForUpdate, keys: hotKeys},
		{name: "rmw-atomic", scenario: scenarioRMW, mode: modeAtomic, keys: hotKeys},
		{name: "blind-write", scenario: scenarioBlind},
		{name: "mixed", scenario: scenarioMixed, mode: modeAtomic},
		{name: "long-read", scenario: scenarioLong, mode: modeAtomic},
	}
}

func (workload) Variants(sys harness.System) []harness.Variant {
	var specs []spec
	switch sys.(type) {
	case *kvsys.System:
		specs = kvutilSpecs()
	case *pgsys.System:
		specs = postgresSpecs()
	}
	vs := make([]harness.Variant, len(specs))
	for i, s := range specs {
		vs[i] = &variant{spec: s}
	}
	return vs
}

// env is a database with the tables of the scenarios, of a system.
type env interface {
	// ResetCounters makes the counters 0 to n-1, each 0, and no other.
	ResetCounters(ctx context.Context, n int64) error
	// Inc adds one to a counter in a transaction of its own, the way of the variant.
	Inc(ctx context.Context, key int64) error
	// SumCounters returns the sum of all the counters.
	SumCounters(ctx context.Context) (int64, error)
	// LongRead reads all the counters in one snapshot, again and again, until stop is closed, and
	// notes what it read.
	LongRead(ctx context.Context, stop <-chan struct{}, st *readerStats) error

	// ResetBlind empties the table of the blind writes.
	ResetBlind(ctx context.Context) error
	// Blind writes a new key.
	Blind(ctx context.Context, id int64, value []byte) error
	CountBlind(ctx context.Context) (int64, error)

	// LoadOrders writes the records [1, n] of the dataset.
	LoadOrders(ctx context.Context, spec dataset.Spec, n int64) error
	ReadOrder(ctx context.Context, id int64) error
	// BumpOrder adds one to the Qty of a record in a transaction of its own.
	BumpOrder(ctx context.Context, id int64) error
	SumQty(ctx context.Context) (int64, error)

	// Facts are numbers about the state of the database that a case reports, like the dead
	// tuples of PostgreSQL.
	Facts(ctx context.Context) map[string]float64
	Settle(ctx context.Context) error
	Sizes(ctx context.Context) (harness.Sizes, error)
	Close(ctx context.Context) error
}

// readerStats is what the long read transaction noted.
//
// Scans can be read while the reader runs; the rest is read once it has returned.
type readerStats struct {
	Scans atomic.Int64
	// Unstable counts the scans that found another sum or number of counters than the first.
	Unstable int64
	Sum      int64
	Counters int64
	Hist     *metrics.Hist
	// First and Last are the durations of the first and of the last scan.
	First, Last time.Duration
}

func newReaderStats() *readerStats { return &readerStats{Hist: metrics.NewHist()} }

// note records a scan.
func (s *readerStats) note(sum, counters int64, d time.Duration) {
	if s.Scans.Load() == 0 {
		s.Sum, s.Counters, s.First = sum, counters, d
	} else if sum != s.Sum || counters != s.Counters {
		s.Unstable++
	}
	s.Last = d
	s.Hist.Record(d)
	s.Scans.Add(1)
}

// waitFirstScan waits until the reader has taken its snapshot, or has failed.
func (s *readerStats) waitFirstScan(ctx context.Context, done <-chan error) error {
	for s.Scans.Load() == 0 {
		select {
		case err := <-done:
			if err == nil {
				err = fmt.Errorf("the reader ended before it read")
			}
			return err
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
	return nil
}

type variant struct{ spec }

func (v *variant) Name() string { return v.name }

func (v *variant) open(ctx context.Context, sys harness.System) (env, error) {
	switch s := sys.(type) {
	case *kvsys.System:
		return openKV(ctx, s, v.spec)
	case *pgsys.System:
		return openPG(ctx, s, v.spec)
	}
	return nil, fmt.Errorf("concurrency does not run on %s", sys.Name())
}

func (v *variant) Run(ctx context.Context, rc *harness.RunContext) error {
	e, err := v.open(ctx, rc.Sys)
	if err != nil {
		return err
	}
	defer e.Close(context.WithoutCancel(ctx))
	rc.SizeFn = e.Sizes

	switch v.scenario {
	case scenarioRMW:
		return v.runRMW(ctx, rc, e)
	case scenarioBlind:
		return v.runBlind(ctx, rc, e)
	case scenarioMixed:
		return v.runMixed(ctx, rc, e)
	default:
		return v.runLong(ctx, rc, e)
	}
}

// caseParams are the parameters that the cases of a variant have besides their own.
func (v *variant) caseParams(extra map[string]any) map[string]any {
	p := map[string]any{"scenario": string(v.scenario)}
	if v.mode != "" {
		p["increment"] = string(v.mode)
	}
	if v.mode == modeUpdate {
		p["retry_limit"] = v.retryLimit()
	}
	for k, x := range extra {
		p[k] = x
	}
	return p
}

func (v *variant) retryLimit() int {
	if v.retries > 0 {
		return v.retries
	}
	return retryLimit
}

// checkSum fails if the counters do not add up to the increments that succeeded: a lost update.
func checkSum(ctx context.Context, e env, label string, want int64) error {
	got, err := e.SumCounters(ctx)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("%s: the counters add up to %d, and %d increments succeeded", label, got, want)
	}
	return nil
}

func (v *variant) runRMW(ctx context.Context, rc *harness.RunContext, e env) error {
	for _, n := range v.keys {
		keys := harness.NewKeys(harness.Uniform, n)
		for _, c := range levels {
			if err := e.ResetCounters(ctx, n); err != nil {
				return err
			}
			if err := e.Settle(ctx); err != nil {
				return err
			}
			var ok atomic.Int64
			cs := harness.Case{Name: fmt.Sprintf("rmw-k%d", n), Concurrency: c, Params: v.caseParams(map[string]any{"keys": n})}
			err := rc.Measure(ctx, cs, func(ctx context.Context, w *harness.Worker) error {
				key := keys.Next(w.Rand)
				w.Begin()
				err := e.Inc(ctx, key)
				if err == nil {
					ok.Add(1)
				}
				return err
			})
			if err != nil {
				return err
			}
			if err := checkSum(ctx, e, cs.Name, ok.Load()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (v *variant) runBlind(ctx context.Context, rc *harness.RunContext, e env) error {
	spec := rc.Params.Dataset()
	var next atomic.Int64
	init := func(w *harness.Worker) { w.State = &dataset.Order{} }
	var total int64
	if err := e.ResetBlind(ctx); err != nil {
		return err
	}
	for _, c := range levels {
		var ok atomic.Int64
		cs := harness.Case{Name: "blind-write", Concurrency: c, Init: init, Params: v.caseParams(nil)}
		err := rc.Measure(ctx, cs, func(ctx context.Context, w *harness.Worker) error {
			id := next.Add(1)
			o := w.State.(*dataset.Order)
			spec.GenInto(o, id, 0)
			value := encodeOrder(o)
			w.Begin()
			err := e.Blind(ctx, id, value)
			if err == nil {
				ok.Add(1)
			}
			return err
		})
		if err != nil {
			return err
		}
		total += ok.Load()
		got, err := e.CountBlind(ctx)
		if err != nil {
			return err
		}
		if got != total {
			return fmt.Errorf("blind-write: %d rows, and %d writes succeeded", got, total)
		}
	}
	return nil
}

// distributions are the ways that the mixed cases draw keys.
var distributions = []harness.Dist{harness.Uniform, harness.Zipf}

// writeShare is the part of the operations of the mixed cases that write.
const writeShare = 0.10

func (v *variant) runMixed(ctx context.Context, rc *harness.RunContext, e env) error {
	n := int64(rc.Params.Scale)
	spec := rc.Params.Dataset()
	err := rc.Phase(ctx, harness.Case{Name: "load", Concurrency: 1}, func(ctx context.Context) (int64, error) {
		return n, e.LoadOrders(ctx, spec, n)
	})
	if err != nil {
		return err
	}
	if err := e.Settle(ctx); err != nil {
		return err
	}

	for _, d := range distributions {
		keys := harness.NewKeys(d, n)
		for _, c := range levels {
			before, err := e.SumQty(ctx)
			if err != nil {
				return err
			}
			var writes atomic.Int64
			cs := harness.Case{Name: "mixed-" + string(d), Concurrency: c,
				Params: v.caseParams(map[string]any{"distribution": string(d), "write_share": writeShare})}
			err = rc.Measure(ctx, cs, func(ctx context.Context, w *harness.Worker) error {
				id := 1 + keys.Next(w.Rand)
				if w.Rand.Float64() < writeShare {
					w.Begin()
					err := e.BumpOrder(ctx, id)
					if err == nil {
						writes.Add(1)
					}
					return err
				}
				w.Begin()
				return e.ReadOrder(ctx, id)
			})
			if err != nil {
				return err
			}
			after, err := e.SumQty(ctx)
			if err != nil {
				return err
			}
			if after-before != writes.Load() {
				return fmt.Errorf("%s: the quantities grew by %d, and %d writes succeeded", cs.Name, after-before, writes.Load())
			}
		}
	}
	return nil
}

// longCounters is the number of counters of the long read cases.
func longCounters(scale int) int64 {
	return int64(min(max(scale, 100), 10_000))
}

func (v *variant) runLong(ctx context.Context, rc *harness.RunContext, e env) error {
	n := longCounters(rc.Params.Scale)
	keys := harness.NewKeys(harness.Uniform, n)

	for _, c := range levels {
		// Writers alone, then the same with the long read transaction beside them.
		for _, withReader := range []bool{false, true} {
			if err := e.ResetCounters(ctx, n); err != nil {
				return err
			}
			if err := e.Settle(ctx); err != nil {
				return err
			}
			before := e.Facts(ctx)
			sizeBefore, _ := e.Sizes(ctx)

			stats := newReaderStats()
			stop := make(chan struct{})
			done := make(chan error, 1)
			readStart := time.Now()
			if withReader {
				go func() { done <- e.LongRead(ctx, stop, stats) }()
				// The snapshot is taken before the writers start.
				if err := stats.waitFirstScan(ctx, done); err != nil {
					close(stop)
					return fmt.Errorf("the long read transaction: %w", err)
				}
			}

			var ok atomic.Int64
			name := "writers-alone"
			if withReader {
				name = "writers-with-reader"
			}
			cs := harness.Case{Name: name, Concurrency: c, Params: v.caseParams(map[string]any{"counters": n})}
			err := rc.Measure(ctx, cs, func(ctx context.Context, w *harness.Worker) error {
				key := keys.Next(w.Rand)
				w.Begin()
				err := e.Inc(ctx, key)
				if err == nil {
					ok.Add(1)
				}
				return err
			})
			readEnd := time.Now()
			var readErr error
			if withReader {
				close(stop)
				readErr = <-done
			}
			if err != nil {
				return err
			}
			if readErr != nil {
				return fmt.Errorf("the long read transaction: %w", readErr)
			}
			if err := checkSum(ctx, e, name, ok.Load()); err != nil {
				return err
			}
			if !withReader {
				continue
			}
			if stats.Unstable > 0 {
				return fmt.Errorf("the long read transaction saw %d of %d scans differ from its first", stats.Unstable, stats.Scans.Load())
			}

			after := e.Facts(ctx)
			sizeAfter, _ := e.Sizes(ctx)
			extra := map[string]float64{
				"scans":                    float64(stats.Scans.Load()),
				"unstable_scans":           float64(stats.Unstable),
				"snapshot_sum":             float64(stats.Sum),
				"counters":                 float64(stats.Counters),
				"writes_during_snapshot":   float64(ok.Load()),
				"scan_first_ms":            stats.First.Seconds() * 1000,
				"scan_last_ms":             stats.Last.Seconds() * 1000,
				"disk_growth_bytes":        float64(sizeAfter.Data - sizeBefore.Data),
				"snapshot_seconds":         readEnd.Sub(readStart).Seconds(),
				"scans_per_sec":            float64(stats.Scans.Load()) / readEnd.Sub(readStart).Seconds(),
				"writes_during_per_second": float64(ok.Load()) / readEnd.Sub(readStart).Seconds(),
			}
			if stats.Hist.Count() > 0 {
				l := stats.Hist.Summary()
				extra["scan_p50_ms"], extra["scan_p99_ms"], extra["scan_max_ms"] = l.P50Us/1000, l.P99Us/1000, l.MaxUs/1000
			}
			for k, a := range after {
				extra[k+"_after"] = a
				extra[k+"_before"] = before[k]
			}
			rcase := harness.Case{Name: "reader", Concurrency: c, Params: v.caseParams(map[string]any{"counters": n, "writers": c})}
			err = rc.Report(ctx, rcase, readStart, readEnd, metrics.Metrics{
				ElapsedSec: readEnd.Sub(readStart).Seconds(), Extra: extra,
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// workers runs fn on n goroutines, ops times each, and returns the number of calls that
// succeeded.
func workers(n, ops int, fn func(worker int, r *harness.Rand) error) int64 {
	var wg sync.WaitGroup
	var ok atomic.Int64
	for w := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := harness.NewRand(1, uint64(w)+1)
			for range ops {
				if fn(w, r) == nil {
					ok.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	return ok.Load()
}

// Verify runs every scenario on a small number of operations at 4 workers, and says whether
// what the database holds afterwards is what the operations that succeeded make: the counters
// add up to the increments, the rows to the writes, the quantities to the writes. Every system
// must say the same, so the lines do not hold the numbers that depend on the conflicts.
func (workload) Verify(ctx context.Context, sys harness.System, p harness.Params) ([]harness.Output, error) {
	var outs []harness.Output
	for _, hv := range (workload{}).Variants(sys) {
		v := hv.(*variant)
		lines, err := v.verify(ctx, sys, p)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", v.name, err)
		}
		// The variants of a scenario answer the same, and those of the others another thing.
		outs = append(outs, harness.Output{Variant: v.name, Group: string(v.scenario), Lines: lines})
	}
	return outs, nil
}

func yes(ok bool) string {
	if ok {
		return "yes"
	}
	return "NO"
}

func (v *variant) verify(ctx context.Context, sys harness.System, p harness.Params) ([]string, error) {
	e, err := v.open(ctx, sys)
	if err != nil {
		return nil, err
	}
	defer e.Close(context.WithoutCancel(ctx))

	const nWorkers, nOps = 4, 150
	var lines []string
	add := func(format string, args ...any) { lines = append(lines, fmt.Sprintf(format, args...)) }

	switch v.scenario {
	case scenarioRMW:
		for _, n := range []int64{1, 10} {
			if err := e.ResetCounters(ctx, n); err != nil {
				return nil, err
			}
			ok := workers(nWorkers, nOps, func(_ int, r *harness.Rand) error { return e.Inc(ctx, r.Int64N(n)) })
			sum, err := e.SumCounters(ctx)
			if err != nil {
				return nil, err
			}
			add("rmw keys=%d: the counters add up to the increments that succeeded: %s", n, yes(sum == ok))
			add("rmw keys=%d: increments succeeded: %s", n, yes(ok > 0))
		}

	case scenarioBlind:
		if err := e.ResetBlind(ctx); err != nil {
			return nil, err
		}
		spec := p.Dataset()
		var next atomic.Int64
		ok := workers(nWorkers, nOps, func(int, *harness.Rand) error {
			id := next.Add(1)
			return e.Blind(ctx, id, encodeOrder(spec.Gen(id, 0)))
		})
		rows, err := e.CountBlind(ctx)
		if err != nil {
			return nil, err
		}
		add("blind: the rows are the writes: %s", yes(rows == ok && ok == nWorkers*nOps))

	case scenarioMixed:
		n := int64(p.Scale)
		if err := e.LoadOrders(ctx, p.Dataset(), n); err != nil {
			return nil, err
		}
		for _, d := range distributions {
			keys := harness.NewKeys(d, n)
			before, err := e.SumQty(ctx)
			if err != nil {
				return nil, err
			}
			var writes atomic.Int64
			var readErrs atomic.Int64
			workers(nWorkers, nOps, func(_ int, r *harness.Rand) error {
				id := 1 + keys.Next(r)
				if r.Float64() < 0.3 {
					err := e.BumpOrder(ctx, id)
					if err == nil {
						writes.Add(1)
					}
					return err
				}
				if err := e.ReadOrder(ctx, id); err != nil {
					readErrs.Add(1)
				}
				return nil
			})
			after, err := e.SumQty(ctx)
			if err != nil {
				return nil, err
			}
			add("mixed %s: the quantities grew by the writes that succeeded: %s", d, yes(after-before == writes.Load()))
			add("mixed %s: every read found its record: %s", d, yes(readErrs.Load() == 0))
		}

	case scenarioLong:
		const n = 100
		if err := e.ResetCounters(ctx, n); err != nil {
			return nil, err
		}
		stats := newReaderStats()
		stop := make(chan struct{})
		done := make(chan error, 1)
		go func() { done <- e.LongRead(ctx, stop, stats) }()
		if err := stats.waitFirstScan(ctx, done); err != nil {
			close(stop)
			return nil, err
		}
		ok := workers(nWorkers, nOps, func(_ int, r *harness.Rand) error { return e.Inc(ctx, r.Int64N(n)) })
		// Let the reader scan twice more after the writes.
		for scansThen := stats.Scans.Load(); stats.Scans.Load() < scansThen+2 && len(done) == 0; {
			time.Sleep(time.Millisecond)
		}
		close(stop)
		if err := <-done; err != nil {
			return nil, err
		}
		sum, err := e.SumCounters(ctx)
		if err != nil {
			return nil, err
		}
		add("long-read: the snapshot did not change under the writers: %s", yes(stats.Unstable == 0 && stats.Sum == 0))
		add("long-read: the counters add up to the increments that succeeded: %s", yes(sum == ok))
	}
	return lines, nil
}
