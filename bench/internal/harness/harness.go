// Package harness is the core of the benchmark suite: the interfaces that systems and workloads
// implement, the runner that drives operations at a given concurrency and measures them, and
// the registry of workloads. A new workload is a package that implements Workload and registers
// itself; the harness knows nothing of what a workload does.
package harness

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/metrics"
)

// Durability says what a commit waits for.
type Durability string

const (
	// Durable waits for the data to reach the disk before a commit returns: SyncWrites on
	// Badger, fsync and synchronous_commit on PostgreSQL.
	Durable Durability = "durable"
	// Relaxed does not wait: SyncWrites off on Badger, synchronous_commit off on PostgreSQL
	// (fsync stays on there, so that the database is not corrupted by a crash).
	Relaxed Durability = "relaxed"
)

// Valid reports whether the durability is one that is known.
func (d Durability) Valid() bool { return d == Durable || d == Relaxed }

// Params are the parameters of a run.
type Params struct {
	Scale       int
	Values      dataset.ValueSize
	Durability  Durability
	Concurrency int
	Duration    time.Duration
	Warmup      time.Duration
	Seed        uint64
}

// Dataset returns the specification of the records of the run.
func (p Params) Dataset() dataset.Spec {
	return dataset.Spec{Seed: p.Seed, Scale: p.Scale, Values: p.Values}
}

func (p Params) metrics(c Case, conc int) metrics.Params {
	mp := metrics.Params{
		Scale:       p.Scale,
		Values:      string(p.Values),
		Durability:  string(p.Durability),
		Concurrency: conc,
		Seed:        p.Seed,
		Case:        c.Params,
	}
	if c.Timed {
		mp.DurationSec = p.Duration.Seconds()
		mp.WarmupSec = p.Warmup.Seconds()
	}
	return mp
}

// System is a database that the workloads run on. A workload finds what it needs of a system by
// its type: the harness only knows what is common to all of them.
type System interface {
	// Name is the name of the system in the results: kvutil or postgres.
	Name() string
	// Version says what was measured: the commit of kvutil, the version of PostgreSQL.
	Version() string
	// Settings are the settings of the system that matter for the numbers.
	Settings() map[string]string
	Close() error
}

// RetryCounter is implemented by the systems that run transactions again when they conflict.
type RetryCounter interface {
	// Retries is the number of times, since the system was opened, that a transaction was run
	// again.
	Retries() int64
}

// Sizes is the size of the database on disk.
type Sizes struct {
	Data int64
	WAL  int64
}

// Workload is a set of measurements. It has a variant for each way that a system does what the
// workload is about, like the codec of the values, and the same variants of a workload are
// verified to give the same results.
type Workload interface {
	// Name identifies the workload in the command line and in the results.
	Name() string
	// Group names the kind of the workload, so that a group can be run at once.
	Group() string
	// Description says what the workload measures.
	Description() string
	// Variants returns the variants that run on the system; none if it does not run on it.
	Variants(sys System) []Variant
	// Verify runs the queries of the workload on the system, on the dataset of the parameters,
	// and returns what each variant returned in a form that can be compared with the others.
	Verify(ctx context.Context, sys System, p Params) ([]Output, error)
}

// Variant is a workload on a system, in one of its variants.
type Variant interface {
	// Name identifies the variant in the results.
	Name() string
	// Run prepares what it needs, measures and emits results through the context, and cleans up.
	Run(ctx context.Context, rc *RunContext) error
}

// Output is what a variant returned, line by line, for the verification.
type Output struct {
	Variant string
	// Group says which outputs are to be the same: only the ones of a group are compared. The
	// variants of most workloads are in the group "", but a workload whose variants answer
	// different questions (an index of each kind) names them.
	Group string
	Lines []string
}

// Case describes a measurement.
type Case struct {
	// Name identifies the case in the results.
	Name string
	// Concurrency is the number of workers, or the run's if it is 0.
	Concurrency int
	// Params are the parameters that only the case has, like a batch size.
	Params map[string]any
	// Unit names what Worker.Add counts, like rows.
	Unit string
	// Ops makes a case of operations of a case of time: it runs that many operations in all, the
	// first WarmupOps of them as the warm-up, and measures the rest for the duration of the run
	// at most. It is for operations that use up what they work on, like deleting a pool of
	// records, which would be used up in a warm-up of any length. The resources of such a case
	// are those of the case as a whole, the warm-up included.
	Ops, WarmupOps int64
	// Timed marks a case that runs for the duration of the run, with a warm-up. It is set by
	// Measure.
	Timed bool
	// Init sets up the state of a worker before it starts.
	Init func(w *Worker)
	// Info is text that goes into the result as it is, like the plan that a query ran with.
	Info map[string]string
}

// RunContext is what a variant runs with.
type RunContext struct {
	Params   Params
	Sys      System
	Workload string
	Variant  string
	Sink     *metrics.Sink
	Host     metrics.Host
	Log      io.Writer
	// SizeFn tells the size of the database that the variant works on; the variant sets it.
	SizeFn func(ctx context.Context) (Sizes, error)
}

// Logf writes a line to the log of the run.
func (rc *RunContext) Logf(format string, args ...any) {
	if rc.Log != nil {
		fmt.Fprintf(rc.Log, format+"\n", args...)
	}
}

func (rc *RunContext) retries() int64 {
	if r, ok := rc.Sys.(RetryCounter); ok {
		return r.Retries()
	}
	return 0
}

// emit writes a result of a case.
func (rc *RunContext) emit(ctx context.Context, c Case, conc int, start, end time.Time, m metrics.Metrics) error {
	if len(c.Info) > 0 {
		m.Info = c.Info
	}
	if rc.SizeFn != nil {
		if sz, err := rc.SizeFn(ctx); err == nil {
			m.DiskBytes, m.WALBytes = sz.Data, sz.WAL
		} else {
			rc.Logf("warning: size of the database: %v", err)
		}
	}
	r := &metrics.Result{
		Start:         start.UTC(),
		End:           end.UTC(),
		System:        rc.Sys.Name(),
		SystemVersion: rc.Sys.Version(),
		Settings:      rc.Sys.Settings(),
		Workload:      rc.Workload,
		Variant:       rc.Variant,
		Case:          c.Name,
		Params:        rc.Params.metrics(c, conc),
		Host:          rc.Host,
		Metrics:       m,
	}
	return rc.Sink.Write(r)
}

// Report emits a result for a value that the variant measured itself, like the time that it took
// to build an index. It is for what is neither a timed run nor a phase of Phase.
func (rc *RunContext) Report(ctx context.Context, c Case, start, end time.Time, m metrics.Metrics) error {
	conc := c.Concurrency
	if conc <= 0 {
		conc = rc.Params.Concurrency
	}
	return rc.emit(ctx, c, conc, start, end, m)
}

// Phase runs fn once and emits its duration, the resources that it used, and the records per
// second for the number of records that it returns. It is for loads and builds.
func (rc *RunContext) Phase(ctx context.Context, c Case, fn func(ctx context.Context) (records int64, err error)) error {
	conc := c.Concurrency
	if conc <= 0 {
		conc = rc.Params.Concurrency
	}

	retries0 := rc.retries()
	start := time.Now()
	probe := metrics.StartProbe()
	records, err := fn(ctx)
	res := probe.Stop()
	end := time.Now()
	if err != nil {
		return fmt.Errorf("%s: %w", c.Name, err)
	}

	m := metrics.Metrics{
		Records:    records,
		ElapsedSec: res.WallSec,
		Retries:    rc.retries() - retries0,
		Resources:  res,
	}
	// From the time as it was measured: the wall time of the result is rounded to milliseconds,
	// which is nothing for a phase that is faster than that.
	if wall := end.Sub(start).Seconds(); wall > 0 {
		m.RecordsPerSec = float64(records) / wall
	}
	return rc.emit(ctx, c, conc, start, end, m)
}

// ErrDone is returned by an operation that has nothing left to do, like deleting from a pool of
// records that is empty. It ends the worker and the measurement without an error.
var ErrDone = errors.New("harness: nothing left to do")

// OpFunc is an operation. Its duration is measured from when it is called, or from Worker.Begin
// if that is called, to when it returns, so that what it needs to prepare is not measured.
// It is called from several workers at once.
type OpFunc func(ctx context.Context, w *Worker) error

// Worker is the state of one of the goroutines that run the operations of a case.
type Worker struct {
	// ID is the number of the worker, from 0.
	ID int
	// Rand is the random numbers of the worker, from a generator seeded by the run's seed and
	// the ID, so that a run can be repeated.
	Rand *Rand
	// State is for whatever the operations keep per worker; see Case.Init.
	State any

	t0        time.Time
	measuring bool
	units     int64
}

// Begin starts the clock of the operation now, which is after it has prepared what it needs.
func (w *Worker) Begin() {
	w.t0 = time.Now()
}

// Add counts units of work (like the rows of a scan) that the operation did. They are counted
// when the operation is in the measured interval.
func (w *Worker) Add(n int) {
	if w.measuring {
		w.units += int64(n)
	}
}

// Measure runs the operation on c.Concurrency workers (the run's, if it is 0) for the warm-up and
// then for the duration of the run, and emits the throughput, the latencies of the operations
// that started after the warm-up, the errors and retries, and the resources used in that time.
// An error of an operation is counted, not returned. Measure returns an error only if every
// operation failed (after emitting what it found), or if the run was cancelled.
func (rc *RunContext) Measure(ctx context.Context, c Case, op OpFunc) error {
	c.Timed = true
	p := rc.Params
	conc := c.Concurrency
	if conc <= 0 {
		conc = p.Concurrency
	}
	if conc <= 0 {
		conc = 1
	}

	type tally struct {
		hist    *metrics.Hist
		ops     int64
		errs    int64
		sample  []string
		lastEnd time.Time
	}
	workers := make([]*Worker, conc)
	tallies := make([]*tally, conc)
	for i := range workers {
		workers[i] = &Worker{ID: i, Rand: NewRand(p.Seed, uint64(i)+1)}
		tallies[i] = &tally{hist: metrics.NewHist()}
		if c.Init != nil {
			c.Init(workers[i])
		}
	}

	// The measured interval starts at measureNs, in Unix nanoseconds: at the end of the warm-up
	// for a case of time, and when the first operation after the warm-up starts for a case
	// of operations. It lasts the duration of the run, or until the operations are used up.
	start := time.Now()
	opsMode := c.Ops > 0
	var measureNs, tickets atomic.Int64
	if !opsMode {
		measureNs.Store(start.Add(p.Warmup).UnixNano())
	}
	duration := p.Duration.Nanoseconds()

	var wg sync.WaitGroup
	for i := range workers {
		w, t := workers[i], tallies[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				now := time.Now()
				if opsMode {
					n := tickets.Add(1) - 1
					if n >= c.Ops {
						return
					}
					if w.measuring = n >= c.WarmupOps; w.measuring {
						measureNs.CompareAndSwap(0, now.UnixNano())
					}
				} else {
					w.measuring = now.UnixNano() >= measureNs.Load()
				}
				if w.measuring && now.UnixNano()-measureNs.Load() >= duration {
					return
				}
				w.t0 = now
				err := op(ctx, w)
				finished := time.Now()
				if errors.Is(err, ErrDone) {
					return
				}
				if !w.measuring {
					continue
				}
				t.lastEnd = finished
				if err != nil {
					t.errs++
					if len(t.sample) < 3 && ctx.Err() == nil {
						t.sample = append(t.sample, err.Error())
					}
					continue
				}
				t.ops++
				t.hist.Record(finished.Sub(w.t0))
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// The resources and the retries are of the measured interval only, for a case of time. A case
	// of operations does not know when that starts until it has, so they are of the whole case.
	if !opsMode {
		select {
		case <-time.After(time.Until(time.Unix(0, measureNs.Load()))):
		case <-done:
		case <-ctx.Done():
		}
	}
	retries0 := rc.retries()
	probe := metrics.StartProbe()
	<-done
	res := probe.Stop()
	retries1 := rc.retries()
	finish := time.Now()
	if err := ctx.Err(); err != nil {
		return err
	}

	hist := metrics.NewHist()
	m := metrics.Metrics{Resources: res, Retries: retries1 - retries0}
	var lastEnd time.Time
	var samples []string
	for i, t := range tallies {
		hist.Merge(t.hist)
		m.Ops += t.ops
		m.Errors += t.errs
		m.Units += workers[i].units
		samples = append(samples, t.sample...)
		if t.lastEnd.After(lastEnd) {
			lastEnd = t.lastEnd
		}
	}
	if len(samples) > 3 {
		samples = samples[:3]
	}
	m.ErrorSample = samples
	if ms := time.Unix(0, measureNs.Load()); measureNs.Load() != 0 && lastEnd.After(ms) {
		m.ElapsedSec = lastEnd.Sub(ms).Seconds()
	}
	if m.ElapsedSec > 0 {
		m.OpsPerSec = float64(m.Ops) / m.ElapsedSec
		if m.Units > 0 {
			m.UnitName = c.Unit
			m.UnitsPerSec = float64(m.Units) / m.ElapsedSec
		}
	}
	if m.Ops > 0 {
		l := hist.Summary()
		m.Latency = &l
	}

	if err := rc.emit(ctx, c, conc, start, finish, m); err != nil {
		return err
	}
	if m.Ops == 0 && m.Errors > 0 {
		return fmt.Errorf("%s: every operation failed (%d), the first with: %s", c.Name, m.Errors, strings.Join(samples, "; "))
	}
	return nil
}

var (
	registryMu sync.Mutex
	registry   []Workload
)

// Register adds a workload to the registry. It panics on a name that is taken.
func Register(w Workload) {
	registryMu.Lock()
	defer registryMu.Unlock()
	for _, x := range registry {
		if x.Name() == w.Name() {
			panic("harness: the workload " + w.Name() + " is registered twice")
		}
	}
	registry = append(registry, w)
}

// All returns the registered workloads in the order that they were registered.
func All() []Workload {
	registryMu.Lock()
	defer registryMu.Unlock()
	return append([]Workload(nil), registry...)
}

// Select returns the workloads that a command line names: names and groups separated by commas,
// or all.
func Select(spec string) ([]Workload, error) {
	all := All()
	var selected []Workload
	seen := map[string]bool{}
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		found := false
		for _, w := range all {
			if part == "all" || part == w.Name() || part == w.Group() {
				found = true
				if !seen[w.Name()] {
					seen[w.Name()] = true
					selected = append(selected, w)
				}
			}
		}
		if !found {
			names := make([]string, 0, len(all))
			for _, w := range all {
				names = append(names, w.Name())
			}
			sort.Strings(names)
			return nil, fmt.Errorf("no workload or group %q; there are %s", part, strings.Join(names, ", "))
		}
	}
	if len(selected) == 0 {
		return nil, errors.New("no workload selected")
	}
	return selected, nil
}

// Execute runs the variants of a workload that run on a system. A variant that fails does not
// stop the others; the errors are joined.
func Execute(ctx context.Context, w Workload, sys System, p Params, sink *metrics.Sink, host metrics.Host, log io.Writer) error {
	variants := w.Variants(sys)
	if len(variants) == 0 {
		return fmt.Errorf("the workload %s does not run on %s", w.Name(), sys.Name())
	}
	var errs []error
	for _, v := range variants {
		if err := ctx.Err(); err != nil {
			return err
		}
		rc := &RunContext{
			Params: p, Sys: sys, Workload: w.Name(), Variant: v.Name(),
			Sink: sink, Host: host, Log: log,
		}
		if log != nil {
			fmt.Fprintf(log, "== %s %s/%s\n", sys.Name(), w.Name(), v.Name())
		}
		if err := v.Run(ctx, rc); err != nil {
			errs = append(errs, fmt.Errorf("%s %s/%s: %w", sys.Name(), w.Name(), v.Name(), err))
		}
	}
	return errors.Join(errs...)
}

// Labeled is an output with the system that it came from.
type Labeled struct {
	System string
	Output
}

// Label is the name of the output in a message.
func (l Labeled) Label() string { return l.System + "/" + l.Variant }

// Compare compares outputs line by line against the first one, and returns a description of
// each difference (at most limit per output; 0 is 10).
func Compare(outs []Labeled, limit int) []string {
	if limit <= 0 {
		limit = 10
	}
	var diffs []string
	refs := map[string]Labeled{}
	for _, o := range outs {
		ref, ok := refs[o.Group]
		if !ok {
			refs[o.Group] = o
			continue
		}
		n := 0
		add := func(format string, args ...any) {
			if n == limit {
				return
			}
			n++
			diffs = append(diffs, fmt.Sprintf("%s vs %s: ", ref.Label(), o.Label())+fmt.Sprintf(format, args...))
		}
		if len(ref.Lines) != len(o.Lines) {
			add("%d lines against %d", len(ref.Lines), len(o.Lines))
		}
		for i := 0; i < len(ref.Lines) && i < len(o.Lines); i++ {
			if ref.Lines[i] != o.Lines[i] {
				add("line %d: %q against %q", i+1, ref.Lines[i], o.Lines[i])
			}
		}
	}
	return diffs
}

// Verify runs the verification of a workload on the systems and compares everything that comes
// out of it: every variant on every system must return the same.
func Verify(ctx context.Context, w Workload, systems []System, p Params) (outs []Labeled, diffs []string, err error) {
	for _, sys := range systems {
		if len(w.Variants(sys)) == 0 {
			continue
		}
		got, err := w.Verify(ctx, sys, p)
		if err != nil {
			return nil, nil, fmt.Errorf("%s on %s: %w", w.Name(), sys.Name(), err)
		}
		for _, o := range got {
			outs = append(outs, Labeled{System: sys.Name(), Output: o})
		}
	}
	return outs, Compare(outs, 10), nil
}
