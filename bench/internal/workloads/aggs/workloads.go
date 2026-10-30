// Package aggs is the workloads of stored aggregations: the sum, count and maximum of time-series
// events for each customer, by day, week and month, kept by kvutil (agg.Sum, agg.Count, agg.Max with
// a collector), by TimescaleDB (a hypertable and continuous aggregates with refresh policies) and,
// as the baselines, by PostgreSQL (computed at read time, and a summary table kept by a trigger).
//
// There are five workloads, all in the group "agg":
//
//	agg-write      inserts, updates and deletes of events, with the aggregates maintained
//	agg-hot-group  writers inserting events of one customer at the same time
//	agg-read       the reads of the series, the totals and the top customers
//	agg-freshness  how long until the stored aggregates have what was written, and what an exact
//	               read costs while it is pending
//	agg-rebuild    building the aggregates over events that are there, and what they take to store
//
// A variant is a way that a system keeps and reads the aggregates (env.go). Every variant has a
// database of its own with the same events, which the workload loads and removes.
package aggs

import (
	"context"
	"fmt"
	"time"

	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/metrics"
)

func init() {
	for _, w := range []*workload{
		{
			name: "agg-write", vs: writeVariants, run: runWrite, verify: verifyReads,
			desc: "insert, update, delete of events with sum/count/max by day, week and month maintained: kvutil with a collector, TimescaleDB with refresh policies, PostgreSQL computed or with a trigger",
		},
		{
			name: "agg-hot-group", vs: writeVariants, run: runHot, verify: verifyBurst,
			desc: "writers inserting events of one customer at the same time: throughput, conflicts, latency",
		},
		{
			name: "agg-read", vs: readVariants, run: runRead, verify: verifyReads,
			desc: "daily, weekly and monthly series of a customer, a total, and the top 10 customers of a month: exact and collected (kvutil), real-time and materialized (TimescaleDB), computed and from a summary (PostgreSQL)",
		},
		{
			name: "agg-freshness", vs: readVariants, run: runFreshness, verify: verifyBurst,
			desc: "how long until the stored aggregates include what was written, and the cost of an exact read while data is pending",
		},
		{
			name: "agg-rebuild", vs: rebuildVariants, run: runRebuild, verify: verifyRebuild,
			desc: "building the aggregates over the events that are there (Migrate and Rebuild, the initial materialization and a refresh, a summary by INSERT SELECT), and their size",
		},
	} {
		harness.Register(w)
	}
}

// workload is one of the five.
type workload struct {
	name, desc string
	vs         variantSet
	run        func(ctx context.Context, rc *harness.RunContext, variant string) error
	verify     func(ctx context.Context, sys harness.System, p harness.Params, variant, schema string) ([]harness.Output, error)
}

func (w *workload) Name() string        { return w.name }
func (w *workload) Group() string       { return "agg" }
func (w *workload) Description() string { return w.desc }

func (w *workload) Variants(sys harness.System) []harness.Variant {
	var vs []harness.Variant
	for _, name := range w.vs.of(sys) {
		vs = append(vs, &variant{name: name, w: w})
	}
	return vs
}

func (w *workload) Verify(ctx context.Context, sys harness.System, p harness.Params) ([]harness.Output, error) {
	var outs []harness.Output
	for _, name := range w.vs.of(sys) {
		got, err := w.verify(ctx, sys, p, name, w.name+"-verify")
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		outs = append(outs, got...)
	}
	return outs, nil
}

type variant struct {
	name string
	w    *workload
}

func (v *variant) Name() string { return v.name }

func (v *variant) Run(ctx context.Context, rc *harness.RunContext) error {
	return v.w.run(ctx, rc, v.name)
}

// session is a variant's database with the events loaded and the aggregates caught up.
type session struct {
	rc *harness.RunContext
	e  env
	s  setup
	n  int64
	// bg is set while the background work (the collector, the policies) runs.
	bg bool
}

// background starts or stops the background work of the variant.
func (ss *session) background(ctx context.Context, on bool) error {
	if err := ss.e.Background(ctx, on); err != nil {
		return err
	}
	ss.bg = on
	return nil
}

// begin opens the database of a variant and loads the events as phases of the run: the load (the
// events with their aggregates noted, invalidated or summarized), and the first catch-up, which
// is the collection of all the notes, or the refresh of all the continuous aggregates.
func begin(ctx context.Context, rc *harness.RunContext, variant, schema string) (*session, error) {
	s := newSetup(rc.Params)
	e, err := openEnv(ctx, rc.Sys, variant, s, schema, false)
	if err != nil {
		return nil, err
	}
	ss := &session{rc: rc, e: e, s: s, n: int64(rc.Params.Scale)}
	rc.SizeFn = e.Sizes
	params := s.caseParams(map[string]any{"events": ss.n})
	if err := rc.Phase(ctx, harness.Case{Name: "load", Concurrency: s.workers, Params: params}, func(ctx context.Context) (int64, error) {
		return ss.n, e.Load(ctx, ss.n)
	}); err != nil {
		ss.close(ctx)
		return nil, err
	}
	if hasCatchUp(variant) {
		if err := rc.Phase(ctx, harness.Case{Name: "catch-up", Concurrency: 1, Params: params}, func(ctx context.Context) (int64, error) {
			return ss.n, e.CatchUp(ctx)
		}); err != nil {
			ss.close(ctx)
			return nil, err
		}
	}
	if err := ss.settle(ctx); err != nil {
		ss.close(ctx)
		return nil, err
	}
	return ss, nil
}

// hasCatchUp says whether a variant keeps stored aggregates that lag behind the events.
func hasCatchUp(variant string) bool {
	switch variant {
	case pgComputed, pgTrigger:
		return false
	}
	return true
}

// exactReads says whether the reads of a variant are exact the moment that a write returns, as
// far as the events that are new go.
func exactReads(variant string) bool {
	switch variant {
	case kvCollector, kvExact, pgComputed, pgTrigger, tsRealtime:
		return true
	}
	return false
}

// settle gets the database ready to be measured: everything collected or refreshed, statistics
// and a checkpoint.
func (ss *session) settle(ctx context.Context) error {
	if err := ss.e.CatchUp(ctx); err != nil {
		return err
	}
	if st, ok := ss.e.(settler); ok {
		return st.settle(ctx)
	}
	return nil
}

func (ss *session) close(ctx context.Context) {
	ss.e.Close(context.WithoutCancel(ctx))
}

// measure runs a timed case, then reports the counters of the background work during it (the
// conflicts of the collector), if the system has any.
func (ss *session) measure(ctx context.Context, c harness.Case, op harness.OpFunc) error {
	before := ss.e.Stats()
	start := time.Now()
	if err := ss.rc.Measure(ctx, c, op); err != nil {
		return err
	}
	after := ss.e.Stats()
	if len(after) == 0 || !ss.bg {
		return nil
	}
	extra := map[string]float64{}
	for k, v := range after {
		extra[k] = v - before[k]
	}
	return ss.rc.Report(ctx, harness.Case{Name: c.Name + "-background", Concurrency: c.Concurrency, Params: c.Params},
		start, time.Now(), metrics.Metrics{Extra: extra})
}
