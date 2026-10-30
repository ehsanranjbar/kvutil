package aggs

import (
	"context"
	"fmt"
	"time"

	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/metrics"
)

// openPlain opens the database of a variant with the events loaded and no aggregate defined.
func openPlain(ctx context.Context, rc *harness.RunContext, variant, schema string) (*session, rebuilder, error) {
	s := newSetup(rc.Params)
	e, err := openEnv(ctx, rc.Sys, variant, s, schema, true)
	if err != nil {
		return nil, nil, err
	}
	rb, ok := e.(rebuilder)
	if !ok {
		e.Close(ctx)
		return nil, nil, fmt.Errorf("%T cannot build aggregates", e)
	}
	ss := &session{rc: rc, e: e, s: s, n: int64(rc.Params.Scale)}
	rc.SizeFn = e.Sizes
	params := s.caseParams(map[string]any{"events": ss.n})
	err = rc.Phase(ctx, harness.Case{Name: "load-events", Concurrency: s.workers, Params: params}, func(ctx context.Context) (int64, error) {
		return ss.n, rb.LoadPlain(ctx, ss.n)
	})
	if err != nil {
		ss.close(ctx)
		return nil, nil, err
	}
	if st, ok := e.(settler); ok {
		if err := st.settle(ctx); err != nil {
			ss.close(ctx)
			return nil, nil, err
		}
	}
	return ss, rb, nil
}

// build builds the aggregates; each step that the variant has is a phase of the run.
func (ss *session) build(ctx context.Context, rb rebuilder) error {
	params := ss.s.caseParams(map[string]any{"events": ss.n})
	return rb.Build(ctx, func(name string, fn func(context.Context) error) error {
		return ss.rc.Phase(ctx, harness.Case{Name: name, Concurrency: 1, Params: params}, func(ctx context.Context) (int64, error) {
			return ss.n, fn(ctx)
		})
	})
}

// runRebuild loads the events with no aggregate, builds the aggregates over them as phases, and
// then reports what they take to store.
//
//	kvutil      migrate (store.Migrate of the store with the three aggregations added, which
//	            builds them), then rebuild-sum, rebuild-count and rebuild-max (agg.Rebuild of
//	            each of them again, alone)
//	TimescaleDB create-with-data (the three continuous aggregates created WITH DATA: the initial
//	            materialization), then they are dropped and created WITH NO DATA, and
//	            refresh-day, refresh-week, refresh-month (refresh_continuous_aggregate over the
//	            whole range, in the order that they depend on one another)
//	PostgreSQL  build-summary (INSERT ... SELECT ... GROUP BY into the summary table)
//
// The last case, `storage`, has the size of the events and of the aggregates.
func runRebuild(ctx context.Context, rc *harness.RunContext, variant string) error {
	ss, rb, err := openPlain(ctx, rc, variant, "agg-rebuild")
	if err != nil {
		return err
	}
	defer ss.close(ctx)
	if err := ss.build(ctx, rb); err != nil {
		return err
	}
	if err := ss.settle(ctx); err != nil {
		return err
	}

	// What was built has the exact answers.
	for _, c := range readCases() {
		q := c.gen(ss.s, harness.NewRand(rc.Params.Seed^0xa66, 1))
		got, err := ss.e.Read(ctx, q)
		if err != nil {
			return fmt.Errorf("%s: %w", c.name, err)
		}
		want, err := ss.e.Truth(ctx, q)
		if err != nil {
			return fmt.Errorf("%s: %w", c.name, err)
		}
		if !equalRows(got, want) {
			return fmt.Errorf("%s: what was built does not read the exact answer for %s", c.name, q.label(c.name))
		}
	}

	st, err := ss.e.Storage(ctx)
	if err != nil {
		return err
	}
	now := time.Now()
	extra := map[string]float64{
		"events_bytes":         float64(st.Events),
		"aggregate_bytes":      float64(st.Aggs),
		"aggregate_pct":        100 * float64(st.Aggs) / float64(max(st.Events, 1)),
		"events_bytes_each":    float64(st.Events) / float64(ss.n),
		"aggregate_bytes_each": float64(st.Aggs) / float64(ss.n),
	}
	info := map[string]string{"measured": "disk (relations with indexes)"}
	if st.Logical {
		info["measured"] = "logical (lengths of the keys and values, before compression)"
	}
	return rc.Report(ctx, harness.Case{
		Name: "storage", Concurrency: 1, Info: info,
		Params: ss.s.caseParams(map[string]any{"events": ss.n, "logical": st.Logical}),
	}, now, now, metrics.Metrics{Extra: extra})
}
