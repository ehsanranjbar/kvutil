package aggs

import (
	"context"
	"fmt"

	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
)

// runRead measures the reads of the aggregates over events that are all there and all caught up, at
// 1 worker and at the concurrency of the run. The reads are drawn at random, with the customers
// that the events have (the popular ones more often). Before a case is measured, the first query
// of it is checked against the exact answer.
func runRead(ctx context.Context, rc *harness.RunContext, variant string) error {
	ss, err := begin(ctx, rc, variant, "agg-read")
	if err != nil {
		return err
	}
	defer ss.close(ctx)

	for _, c := range readCases() {
		r := harness.NewRand(rc.Params.Seed^0xa66, 1)
		sample := c.gen(ss.s, r)
		got, err := ss.e.Read(ctx, sample)
		if err != nil {
			return fmt.Errorf("%s: %w", c.name, err)
		}
		want, err := ss.e.Truth(ctx, sample)
		if err != nil {
			return fmt.Errorf("%s: %w", c.name, err)
		}
		if !equalRows(got, want) {
			return fmt.Errorf("%s: the read of %s differs from the exact answer (%d rows against %d)",
				c.name, sample.label(c.name), len(got), len(want))
		}

		for _, conc := range harness.Levels(rc.Params.Concurrency) {
			err := ss.measure(ctx, harness.Case{
				Name: c.name, Concurrency: conc, Unit: "rows",
				Params: ss.s.caseParams(map[string]any{"events": ss.n}),
			}, func(ctx context.Context, w *harness.Worker) error {
				q := c.gen(ss.s, w.Rand)
				w.Begin()
				rows, err := ss.e.Read(ctx, q)
				w.Add(len(rows))
				return err
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}
