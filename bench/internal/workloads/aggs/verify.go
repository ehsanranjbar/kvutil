package aggs

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
)

// The verification of the workloads of aggregations. Every variant of every system loads the same
// events, makes the same changes, and answers the same reads; the answers are compared, line by line.
//
// The end of the span of the events is fixed (a Monday that is the first of a month, so that the
// last day, week and month of the events are complete and the new events after them begin new
// ones), and the stored aggregates are refreshed or collected where a stage says so. The outputs
// are in groups, each of which must be the same from all the variants in it:
//
//	caught-up    every variant, after the stored aggregates have everything that was written:
//	             S1 the events loaded, S4 after the changes, S5 the same read through a session with
//	             another time zone (the answers do not depend on it)
//	pending      the variants that are exact before they catch up (kvutil exact, PostgreSQL both ways,
//	             TimescaleDB real-time): S0 the events loaded and nothing collected or refreshed,
//	             S2 after new events
//	pending-old  the same, without TimescaleDB real-time: S3 after updates, moves and deletes of old
//	             events. A real-time aggregate is exact for what is newer than its watermark and
//	             not for a change to what it has materialized, until the next refresh: its
//	             answer here is the old one (see docs/workloads.md)
var verifyEnd = time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

// verifySetup is the setup of a verification of the scale in the parameters.
func verifySetup(p harness.Params) setup {
	s := newSetup(p)
	s.spec.End, s.spec.SpanDays = verifyEnd, dataset.DefaultSpanDays
	s.interval = time.Second
	return s
}

// exactBeforeCatchUp says whether the variant is exact for new events before it catches up.
func exactBeforeCatchUp(variant string) bool { return exactReads(variant) }

// boundaryTimes are instants just before and at the starts of a month, a week, a day and a year,
// and just before the end of the span: the events at them are in the span of one bucket or the
// other, depending on how a system draws the boundaries.
func boundaryTimes(spec dataset.EventSpec) []time.Time {
	start := spec.Start()
	m := alignUp(month, start.AddDate(0, 0, 60))
	w := alignUp(week, start.AddDate(0, 0, 120))
	d := start.AddDate(0, 0, 200)
	y := time.Date(start.Year()+1, 1, 1, 0, 0, 0, 0, time.UTC)
	var ts []time.Time
	for _, b := range []time.Time{m, w, d, y} {
		ts = append(ts, b.Add(-time.Second), b)
	}
	ts = append(ts, spec.End.Add(-time.Second))
	var in []time.Time
	for _, t := range ts {
		if !t.Before(start) && t.Before(spec.End) && !slices.ContainsFunc(in, t.Equal) {
			in = append(in, t)
		}
	}
	return in
}

// verifyQueries are the reads of the verification: for four customers (the most popular, the
// second, a middling one, the last) the series by day, week and month over the whole span and the
// days after it, and the total; a window of 30 days; and the top 10 of every month.
func (s setup) verifyQueries() []query {
	spec := s.spec
	to := func(u unit) time.Time { return alignUp(u, spec.End.AddDate(0, 0, 3)) }
	nc := spec.NumCustomers()
	var ranks []int
	for _, r := range []int{0, 1, 7, nc - 1} {
		if r < nc && !slices.Contains(ranks, r) {
			ranks = append(ranks, r)
		}
	}
	var qs []query
	for _, rank := range ranks {
		c := dataset.CustomerName(rank)
		for _, u := range []unit{day, week, month} {
			qs = append(qs, query{Kind: qSeries, Unit: u, Customer: c, From: startOf(u, spec.Start()), To: to(u)})
		}
		qs = append(qs, query{Kind: qTotal, Customer: c})
	}
	qs = append(qs, query{Kind: qSeries, Unit: day, Customer: dataset.CustomerName(0),
		From: spec.End.AddDate(0, 0, -35), To: spec.End.AddDate(0, 0, -5)})
	for t := startOf(month, spec.Start()); t.Before(to(month)); t = nextOf(month, t) {
		qs = append(qs, query{Kind: qTop, Unit: month, From: t, To: nextOf(month, t)})
	}
	return qs
}

// readAll answers the queries and returns a line for each, with the stage in it.
func readAll(ctx context.Context, e env, stage string, qs []query) ([]string, error) {
	var out []string
	for _, q := range qs {
		rows, err := e.Read(ctx, q)
		if err != nil {
			return nil, fmt.Errorf("%s: %s: %w", stage, q.label("read"), err)
		}
		out = append(out, fmt.Sprintf("%s %s: %s", stage, q.label("read"), summarize(q, rows)))
	}
	return out, nil
}

// checkTruth fails if the variant does not read the exact answer to the queries.
func checkTruth(ctx context.Context, e env, stage string, qs []query) error {
	for _, q := range qs {
		got, err := e.Read(ctx, q)
		if err != nil {
			return err
		}
		want, err := e.Truth(ctx, q)
		if err != nil {
			return err
		}
		if !equalRows(got, want) {
			return fmt.Errorf("%s: %s: the read is not the exact answer: %s against %s", stage, q.label("read"), summarize(q, got), summarize(q, want))
		}
	}
	return nil
}

// verifyReads is the verification of the reads and of the changes of old events (see above).
func verifyReads(ctx context.Context, sys harness.System, p harness.Params, variant, schema string) ([]harness.Output, error) {
	s := verifySetup(p)
	spec := s.spec
	e, err := openEnv(ctx, sys, variant, s, schema, false)
	if err != nil {
		return nil, err
	}
	defer e.Close(context.WithoutCancel(ctx))
	n := int64(p.Scale)
	qs := s.verifyQueries()

	if err := e.Load(ctx, n); err != nil {
		return nil, err
	}
	id := n
	// Events at the instants of the boundaries of the buckets, for the most popular customer.
	for i, t := range boundaryTimes(spec) {
		id++
		ev := spec.Gen(id, 0)
		ev.Customer, ev.At, ev.Amount = dataset.CustomerName(0), t, int64(1000+7*i)
		if err := e.Insert(ctx, ev); err != nil {
			return nil, err
		}
	}

	var pending, pendingOld, caught []string
	add := func(dst *[]string, stage string) error {
		ls, err := readAll(ctx, e, stage, qs)
		*dst = append(*dst, ls...)
		return err
	}
	exact := exactBeforeCatchUp(variant)

	if exact {
		if err := add(&pending, "S0 loaded"); err != nil {
			return nil, err
		}
	}
	if err := e.CatchUp(ctx); err != nil {
		return nil, err
	}
	if err := add(&caught, "S1 caught-up"); err != nil {
		return nil, err
	}

	// New events, after the span: the first instant of the next day, and the last one before it.
	var fresh []*dataset.Event
	for k := 0; k < 40; k++ {
		id++
		ev := &dataset.Event{}
		at := spec.End.Add(time.Duration(k) * 83 * time.Minute)
		switch k {
		case 0:
			at = spec.End
		case 1:
			at = spec.End.AddDate(0, 0, 1).Add(-time.Second)
		case 2:
			at = spec.End.AddDate(0, 0, 1)
		}
		spec.GenStream(ev, id, at)
		fresh = append(fresh, ev)
	}
	for _, ev := range fresh[:20] {
		if err := e.Insert(ctx, ev); err != nil {
			return nil, err
		}
	}
	if err := e.InsertBatch(ctx, fresh[20:]); err != nil {
		return nil, err
	}
	if exact {
		if err := add(&pending, "S2 new"); err != nil {
			return nil, err
		}
	}

	// Changes of old events: a new amount, a move to another customer and time, a deletion.
	for i := int64(1); i <= n; i++ {
		old := spec.Gen(i, 0)
		switch {
		case i%11 == 0:
			if err := e.Update(ctx, old, spec.Gen(i, 1)); err != nil {
				return nil, err
			}
		case i%13 == 0:
			moved := &dataset.Event{}
			spec.GenMoved(moved, i, 2)
			if err := e.Update(ctx, old, moved); err != nil {
				return nil, err
			}
		case i%17 == 0:
			if err := e.Delete(ctx, old); err != nil {
				return nil, err
			}
		}
	}
	if exact && variant != tsRealtime {
		if err := add(&pendingOld, "S3 old-changed"); err != nil {
			return nil, err
		}
	}
	if err := e.CatchUp(ctx); err != nil {
		return nil, err
	}
	if err := add(&caught, "S4 final"); err != nil {
		return nil, err
	}

	// The same reads in a session with another time zone: Auckland is 12 or 13 hours from UTC, so
	// any bucket drawn in local time would show.
	if z, ok := e.(zoner); ok {
		restore, err := z.SetZone(ctx, "Pacific/Auckland")
		if err != nil {
			return nil, err
		}
		err = add(&caught, "S5 zone")
		restore()
		if err != nil {
			return nil, err
		}
	} else if err := add(&caught, "S5 zone"); err != nil {
		return nil, err
	}

	outs := []harness.Output{{Variant: variant, Group: "caught-up", Lines: caught}}
	if exact {
		outs = append(outs, harness.Output{Variant: variant + "-pending", Group: "pending", Lines: pending})
		if variant != tsRealtime {
			outs = append(outs, harness.Output{Variant: variant + "-pending-old", Group: "pending-old", Lines: pendingOld})
		}
	}
	return outs, nil
}

// verifyBurst is the verification of the workloads about writers: events of one customer are
// written by several writers at the same time, and the reads must have all of them, pending and
// after the stored aggregates have caught up.
func verifyBurst(ctx context.Context, sys harness.System, p harness.Params, variant, schema string) ([]harness.Output, error) {
	s := verifySetup(p)
	spec := s.spec
	e, err := openEnv(ctx, sys, variant, s, schema, false)
	if err != nil {
		return nil, err
	}
	defer e.Close(context.WithoutCancel(ctx))
	n := int64(p.Scale)
	if err := e.Load(ctx, n); err != nil {
		return nil, err
	}
	if err := e.CatchUp(ctx); err != nil {
		return nil, err
	}

	hot := hotCustomer()
	st := newStream(spec, n+1)
	var wg sync.WaitGroup
	errs := make([]error, 4)
	for w := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ev := &dataset.Event{}
			for i := 0; i < 50 && errs[w] == nil; i++ {
				id := st.next.Add(1) - 1
				// The same events whatever the order that the writers run in.
				spec.GenStream(ev, id, spec.End.Add(time.Duration(id%5000)*time.Second))
				ev.Customer = hot
				errs[w] = e.Insert(ctx, ev)
			}
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}

	var qs []query
	for _, u := range []unit{day, week, month} {
		qs = append(qs, query{Kind: qSeries, Unit: u, Customer: hot, From: startOf(u, spec.End.AddDate(0, 0, -40)), To: alignUp(u, spec.End.AddDate(0, 0, 3))})
	}
	qs = append(qs, query{Kind: qTotal, Customer: hot})

	var outs []harness.Output
	exact := exactBeforeCatchUp(variant)
	if exact {
		ls, err := readAll(ctx, e, "S0 written", qs)
		if err != nil {
			return nil, err
		}
		outs = append(outs, harness.Output{Variant: variant + "-pending", Group: "pending", Lines: ls})
	}
	if err := e.CatchUp(ctx); err != nil {
		return nil, err
	}
	ls, err := readAll(ctx, e, "S1 caught-up", qs)
	if err != nil {
		return nil, err
	}
	if err := checkTruth(ctx, e, "S1 caught-up", qs); err != nil {
		return nil, err
	}
	return append(outs, harness.Output{Variant: variant, Group: "caught-up", Lines: ls}), nil
}

// verifyRebuild is the verification of the building of the aggregates: the events are loaded
// with no aggregate, the aggregates are built, and they read what the other variants read.
func verifyRebuild(ctx context.Context, sys harness.System, p harness.Params, variant, schema string) ([]harness.Output, error) {
	s := verifySetup(p)
	e, err := openEnv(ctx, sys, variant, s, schema, true)
	if err != nil {
		return nil, err
	}
	defer e.Close(context.WithoutCancel(ctx))
	rb, ok := e.(rebuilder)
	if !ok {
		return nil, fmt.Errorf("%T cannot build aggregates", e)
	}
	n := int64(p.Scale)
	if err := rb.LoadPlain(ctx, n); err != nil {
		return nil, err
	}
	if err := rb.Build(ctx, func(_ string, fn func(context.Context) error) error { return fn(ctx) }); err != nil {
		return nil, err
	}
	if err := e.CatchUp(ctx); err != nil {
		return nil, err
	}
	qs := s.verifyQueries()
	ls, err := readAll(ctx, e, "S1 caught-up", qs)
	if err != nil {
		return nil, err
	}
	if err := checkTruth(ctx, e, "S1 caught-up", qs); err != nil {
		return nil, err
	}
	return []harness.Output{{Variant: variant, Group: "caught-up", Lines: ls}}, nil
}
