package aggs

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/wl"
)

// stream hands out new events: ids past the ones of the dataset, with the time that they are made
// at, which is now.
type stream struct {
	spec dataset.EventSpec
	next atomic.Int64
}

func newStream(spec dataset.EventSpec, first int64) *stream {
	s := &stream{spec: spec}
	s.next.Store(first)
	return s
}

// fill makes the next event of the stream. A customer that is given replaces the one of the event.
func (s *stream) fill(e *event, customer string) {
	id := s.next.Add(1) - 1
	s.spec.GenStream(e, id, time.Now().UTC().Truncate(time.Second))
	if customer != "" {
		e.Customer = customer
	}
}

func initEvent(w *harness.Worker) { w.State = &event{} }

// deletePool is the number of events that a level of the delete case deletes, a tenth of them as the
// warm-up. Two levels take them from the upper half of the events.
func deletePool(n int64) int64 {
	return min(max(n/8, 500), 50_000)
}

// runWrite measures the writes with the aggregates maintained, each at 1 worker and at the
// concurrency of the run, with the background work (the collector, the refresh policies) running:
// inserts of new events, updates of old ones (a new amount), and deletes of old ones. Between the
// cases the aggregates are brought up to date, so that a case does not start with the debt of the
// one before.
func runWrite(ctx context.Context, rc *harness.RunContext, variant string) error {
	ss, err := begin(ctx, rc, variant, "agg-write")
	if err != nil {
		return err
	}
	defer ss.close(ctx)
	e, spec, n := ss.e, ss.s.spec, ss.n
	if err := ss.background(ctx, true); err != nil {
		return err
	}
	params := ss.s.caseParams(map[string]any{"events": n})

	st := newStream(spec, n+1)
	for _, c := range harness.Levels(rc.Params.Concurrency) {
		err := ss.measure(ctx, harness.Case{Name: "insert", Concurrency: c, Params: params, Init: initEvent},
			func(ctx context.Context, w *harness.Worker) error {
				ev := w.State.(*event)
				st.fill(ev, "")
				w.Begin()
				return e.Insert(ctx, ev)
			})
		if err != nil {
			return err
		}
		if err := ss.settle(ctx); err != nil {
			return err
		}
	}

	for _, c := range harness.Levels(rc.Params.Concurrency) {
		err := ss.measure(ctx, harness.Case{Name: "update", Concurrency: c, Params: params},
			func(ctx context.Context, w *harness.Worker) error {
				id := 1 + w.Rand.Int64N(n)
				old, ev := spec.Gen(id, 0), spec.Gen(id, 1+w.Rand.Uint32N(1<<20))
				w.Begin()
				return e.Update(ctx, old, ev)
			})
		if err != nil {
			return err
		}
		if err := ss.settle(ctx); err != nil {
			return err
		}
	}

	pool := deletePool(n)
	first := n/2 + 1
	for _, c := range harness.Levels(rc.Params.Concurrency) {
		cur := wl.NewCursor(first, first+pool)
		first += pool
		cp := ss.s.caseParams(map[string]any{"events": n, "pool": pool})
		err := ss.measure(ctx, harness.Case{Name: "delete", Concurrency: c, Params: cp, Ops: pool, WarmupOps: pool / 10},
			func(ctx context.Context, w *harness.Worker) error {
				id, ok := cur.Take()
				if !ok {
					return harness.ErrDone
				}
				old := spec.Gen(id, 0)
				w.Begin()
				return e.Delete(ctx, old)
			})
		if err != nil {
			return err
		}
		if err := ss.settle(ctx); err != nil {
			return err
		}
	}
	return nil
}

// hotCustomer is the customer that the writers of agg-hot-group all write the events of: the one of
// rank 0, who has the most events.
func hotCustomer() string { return dataset.CustomerName(0) }

// runHot measures writers that insert events of one customer at the same time, at the same time of
// the day: the events of one group and one span of every aggregation. `insert-hot` is that, and
// `insert-spread` is the same writers with the customers of the dataset, for comparison. The
// results have the errors and the retries (conflicts of transactions of kvutil that the library
// ran again) of the writers, and `-background` ones have those of the collector.
func runHot(ctx context.Context, rc *harness.RunContext, variant string) error {
	ss, err := begin(ctx, rc, variant, "agg-hot-group")
	if err != nil {
		return err
	}
	defer ss.close(ctx)
	e, n := ss.e, ss.n
	if err := ss.background(ctx, true); err != nil {
		return err
	}
	st := newStream(ss.s.spec, n+1)
	for _, c := range harness.Levels(rc.Params.Concurrency) {
		for _, hot := range []bool{true, false} {
			customer, name := "", "insert-spread"
			if hot {
				customer, name = hotCustomer(), "insert-hot"
			}
			params := ss.s.caseParams(map[string]any{"events": n, "hot": hot, "customer": customer})
			err := ss.measure(ctx, harness.Case{Name: name, Concurrency: c, Params: params, Init: initEvent},
				func(ctx context.Context, w *harness.Worker) error {
					ev := w.State.(*event)
					st.fill(ev, customer)
					w.Begin()
					return e.Insert(ctx, ev)
				})
			if err != nil {
				return err
			}
			if err := ss.settle(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}
