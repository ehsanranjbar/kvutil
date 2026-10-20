// Package kvbasic is the workload of the basic operations of a key-value store: Get, Set (insert
// and update), Delete, and range scans by the primary key, short and long, at one worker and at
// the concurrency of the run.
//
// The variants of kvutil are three layers over the same Badger: raw (kv.Tx with 8 byte keys and the
// bytes of the record), typed-bytes (typed.Store with values that are bytes: the cost of the typed
// layer alone) and typed (typed.Store of the record, which adds the msgpack encoding and decoding
// of a struct). PostgreSQL has the table kv(k bigint PRIMARY KEY, v bytea) with the same bytes.
package kvbasic

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/kvsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/pgsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/wl"
)

func init() { harness.Register(workload{}) }

type workload struct{}

func (workload) Name() string  { return "kv-basic" }
func (workload) Group() string { return "kv" }
func (workload) Description() string {
	return "get, set (insert, update), delete and range scans by the primary key; kvutil raw, typed-bytes and typed against a bytea table"
}

func (workload) Variants(sys harness.System) []harness.Variant {
	switch sys.(type) {
	case *kvsys.System:
		return []harness.Variant{
			&variant{name: "typed", open: openTyped},
			&variant{name: "raw", open: openRaw},
			&variant{name: "typed-bytes", open: openBlob},
		}
	case *pgsys.System:
		return []harness.Variant{&variant{name: "bytea", open: openPG}}
	}
	return nil
}

type variant struct {
	name string
	open func(ctx context.Context, sys harness.System, p harness.Params) (store, error)
}

func (v *variant) Name() string { return v.name }

const (
	shortScan = 100
	longScan  = 10000
)

// poolSize is the number of records that the delete case deletes, a tenth of them as the warm-up.
func poolSize(n int64) int64 {
	return min(max(n, 20_000), 300_000)
}

func (v *variant) Run(ctx context.Context, rc *harness.RunContext) error {
	p := rc.Params
	spec := p.Dataset()
	n := int64(p.Scale)

	st, err := v.open(ctx, rc.Sys, p)
	if err != nil {
		return err
	}
	defer st.Close(context.WithoutCancel(ctx))
	rc.SizeFn = st.Sizes

	err = rc.Phase(ctx, harness.Case{Name: "load", Concurrency: 1}, func(ctx context.Context) (int64, error) {
		return n, st.Load(ctx, spec, 0, n, 1)
	})
	if err != nil {
		return err
	}
	if err := st.Settle(ctx); err != nil {
		return err
	}

	short := int(min(shortScan, max(n/10, 1)))
	long := int(min(longScan, max(n/2, 1)))
	scan := func(limit int) harness.OpFunc {
		return func(ctx context.Context, w *harness.Worker) error {
			from := w.Rand.Int64N(n - int64(limit) + 1)
			w.Begin()
			rows, err := st.Scan(ctx, from, limit)
			if err != nil {
				return err
			}
			if rows != limit {
				return fmt.Errorf("a scan of %d from %d read %d", limit, from, rows)
			}
			w.Add(rows)
			return nil
		}
	}
	initOrder := func(w *harness.Worker) { w.State = &dataset.Order{} }

	for _, c := range harness.Levels(p.Concurrency) {
		err := rc.Measure(ctx, harness.Case{Name: "get", Concurrency: c}, func(ctx context.Context, w *harness.Worker) error {
			id := w.Rand.Int64N(n)
			w.Begin()
			found, err := st.Get(ctx, id)
			if err == nil && !found {
				err = fmt.Errorf("record %d not found", id)
			}
			return err
		})
		if err != nil {
			return err
		}
	}
	for _, c := range harness.Levels(p.Concurrency) {
		if err := rc.Measure(ctx, harness.Case{Name: "scan-short", Concurrency: c, Unit: "rows",
			Params: map[string]any{"rows": short}}, scan(short)); err != nil {
			return err
		}
	}
	for _, c := range harness.Levels(p.Concurrency) {
		if err := rc.Measure(ctx, harness.Case{Name: "scan-long", Concurrency: c, Unit: "rows",
			Params: map[string]any{"rows": long}}, scan(long)); err != nil {
			return err
		}
	}
	for _, c := range harness.Levels(p.Concurrency) {
		err := rc.Measure(ctx, harness.Case{Name: "set-update", Concurrency: c, Init: initOrder},
			func(ctx context.Context, w *harness.Worker) error {
				id := w.Rand.Int64N(n)
				o := w.State.(*dataset.Order)
				spec.GenInto(o, id, 1+w.Rand.Uint32N(1<<20))
				prepared := st.Prepare(o)
				w.Begin()
				return st.Set(ctx, id, prepared)
			})
		if err != nil {
			return err
		}
	}

	// The records that are deleted are loaded right before, past the ones of the scale, and
	// each is deleted once.
	next := n
	for _, c := range harness.Levels(p.Concurrency) {
		pool := poolSize(n)
		if err := st.Load(ctx, spec, next, next+pool, 1); err != nil {
			return err
		}
		if err := st.Settle(ctx); err != nil {
			return err
		}
		cur := wl.NewCursor(next, next+pool)
		next += pool
		err := rc.Measure(ctx, harness.Case{Name: "delete", Concurrency: c, Ops: pool, WarmupOps: pool / 10,
			Params: map[string]any{"pool": pool}},
			func(ctx context.Context, w *harness.Worker) error {
				id, ok := cur.Take()
				if !ok {
					return harness.ErrDone
				}
				w.Begin()
				return st.Delete(ctx, id)
			})
		if err != nil {
			return err
		}
	}

	// New records, with ids past all the others.
	var fresh atomic.Int64
	fresh.Store(next)
	for _, c := range harness.Levels(p.Concurrency) {
		err := rc.Measure(ctx, harness.Case{Name: "set-insert", Concurrency: c, Init: initOrder},
			func(ctx context.Context, w *harness.Worker) error {
				id := fresh.Add(1) - 1
				o := w.State.(*dataset.Order)
				spec.GenInto(o, id, 0)
				prepared := st.Prepare(o)
				w.Begin()
				return st.Set(ctx, id, prepared)
			})
		if err != nil {
			return err
		}
	}
	return nil
}

// Verify loads the dataset, changes it in a fixed way, and says what the store has at points of
// the way.
func (workload) Verify(ctx context.Context, sys harness.System, p harness.Params) ([]harness.Output, error) {
	var outs []harness.Output
	for _, hv := range (workload{}).Variants(sys) {
		v := hv.(*variant)
		lines, err := v.verify(ctx, sys, p)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", v.name, err)
		}
		outs = append(outs, harness.Output{Variant: v.name, Lines: lines})
	}
	return outs, nil
}

func (v *variant) verify(ctx context.Context, sys harness.System, p harness.Params) ([]string, error) {
	spec := p.Dataset()
	n := int64(p.Scale)
	st, err := v.open(ctx, sys, p)
	if err != nil {
		return nil, err
	}
	defer st.Close(context.WithoutCancel(ctx))

	var out []string
	say := func(format string, args ...any) { out = append(out, fmt.Sprintf(format, args...)) }
	dump := func(label string, from int64, limit int) error {
		lines, err := st.Dump(ctx, from, limit)
		if err != nil {
			return err
		}
		say("%s: %d records", label, len(lines))
		out = append(out, lines...)
		return nil
	}

	if err := st.Load(ctx, spec, 0, n, 2); err != nil {
		return nil, err
	}
	if err := dump("all after the load", 0, int(n)+1000); err != nil {
		return nil, err
	}
	for _, id := range []int64{0, 1, n / 2, n - 1, n, n + 5} {
		found, err := st.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		say("get %d: %v", id, found)
	}

	o := &dataset.Order{}
	for id := int64(0); id < n; id += 7 {
		spec.GenInto(o, id, 1)
		if err := st.Set(ctx, id, st.Prepare(o)); err != nil {
			return nil, err
		}
	}
	for id := int64(0); id < n; id += 11 {
		if err := st.Delete(ctx, id); err != nil {
			return nil, err
		}
	}
	for id := n; id < n+20; id++ {
		spec.GenInto(o, id, 0)
		if err := st.Set(ctx, id, st.Prepare(o)); err != nil {
			return nil, err
		}
	}
	for _, id := range []int64{0, 7, 11, 14, n, n + 19, n + 20} {
		found, err := st.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		say("get %d: %v", id, found)
	}
	if err := dump("all after the changes", 0, int(n)+1000); err != nil {
		return nil, err
	}
	if err := dump("range from a third", n/3, 50); err != nil {
		return nil, err
	}
	for _, from := range []int64{0, n / 3, n - 5} {
		rows, err := st.Scan(ctx, from, 50)
		if err != nil {
			return nil, err
		}
		say("scan %d rows from %d", rows, from)
	}
	return out, nil
}
