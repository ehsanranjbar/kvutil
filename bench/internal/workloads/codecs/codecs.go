// Package codecs is the workload of the encodings of a record: a kvutil record store (record.New)
// with the default msgpack codec, a JSON codec and a binary codec (MarshalBinary), against a
// PostgreSQL table of columns, a table with a jsonb column and a table with the msgpack bytes in a
// bytea. It measures Get, and Set of an existing and of a new record, and says how many bytes a
// record takes.
//
// The ids start at 1: a record store takes the zero id for a record that has none.
package codecs

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/metrics"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/kvsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/pgsys"
)

func init() { harness.Register(workload{}) }

type workload struct{}

func (workload) Name() string  { return "record-codecs" }
func (workload) Group() string { return "records" }
func (workload) Description() string {
	return "Set/Get of a record with msgpack, JSON and binary codecs against a row of columns, jsonb and bytea"
}

func (workload) Variants(sys harness.System) []harness.Variant {
	switch sys.(type) {
	case *kvsys.System:
		return []harness.Variant{
			&variant{name: "msgpack", open: openMsgpack},
			&variant{name: "json", open: openJSON},
			&variant{name: "binary", open: openBinary},
		}
	case *pgsys.System:
		return []harness.Variant{
			&variant{name: "columns", open: openColumns},
			&variant{name: "jsonb", open: openJSONB},
			&variant{name: "bytea-msgpack", open: openBytea},
		}
	}
	return nil
}

// store is a store of records, of ids from 1.
type store interface {
	// Load writes the records [1, n].
	Load(ctx context.Context, spec dataset.Spec, n int64) error
	// Get returns the record, or nil if there is none.
	Get(ctx context.Context, id int64) (*dataset.Order, error)
	// Set writes the record, which is a new one or replaces the one of its id.
	Set(ctx context.Context, o *dataset.Order) error
	// AvgBytes is the average size of a stored record.
	AvgBytes(ctx context.Context, spec dataset.Spec, n int64) (float64, error)
	Settle(ctx context.Context) error
	Sizes(ctx context.Context) (harness.Sizes, error)
	Close(ctx context.Context) error
}

type variant struct {
	name string
	open func(ctx context.Context, sys harness.System, p harness.Params) (store, error)
}

func (v *variant) Name() string { return v.name }

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

	if err := rc.Phase(ctx, harness.Case{Name: "load", Concurrency: 1}, func(ctx context.Context) (int64, error) {
		return n, st.Load(ctx, spec, n)
	}); err != nil {
		return err
	}
	if err := st.Settle(ctx); err != nil {
		return err
	}
	avg, err := st.AvgBytes(ctx, spec, n)
	if err != nil {
		return err
	}
	now := time.Now()
	if err := rc.Report(ctx, harness.Case{Name: "size"}, now, now, metrics.Metrics{
		Extra: map[string]float64{"avg_record_bytes": avg},
	}); err != nil {
		return err
	}

	initOrder := func(w *harness.Worker) { w.State = &dataset.Order{} }
	for _, c := range harness.Levels(p.Concurrency) {
		err := rc.Measure(ctx, harness.Case{Name: "get", Concurrency: c}, func(ctx context.Context, w *harness.Worker) error {
			id := 1 + w.Rand.Int64N(n)
			w.Begin()
			o, err := st.Get(ctx, id)
			if err == nil && o == nil {
				err = fmt.Errorf("record %d not found", id)
			}
			return err
		})
		if err != nil {
			return err
		}
	}
	for _, c := range harness.Levels(p.Concurrency) {
		err := rc.Measure(ctx, harness.Case{Name: "set-update", Concurrency: c, Init: initOrder},
			func(ctx context.Context, w *harness.Worker) error {
				id := 1 + w.Rand.Int64N(n)
				o := w.State.(*dataset.Order)
				spec.GenInto(o, id, 1+w.Rand.Uint32N(1<<20))
				w.Begin()
				return st.Set(ctx, o)
			})
		if err != nil {
			return err
		}
	}
	var fresh atomic.Int64
	fresh.Store(n + 1)
	for _, c := range harness.Levels(p.Concurrency) {
		err := rc.Measure(ctx, harness.Case{Name: "set-insert", Concurrency: c, Init: initOrder},
			func(ctx context.Context, w *harness.Worker) error {
				id := fresh.Add(1) - 1
				o := w.State.(*dataset.Order)
				spec.GenInto(o, id, 0)
				w.Begin()
				return st.Set(ctx, o)
			})
		if err != nil {
			return err
		}
	}
	return nil
}

// Verify loads the dataset, changes some of the records, and returns them all as they come back.
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

	if err := st.Load(ctx, spec, n); err != nil {
		return nil, err
	}
	o := &dataset.Order{}
	for id := int64(1); id <= n; id += 5 {
		spec.GenInto(o, id, 3)
		if err := st.Set(ctx, o); err != nil {
			return nil, err
		}
	}
	for id := n + 1; id <= n+10; id++ {
		spec.GenInto(o, id, 0)
		if err := st.Set(ctx, o); err != nil {
			return nil, err
		}
	}

	var lines []string
	for id := int64(1); id <= n+12; id++ {
		got, err := st.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		if got == nil {
			lines = append(lines, fmt.Sprintf("id=%d missing", id))
			continue
		}
		got.ID = id
		lines = append(lines, dataset.Canon(got))
	}
	return lines, nil
}
