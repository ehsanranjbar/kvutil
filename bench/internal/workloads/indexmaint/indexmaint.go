// Package indexmaint is the workload of the cost of secondary indexes: the throughput of
// inserts, updates and deletes of records with no index, one, three and five plain ones, and with
// an index of each kind alone, and the time to build the indexes over records that are there.
//
// A variant is a configuration of indexes (configs), on a fresh database that has the records
// loaded before the indexes are built:
//
//	none, n1, n3, n5  0, 1, 3 and 5 indexes of one field each: Customer, Status, Total, At, Name
//	unique            a unique index on Code
//	composite         an index on (Customer, At)
//	multi             an index on Tags, of which a record has up to four: kvutil index.On on
//	                  a slice field; PostgreSQL GIN on the text[] column
//	expr              an index on lower(Name): sql.Expr; an expression index
//	conditional       an index on Customer of the records with Total >= 90000: index.Where; a
//	                  partial index
//	covering          an index on Customer that holds Total: index.Including; INCLUDE (total)
//
// The inserts are of new records, the updates change every field of an existing record (so that
// every index entry changes), and the deletes take records from a pool that is loaded right
// before, each once.
package indexmaint

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/kvsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/pgsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/wl"
)

func init() { harness.Register(workload{}) }

// configs are the configurations of indexes, in the order that they run.
var configs = []string{"none", "n1", "n3", "n5", "unique", "composite", "multi", "expr", "conditional", "covering"}

type workload struct{}

func (workload) Name() string  { return "index-maintenance" }
func (workload) Group() string { return "index" }
func (workload) Description() string {
	return "insert, update, delete with 0, 1, 3, 5 indexes and one of each kind; the time to build them"
}

func (workload) Variants(sys harness.System) []harness.Variant {
	var vs []harness.Variant
	switch sys.(type) {
	case *kvsys.System:
		for _, c := range configs {
			vs = append(vs, &variant{name: c, open: openKV})
		}
	case *pgsys.System:
		for _, c := range configs {
			vs = append(vs, &variant{name: c, open: openPG})
		}
	}
	return vs
}

// The ids of the records are from 1.

// target is a database with a table or a store of the records, and the indexes of a configuration.
type target interface {
	// Load writes the records [1, n], before there are any indexes.
	Load(ctx context.Context, spec dataset.Spec, n int64) error
	// Build creates the indexes of the configuration over the records that are there and returns
	// their names; there are none to build in the configuration none.
	Build(ctx context.Context) ([]string, error)
	// Add writes more records, with the indexes kept up to date, outside of the measured time.
	Add(ctx context.Context, spec dataset.Spec, from, to int64) error
	Insert(ctx context.Context, o *dataset.Order) error
	Update(ctx context.Context, o *dataset.Order) error
	Delete(ctx context.Context, id int64) error
	// Probe returns the ids of the records that the probe finds, sorted.
	Probe(ctx context.Context, p probe) ([]int64, error)
	Settle(ctx context.Context) error
	Sizes(ctx context.Context) (harness.Sizes, error)
	Close(ctx context.Context) error
}

type variant struct {
	name string
	open func(ctx context.Context, sys harness.System, config string) (target, error)
}

func (v *variant) Name() string { return v.name }

// poolSize is the number of records that the delete case deletes, a tenth of them as the warm-up.
func poolSize(n int64) int64 {
	return min(max(n, 20_000), 200_000)
}

func (v *variant) Run(ctx context.Context, rc *harness.RunContext) error {
	p := rc.Params
	spec := p.Dataset()
	n := int64(p.Scale)

	t, err := v.open(ctx, rc.Sys, v.name)
	if err != nil {
		return err
	}
	defer t.Close(context.WithoutCancel(ctx))
	rc.SizeFn = t.Sizes

	if err := rc.Phase(ctx, harness.Case{Name: "load", Concurrency: 1}, func(ctx context.Context) (int64, error) {
		return n, t.Load(ctx, spec, n)
	}); err != nil {
		return err
	}
	if err := t.Settle(ctx); err != nil {
		return err
	}

	if v.name != "none" {
		var built []string
		err := rc.Phase(ctx, harness.Case{Name: "build", Concurrency: 1}, func(ctx context.Context) (int64, error) {
			var err error
			built, err = t.Build(ctx)
			return n, err
		})
		if err != nil {
			return err
		}
		rc.Logf("   built %s", strings.Join(built, ", "))
		if err := t.Settle(ctx); err != nil {
			return err
		}
	}

	initOrder := func(w *harness.Worker) { w.State = &dataset.Order{} }
	for _, c := range harness.Levels(p.Concurrency) {
		err := rc.Measure(ctx, harness.Case{Name: "update", Concurrency: c, Init: initOrder},
			func(ctx context.Context, w *harness.Worker) error {
				id := 1 + w.Rand.Int64N(n)
				o := w.State.(*dataset.Order)
				spec.GenInto(o, id, 1+w.Rand.Uint32N(1<<20))
				w.Begin()
				return t.Update(ctx, o)
			})
		if err != nil {
			return err
		}
	}

	next := n + 1
	for _, c := range harness.Levels(p.Concurrency) {
		pool := poolSize(n)
		if err := t.Add(ctx, spec, next, next+pool); err != nil {
			return err
		}
		if err := t.Settle(ctx); err != nil {
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
				return t.Delete(ctx, id)
			})
		if err != nil {
			return err
		}
	}

	var fresh atomic.Int64
	fresh.Store(next)
	for _, c := range harness.Levels(p.Concurrency) {
		err := rc.Measure(ctx, harness.Case{Name: "insert", Concurrency: c, Init: initOrder},
			func(ctx context.Context, w *harness.Worker) error {
				id := fresh.Add(1) - 1
				o := w.State.(*dataset.Order)
				spec.GenInto(o, id, 0)
				w.Begin()
				return t.Insert(ctx, o)
			})
		if err != nil {
			return err
		}
	}
	return nil
}

// probe is a question that is asked of the indexes of a configuration, which both systems
// answer with the ids of the records: through the indexes in kvutil, in SQL in PostgreSQL.
type probe struct {
	// Kind is customer, status, total, created, name, code, composite, tag, lower or
	// conditional.
	Kind string
	// S is the string to look for, and Lo and Hi the range of a number: Total, or the seconds
	// of Created.
	S      string
	Lo, Hi int64
}

func (p probe) Label() string {
	switch p.Kind {
	case "total", "created":
		return fmt.Sprintf("%s in [%d, %d]", p.Kind, p.Lo, p.Hi)
	case "composite":
		return fmt.Sprintf("customer=%s and created in [%d, %d]", p.S, p.Lo, p.Hi)
	case "conditional":
		return fmt.Sprintf("customer=%s and total >= %d", p.S, dataset.BigTotal)
	}
	return fmt.Sprintf("%s=%s", p.Kind, p.S)
}

const (
	day  = 86400
	base = 1704067200 // the first second of the times of the dataset
)

// probes returns the questions for a configuration.
func probes(config string, spec dataset.Spec) []probe {
	customers := []string{"cust-000001", "cust-000007", "cust-000013"}
	if config == "conditional" {
		// Only a tenth of the records are in the index.
		customers = nil
		for i := 1; i <= 12; i++ {
			customers = append(customers, fmt.Sprintf("cust-%06d", i))
		}
	}
	// Names of records that the changes of the verification leave alone.
	names := []string{spec.Gen(3, 0).Name, spec.Gen(9, 0).Name, "No Such Thing 1"}
	var ps []probe
	for _, c := range customers {
		switch config {
		case "composite":
			ps = append(ps, probe{Kind: "composite", S: c, Lo: base, Hi: base + 365*day})
		case "conditional":
			ps = append(ps, probe{Kind: "conditional", S: c})
		case "none", "unique", "multi", "expr":
		default:
			ps = append(ps, probe{Kind: "customer", S: c})
		}
	}
	switch config {
	case "n3", "n5":
		ps = append(ps, probe{Kind: "status", S: "late"}, probe{Kind: "total", Lo: 20000, Hi: 23000})
		if config == "n5" {
			ps = append(ps, probe{Kind: "created", Lo: base + 30*day, Hi: base + 40*day})
			for _, name := range names {
				ps = append(ps, probe{Kind: "name", S: name})
			}
		}
	case "unique":
		for _, k := range [][2]int{{2, 0}, {1, 0}, {1, 3}, {6, 3}, {7, 0}, {8, 0}} {
			ps = append(ps, probe{Kind: "code", S: spec.Gen(int64(k[0]), uint32(k[1])).Code})
		}
	case "multi":
		ps = append(ps, probe{Kind: "tag", S: "tag-07"}, probe{Kind: "tag", S: "tag-33"}, probe{Kind: "tag", S: "tag-49"})
	case "expr":
		for _, name := range names {
			ps = append(ps, probe{Kind: "lower", S: strings.ToLower(name)})
		}
	case "none":
		ps = append(ps, probe{Kind: "all"})
	}
	return ps
}

// Verify loads the dataset, builds the indexes, changes records in a fixed way, and asks the
// indexes the questions of the configuration.
func (workload) Verify(ctx context.Context, sys harness.System, p harness.Params) ([]harness.Output, error) {
	var outs []harness.Output
	for _, hv := range (workload{}).Variants(sys) {
		v := hv.(*variant)
		lines, err := v.verify(ctx, sys, p)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", v.name, err)
		}
		outs = append(outs, harness.Output{Variant: v.name, Group: v.name, Lines: lines})
	}
	return outs, nil
}

func (v *variant) verify(ctx context.Context, sys harness.System, p harness.Params) ([]string, error) {
	spec := p.Dataset()
	n := int64(p.Scale)
	t, err := v.open(ctx, sys, v.name)
	if err != nil {
		return nil, err
	}
	defer t.Close(context.WithoutCancel(ctx))

	if err := t.Load(ctx, spec, n); err != nil {
		return nil, err
	}
	if _, err := t.Build(ctx); err != nil {
		return nil, err
	}

	o := &dataset.Order{}
	for id := int64(1); id <= n; id++ {
		switch {
		case id%7 == 0:
			if err := t.Delete(ctx, id); err != nil {
				return nil, err
			}
		case id%5 == 1:
			spec.GenInto(o, id, 3)
			if err := t.Update(ctx, o); err != nil {
				return nil, err
			}
		}
	}
	for id := n + 1; id <= n+25; id++ {
		spec.GenInto(o, id, 0)
		if err := t.Insert(ctx, o); err != nil {
			return nil, err
		}
	}

	var lines []string
	for _, pr := range probes(v.name, spec) {
		ids, err := t.Probe(ctx, pr)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", pr.Label(), err)
		}
		slices.Sort(ids)
		parts := make([]string, len(ids))
		for i, id := range ids {
			parts[i] = strconv.FormatInt(id, 10)
		}
		lines = append(lines, fmt.Sprintf("%s: %d ids %s", pr.Label(), len(ids), strings.Join(parts, ",")))
	}
	return lines, nil
}
