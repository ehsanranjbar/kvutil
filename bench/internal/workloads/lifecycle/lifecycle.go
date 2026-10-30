// Package lifecycle is the workload of changing a database that has records in it: adding an
// index, dropping one, changing the value of every record, and starting up. The records are loaded
// first and the change is measured over them (as phases: the time, and records per second).
//
// A variant is a kind of change, on a fresh database:
//
//	index-single     a B-tree index on one field: Migrate builds it / CREATE INDEX
//	index-unique     a unique index on Code
//	index-composite  an index on (Customer, At)
//	index-multi      an index on Tags, which has an entry per tag: kvutil index.On on a slice /
//	                 GIN on the text[] column
//	migrate          a value change in place: every record gets a priority computed from its
//	                 total: MigrateFrom (a store of version 2 converts the records of version 1) /
//	                 ALTER TABLE ADD COLUMN and UPDATE of every row; then a field that is added
//	                 with no migration / ADD COLUMN with a default (metadata only)
//	migrate-indexed  the same with three indexes on the records, which are built again (kvutil)
//	                 or updated with the rows (PostgreSQL)
//	open             the time to be ready: a database with nothing in it, one with records
//	                 reopened, and one restarted
//
// Cases of the index variants: add-index, drop-index and, on PostgreSQL, add-index-concurrently.
// The databases are alone while they are changed, as Migrate needs.
package lifecycle

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/metrics"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/kvsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/pgsys"
)

func init() { harness.Register(workload{}) }

type workload struct{}

func (workload) Name() string  { return "lifecycle" }
func (workload) Group() string { return "lifecycle" }
func (workload) Description() string {
	return "add and drop an index over N records (Migrate / CREATE INDEX [CONCURRENTLY]), change the value of every record (MigrateFrom / ALTER TABLE + UPDATE), additive changes, time to open and to restart"
}

var errUnsupported = errors.New("not supported by the system")

// stepFunc runs a part of a change as a case of its own.
type stepFunc func(name string, fn func(ctx context.Context) error) error

// driver is the database of a system, with the changes that the variants make to it.
type driver interface {
	Load(ctx context.Context, spec dataset.Spec, n int64) error
	AddIndexes(ctx context.Context, kinds []string) error
	DropIndexes(ctx context.Context, kinds []string) error
	CreateConcurrently(ctx context.Context, kind string) error
	// Probe answers a question through the index of the kind (a scan or a plan of PostgreSQL's
	// choice there): the ids, in any order.
	Probe(ctx context.Context, kind string, p probe) ([]int64, error)
	// Convert gives every record a priority; the parts it has are reported as cases through step.
	Convert(ctx context.Context, step stepFunc) error
	// Priority returns the ids of the records with the priority.
	Priority(ctx context.Context, k int32) ([]int64, error)
	// AddField adds a field to the records that old records do not have.
	AddField(ctx context.Context) error
	Exists(ctx context.Context, id int64) (bool, error)
	Settle(ctx context.Context) error
	Sizes(ctx context.Context) (data, wal int64, err error)

	// FreshOpen is the time to be ready on a database with nothing in it.
	FreshOpen(ctx context.Context) (time.Duration, error)
	// Reopen is the time to be ready on the database with records, after it was closed: the
	// parts are the time to close (kvutil), to open (or connect) and to answer a first query.
	Reopen(ctx context.Context) (closeD, openD, readyD time.Duration, err error)
	// Restart is the time from the restart of the system to the first answer, and how it was done.
	Restart(ctx context.Context) (time.Duration, string, error)
	Close(ctx context.Context) error
}

// probe is a question asked of the index of a kind.
type probe struct {
	S      string
	Lo, Hi int64
}

func (p probe) label(kind string) string {
	switch kind {
	case "total":
		return fmt.Sprintf("total in [%d, %d]", p.Lo, p.Hi)
	case "composite":
		return fmt.Sprintf("customer=%s and at in [%d, %d]", p.S, p.Lo, p.Hi)
	}
	return fmt.Sprintf("%s=%s", kind, p.S)
}

const (
	day  = 86400
	base = 1704067200 // the first second of the times of the dataset
)

// probesOf returns the questions for the index of a kind.
func probesOf(kind string, spec dataset.Spec) []probe {
	switch kind {
	case "customer":
		return []probe{{S: "cust-000001"}, {S: "cust-000007"}, {S: "cust-000013"}}
	case "status":
		return []probe{{S: "late"}, {S: "new"}}
	case "total":
		return []probe{{Lo: 20000, Hi: 23000}, {Lo: 99000, Hi: 99999}}
	case "code":
		return []probe{{S: spec.Gen(2, 0).Code}, {S: spec.Gen(5, 0).Code}, {S: spec.Gen(5, 1).Code}}
	case "composite":
		return []probe{{S: "cust-000001", Lo: base, Hi: base + 365*day}, {S: "cust-000007", Lo: base + 100*day, Hi: base + 500*day}}
	case "tags":
		return []probe{{S: "tag-07"}, {S: "tag-33"}, {S: "tag-49"}}
	}
	panic("unknown kind " + kind)
}

// The variants.
type variant struct {
	name    string
	indexes []string
	// what is one of index, migrate, open.
	what string
}

var (
	threeIndexes = []string{"customer", "status", "total"}
	variants     = []variant{
		{name: "index-single", what: "index", indexes: []string{"customer"}},
		{name: "index-unique", what: "index", indexes: []string{"code"}},
		{name: "index-composite", what: "index", indexes: []string{"composite"}},
		{name: "index-multi", what: "index", indexes: []string{"tags"}},
		{name: "migrate", what: "migrate"},
		{name: "migrate-indexed", what: "migrate", indexes: threeIndexes},
		{name: "open", what: "open", indexes: threeIndexes},
	}
)

func (workload) Variants(sys harness.System) []harness.Variant {
	switch sys.(type) {
	case *kvsys.System, *pgsys.System:
		vs := make([]harness.Variant, len(variants))
		for i := range variants {
			vs[i] = &variants[i]
		}
		return vs
	}
	return nil
}

func (v *variant) Name() string { return v.name }

func openDriver(ctx context.Context, sys harness.System) (driver, error) {
	switch s := sys.(type) {
	case *kvsys.System:
		return openKVDriver(ctx, s)
	case *pgsys.System:
		return openPGDriver(ctx, s)
	}
	return nil, fmt.Errorf("lifecycle does not run on %s", sys.Name())
}

func (v *variant) Run(ctx context.Context, rc *harness.RunContext) error {
	p := rc.Params
	spec := p.Dataset()
	n := int64(p.Scale)
	d, err := openDriver(ctx, rc.Sys)
	if err != nil {
		return err
	}
	defer d.Close(context.WithoutCancel(ctx))
	rc.SizeFn = func(ctx context.Context) (harness.Sizes, error) {
		data, wal, err := d.Sizes(ctx)
		return harness.Sizes{Data: data, WAL: wal}, err
	}

	err = rc.Phase(ctx, harness.Case{Name: "load", Concurrency: 1}, func(ctx context.Context) (int64, error) {
		return n, d.Load(ctx, spec, n)
	})
	if err != nil {
		return err
	}
	if err := d.Settle(ctx); err != nil {
		return err
	}

	// phase runs a change over the n records as a case.
	phase := func(name string, fn func(ctx context.Context) error) error {
		err := rc.Phase(ctx, harness.Case{Name: name, Concurrency: 1, Params: map[string]any{"indexes": strings.Join(v.indexes, ",")}},
			func(ctx context.Context) (int64, error) { return n, fn(ctx) })
		if err != nil {
			return err
		}
		return d.Settle(ctx)
	}

	switch v.what {
	case "index":
		return v.runIndex(ctx, rc, d, phase, spec)
	case "migrate":
		return v.runMigrate(ctx, rc, d, phase, n)
	}
	if err := phase("build-indexes", func(ctx context.Context) error { return d.AddIndexes(ctx, v.indexes) }); err != nil {
		return err
	}
	return v.runOpen(ctx, rc, d)
}

func (v *variant) runIndex(ctx context.Context, rc *harness.RunContext, d driver, phase func(string, func(context.Context) error) error, spec dataset.Spec) error {
	kind := v.indexes[0]
	if err := phase("add-index", func(ctx context.Context) error { return d.AddIndexes(ctx, v.indexes) }); err != nil {
		return err
	}
	// The index answers: a question that finds records is asked of it.
	found := 0
	for _, pr := range probesOf(kind, spec) {
		ids, err := d.Probe(ctx, kind, pr)
		if err != nil {
			return fmt.Errorf("a question of the new index: %w", err)
		}
		found += len(ids)
	}
	if found == 0 {
		return fmt.Errorf("the new index %s finds nothing", kind)
	}
	if err := phase("drop-index", func(ctx context.Context) error { return d.DropIndexes(ctx, v.indexes) }); err != nil {
		return err
	}
	err := phase("add-index-concurrently", func(ctx context.Context) error { return d.CreateConcurrently(ctx, kind) })
	if errors.Is(err, errUnsupported) {
		rc.Logf("   no add-index-concurrently on %s", rc.Sys.Name())
		return nil
	}
	return err
}

func (v *variant) runMigrate(ctx context.Context, rc *harness.RunContext, d driver, phase func(string, func(context.Context) error) error, n int64) error {
	if len(v.indexes) > 0 {
		if err := phase("build-indexes", func(ctx context.Context) error { return d.AddIndexes(ctx, v.indexes) }); err != nil {
			return err
		}
	}

	// The change is a case of its own whose parts, on PostgreSQL, are cases too.
	// The time of the whole is the time of its parts, without what it takes to report them.
	var parts time.Duration
	step := func(name string, fn func(ctx context.Context) error) error {
		c := harness.Case{Name: "convert-" + name, Concurrency: 1, Params: map[string]any{"indexes": strings.Join(v.indexes, ",")}}
		return rc.Phase(ctx, c, func(ctx context.Context) (int64, error) {
			s := time.Now()
			err := fn(ctx)
			parts += time.Since(s)
			return n, err
		})
	}
	t0 := time.Now()
	probe := metrics.StartProbe()
	if err := d.Convert(ctx, step); err != nil {
		return err
	}
	res := probe.Stop()
	t1 := time.Now()
	elapsed := t1.Sub(t0)
	if parts > 0 {
		elapsed = parts
	}
	if err := d.Settle(ctx); err != nil {
		return err
	}
	m := metrics.Metrics{Records: n, ElapsedSec: elapsed.Seconds(), Resources: res}
	m.RecordsPerSec = float64(n) / elapsed.Seconds()
	if err := rc.Report(ctx, harness.Case{Name: "convert", Concurrency: 1, Params: map[string]any{"indexes": strings.Join(v.indexes, ",")}}, t0, t1, m); err != nil {
		return err
	}
	ids, err := d.Priority(ctx, 3)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return fmt.Errorf("no record has the priority 3 after the change")
	}

	// The additive change.
	t0 = time.Now()
	if err := d.AddField(ctx); err != nil {
		return err
	}
	t1 = time.Now()
	return rc.Report(ctx, harness.Case{Name: "add-field", Concurrency: 1, Params: map[string]any{"records": n}}, t0, t1,
		metrics.Metrics{ElapsedSec: t1.Sub(t0).Seconds()})
}

// trials is the number of times that the time to open is measured, and restarts is the number for
// a restart, which takes much longer.
const (
	trials   = 7
	restarts = 3
)

// summary describes durations in milliseconds.
func summary(ds []time.Duration, prefix string, into map[string]float64) {
	if len(ds) == 0 {
		return
	}
	sorted := slices.Clone(ds)
	slices.Sort(sorted)
	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}
	ms := func(d time.Duration) float64 { return d.Seconds() * 1000 }
	into[prefix+"min"] = ms(sorted[0])
	into[prefix+"median"] = ms(sorted[len(sorted)/2])
	into[prefix+"mean"] = ms(sum / time.Duration(len(sorted)))
	into[prefix+"max"] = ms(sorted[len(sorted)-1])
}

func (v *variant) runOpen(ctx context.Context, rc *harness.RunContext, d driver) error {
	report := func(name string, start time.Time, extra map[string]float64, info map[string]string) error {
		end := time.Now()
		c := harness.Case{Name: name, Concurrency: 1, Info: info}
		return rc.Report(ctx, c, start, end, metrics.Metrics{ElapsedSec: extra["ready_ms_median"] / 1000, Extra: extra})
	}

	// A database with nothing in it.
	start := time.Now()
	var fresh []time.Duration
	for range trials {
		dur, err := d.FreshOpen(ctx)
		if err != nil {
			return fmt.Errorf("open-new: %w", err)
		}
		fresh = append(fresh, dur)
	}
	extra := map[string]float64{"trials": trials}
	summary(fresh, "ready_ms_", extra)
	if err := report("open-new", start, extra, nil); err != nil {
		return err
	}

	// The database with records and indexes, closed and opened again.
	start = time.Now()
	var closes, opens, readies, totals []time.Duration
	for range trials {
		c, o, r, err := d.Reopen(ctx)
		if err != nil {
			return fmt.Errorf("open-loaded: %w", err)
		}
		closes, opens, readies = append(closes, c), append(opens, o), append(readies, r)
		totals = append(totals, o+r)
	}
	extra = map[string]float64{"trials": trials}
	summary(totals, "ready_ms_", extra)
	summary(closes, "close_ms_", extra)
	summary(opens, "open_ms_", extra)
	summary(readies, "first_ms_", extra)
	if err := report("open-loaded", start, extra, nil); err != nil {
		return err
	}

	// A restart.
	start = time.Now()
	var restart []time.Duration
	var method string
	for range restarts {
		dur, how, err := d.Restart(ctx)
		if err != nil {
			return fmt.Errorf("restart-loaded: %w", err)
		}
		restart = append(restart, dur)
		method = how
	}
	extra = map[string]float64{"trials": restarts}
	summary(restart, "ready_ms_", extra)
	return report("restart-loaded", start, extra, map[string]string{"method": method})
}

// Verify asks the same questions of the new index, of the records after the change and of the
// database after it was opened again, on both systems.
func (workload) Verify(ctx context.Context, sys harness.System, p harness.Params) ([]harness.Output, error) {
	var outs []harness.Output
	for i := range variants {
		v := &variants[i]
		lines, err := v.verify(ctx, sys, p)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", v.name, err)
		}
		outs = append(outs, harness.Output{Variant: v.name, Group: v.name, Lines: lines})
	}
	return outs, nil
}

func idLine(label string, ids []int64) string {
	sorted := slices.Clone(ids)
	slices.Sort(sorted)
	parts := make([]string, len(sorted))
	for i, id := range sorted {
		parts[i] = strconv.FormatInt(id, 10)
	}
	text := strings.Join(parts, ",")
	if len(sorted) > 40 {
		sum := sha1.Sum([]byte(text))
		text = "digest " + hex.EncodeToString(sum[:6])
	}
	return fmt.Sprintf("%s: %d ids %s", label, len(sorted), text)
}

func (v *variant) verify(ctx context.Context, sys harness.System, p harness.Params) ([]string, error) {
	spec := p.Dataset()
	n := int64(p.Scale)
	d, err := openDriver(ctx, sys)
	if err != nil {
		return nil, err
	}
	defer d.Close(context.WithoutCancel(ctx))
	if err := d.Load(ctx, spec, n); err != nil {
		return nil, err
	}

	var lines []string
	probeAll := func(stage string, kinds []string) error {
		for _, kind := range kinds {
			for _, pr := range probesOf(kind, spec) {
				ids, err := d.Probe(ctx, kind, pr)
				if err != nil {
					return fmt.Errorf("%s: %w", pr.label(kind), err)
				}
				lines = append(lines, idLine(stage+" "+pr.label(kind), ids))
			}
		}
		return nil
	}

	if len(v.indexes) > 0 {
		if err := d.AddIndexes(ctx, v.indexes); err != nil {
			return nil, err
		}
		if err := probeAll("new index", v.indexes); err != nil {
			return nil, err
		}
	}

	switch v.what {
	case "index":
		// The index is dropped and added again, and answers the same.
		if err := d.DropIndexes(ctx, v.indexes); err != nil {
			return nil, err
		}
		if err := d.AddIndexes(ctx, v.indexes); err != nil {
			return nil, err
		}
		if err := probeAll("rebuilt index", v.indexes); err != nil {
			return nil, err
		}

	case "migrate":
		if err := d.Convert(ctx, func(_ string, fn func(context.Context) error) error { return fn(ctx) }); err != nil {
			return nil, err
		}
		prios := make([]int64, 0, 10)
		for k := int32(0); k < 10; k++ {
			ids, err := d.Priority(ctx, k)
			if err != nil {
				return nil, err
			}
			prios = append(prios, int64(len(ids)))
			lines = append(lines, idLine(fmt.Sprintf("priority %d", k), ids))
		}
		lines = append(lines, fmt.Sprintf("priorities cover the records: %t", sumOf(prios) == n))
		if err := probeAll("after the change, index", v.indexes); err != nil {
			return nil, err
		}
		if err := d.AddField(ctx); err != nil {
			return nil, err
		}
		ok, err := d.Exists(ctx, 1)
		if err != nil {
			return nil, err
		}
		lines = append(lines, fmt.Sprintf("record 1 is there after the field was added: %t", ok))
		ids, err := d.Priority(ctx, 3)
		if err != nil {
			return nil, err
		}
		lines = append(lines, idLine("priority 3 after the field was added", ids))

	case "open":
		if _, _, _, err := d.Reopen(ctx); err != nil {
			return nil, err
		}
		if err := probeAll("after reopening, index", v.indexes); err != nil {
			return nil, err
		}
	}
	return lines, nil
}

func sumOf(xs []int64) int64 {
	var s int64
	for _, x := range xs {
		s += x
	}
	return s
}
