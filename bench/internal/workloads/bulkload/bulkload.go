// Package bulkload is the workload of loading N records into an empty database: kvutil with a
// Badger WriteBatch (kv.NewBulkWriter) and with transactions of 100, 1000 and 1 records, and
// PostgreSQL with COPY and with multi-row INSERTs of 100, 1000 and 1 rows, each on one worker and
// on the concurrency of the run (the workers load contiguous parts of the ids). PostgreSQL
// loads both a table of key and bytea, as kv-basic has, and a table of columns.
//
// A load of single records is capped (maxSingles), as every commit waits for the disk when the
// run is durable. The time includes the last flush or commit; it does not include the
// preparation of the records (they are generated on the way, which costs the same for all).
package bulkload

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"hash"
	"strings"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/kvsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/pgsys"
)

func init() { harness.Register(workload{}) }

// maxSingles is the most records that a load in transactions of one record loads.
const maxSingles = 5000

type workload struct{}

func (workload) Name() string  { return "bulk-load" }
func (workload) Group() string { return "load" }
func (workload) Description() string {
	return "loading N records: WriteBatch and transactions of 100, 1000 and 1 against COPY and multi-row INSERTs"
}

func (workload) Variants(sys harness.System) []harness.Variant {
	switch sys.(type) {
	case *kvsys.System:
		return []harness.Variant{&variant{name: "typed", open: openKV, strategies: kvStrategies()}}
	case *pgsys.System:
		return []harness.Variant{
			&variant{name: "bytea", open: openPGBytea, strategies: pgStrategies()},
			&variant{name: "rows", open: openPGRows, strategies: pgStrategies()},
		}
	}
	return nil
}

// target is a database to load into, which is fresh for each strategy.
type target interface {
	// Check counts the records, and, if it is asked to, computes a digest of all of them in
	// their canonical form.
	Check(ctx context.Context, digest bool) (count int64, sum string, err error)
	Sizes(ctx context.Context) (harness.Sizes, error)
	Close(ctx context.Context) error
}

type strategy struct {
	name   string
	params map[string]any
	// singles marks a strategy that commits each record, which is capped.
	singles bool
	load    func(ctx context.Context, t target, spec dataset.Spec, n int64, workers int) error
}

type variant struct {
	name       string
	open       func(ctx context.Context, sys harness.System, p harness.Params) (target, error)
	strategies []strategy
}

func (v *variant) Name() string { return v.name }

func (v *variant) Run(ctx context.Context, rc *harness.RunContext) error {
	p := rc.Params
	spec := p.Dataset()
	for _, s := range v.strategies {
		for _, workers := range harness.Levels(p.Concurrency) {
			n := int64(p.Scale)
			params := map[string]any{"workers": workers}
			for k, val := range s.params {
				params[k] = val
			}
			if s.singles && n > maxSingles {
				n = maxSingles
				params["capped"] = true
			}
			if err := v.one(ctx, rc, spec, s, n, workers, params); err != nil {
				return fmt.Errorf("%s with %d workers: %w", s.name, workers, err)
			}
		}
	}
	return nil
}

func (v *variant) one(ctx context.Context, rc *harness.RunContext, spec dataset.Spec, s strategy, n int64, workers int, params map[string]any) error {
	t, err := v.open(ctx, rc.Sys, rc.Params)
	if err != nil {
		return err
	}
	defer t.Close(context.WithoutCancel(ctx))
	rc.SizeFn = t.Sizes

	c := harness.Case{Name: s.name, Concurrency: workers, Params: params}
	err = rc.Phase(ctx, c, func(ctx context.Context) (int64, error) {
		return n, s.load(ctx, t, spec, n, workers)
	})
	if err != nil {
		return err
	}
	count, _, err := t.Check(ctx, false)
	if err != nil {
		return err
	}
	if count != n {
		return fmt.Errorf("loaded %d records, found %d", n, count)
	}
	return nil
}

// Verify loads the dataset with every strategy and says what is in the database: they all must
// have the same.
func (workload) Verify(ctx context.Context, sys harness.System, p harness.Params) ([]harness.Output, error) {
	spec := p.Dataset()
	n := int64(p.Scale)
	var outs []harness.Output
	for _, hv := range (workload{}).Variants(sys) {
		v := hv.(*variant)
		var line string
		for _, s := range v.strategies {
			for _, workers := range []int{1, 3} {
				got, err := func() (string, error) {
					t, err := v.open(ctx, sys, p)
					if err != nil {
						return "", err
					}
					defer t.Close(context.WithoutCancel(ctx))
					if err := s.load(ctx, t, spec, n, workers); err != nil {
						return "", err
					}
					count, sum, err := t.Check(ctx, true)
					return fmt.Sprintf("%d records, digest %s", count, sum), err
				}()
				if err != nil {
					return nil, fmt.Errorf("%s/%s with %d workers: %w", v.name, s.name, workers, err)
				}
				if line != "" && got != line {
					return nil, fmt.Errorf("%s/%s with %d workers loaded %s, another strategy %s", v.name, s.name, workers, got, line)
				}
				line = got
			}
		}
		outs = append(outs, harness.Output{Variant: v.name, Lines: []string{line}})
	}
	return outs, nil
}

// digester computes a digest of the canonical form of records.
type digester struct {
	h hash.Hash
	n int64
}

func newDigester() *digester { return &digester{h: sha1.New()} }

func (d *digester) add(o *dataset.Order) {
	d.n++
	d.h.Write([]byte(dataset.Canon(o)))
	d.h.Write([]byte{'\n'})
}

func (d *digester) sum() string { return hex.EncodeToString(d.h.Sum(nil)[:8]) }

// placeholders returns the VALUES of a multi-row INSERT: (...), (...), with columns parameters
// for each of rows.
func placeholders(rows, columns int) string {
	var sb strings.Builder
	n := 1
	for r := 0; r < rows; r++ {
		if r > 0 {
			sb.WriteString(", ")
		}
		sb.WriteByte('(')
		for c := 0; c < columns; c++ {
			if c > 0 {
				sb.WriteString(", ")
			}
			fmt.Fprintf(&sb, "$%d", n)
			n++
		}
		sb.WriteByte(')')
	}
	return sb.String()
}
