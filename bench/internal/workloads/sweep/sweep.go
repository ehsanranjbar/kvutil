// Package sweep is the workload of the selectivity sweep: a range condition on an indexed field
// (Score, uniform, with an index) whose selectivity goes from about 10 rows to half of the table, with
// and without ORDER BY the primary key, and the plan that each system chose at each step.
//
// It shows where PostgreSQL goes from an Index Scan to a Bitmap Heap Scan to a Seq Scan, and what the
// planner of kvutil, which has no statistics, does. kvutil has three variants: planner (as the
// query is planned), index (hinted to read the index of Score) and scan (hinted to scan the store),
// so that the point where reading the index stops paying can be found by comparing them whatever the
// planner picks. PostgreSQL has the variant planner.
//
// The steps are 0.001%, 0.01%, 0.1%, 1%, 5%, 10%, 25% and 50% of the table, but never fewer than 10
// rows: at a small scale the first steps are the same and only the first is run. The arguments of a
// step are the ranges of a pool of 256, at random places of the scores.
package sweep

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/ehsanranjbar/kvutil/bench/internal/bookdb"
	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/plans"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/kvsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/pgsys"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/query"
)

func init() { harness.Register(workload{}) }

const (
	poolSize   = 256
	planSample = 20
	// verifyRuns is the number of ranges of a step that are verified.
	verifyRuns = 2
)

type book = dataset.Book

// percents are the selectivities of the steps, in percent.
var percents = []float64{0.001, 0.01, 0.1, 1, 5, 10, 25, 50}

type workload struct{}

func (workload) Name() string  { return "selectivity-sweep" }
func (workload) Group() string { return "query" }
func (workload) Description() string {
	return "a range on an indexed field from 10 rows to half of the table, with and without ORDER BY the key; the plans each system chooses"
}

// step is a selectivity.
type step struct {
	rows  int64
	pct   float64
	width int64
}

// name labels the step in the cases: its percentage of the table.
func (s step) name() string {
	// Three significant digits: 10 rows of 300,000 are 0.00333%.
	rounded, _ := strconv.ParseFloat(strconv.FormatFloat(s.pct, 'g', 3, 64), 64)
	return strconv.FormatFloat(rounded, 'f', -1, 64) + "pct"
}

// steps returns the steps for a table of n rows: the rows of each percentage of the table, at
// least 10, each only once.
func steps(n int64) []step {
	var out []step
	var last int64
	for _, p := range percents {
		rows := max(10, int64(float64(n)*p/100+0.5))
		if rows <= last || rows > n/2+1 {
			continue
		}
		last = rows
		out = append(out, step{
			rows:  rows,
			pct:   float64(rows) / float64(n) * 100,
			width: max(1, int64(float64(rows)/float64(n)*dataset.ScoreRange)),
		})
	}
	return out
}

// The two orders of a step.
var orders = []struct {
	name    string
	ordered bool
}{{"unordered", false}, {"by-pk", true}}

// ranges makes the pool of ranges of a step: [lo, hi] of Score, the same in every run with the seed.
func ranges(seed uint64, idx int, s step) [][2]int64 {
	r := harness.NewRand(seed^0x5eed, uint64(idx)+1)
	pool := make([][2]int64, poolSize)
	for i := range pool {
		lo := r.Int64N(dataset.ScoreRange - s.width)
		pool[i] = [2]int64{lo, lo + s.width - 1}
	}
	return pool
}

var sink atomic.Int64

// env is the database of a system.
type env interface {
	sizes(ctx context.Context) (harness.Sizes, error)
	close(ctx context.Context)
}

func (workload) Variants(sys harness.System) []harness.Variant {
	switch sys.(type) {
	case *kvsys.System:
		sh := bookdb.NewShared(3, func(ctx context.Context, rc *harness.RunContext) (env, error) {
			return openKV(ctx, rc, "sweep")
		}, func(e env) { e.close(context.Background()) })
		return []harness.Variant{&variant{name: "planner", sh: sh}, &variant{name: "index", sh: sh}, &variant{name: "scan", sh: sh}}
	case *pgsys.System:
		sh := bookdb.NewShared(1, func(ctx context.Context, rc *harness.RunContext) (env, error) {
			return openPG(ctx, rc, "sweep")
		}, func(e env) { e.close(context.Background()) })
		return []harness.Variant{&variant{name: "planner", sh: sh}}
	}
	return nil
}

type variant struct {
	name string
	sh   *bookdb.Shared[env]
}

func (v *variant) Name() string { return v.name }

func (v *variant) Run(ctx context.Context, rc *harness.RunContext) error {
	defer v.sh.Release()
	e, err := v.sh.Acquire(ctx, rc)
	if err != nil {
		return err
	}
	rc.SizeFn = e.sizes

	for idx, s := range steps(int64(rc.Params.Scale)) {
		pool := ranges(rc.Params.Seed, idx, s)
		for _, o := range orders {
			var info map[string]string
			var op harness.OpFunc
			switch e := e.(type) {
			case *kvEnv:
				qs := make([]*query.Query[int64, book], len(pool))
				for i, r := range pool {
					qs[i] = kvQuery(e.kv.Store, v.name, o.ordered, r[0], r[1])
				}
				if info, err = e.plans(qs); err != nil {
					return fmt.Errorf("%s-%s: plans: %w", s.name(), o.name, err)
				}
				op = func(ctx context.Context, w *harness.Worker) error {
					q := qs[w.Rand.IntN(len(qs))]
					w.Begin()
					n, err := e.run(q)
					w.Add(n)
					return err
				}
			case *pgEnv:
				stmt := pgStatement(e.pg.Table, o.ordered)
				if info, err = e.plans(ctx, stmt, pool); err != nil {
					return fmt.Errorf("%s-%s: plans: %w", s.name(), o.name, err)
				}
				op = func(ctx context.Context, w *harness.Worker) error {
					r := pool[w.Rand.IntN(len(pool))]
					w.Begin()
					n, err := e.run(ctx, stmt, r[0], r[1], w.State.(*book))
					w.Add(n)
					return err
				}
			}
			for _, conc := range harness.Levels(rc.Params.Concurrency) {
				err := rc.Measure(ctx, harness.Case{
					Name: s.name() + "-" + o.name, Concurrency: conc, Unit: "rows", Info: info,
					Params: map[string]any{"selectivity_pct": s.pct, "target_rows": s.rows, "order": o.name},
					Init:   func(w *harness.Worker) { w.State = &book{} },
				}, op)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// --- kvutil

type kvEnv struct {
	kv *bookdb.KV
}

func (e *kvEnv) sizes(context.Context) (harness.Sizes, error) {
	n, err := e.kv.DB.DiskBytes()
	return harness.Sizes{Data: n}, err
}

func (e *kvEnv) close(context.Context) { e.kv.DB.Destroy() }

func openKV(ctx context.Context, rc *harness.RunContext, name string) (*kvEnv, error) {
	k, err := bookdb.OpenKV(ctx, rc.Sys.(*kvsys.System), name)
	if err != nil {
		return nil, err
	}
	e := &kvEnv{kv: k}
	rc.SizeFn = e.sizes
	spec, n := rc.Params.Dataset(), int64(rc.Params.Scale)

	err = bookdb.Phase(ctx, rc, "load", func(context.Context) (int64, error) { return n, k.Load(spec, n) })
	if err == nil {
		err = k.Settle()
	}
	if err == nil {
		err = bookdb.Phase(ctx, rc, "build", func(ctx context.Context) (int64, error) {
			_, err := k.Build(ctx, bookdb.AddScoreIndex)
			return n, err
		})
	}
	if err == nil {
		err = k.Settle()
	}
	if err != nil {
		k.DB.Destroy()
		return nil, err
	}
	return e, nil
}

// kvQuery is the query of a range of scores in a variant: as the planner plans it, or hinted to read
// the index or to scan the store.
func kvQuery(st *bookdb.Store, variant string, ordered bool, lo, hi int64) *query.Query[int64, book] {
	q := query.From(st).Where(bookdb.FScore.Between(lo, hi))
	if ordered {
		q = q.OrderBy(bookdb.FID)
	}
	switch variant {
	case "index":
		q = q.Using(bookdb.IdxScore)
	case "scan":
		q = q.FullScan()
	}
	return q
}

// run runs a query and reads the books it gives, as a client does.
func (e *kvEnv) run(q *query.Query[int64, book]) (int, error) {
	rows := 0
	err := e.kv.DB.View(func(tx kv.Tx) error {
		rows = 0
		it := q.Run(tx)
		defer it.Close()
		var bytes int64
		for ; it.Valid(); it.Next() {
			b, err := it.Value()
			if err != nil {
				return err
			}
			bytes += int64(len(b.Title))
			rows++
		}
		sink.Add(bytes)
		return it.Err()
	})
	return rows, err
}

func (e *kvEnv) ids(q *query.Query[int64, book]) ([]int64, error) {
	var ids []int64
	err := e.kv.DB.View(func(tx kv.Tx) error {
		ids = ids[:0]
		it := q.Run(tx)
		defer it.Close()
		for ; it.Valid(); it.Next() {
			if _, err := it.Value(); err != nil {
				return err
			}
			id, err := it.Key()
			if err != nil {
				return err
			}
			ids = append(ids, id)
		}
		return it.Err()
	})
	return ids, err
}

func (e *kvEnv) plans(qs []*query.Query[int64, book]) (map[string]string, error) {
	var s plans.Survey
	for i := 0; i < len(qs) && i < planSample; i++ {
		text, err := qs[i].Explain()
		if err != nil {
			return nil, err
		}
		s.Add(plans.KV(text))
	}
	return s.Info(), nil
}

// --- PostgreSQL

type pgEnv struct {
	pg *bookdb.PG
}

func (e *pgEnv) sizes(ctx context.Context) (harness.Sizes, error) { return e.pg.Sizes(ctx) }

func (e *pgEnv) close(ctx context.Context) { e.pg.Close(ctx) }

func openPG(ctx context.Context, rc *harness.RunContext, name string) (*pgEnv, error) {
	pg, err := bookdb.OpenPG(ctx, rc.Sys.(*pgsys.System), name)
	if err != nil {
		return nil, err
	}
	e := &pgEnv{pg: pg}
	rc.SizeFn = e.sizes
	spec, n := rc.Params.Dataset(), int64(rc.Params.Scale)

	err = bookdb.Phase(ctx, rc, "load", func(ctx context.Context) (int64, error) { return n, pg.Load(ctx, spec, n) })
	if err == nil {
		err = bookdb.Phase(ctx, rc, "build", func(ctx context.Context) (int64, error) {
			return n, pg.Build(ctx, bookdb.ScoreIndexDDL(pg.Table))
		})
	}
	if err == nil {
		err = pg.Settle(ctx)
	}
	if err != nil {
		pg.Close(context.WithoutCancel(ctx))
		return nil, err
	}
	return e, nil
}

// pgStatement is the prepared statement of a range of scores.
func pgStatement(table string, ordered bool) string {
	s := "SELECT " + bookdb.Columns + " FROM " + table + " WHERE score BETWEEN $1 AND $2"
	if ordered {
		s += " ORDER BY id"
	}
	return s
}

func (e *pgEnv) run(ctx context.Context, stmt string, lo, hi int64, b *book) (int, error) {
	rows, err := e.pg.Sys.Pool().Query(ctx, stmt, lo, hi)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	n := 0
	var bytes int64
	for rows.Next() {
		if err := bookdb.ScanBook(rows, b); err != nil {
			return n, err
		}
		bytes += int64(len(b.Title))
		n++
	}
	sink.Add(bytes)
	return n, rows.Err()
}

func (e *pgEnv) ids(ctx context.Context, stmt string, lo, hi int64) ([]int64, error) {
	rows, err := e.pg.Sys.Pool().Query(ctx, stmt, lo, hi)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	var b book
	for rows.Next() {
		if err := bookdb.ScanBook(rows, &b); err != nil {
			return nil, err
		}
		ids = append(ids, b.ID)
	}
	return ids, rows.Err()
}

func (e *pgEnv) plans(ctx context.Context, stmt string, pool [][2]int64) (map[string]string, error) {
	n := min(planSample, len(pool))
	sets := make([][]any, n)
	for i := range sets {
		sets[i] = []any{pool[i][0], pool[i][1]}
	}
	ps, err := plans.ExplainPrepared(ctx, e.pg.Sys.Pool(), stmt, sets)
	if err != nil {
		return nil, err
	}
	var s plans.Survey
	for _, p := range ps {
		s.Add(p)
	}
	return s.Info(), nil
}

// --- verification

// Verify has the system answer the first ranges of every step in both orders, with every
// variant: the ids of the books, which are in the order of the key when asked for, and as a set
// otherwise.
func (workload) Verify(ctx context.Context, sys harness.System, p harness.Params) ([]harness.Output, error) {
	rc := &harness.RunContext{Params: p, Sys: sys, Workload: "selectivity-sweep", Variant: "verify"}
	st := steps(int64(p.Scale))

	line := func(label string, ids []int64, ordered bool) string {
		ids = append([]int64(nil), ids...)
		if !ordered {
			sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		}
		parts := make([]string, len(ids))
		for i, id := range ids {
			parts[i] = strconv.FormatInt(id, 10)
		}
		sum := sha1.Sum([]byte(strings.Join(parts, ",")))
		return fmt.Sprintf("%s: %d rows digest=%s", label, len(ids), hex.EncodeToString(sum[:6]))
	}

	var outs []harness.Output
	switch sys.(type) {
	case *kvsys.System:
		e, err := openKV(ctx, rc, "sweep-verify")
		if err != nil {
			return nil, err
		}
		defer e.close(ctx)
		for _, name := range []string{"planner", "index", "scan"} {
			out := harness.Output{Variant: name}
			for idx, s := range st {
				pool := ranges(p.Seed, idx, s)
				for _, o := range orders {
					for i := 0; i < verifyRuns; i++ {
						ids, err := e.ids(kvQuery(e.kv.Store, name, o.ordered, pool[i][0], pool[i][1]))
						if err != nil {
							return nil, fmt.Errorf("%s %s-%s: %w", name, s.name(), o.name, err)
						}
						out.Lines = append(out.Lines, line(fmt.Sprintf("%s-%s #%d", s.name(), o.name, i), ids, o.ordered))
					}
				}
			}
			outs = append(outs, out)
		}
	case *pgsys.System:
		e, err := openPG(ctx, rc, "sweep-verify")
		if err != nil {
			return nil, err
		}
		defer e.close(ctx)
		out := harness.Output{Variant: "planner"}
		for idx, s := range st {
			pool := ranges(p.Seed, idx, s)
			for _, o := range orders {
				for i := 0; i < verifyRuns; i++ {
					ids, err := e.ids(ctx, pgStatement(e.pg.Table, o.ordered), pool[i][0], pool[i][1])
					if err != nil {
						return nil, fmt.Errorf("%s-%s: %w", s.name(), o.name, err)
					}
					out.Lines = append(out.Lines, line(fmt.Sprintf("%s-%s #%d", s.name(), o.name, i), ids, o.ordered))
				}
			}
		}
		outs = append(outs, out)
	}
	return outs, nil
}
