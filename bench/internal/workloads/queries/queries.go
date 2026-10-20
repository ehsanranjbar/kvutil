// Package queries is the workload of the queries that a catalogue of books answers, on kvutil and
// PostgreSQL with the same indexes: lookups by a unique code and by a skewed author, ranges, a
// composite index in the order asked for, a sort, deep paging, lists, unions and intersections of
// indexes, tags, prefixes, an expression, a conditional index, an index-only selection and a
// count (see cases).
//
// The variants are the ways of asking, on one database that they share, which is loaded and
// indexed by the first of them (its load and build are reported with it):
//
//	kvutil      typed     the query is built from typed conditions for each operation, and planned
//	            sql       the WHERE clause is parsed (and planned) for each operation
//	            prepared  the query is built, parsed and planned beforehand, one for each of the
//	                      arguments of a pool, and only run
//	PostgreSQL  prepared  the statement is prepared once per connection, and run with parameters
//	            simple    the statement is sent as text with the values in it, with the simple
//	                      protocol: the server parses and plans it every time
//
// Each case runs with the arguments of a pool of 1,024 sets that is the same in every variant and in
// both systems, picked at random by each operation. The plans each system chose are in the
// results (info: plan, plan_nodes, plans_seen).
package queries

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/ehsanranjbar/kvutil/bench/internal/bookdb"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/kvsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/pgsys"
)

func init() { harness.Register(workload{}) }

const (
	// poolSize is the number of sets of arguments that a case picks from.
	poolSize = 1024
	// planSample is the number of sets of arguments whose plans are recorded.
	planSample = 20
	// verifyRuns is the number of sets of arguments of a case that are verified.
	verifyRuns = 4
)

type workload struct{}

func (workload) Name() string  { return "queries" }
func (workload) Group() string { return "query" }
func (workload) Description() string {
	return "17 queries on a catalogue of books with 8 indexes: typed, SQL text and prepared in kvutil; prepared and simple protocol in PostgreSQL; with the plans"
}

// env is a database with the books in it, which the variants of a system share.
type env interface {
	sizes(ctx context.Context) (harness.Sizes, error)
	close(ctx context.Context)
}

func newDataOf(rc *harness.RunContext) *data {
	return newData(rc.Params.Dataset(), int64(rc.Params.Scale))
}

func (workload) Variants(sys harness.System) []harness.Variant {
	switch sys.(type) {
	case *kvsys.System:
		sh := bookdb.NewShared(3, func(ctx context.Context, rc *harness.RunContext) (env, error) {
			return openKV(ctx, rc, "queries", newDataOf(rc))
		}, func(e env) { e.close(context.Background()) })
		return []harness.Variant{
			&variant{name: "typed", sh: sh}, &variant{name: "sql", sh: sh}, &variant{name: "prepared", sh: sh},
		}
	case *pgsys.System:
		sh := bookdb.NewShared(2, func(ctx context.Context, rc *harness.RunContext) (env, error) {
			return openPG(ctx, rc, "queries", newDataOf(rc))
		}, func(e env) { e.close(context.Background()) })
		return []harness.Variant{&variant{name: "prepared", sh: sh}, &variant{name: "simple", sh: sh}}
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

	for _, c := range cases() {
		info, err := v.plans(ctx, e, c)
		if err != nil {
			return fmt.Errorf("%s: plans: %w", c.name, err)
		}
		op := v.op(ctx, e, c)
		for _, conc := range harness.Levels(rc.Params.Concurrency) {
			err := rc.Measure(ctx, harness.Case{
				Name: c.name, Concurrency: conc, Unit: "rows", Info: info,
				Params: map[string]any{"pool": poolSize},
				Init:   func(w *harness.Worker) { w.State = &book{} },
			}, op)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (v *variant) plans(ctx context.Context, e env, c *qcase) (map[string]string, error) {
	switch e := e.(type) {
	case *kvEnv:
		return e.plans(c, v.name)
	case *pgEnv:
		return e.plans(ctx, c, v.name == "simple")
	}
	return nil, fmt.Errorf("unknown database %T", e)
}

// op returns the operation of a case in this way of asking.
func (v *variant) op(ctx context.Context, e env, c *qcase) harness.OpFunc {
	switch e := e.(type) {
	case *kvEnv:
		pool := e.pools[c.name]
		return func(ctx context.Context, w *harness.Worker) error {
			ent := &pool[w.Rand.IntN(len(pool))]
			w.Begin()
			var k kvq
			switch v.name {
			case "typed":
				k = c.typed(e.kv.Store, ent.a)
			case "sql":
				var err error
				if k, err = c.parsed(e.parser, ent.text, ent.a); err != nil {
					return err
				}
			default:
				k = ent.prep
			}
			o, err := e.exec(k)
			w.Add(o.rows)
			return err
		}
	case *pgEnv:
		pool := e.pools[c.name]
		simple := v.name == "simple"
		return func(ctx context.Context, w *harness.Worker) error {
			ent := &pool[w.Rand.IntN(len(pool))]
			w.Begin()
			o, err := e.run(ctx, c, ent, simple, w.State.(*book))
			w.Add(o.rows)
			return err
		}
	}
	panic("unknown database")
}

// entry is a set of arguments of a case, with what each system needs for it made beforehand.
type entry struct {
	a args
	// text is the WHERE clause for kvutil.
	text string
	// prep is the query for kvutil, built beforehand (the prepared way).
	prep kvq
	// pgArgs are the parameters of the prepared statement, and pgText the statement with the
	// values in it.
	pgArgs []any
	pgText string
}

// buildPools makes the sets of arguments of every case, the same in every run with the seed. table
// is the table of PostgreSQL, or empty for kvutil.
func buildPools(d *data, seed uint64, table string) map[string][]entry {
	pools := map[string][]entry{}
	for i, c := range cases() {
		r := harness.NewRand(seed^0xb007, uint64(i)+1)
		pool := make([]entry, poolSize)
		for j := range pool {
			a := c.gen(d, r)
			pool[j].a = a
			if table == "" {
				pool[j].text = c.text(a)
			} else {
				pool[j].pgArgs = c.pgArgs(a)
				pool[j].pgText = c.pgStatement(table, a)
			}
		}
		pools[c.name] = pool
	}
	return pools
}

// answer is what a query returned, in a form that does not depend on the system.
type answer struct {
	// ids and lines are the books (their ids, and their canonical text) or the rows that a
	// projection gave, in the order that they came in.
	ids   []int64
	lines []string
	count int
}

// format returns the lines of the verification for an answer: the number of rows and a digest of
// them, in the order of the case if it has one and sorted otherwise, and the ids if they are few.
func (c *qcase) format(a args, ans answer) string {
	label := c.label(a)
	if c.kind == countKind {
		return fmt.Sprintf("%s: count=%d", label, ans.count)
	}
	lines := ans.lines
	ids := ans.ids
	if !c.ordered {
		lines = append([]string(nil), lines...)
		sort.Strings(lines)
		ids = append([]int64(nil), ids...)
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	}
	sum := sha1.Sum([]byte(strings.Join(lines, "\n")))
	s := fmt.Sprintf("%s: %d rows digest=%s", label, len(lines), hex.EncodeToString(sum[:6]))
	if len(ids) > 0 && len(ids) <= 40 {
		parts := make([]string, len(ids))
		for i, id := range ids {
			parts[i] = itoa(id)
		}
		s += " ids=" + strings.Join(parts, ",")
	}
	return s
}

// Verify opens the database of the system once, and has each way of asking answer the first
// arguments of every case. All the answers must be the same, in every way and on both systems:
// the books in the order of the case if it is ordered, as a set if it is not, and the counts.
func (workload) Verify(ctx context.Context, sys harness.System, p harness.Params) ([]harness.Output, error) {
	rc := &harness.RunContext{Params: p, Sys: sys, Workload: "queries", Variant: "verify"}
	d := newData(p.Dataset(), int64(p.Scale))
	cs := cases()

	switch sys.(type) {
	case *kvsys.System:
		e, err := openKV(ctx, rc, "queries-verify", d)
		if err != nil {
			return nil, err
		}
		defer e.close(ctx)
		outs := map[string]*harness.Output{}
		order := []string{"typed", "sql", "prepared"}
		for _, w := range order {
			outs[w] = &harness.Output{Variant: w}
		}
		for _, c := range cs {
			for i := 0; i < verifyRuns; i++ {
				ent := &e.pools[c.name][i]
				for _, w := range order {
					var k kvq
					switch w {
					case "typed":
						k = c.typed(e.kv.Store, ent.a)
					case "sql":
						var err error
						if k, err = c.parsed(e.parser, ent.text, ent.a); err != nil {
							return nil, fmt.Errorf("%s: %w", c.label(ent.a), err)
						}
					default:
						k = ent.prep
					}
					ans, err := e.answer(c, k)
					if err != nil {
						return nil, fmt.Errorf("%s (%s): %w", c.label(ent.a), w, err)
					}
					outs[w].Lines = append(outs[w].Lines, c.format(ent.a, ans))
				}
			}
		}
		var res []harness.Output
		for _, w := range order {
			res = append(res, *outs[w])
		}
		return res, nil

	case *pgsys.System:
		e, err := openPG(ctx, rc, "queries-verify", d)
		if err != nil {
			return nil, err
		}
		defer e.close(ctx)
		outs := map[string]*harness.Output{}
		order := []string{"prepared", "simple"}
		for _, w := range order {
			outs[w] = &harness.Output{Variant: w}
		}
		for _, c := range cs {
			for i := 0; i < verifyRuns; i++ {
				ent := &e.pools[c.name][i]
				for _, w := range order {
					ans, err := e.answer(ctx, c, ent, w == "simple")
					if err != nil {
						return nil, fmt.Errorf("%s (%s): %w", c.label(ent.a), w, err)
					}
					outs[w].Lines = append(outs[w].Lines, c.format(ent.a, ans))
				}
			}
		}
		var res []harness.Output
		for _, w := range order {
			res = append(res, *outs[w])
		}
		return res, nil
	}
	return nil, fmt.Errorf("unknown system %T", sys)
}
