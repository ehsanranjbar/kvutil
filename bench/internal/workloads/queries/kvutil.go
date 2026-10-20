package queries

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/ehsanranjbar/kvutil/bench/internal/bookdb"
	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/plans"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/kvsys"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/query"
	sqlq "github.com/ehsanranjbar/kvutil/query/sql"
)

// kvEnv is the database of kvutil with the books and their indexes.
type kvEnv struct {
	kv     *bookdb.KV
	parser *sqlq.Parser[int64, book]
	pools  map[string][]entry
}

func (e *kvEnv) sizes(context.Context) (harness.Sizes, error) {
	n, err := e.kv.DB.DiskBytes()
	return harness.Sizes{Data: n}, err
}

func (e *kvEnv) close(context.Context) { e.kv.DB.Destroy() }

// openKV creates the database, loads the books and builds the indexes, as phases of the run that
// asked for it.
func openKV(ctx context.Context, rc *harness.RunContext, name string, d *data) (*kvEnv, error) {
	k, err := bookdb.OpenKV(ctx, rc.Sys.(*kvsys.System), name)
	if err != nil {
		return nil, err
	}
	e := &kvEnv{kv: k}
	rc.SizeFn = e.sizes

	n := int64(rc.Params.Scale)
	err = bookdb.Phase(ctx, rc, "load", func(context.Context) (int64, error) {
		return n, k.Load(d.spec, n)
	})
	if err == nil {
		err = k.Settle()
	}
	if err == nil {
		err = bookdb.Phase(ctx, rc, "build", func(ctx context.Context) (int64, error) {
			names, err := k.Build(ctx, bookdb.AddQueryIndexes)
			rc.Logf("   built %s", strings.Join(names, ", "))
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

	e.parser = sqlq.For(k.Store)
	e.pools = buildPools(d, rc.Params.Seed, "")
	cs := cases()
	for _, c := range cs {
		for i := range e.pools[c.name] {
			ent := &e.pools[c.name][i]
			ent.prep = c.typed(k.Store, ent.a)
		}
	}
	return e, nil
}

// kvq is a query of kvutil ready to run: a query that gives books, a projection that gives rows,
// or a query that is counted.
type kvq struct {
	q     *kvQry
	p     *query.Projection[int64, book]
	count bool
}

// finish puts the order and the page of the case on a query with its conditions.
func (c *qcase) finish(q *kvQry, a args) kvq {
	if c.shape != nil {
		q = c.shape(q, a)
	}
	switch c.kind {
	case selectKind:
		return kvq{p: q.Select(c.items...)}
	case countKind:
		return kvq{q: q, count: true}
	}
	return kvq{q: q}
}

// typed builds the query from the conditions as typed values.
func (c *qcase) typed(st *bookdb.Store, a args) kvq {
	q := query.From(st)
	if c.conds != nil {
		q = q.Where(c.conds(a)...)
	}
	return c.finish(q, a)
}

// parsed builds the query from the text of its conditions.
func (c *qcase) parsed(p *sqlq.Parser[int64, book], text string, a args) (kvq, error) {
	q, err := p.Where(text)
	if err != nil {
		return kvq{}, err
	}
	return c.finish(q, a), nil
}

func (k kvq) explain() (string, error) {
	switch {
	case k.p != nil:
		return k.p.Explain()
	case k.count:
		// Count is a projection of the keys, with no order.
		return k.q.Select().Explain()
	}
	return k.q.Explain()
}

// sink keeps the values that the queries read from being optimised away.
var sink atomic.Int64

// outcome is what a run of a query gave: how many books or rows, or the count.
type outcome struct {
	rows, count int
}

// run runs the query in a transaction and reads everything it gives, as a client does: the books
// are decoded, the values of the rows are read.
func (k kvq) run(tx kv.Tx) (outcome, error) {
	var o outcome
	switch {
	case k.count:
		n, err := k.q.Count(tx)
		o.count = n
		return o, err

	case k.p != nil:
		it := k.p.Run(tx)
		defer it.Close()
		var bytes int64
		for ; it.Valid(); it.Next() {
			row, err := it.Value()
			if err != nil {
				return o, err
			}
			title, _ := row.Get(bookdb.FTitle)
			price, _ := row.Get(bookdb.FPrice)
			bytes += int64(len(title)) + price
			o.rows++
		}
		sink.Add(bytes)
		return o, it.Err()
	}

	it := k.q.Run(tx)
	defer it.Close()
	var bytes int64
	for ; it.Valid(); it.Next() {
		b, err := it.Value()
		if err != nil {
			return o, err
		}
		bytes += int64(len(b.Title))
		o.rows++
	}
	sink.Add(bytes)
	return o, it.Err()
}

func (e *kvEnv) exec(k kvq) (outcome, error) {
	var o outcome
	err := e.kv.DB.View(func(tx kv.Tx) error {
		var err error
		o, err = k.run(tx)
		return err
	})
	return o, err
}

// answer runs the query and returns what it gave in a form that is the same for any system: see
// answerOf.
func (e *kvEnv) answer(c *qcase, k kvq) (answer, error) {
	var ans answer
	err := e.kv.DB.View(func(tx kv.Tx) error {
		ans = answer{}
		switch {
		case k.count:
			n, err := k.q.Count(tx)
			ans.count = n
			return err

		case k.p != nil:
			it := k.p.Run(tx)
			defer it.Close()
			for ; it.Valid(); it.Next() {
				row, err := it.Value()
				if err != nil {
					return err
				}
				title, _ := row.Get(bookdb.FTitle)
				price, _ := row.Get(bookdb.FPrice)
				ans.lines = append(ans.lines, fmt.Sprintf("%s|%d", title, price))
			}
			return it.Err()
		}

		it := k.q.Run(tx)
		defer it.Close()
		for ; it.Valid(); it.Next() {
			b, err := it.Value()
			if err != nil {
				return err
			}
			// The id is the key, which a book read from a store has set.
			key, err := it.Key()
			if err != nil {
				return err
			}
			b.ID = key
			ans.ids = append(ans.ids, key)
			ans.lines = append(ans.lines, dataset.BookCanon(b))
		}
		return it.Err()
	})
	return ans, err
}

// kvPlans records the plans that the query has for the first arguments of the pool.
func (e *kvEnv) plans(c *qcase, way string) (map[string]string, error) {
	var s plans.Survey
	pool := e.pools[c.name]
	for i := 0; i < len(pool) && i < planSample; i++ {
		var k kvq
		var err error
		if way == "typed" {
			k = c.typed(e.kv.Store, pool[i].a)
		} else if k, err = c.parsed(e.parser, pool[i].text, pool[i].a); err != nil {
			return nil, err
		}
		text, err := k.explain()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", c.label(pool[i].a), err)
		}
		s.Add(plans.KV(text))
	}
	return s.Info(), nil
}
