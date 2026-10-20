package query

import (
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
)

// Combined is a plan that reads several indexes, which the planner considered, for the tests to
// run whatever it costs and however many keys it holds.
type Combined[K, V any] struct {
	q  *Query[K, V]
	pl *plan[K, V]

	// Union is set for a union of the indexes, and not set for an intersection.
	Union bool
	// Indexes are the names of the indexes that the plan reads, the streamed one first for an
	// intersection.
	Indexes []string
}

// CombinedPlans returns every plan that reads several indexes which the planner considers for the
// query: the one it would choose among them for each disjunction, a few ways to read the branches
// of each disjunction, and the intersections of up to four of the indexes, each with every index
// streamed. It ignores the hint and MaxKeys.
func (q *Query[K, V]) CombinedPlans() ([]*Combined[K, V], error) {
	order, err := q.orderFields()
	if err != nil {
		return nil, err
	}
	e, skeleton, err := q.prepare(nil, order)
	if err != nil || skeleton.empty {
		return nil, err
	}

	var plans []*Combined[K, V]
	for _, c := range e.allCombined() {
		pl := *skeleton
		e.fill(&pl, c)
		p := &Combined[K, V]{q: q, pl: &pl, Union: c.combo.union}
		for _, part := range c.combo.parts {
			p.Indexes = append(p.Indexes, part.index.Name())
		}
		plans = append(plans, p)
	}
	return plans, nil
}

// Explain returns the plan as text.
func (c *Combined[K, V]) Explain() string {
	return c.pl.String()
}

// Run runs the plan for the records.
func (c *Combined[K, V]) Run(tx kv.Tx) iterutil.AdvancedIterator[K, *V] {
	return iterutil.Of(c.pl.open(tx, c.q.store))
}

// RunRows runs the plan for the rows of a projection on items.
func (c *Combined[K, V]) RunRows(tx kv.Tx, items ...field.Item[V]) iterutil.AdvancedIterator[K, *Row[K, V]] {
	return iterutil.Of(c.pl.openRows(tx, c.q.Select(items...)))
}
