package query

import (
	"fmt"
	"math"

	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
)

// Nearness orders the records of a store by their distance from something, which an index of the
// store measures: the nearest first. The vector indexes of package vector make one with To.
type Nearness[K, V any] interface {
	// IndexName returns the name of the index that measures the distance.
	IndexName() string
	// Describe says how the index searches, for Explain, like "partitioned, quantized". It may be
	// empty.
	Describe() string
	// Nearest runs the search in a transaction and returns the records nearest first, at most limit
	// of them (all if limit is negative), that satisfy all the conditions, which it checks on the
	// records. If among is not nil, only its records are candidates, and Nearest closes it whatever
	// it returns. The iterator that it returns has a method Distance() float32, which is the
	// distance of its current record.
	Nearest(
		tx kv.Tx,
		among iterutil.Iterator[K, *V],
		conds []cond.Cond[V],
		limit int,
	) (iterutil.Iterator[K, *V], error)
}

// Nearest returns the query with its records in the order of their distance (the nearest first,
// and by key at the same distance), replacing the order that the query had (OrderBy). A record
// that the index has no distance for (one without a vector) is not among its records. OrderBy
// after Nearest replaces it too.
//
// The search is done one of two ways, see the package documentation: it checks the conditions on
// the records, nearest first, or it ranks the records that an access path of the conditions finds.
// It panics if n is nil.
func (q *Query[K, V]) Nearest(n Nearness[K, V]) *Query[K, V] {
	if n == nil {
		panic("query: the nearness of a query must not be nil")
	}
	c := q.derive()
	c.near = n
	c.order = nil
	return c
}

// distancer is a node that can tell the distance of its current record from what the query is
// ordered by. The nodes that wrap another one pass it through.
type distancer interface {
	// distance returns the distance, and false if the node has none or is not at a record.
	distance() (float32, bool)
}

// Distance returns the distance of the current record of an iterator that a query with Nearest
// gave, as the index measures it, and false for any other iterator or when it is not at a record.
func Distance[K, V any](it iterutil.Iterator[K, *V]) (float32, bool) {
	if advanced, ok := it.(iterutil.AdvancedIterator[K, *V]); ok {
		it = advanced.Iterator
	}
	d, ok := it.(distancer)
	if !ok {
		return 0, false
	}
	return d.distance()
}

// Distance returns the distance of the record of the row from what the query is ordered by (see
// Nearest), and false for a row of a query without one.
func (r *Row[K, V]) Distance() (float32, bool) {
	return r.dist, r.hasDist
}

// ranked is the node of the records that a Nearness gave.
type ranked[K, V any] struct {
	source[K, *V]
}

// distance implements the distancer interface.
func (r *ranked[K, V]) distance() (float32, bool) {
	d, ok := r.Iterator.(interface{ Distance() float32 })
	if !ok || !r.Valid() {
		return 0, false
	}
	return d.Distance(), true
}

// nearestLimit returns the number of records that the search is asked for, which is a negative
// number for all of them: the offset is skipped by a page on top of it.
func (pl *plan[K, V]) nearestLimit() int {
	if !pl.limited || pl.limit > math.MaxInt-pl.offset {
		return -1
	}
	return pl.offset + pl.limit
}

// openNearest starts the search in a transaction. The records that an access path finds are what
// it ranks if the plan says so, and otherwise it checks the conditions on the records itself.
func (pl *plan[K, V]) openNearest(tx kv.Tx, store Store[K, V]) node[K, *V] {
	var (
		among iterutil.Iterator[K, *V]
		conds []cond.Cond[V]
	)
	if pl.prefilter {
		access, err := pl.openAccess(tx, store)
		if err != nil {
			return &failed[K, *V]{err: err}
		}
		among = access
	} else {
		conds = pl.check
	}

	it, err := pl.near.Nearest(tx, among, conds, pl.nearestLimit())
	if err != nil {
		return &failed[K, *V]{err: fmt.Errorf("nearest %q: %w", pl.near.IndexName(), err)}
	}
	var n node[K, *V] = &ranked[K, V]{source[K, *V]{Iterator: it}}
	if pl.offset > 0 || pl.limited {
		n = newPage(n, pl.offset, pl.limit, pl.limited)
	}
	return n
}

// nearestText returns the operator of the search as text, for Explain.
func (pl *plan[K, V]) nearestText() string {
	text := "Nearest " + pl.near.IndexName()
	if d := pl.near.Describe(); d != "" {
		text += " (" + d + ")"
	}
	return text
}

// planNearest chooses how to run a query that is ordered by a nearness: the search checks the
// conditions on the records, or it ranks the records that an access path of the conditions
// finds. The access path is chosen as for a query without an order and a page.
func (q *Query[K, V]) planNearest() (*plan[K, V], error) {
	if q.hint.kind == hintAll {
		return nil, fmt.Errorf("%w: UsingAll reads several indexes, and a query with Nearest is "+
			"ranked by the search", ErrHint)
	}

	// The access path is planned for what the search is given, which is read in full whatever the
	// limit is, in no order.
	base := q.derive()
	base.near, base.order = nil, nil
	base.offset, base.limit, base.limited = 0, 0, false
	postFilter := q.hint.kind == hintIndex && q.hint.index == q.near.IndexName()
	if postFilter {
		base.hint = hint{}
	}
	e, pl, err := base.prepare(nil, nil)
	if err != nil {
		return nil, err
	}
	if pl.empty || (q.limited && q.limit == 0) {
		return &plan[K, V]{empty: true}, nil
	}

	pl.offset, pl.limit, pl.limited = q.offset, q.limit, q.limited
	pl.near, pl.ordered = q.near, true
	if postFilter || (q.hint.kind == hintNone && len(e.conj) == 0) {
		pl.check = e.conj
		return pl, nil
	}

	best, err := e.choose()
	if err != nil {
		return nil, err
	}
	if q.hint.kind == hintNone && !best.prefiltered() {
		pl.check = e.conj
		return pl, nil
	}
	e.fill(pl, best)
	pl.prepareDecode(q.store)
	pl.prefilter, pl.ordered = true, true
	return pl, nil
}

// prefiltered reports whether the search ranks the records that the candidate finds, when nothing
// says how it is to be done: it is an index or a combination of them, and finds few records.
func (c *candidate[K, V]) prefiltered() bool {
	if c.index == nil && c.combo == nil {
		return false
	}
	rows := c.rows
	if len(c.check) > 0 {
		rows *= residualPass
	}
	return rows <= prefilterRows
}
