// Package query builds queries on the records of a store and plans them: it chooses between
// scanning the store and reading one of its indexes, and between sorting the records and reading
// them in the order of an index.
//
//	q := query.From(books).
//		Where(author.Eq("Herbert"), year.Gte(1965)).
//		OrderBy(year.Desc()).
//		Limit(10)
//
//	fmt.Println(q.Explain())
//	it := q.Run(tx)
//
// # Definitions
//
// A Query is a definition, which does not change: every method returns another query, so a
// query can be made in one place, extended in another, and run in any number of transactions
// and from any number of goroutines. A query is planned once, when it is first run or explained,
// with the indexes that the store has then.
//
// # Results
//
// A query that has an order (OrderBy) gives the records that satisfy all the conditions in the
// order of its fields, a field that is Desc from the greatest value to the least, with the
// records that have no value for a field before the others, or after them for a field that is
// Desc. Records that are equal in all the fields come in the order of their keys, as encoded.
// So a result does not depend on the plan, and Offset and Limit take the same records whichever
// index is read.
//
// A query that has no order gives the records in no order that is promised: a scan of the store
// gives them in the order of their keys, and an index in the order of its entries. So Offset and
// Limit of a query without an order page whatever order its plan gives, which can change when
// the indexes of the store do; they always give distinct records that satisfy the conditions,
// as many as are asked for and are there.
//
// # Plans
//
// The conditions are normalised, and then the cheapest of the ways to read the records is
// chosen: scanning the store, or reading one of its indexes. The costs are estimated without
// any statistics, with the numbers in plan.go. A plan sorts the records, unless the way it
// reads them gives the order that is asked for, which an index does if its fields that the
// conditions do not fix to a value are the ones that the order is of, in the same direction or
// all in the opposite one. Explain shows the plan.
//
// An index has no entry for a record that has no value for one of its fields, so an index is
// only read if none of the records that the query asks for lacks one: each of its fields is
// either one that every record has a value for (field.Always), or one that a comparison at the
// top of the conditions needs a value of. A condition that is a negation or a disjunction is
// not such a comparison. An index that no condition limits is read in full, then, if all its
// fields are Always, and costs what that costs: it is chosen for an order that it gives under
// a limit.
//
// An index that has a condition (index.Where) has the records that satisfy it only, so it is
// only read if the conditions of the query imply its condition: every record that the query
// asks for satisfies it. The conditions of the query imply it if each part of it is implied by
// a condition of the query, like Year >= 1970 by Year > 1980 or by Year = 1975, or Kind IN
// ('a', 'b') by Kind = 'a', and a part that is not a comparison, like a function, is implied by
// the identical condition only. The query is still checked against all its conditions, and its
// results do not depend on the index. Hinting an index (Using) whose condition is not implied
// is an error.
package query

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/store/ext"
)

var (
	// ErrHint is returned for a hint that cannot be followed: an index that the store does not
	// have or that cannot serve the query.
	ErrHint = errors.New("query: the hint cannot be followed")
	// ErrOrder is returned for a field that the records cannot be ordered by: one that may have
	// several values for a record.
	ErrOrder = errors.New("query: cannot order by the field")
	// ErrNegative is returned for an offset or a limit that is less than zero.
	ErrNegative = errors.New("query: negative offset or limit")
)

// Store is a store whose records can be queried: one that indexes can be defined on. It is
// implemented by the stores of the packages ext and record.
type Store[K, V any] interface {
	ext.Extensible[K, V]
	// Extensions returns the extensions of the store, which the indexes are among.
	Extensions() []any
}

type hintKind int

const (
	hintNone hintKind = iota
	hintIndex
	hintScan
)

type hint struct {
	kind  hintKind
	index string
}

// Query is a query on the records of a store. It is a definition, which every method leaves as
// it is, returning another one.
type Query[K, V any] struct {
	store   Store[K, V]
	conds   []cond.Cond[V]
	order   []field.Component[V]
	offset  int
	limit   int
	limited bool
	hint    hint

	// planned is the plan of this query, which is made once.
	planned *planned[K, V]
}

type planned[K, V any] struct {
	once sync.Once
	plan *plan[K, V]
	err  error
}

// From returns the query of all the records of a store.
func From[K, V any](store Store[K, V]) *Query[K, V] {
	return &Query[K, V]{store: store, planned: &planned[K, V]{}}
}

// derive returns a copy of the query, to change.
func (q *Query[K, V]) derive() *Query[K, V] {
	c := *q
	c.planned = &planned[K, V]{}
	return &c
}

// Where returns the query with more conditions, which the records must satisfy as well as the
// ones that the query has. Conditions of the same field that are given together are merged
// when the query is planned, and the ones that an index can serve are served by it.
func (q *Query[K, V]) Where(conds ...cond.Cond[V]) *Query[K, V] {
	c := q.derive()
	c.conds = slices.Clone(q.conds)
	for _, cnd := range conds {
		if cnd != nil {
			c.conds = append(c.conds, cnd)
		}
	}
	return c
}

// OrderBy returns the query with the records in the order of fields, replacing the order that
// the query had. A field that is Desc sorts from the greatest value to the least. The records
// that are equal in all the fields are in the order of their keys. A field may have one value
// for a record at most, and a record that has none comes before the others (after them, for a
// field that is Desc); a field of several values is an error when the query is run.
func (q *Query[K, V]) OrderBy(fields ...field.Component[V]) *Query[K, V] {
	c := q.derive()
	c.order = slices.Clone(fields)
	return c
}

// Offset returns the query without its first n records. Without an order (OrderBy) these are
// the first records of whatever order the plan gives.
func (q *Query[K, V]) Offset(n int) *Query[K, V] {
	c := q.derive()
	c.offset = n
	return c
}

// Limit returns the query with the first n records only. There is no limit unless one is set,
// and a limit of 0 returns nothing.
func (q *Query[K, V]) Limit(n int) *Query[K, V] {
	c := q.derive()
	c.limit, c.limited = n, true
	return c
}

// Using returns the query with a hint to read the index of that name. It is an error, when the
// query is run or explained, if the store has no such index or if the index cannot serve the
// query: it has some of the records only and the conditions of the query do not imply that
// they are among them, or it may not have an entry for a record that the query asks for (see the
// package documentation). It replaces FullScan.
func (q *Query[K, V]) Using(index string) *Query[K, V] {
	c := q.derive()
	c.hint = hint{kind: hintIndex, index: index}
	return c
}

// FullScan returns the query with a hint to scan the store. It replaces Using.
func (q *Query[K, V]) FullScan() *Query[K, V] {
	c := q.derive()
	c.hint = hint{kind: hintScan}
	return c
}

// planOnce returns the plan of the query, which is made the first time.
func (q *Query[K, V]) planOnce() (*plan[K, V], error) {
	q.planned.once.Do(func() {
		q.planned.plan, q.planned.err = q.plan()
	})
	return q.planned.plan, q.planned.err
}

// Run runs the query in a transaction. The iterator must be closed.
//
// An error of the plan, and an error while iterating, like one of a condition that cannot be
// evaluated on a record, stop the iterator and are returned by its Err.
func (q *Query[K, V]) Run(tx kv.Tx) iterutil.AdvancedIterator[K, *V] {
	pl, err := q.planOnce()
	if err != nil {
		return iterutil.Of(&failed[K, V]{err: err})
	}
	return iterutil.Of(pl.open(tx, q.store))
}

// Explain says how Run would run the query, as a tree of the operators, from the last one to
// the first, as in
//
//	Limit 10, offset 20
//	└─ TopN by Year desc, Title
//	   └─ IndexLookup by_author_year: 1 range, then check ["Title" LIKE '%Dune']
//
// The operators are:
//
//   - Limit n, offset m: the records from the offset, as many as the limit.
//   - Sort by fields, and TopN by fields: the records in the order of the fields, which are the
//     keys if it is "primary key". TopN keeps the first records that the limit needs.
//   - IndexLookup name: the entries of ranges of an index. The order that the entries give is
//     shown in brackets if the plan relies on it, as in "(reversed, in order of Year desc)",
//     which is then read from the end. After the colon come the number of ranges that are read
//     and the conditions that the records are checked against, if there are any.
//   - StoreScan: all the records of the store, with the conditions that they are checked against.
//   - Empty: nothing is read, as no record can satisfy the conditions or none is asked for.
//
// The same query always has the same text.
func (q *Query[K, V]) Explain() (string, error) {
	pl, err := q.planOnce()
	if err != nil {
		return "", err
	}
	return pl.String(), nil
}

// open starts the plan in a transaction.
func (pl *plan[K, V]) open(tx kv.Tx, store Store[K, V]) node[K, V] {
	if pl.empty {
		return none[K, V]{}
	}

	var n node[K, V]
	if pl.index != nil {
		it, err := pl.index.Instantiate(tx).ScanAll(pl.ranges, pl.reversed)
		if err != nil {
			return &failed[K, V]{err: fmt.Errorf("index %q: %w", pl.index.Name(), err)}
		}
		n = &source[K, V]{Iterator: it}
	} else {
		records, err := store.Records(tx, kvutil.All[K]())
		if err != nil {
			return &failed[K, V]{err: err}
		}
		n = &source[K, V]{Iterator: records}
	}

	if len(pl.check) > 0 {
		n = newFilter(n, pl.check)
	}
	if !pl.ordered {
		keep := 0
		if pl.topN {
			keep = pl.offset + pl.limit
			if keep <= 0 {
				keep = math.MaxInt
			}
		}
		n = newSorter(n, pl.order, store.KeyCodec().Encode, keep)
	}
	if pl.offset > 0 || pl.limited {
		n = newPage(n, pl.offset, pl.limit, pl.limited)
	}
	return n
}

// String returns the plan as a tree.
func (pl *plan[K, V]) String() string {
	if pl.empty {
		return "Empty"
	}

	var lines []string
	switch {
	case pl.limited && pl.offset > 0:
		lines = append(lines, fmt.Sprintf("Limit %d, offset %d", pl.limit, pl.offset))
	case pl.limited:
		lines = append(lines, fmt.Sprintf("Limit %d", pl.limit))
	case pl.offset > 0:
		lines = append(lines, fmt.Sprintf("Offset %d", pl.offset))
	}
	if !pl.ordered {
		name := "Sort"
		if pl.topN {
			name = "TopN"
		}
		lines = append(lines, fmt.Sprintf("%s by %s", name, orderText(pl.order)))
	}

	var access string
	if pl.index != nil {
		access = "IndexLookup " + pl.index.Name()
		switch {
		case pl.inOrder && pl.reversed:
			access += fmt.Sprintf(" (reversed, in order of %s)", orderText(pl.wanted))
		case pl.inOrder:
			access += fmt.Sprintf(" (in order of %s)", orderText(pl.wanted))
		}
		noun := "ranges"
		if len(pl.ranges) == 1 {
			noun = "range"
		}
		access += fmt.Sprintf(": %d %s", len(pl.ranges), noun)
	} else {
		access = "StoreScan"
	}
	if len(pl.check) > 0 {
		access += fmt.Sprintf(", then check [%s]", conditionsText(pl.check))
	}
	lines = append(lines, access)

	var text string
	for i, line := range lines {
		if i == 0 {
			text = line
			continue
		}
		text += "\n"
		for range i - 1 {
			text += "   "
		}
		text += "└─ " + line
	}
	return text
}
