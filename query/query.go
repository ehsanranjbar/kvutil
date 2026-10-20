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
//
// # Several indexes
//
// A plan can read several indexes for one query, and combine what they find by the primary keys
// of the records. A disjunction that is one of the conditions of the query is read as a union if
// each of its branches is served by an index: a branch is read with its conditions and the other
// conditions of the query, so it is exact, and a record that several branches find is returned
// once. The records of a union come in no order. A disjunction of values of one field (Rank = 1
// OR Rank = 2) is one condition of the values (Rank IN (1, 2)) before anything is planned.
//
// A conjunction is read as an intersection if the indexes serve different conditions of it: one
// of them is streamed, the primary keys of the others are remembered, and a record of the
// streamed index is fetched only if all the others have it. The records come in the order of the
// streamed index. The conditions that the streamed index does not serve are checked on the
// records that are fetched, so the result is exact whatever the other indexes find.
//
// The planner takes such a plan if it costs less than the plans of one index, and if the keys it
// holds in memory are not more than MaxKeys allows, by its estimate. UsingAll forces one. The
// keys are held exactly if they are numbers of the same size, of 8 bytes at most (a bitmap), and
// otherwise as hashes, of which two keys may have the same, so a record whose hash is known is
// checked against the index that has it; Explain shows "(hashed)".
//
// # Projections
//
// A query gives records. Select makes of it a projection, which gives rows with the values of the
// items that are selected (fields, and values that an index covers), and Count gives the number
// of records. An index holds the fields of its key, the fields it includes (index.Including) and
// the values it covers (index.Covering), so it can give a row without the record, if it holds
// all that the rows need: that is an IndexOnlyScan in the plan, which costs no fetch of records.
// A row can still give its record (Row.Record), which is read when it is asked for.
package query

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
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
	// ErrNegative is returned for an offset, a limit or a number of keys (MaxKeys) that is less
	// than zero.
	ErrNegative = errors.New("query: negative offset, limit or number of keys")
)

// defaultMaxKeys is the number of keys that a plan may hold in memory unless the query says otherwise.
const defaultMaxKeys = 100_000

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
	hintAll
)

type hint struct {
	kind  hintKind
	index string
	// indexes are the indexes of a hintAll.
	indexes []string
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
	maxKeys int
	hint    hint

	// planned is the plan of this query, which is made once.
	planned *planned[K, V]
	// counting is the projection that Count runs, which is made once.
	counting *counting[K, V]
}

type planned[K, V any] struct {
	once sync.Once
	plan *plan[K, V]
	err  error
}

type counting[K, V any] struct {
	once sync.Once
	p    *Projection[K, V]
}

// From returns the query of all the records of a store.
func From[K, V any](store Store[K, V]) *Query[K, V] {
	return &Query[K, V]{store: store, maxKeys: defaultMaxKeys, planned: &planned[K, V]{}, counting: &counting[K, V]{}}
}

// derive returns a copy of the query, to change.
func (q *Query[K, V]) derive() *Query[K, V] {
	c := *q
	c.planned = &planned[K, V]{}
	c.counting = &counting[K, V]{}
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
// package documentation). It replaces the hint that the query had: FullScan or UsingAll.
func (q *Query[K, V]) Using(index string) *Query[K, V] {
	c := q.derive()
	c.hint = hint{kind: hintIndex, index: index}
	return c
}

// UsingAll returns the query with a hint to read all of these indexes, each for a part of the
// query: an intersection of them if they serve different conditions of the query, or a union of
// them if a disjunction of the query has branches that each is served by one of them, and all of
// them are used. It is an error, when the query is run or explained, if the store lacks one of
// the indexes or if neither can be done with exactly these indexes, or if the plan would hold
// more keys than MaxKeys allows. The last of Using, UsingAll and FullScan is the hint.
func (q *Query[K, V]) UsingAll(indexes ...string) *Query[K, V] {
	c := q.derive()
	c.hint = hint{kind: hintAll, indexes: slices.Clone(indexes)}
	return c
}

// MaxKeys returns the query with a limit on the keys of records that a plan may hold in memory,
// which it does to intersect the results of several indexes (the keys of all but one) and to
// join them (the keys that were seen). A plan that is estimated to hold more is not chosen, nor
// followed if it is hinted (UsingAll). The estimate is the one of the planner, without statistics
// of the data. The default is 100,000 keys, and 0 allows no plan that holds keys.
func (q *Query[K, V]) MaxKeys(n int) *Query[K, V] {
	c := q.derive()
	c.maxKeys = n
	return c
}

// FullScan returns the query with a hint to scan the store. It replaces the hint that the query
// had: Using or UsingAll.
func (q *Query[K, V]) FullScan() *Query[K, V] {
	c := q.derive()
	c.hint = hint{kind: hintScan}
	return c
}

// planOnce returns the plan of the query, which is made the first time.
func (q *Query[K, V]) planOnce() (*plan[K, V], error) {
	q.planned.once.Do(func() {
		q.planned.plan, q.planned.err = q.plan(nil)
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
		return iterutil.Of(&failed[K, *V]{err: err})
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
//   - IndexOnlyScan name: like IndexLookup, for a projection (Select) that the index answers from
//     its entries, without getting the records. It is shown instead of IndexLookup when the
//     index holds what is selected, what is checked, and what the rows are sorted by.
//   - Union: the records that one of its IndexLookup branches finds, each once, in no order. Each
//     branch is read with the conditions of its branch of the disjunction and the other conditions
//     of the query. "(hashed)" says that the keys it remembers are hashes.
//   - Intersect: the records that all of its children find, in the order of the first. The first
//     is an IndexLookup, which is streamed, and the conditions that are checked on the records
//     are shown there; the others are IndexKeys name, the primary keys of the entries of ranges
//     of an index that are remembered, shown "(hashed)" if they are kept as hashes.
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

// open starts the plan in a transaction, for the records that it finds.
func (pl *plan[K, V]) open(tx kv.Tx, store Store[K, V]) node[K, *V] {
	if pl.empty {
		return none[K, *V]{}
	}

	var n node[K, *V]
	if pl.combo != nil {
		combined, err := pl.combo.open(tx, pl.reversed)
		if err != nil {
			return &failed[K, *V]{err: err}
		}
		n = &source[K, *V]{Iterator: combined}
	} else if pl.index != nil {
		it, err := pl.index.Instantiate(tx).ScanAll(pl.ranges, pl.reversed)
		if err != nil {
			return &failed[K, *V]{err: fmt.Errorf("index %q: %w", pl.index.Name(), err)}
		}
		n = &source[K, *V]{Iterator: it}
	} else {
		records, err := store.Records(tx, kvutil.All[K]())
		if err != nil {
			return &failed[K, *V]{err: err}
		}
		n = &source[K, *V]{Iterator: records}
	}

	if len(pl.check) > 0 {
		n = newFilter(n, pl.check)
	}
	if !pl.ordered {
		n = newSorter(n, descendings(pl.order), recordSortKeys(pl.order), store.KeyCodec().Encode, pl.keep())
	}
	if pl.offset > 0 || pl.limited {
		n = newPage(n, pl.offset, pl.limit, pl.limited)
	}
	return n
}

// keep returns the number of the items that a sort keeps, which is 0 for all of them.
func (pl *plan[K, V]) keep() int {
	if !pl.topN {
		return 0
	}
	keep := pl.offset + pl.limit
	if keep <= 0 {
		keep = math.MaxInt
	}
	return keep
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

	leaf := &treeNode{}
	switch {
	case pl.combo != nil:
		leaf = pl.combo.tree(pl)
	case pl.index != nil:
		leaf.text = "IndexLookup " + pl.index.Name()
		if pl.indexOnly {
			leaf.text = "IndexOnlyScan " + pl.index.Name()
		}
		leaf.text += pl.orderText() + rangesText(len(pl.ranges))
	default:
		leaf.text = "StoreScan"
	}
	if pl.combo == nil && len(pl.check) > 0 {
		leaf.text += checkText(pl.check)
	}

	// The operators on top of each other, and the access under them.
	root := leaf
	for i := len(lines) - 1; i >= 0; i-- {
		root = &treeNode{text: lines[i], children: []*treeNode{root}}
	}
	return root.render()
}

// orderText returns the order that the plan relies on, as shown after the name of an index.
func (pl *plan[K, V]) orderText() string {
	switch {
	case pl.inOrder && pl.reversed:
		return fmt.Sprintf(" (reversed, in order of %s)", orderText(pl.wanted))
	case pl.inOrder:
		return fmt.Sprintf(" (in order of %s)", orderText(pl.wanted))
	}
	return ""
}

// rangesText returns the number of the ranges that an index reads, as shown after its name.
func rangesText(n int) string {
	if n == 1 {
		return ": 1 range"
	}
	return fmt.Sprintf(": %d ranges", n)
}

// checkText returns the conditions that the records are checked against, as shown after the access.
func checkText[V any](check []cond.Cond[V]) string {
	return fmt.Sprintf(", then check [%s]", conditionsText(check))
}

// treeNode is an operator of the text of a plan.
type treeNode struct {
	text     string
	children []*treeNode
}

// render returns the tree as text, a node on a line and its children under it, as in
//
//	Limit 10
//	└─ Intersect
//	   ├─ IndexLookup by_author: 1 range
//	   └─ IndexKeys by_tag: 1 range
func (n *treeNode) render() string {
	var b strings.Builder
	b.WriteString(n.text)
	n.renderChildren(&b, "")
	return b.String()
}

func (n *treeNode) renderChildren(b *strings.Builder, prefix string) {
	for i, child := range n.children {
		last := i == len(n.children)-1
		b.WriteString("\n" + prefix)
		if last {
			b.WriteString("└─ ")
		} else {
			b.WriteString("├─ ")
		}
		b.WriteString(child.text)
		if last {
			child.renderChildren(b, prefix+"   ")
		} else {
			child.renderChildren(b, prefix+"│  ")
		}
	}
}
