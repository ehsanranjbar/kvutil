package query

import (
	"fmt"
	"slices"

	"github.com/ehsanranjbar/kvutil/codec/lex"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

// Projection is a query that gives rows, which have the values of the items that were selected
// instead of the records. It is a definition like the query it is made from, and is planned once.
// An index that holds what a projection needs answers it without the records being read; see
// Select.
type Projection[K, V any] struct {
	q     *Query[K, V]
	items []field.Item[V]
	// pos is the position of each item among the items, by what identifies it.
	pos     map[string]int
	planned *planned[K, V]
}

// Select returns the projection of the query on items: fields, or values that an index covers
// (index.Cover). A row has the values of the items, and the key of the record. Without items a row
// has the key only. The conditions, the order and the page are those of the query.
//
// A row is made from the entries of an index, with no record read, when one index serves the
// query and holds what it needs of the records: every selected item (as a field of its key that
// has one value at most for a record, as a field that it includes with index.Including, or as a
// cover), every condition that it does not serve with its ranges (a comparison of a field that the
// index holds, or conjunctions, disjunctions and negations of such), and, when the rows are sorted,
// every field of the order. Anything else is read from the record, which is got for the plan.
// Explain shows an IndexOnlyScan for the first. The values of a row do not depend on the plan:
// they are the same as those that the items have for the record.
func (q *Query[K, V]) Select(items ...field.Item[V]) *Projection[K, V] {
	p := &Projection[K, V]{q: q, pos: map[string]int{}, planned: &planned[K, V]{}}
	for _, item := range items {
		if item == nil {
			continue
		}
		id := itemID(item)
		if _, ok := p.pos[id]; ok {
			continue
		}
		p.pos[id] = len(p.items)
		p.items = append(p.items, item)
	}
	return p
}

// itemID returns what identifies an item among those that are selected: a cover by its name, and
// a field by its path, which are different things even if they are spelled the same.
func itemID[V any](item field.Item[V]) string {
	if _, ok := item.(index.CoverItem[V]); ok {
		return "cover:" + item.Path()
	}
	return "field:" + item.Path()
}

// planOnce returns the plan of the projection, which is made the first time.
func (p *Projection[K, V]) planOnce() (*plan[K, V], error) {
	p.planned.once.Do(func() {
		p.planned.plan, p.planned.err = p.q.plan(&selection[V]{items: p.items})
	})
	return p.planned.plan, p.planned.err
}

// Run runs the projection in a transaction. The iterator must be closed. Its keys are the keys of
// the records, and its values are the rows.
//
// An error of the plan, and an error while iterating, stop the iterator and are returned by its
// Err. A row can be used after the iterator has moved on, but not after the transaction is over.
func (p *Projection[K, V]) Run(tx kv.Tx) iterutil.AdvancedIterator[K, *Row[K, V]] {
	pl, err := p.planOnce()
	if err != nil {
		return iterutil.Of(&failed[K, *Row[K, V]]{err: err})
	}
	return iterutil.Of(pl.openRows(tx, p))
}

// Explain says how Run would run the projection, as Query.Explain does for a query. The plan can
// be an IndexOnlyScan, which makes the rows from the entries of an index:
//
//	Limit 10
//	└─ IndexOnlyScan by_author_year (in order of Year desc): 1 range, then check [Year >= 1970]
func (p *Projection[K, V]) Explain() (string, error) {
	pl, err := p.planOnce()
	if err != nil {
		return "", err
	}
	return pl.String(), nil
}

// Count runs the query and returns the number of records that it gives: the ones that satisfy the
// conditions, from the offset on and as many as the limit. No record is read where an index holds
// what the conditions need, and the order is not what is counted, so it is left out. It is the
// number of rows of a projection without items, whose plan Select(...).Explain shows.
func (q *Query[K, V]) Count(tx kv.Tx) (int, error) {
	q.counting.once.Do(func() {
		c := q.derive()
		c.order = nil
		q.counting.p = c.Select()
	})

	it := q.counting.p.Run(tx)
	defer it.Close()

	n := 0
	for ; it.Valid(); it.Next() {
		n++
	}
	return n, it.Err()
}

// rowRun is what the rows of a run share: the projection, and the way to get a record.
type rowRun[K, V any] struct {
	proj   *Projection[K, V]
	store  Store[K, V]
	tx     kv.Tx
	getter extstore.Getter[K, V]
}

// get returns the record of a key, which it gets from the store.
func (r *rowRun[K, V]) get(key K) (*V, error) {
	if r.getter == nil {
		r.getter = r.store.Getter(r.tx)
	}
	return r.getter.Get(key)
}

// Row is a row of a projection: the key of a record, the values of the items that were selected,
// and the record, which is only read if it is asked for and the plan did not need it. A row stays
// valid while the transaction it was made in is.
type Row[K, V any] struct {
	key    K
	run    *rowRun[K, V]
	values [][]any
	record *V
	// sort are the keys that the row is sorted by, when it is made from an index and sorted.
	sort []sortKey
}

// Key returns the key of the record of the row.
func (r *Row[K, V]) Key() K {
	return r.key
}

// Get returns the first value that the record has for an item that was selected. It reports
// false if the item was not selected, or if the record has no value for it.
func (r *Row[K, V]) Get[F any](s field.Selectable[V, F]) (F, bool) {
	all, ok := r.GetAll(s)
	if !ok {
		var zero F
		return zero, false
	}
	return all[0], true
}

// GetAll returns all the values that the record has for an item that was selected, which are
// several for an item that may have several. It reports false if the item was not selected, or if
// the record has no value for it.
func (r *Row[K, V]) GetAll[F any](s field.Selectable[V, F]) ([]F, bool) {
	i, ok := r.run.proj.pos[itemID[V](s)]
	if !ok || len(r.values[i]) == 0 {
		return nil, false
	}

	values := make([]F, 0, len(r.values[i]))
	for _, value := range r.values[i] {
		f, ok := value.(F)
		if !ok {
			return nil, false
		}
		values = append(values, f)
	}
	return values, true
}

// Record returns the record of the row. If the plan did not read it, it is got from the store
// when it is first asked for, in the transaction the query is run in; the same record is returned
// the next time.
func (r *Row[K, V]) Record() (*V, error) {
	if r.record != nil {
		return r.record, nil
	}
	record, err := r.run.get(r.key)
	if err != nil {
		return nil, fmt.Errorf("failed to get the record %v: %w", r.key, err)
	}
	r.record = record
	return record, nil
}

// openRows starts the plan in a transaction, for the rows of a projection.
func (pl *plan[K, V]) openRows(tx kv.Tx, p *Projection[K, V]) node[K, *Row[K, V]] {
	store := p.q.store
	if pl.empty {
		return none[K, *Row[K, V]]{}
	}
	run := &rowRun[K, V]{proj: p, store: store, tx: tx}
	if !pl.indexOnly {
		return &recordRows[K, V]{base: pl.open(tx, store), run: run}
	}

	it, err := pl.index.Instantiate(tx).ScanAll(pl.ranges, pl.reversed)
	if err != nil {
		return &failed[K, *Row[K, V]]{err: fmt.Errorf("index %q: %w", pl.index.Name(), err)}
	}
	var n node[K, *Row[K, V]] = newIndexRows(it, pl, run)
	if !pl.ordered {
		n = newSorter(n, descendings(pl.order),
			func(r *Row[K, V]) ([]sortKey, error) { return r.sort, nil },
			store.KeyCodec().Encode, pl.keep())
	}
	if pl.offset > 0 || pl.limited {
		n = newPage(n, pl.offset, pl.limit, pl.limited)
	}
	return n
}

// recordRows is the node of the rows of records that another node finds.
type recordRows[K, V any] struct {
	base node[K, *V]
	run  *rowRun[K, V]
	row  *Row[K, V]
}

// Valid implements the iterutil.Iterator interface.
func (r *recordRows[K, V]) Valid() bool { return r.base.Valid() }

// Next implements the iterutil.Iterator interface.
func (r *recordRows[K, V]) Next() {
	r.row = nil
	r.base.Next()
}

// Key implements the iterutil.Iterator interface.
func (r *recordRows[K, V]) Key() (K, error) { return r.base.Key() }

// Value implements the iterutil.Iterator interface. The record is read when the row is asked for.
func (r *recordRows[K, V]) Value() (*Row[K, V], error) {
	if r.row != nil {
		return r.row, nil
	}
	key, err := r.base.Key()
	if err != nil {
		return nil, err
	}
	record, err := r.base.Value()
	if err != nil {
		return nil, err
	}

	row := &Row[K, V]{key: key, run: r.run, record: record, values: make([][]any, len(r.run.proj.items))}
	for i, item := range r.run.proj.items {
		row.values[i] = item.AnyValues(record)
	}
	r.row = row
	return row, nil
}

// Err implements the node interface.
func (r *recordRows[K, V]) Err() error { return r.base.Err() }

// Close implements the iterutil.Iterator interface.
func (r *recordRows[K, V]) Close() error { return r.base.Close() }

// indexRows is the node of the rows that are made from the entries of an index, without the
// records. It skips the entries that the check, which is on the entries, does not accept.
type indexRows[K, V any] struct {
	it   *index.Iterator[K, V]
	pl   *plan[K, V]
	run  *rowRun[K, V]
	row  *Row[K, V]
	err  error
	done bool
}

func newIndexRows[K, V any](it *index.Iterator[K, V], pl *plan[K, V], run *rowRun[K, V]) *indexRows[K, V] {
	n := &indexRows[K, V]{it: it, pl: pl, run: run}
	n.settle()
	return n
}

// settle moves to the first entry, from where the iterator is, that satisfies the check.
func (n *indexRows[K, V]) settle() {
	n.row = nil
	for n.err == nil && n.it.Valid() {
		ok := true
		for _, check := range n.pl.entryChecks {
			var err error
			if ok, err = check(n.it); err != nil {
				n.err = err
				return
			}
			if !ok {
				break
			}
		}
		if ok {
			return
		}
		n.it.Next()
	}
}

// Valid implements the iterutil.Iterator interface.
func (n *indexRows[K, V]) Valid() bool { return n.err == nil && n.it.Valid() }

// Next implements the iterutil.Iterator interface.
func (n *indexRows[K, V]) Next() {
	if !n.Valid() {
		return
	}
	n.it.Next()
	n.settle()
}

// Key implements the iterutil.Iterator interface.
func (n *indexRows[K, V]) Key() (K, error) { return n.it.Key() }

// Value implements the iterutil.Iterator interface.
func (n *indexRows[K, V]) Value() (*Row[K, V], error) {
	if n.err != nil {
		return nil, n.err
	}
	if n.row != nil {
		return n.row, nil
	}

	key, err := n.it.Key()
	if err != nil {
		return nil, err
	}
	row := &Row[K, V]{key: key, run: n.run, values: make([][]any, len(n.pl.accesses))}
	for i, access := range n.pl.accesses {
		if row.values[i], err = n.it.Values(access); err != nil {
			return nil, err
		}
	}

	if !n.pl.ordered {
		// What the rows are sorted by, as the records give it: encoded, and inverted for a
		// field that is descending.
		for i, f := range n.pl.order {
			encoded, err := n.it.Encoded(n.pl.sortAccesses[i])
			if err != nil {
				return nil, err
			}
			if len(encoded) == 0 {
				row.sort = append(row.sort, sortKey{missing: true})
				continue
			}
			bz := slices.Clone(encoded[0])
			if f.Descending() {
				lex.Invert(bz)
			}
			row.sort = append(row.sort, sortKey{key: bz})
		}
	}
	n.row = row
	return row, nil
}

// Err implements the node interface.
func (n *indexRows[K, V]) Err() error { return firstError(n.err, n.it.Err()) }

// Close implements the iterutil.Iterator interface.
func (n *indexRows[K, V]) Close() error {
	if n.done {
		return nil
	}
	n.done = true
	return n.it.Close()
}
