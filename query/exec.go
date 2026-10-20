package query

import (
	"bytes"
	"container/heap"
	"fmt"
	"slices"

	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/iterutil"
)

// node is an operator of a plan: an iterator that can say why it stopped. What it iterates over
// is the records of a query (T is *V), or the rows of a projection (T is *Row).
type node[K, T any] interface {
	iterutil.Iterator[K, T]
	Err() error
}

// source wraps the iterator of a store or of an index as a node.
type source[K, T any] struct {
	iterutil.Iterator[K, T]
	closed bool
}

// Err implements the node interface.
func (s *source[K, T]) Err() error {
	if e, ok := s.Iterator.(interface{ Err() error }); ok {
		return e.Err()
	}
	return nil
}

// Close implements the iterutil.Iterator interface. It can be called more than once.
func (s *source[K, T]) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.Iterator.Close()
}

// failed is the node of a query that stopped before its first record.
type failed[K, T any] struct {
	err error
}

func (f *failed[K, T]) Valid() bool           { return false }
func (f *failed[K, T]) Next()                 {}
func (f *failed[K, T]) Key() (k K, err error) { return k, f.err }
func (f *failed[K, T]) Value() (t T, err error) {
	return t, f.err
}
func (f *failed[K, T]) Close() error { return nil }
func (f *failed[K, T]) Err() error   { return f.err }

// none is the node of a query that reads nothing.
type none[K, T any] struct{}

func (none[K, T]) Valid() bool         { return false }
func (none[K, T]) Next()               {}
func (none[K, T]) Key() (k K, _ error) { return k, nil }
func (none[K, T]) Value() (t T, _ error) {
	return t, nil
}
func (none[K, T]) Close() error { return nil }
func (none[K, T]) Err() error   { return nil }

// filter is the node of the records of another one that satisfy conditions.
type filter[K, V any] struct {
	base   node[K, *V]
	conds  []cond.Cond[V]
	record *V
	err    error
}

func newFilter[K, V any](base node[K, *V], conds []cond.Cond[V]) *filter[K, V] {
	f := &filter[K, V]{base: base, conds: conds}
	f.settle()
	return f
}

// settle moves to the first record, from where the base is, that satisfies the conditions.
func (f *filter[K, V]) settle() {
	f.record = nil
	for f.err == nil && f.base.Valid() {
		record, err := f.base.Value()
		if err != nil {
			f.err = err
			return
		}

		ok := true
		for _, c := range f.conds {
			ok, err = c.Match(record)
			if err != nil {
				f.err = fmt.Errorf("failed to check %s: %w", c, err)
				return
			}
			if !ok {
				break
			}
		}
		if ok {
			f.record = record
			return
		}
		f.base.Next()
	}
}

// Valid implements the iterutil.Iterator interface.
func (f *filter[K, V]) Valid() bool { return f.err == nil && f.base.Valid() }

// Next implements the iterutil.Iterator interface.
func (f *filter[K, V]) Next() {
	if !f.Valid() {
		return
	}
	f.base.Next()
	f.settle()
}

// Key implements the iterutil.Iterator interface.
func (f *filter[K, V]) Key() (K, error) { return f.base.Key() }

// Value implements the iterutil.Iterator interface.
func (f *filter[K, V]) Value() (*V, error) {
	if f.record != nil {
		return f.record, nil
	}
	return f.base.Value()
}

// Err implements the node interface.
func (f *filter[K, V]) Err() error {
	if f.err != nil {
		return f.err
	}
	return f.base.Err()
}

// Close implements the iterutil.Iterator interface.
func (f *filter[K, V]) Close() error { return f.base.Close() }

// row is something that is sorted.
type row[K, T any] struct {
	key   K
	raw   []byte
	value T
	sort  []sortKey
}

// sortKey is the value of a record for a field that it is sorted by.
type sortKey struct {
	missing bool
	key     []byte
}

// recordSortKeys returns the sort keys of a record for fields.
func recordSortKeys[V any](order []field.Component[V]) func(*V) ([]sortKey, error) {
	return func(record *V) ([]sortKey, error) {
		sort := make([]sortKey, 0, len(order))
		for _, f := range order {
			keys, err := f.Keys(record)
			if err != nil {
				return nil, err
			}
			if len(keys) == 0 {
				sort = append(sort, sortKey{missing: true})
			} else {
				sort = append(sort, sortKey{key: keys[0]})
			}
		}
		return sort, nil
	}
}

// descendings returns which of the fields are descending.
func descendings[V any](order []field.Component[V]) []bool {
	desc := make([]bool, len(order))
	for i, f := range order {
		desc[i] = f.Descending()
	}
	return desc
}

// sorter is the node of the items of another one in an order, which it keeps in memory. It
// keeps the first n only, if it is given a number.
type sorter[K, T any] struct {
	base   node[K, T]
	desc   []bool
	n      int
	rows   []row[K, T]
	pos    int
	err    error
	closed bool
}

// newSorter reads all the items of the base, and keeps the ones that come first. The keys are
// what the items are sorted by, given by the function, and desc says which of them are descending.
func newSorter[K, T any](
	base node[K, T],
	desc []bool,
	keys func(T) ([]sortKey, error),
	encode func(K) ([]byte, error),
	n int,
) *sorter[K, T] {
	s := &sorter[K, T]{base: base, desc: desc, n: n}
	s.read(keys, encode)
	return s
}

func (s *sorter[K, T]) read(keys func(T) ([]sortKey, error), encode func(K) ([]byte, error)) {
	defer func() {
		// The items are all read, or it is over: the base is not needed any more.
		s.err = firstError(s.err, s.base.Err())
		s.err = firstError(s.err, s.base.Close())
		s.closed = true
	}()

	var kept *rowHeap[K, T]
	if s.n > 0 {
		kept = &rowHeap[K, T]{desc: s.desc}
	}

	for ; s.base.Valid(); s.base.Next() {
		key, err := s.base.Key()
		if err != nil {
			s.err = err
			return
		}
		value, err := s.base.Value()
		if err != nil {
			s.err = err
			return
		}
		raw, err := encode(key)
		if err != nil {
			s.err = fmt.Errorf("failed to encode key %v: %w", key, err)
			return
		}
		sort, err := keys(value)
		if err != nil {
			s.err = err
			return
		}

		r := row[K, T]{key: key, raw: raw, value: value, sort: sort}
		if kept == nil {
			s.rows = append(s.rows, r)
			continue
		}
		heap.Push(kept, r)
		if kept.Len() > s.n {
			heap.Pop(kept)
		}
	}
	if s.base.Err() != nil {
		return
	}

	if kept != nil {
		s.rows = kept.rows
	}
	slices.SortStableFunc(s.rows, func(a, b row[K, T]) int { return compareRows(s.desc, a, b) })
}

// compareRows compares the rows as they are sorted: by the fields in turn, with the rows that
// have no value for a field before the others if it is ascending and after them if it is
// descending, and then by the primary key.
func compareRows[K, T any](desc []bool, a, b row[K, T]) int {
	for i, descending := range desc {
		ka, kb := a.sort[i], b.sort[i]
		switch {
		case ka.missing && kb.missing:
			continue
		case ka.missing:
			if descending {
				return 1
			}
			return -1
		case kb.missing:
			if descending {
				return -1
			}
			return 1
		}
		if c := bytes.Compare(ka.key, kb.key); c != 0 {
			return c
		}
	}
	return bytes.Compare(a.raw, b.raw)
}

// rowHeap keeps the rows with the one that comes last on top.
type rowHeap[K, T any] struct {
	desc []bool
	rows []row[K, T]
}

func (h *rowHeap[K, T]) Len() int { return len(h.rows) }
func (h *rowHeap[K, T]) Less(i, j int) bool {
	return compareRows(h.desc, h.rows[i], h.rows[j]) > 0
}
func (h *rowHeap[K, T]) Swap(i, j int) { h.rows[i], h.rows[j] = h.rows[j], h.rows[i] }
func (h *rowHeap[K, T]) Push(x any)    { h.rows = append(h.rows, x.(row[K, T])) }
func (h *rowHeap[K, T]) Pop() any {
	last := h.rows[len(h.rows)-1]
	h.rows[len(h.rows)-1] = row[K, T]{}
	h.rows = h.rows[:len(h.rows)-1]
	return last
}

func firstError(a, b error) error {
	if a != nil {
		return a
	}
	return b
}

// Valid implements the iterutil.Iterator interface.
func (s *sorter[K, T]) Valid() bool { return s.err == nil && s.pos < len(s.rows) }

// Next implements the iterutil.Iterator interface.
func (s *sorter[K, T]) Next() {
	if s.Valid() {
		s.pos++
	}
}

// Key implements the iterutil.Iterator interface.
func (s *sorter[K, T]) Key() (key K, err error) {
	if s.err != nil || s.pos >= len(s.rows) {
		return key, s.err
	}
	return s.rows[s.pos].key, nil
}

// Value implements the iterutil.Iterator interface.
func (s *sorter[K, T]) Value() (value T, err error) {
	if s.err != nil || s.pos >= len(s.rows) {
		return value, s.err
	}
	return s.rows[s.pos].value, nil
}

// Err implements the node interface.
func (s *sorter[K, T]) Err() error { return s.err }

// Close implements the iterutil.Iterator interface.
func (s *sorter[K, T]) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.base.Close()
}

// page is the node of the items of another one from the offset on, for as many as the limit.
type page[K, T any] struct {
	base      node[K, T]
	remaining int
	limited   bool
}

func newPage[K, T any](base node[K, T], offset, limit int, limited bool) *page[K, T] {
	for ; offset > 0 && base.Valid(); offset-- {
		base.Next()
	}
	return &page[K, T]{base: base, remaining: limit, limited: limited}
}

// Valid implements the iterutil.Iterator interface.
func (p *page[K, T]) Valid() bool { return (!p.limited || p.remaining > 0) && p.base.Valid() }

// Next implements the iterutil.Iterator interface.
func (p *page[K, T]) Next() {
	if !p.Valid() {
		return
	}
	p.remaining--
	// What comes after the last item is not read.
	if !p.limited || p.remaining > 0 {
		p.base.Next()
	}
}

// Key implements the iterutil.Iterator interface.
func (p *page[K, T]) Key() (K, error) { return p.base.Key() }

// Value implements the iterutil.Iterator interface.
func (p *page[K, T]) Value() (T, error) { return p.base.Value() }

// Err implements the node interface.
func (p *page[K, T]) Err() error { return p.base.Err() }

// Close implements the iterutil.Iterator interface.
func (p *page[K, T]) Close() error { return p.base.Close() }
