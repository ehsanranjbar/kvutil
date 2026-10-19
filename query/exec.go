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

// node is an operator of a plan: an iterator of records that can say why it stopped.
type node[K, V any] interface {
	iterutil.Iterator[K, *V]
	Err() error
}

// source wraps the iterator of a store or of an index as a node.
type source[K, V any] struct {
	iterutil.Iterator[K, *V]
	closed bool
}

// Err implements the node interface.
func (s *source[K, V]) Err() error {
	if e, ok := s.Iterator.(interface{ Err() error }); ok {
		return e.Err()
	}
	return nil
}

// Close implements the iterutil.Iterator interface. It can be called more than once.
func (s *source[K, V]) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.Iterator.Close()
}

// failed is the node of a query that stopped before its first record.
type failed[K, V any] struct {
	err error
}

func (f *failed[K, V]) Valid() bool           { return false }
func (f *failed[K, V]) Next()                 {}
func (f *failed[K, V]) Key() (k K, err error) { return k, f.err }
func (f *failed[K, V]) Value() (*V, error)    { return nil, f.err }
func (f *failed[K, V]) Close() error          { return nil }
func (f *failed[K, V]) Err() error            { return f.err }

// none is the node of a query that reads nothing.
type none[K, V any] struct{}

func (none[K, V]) Valid() bool         { return false }
func (none[K, V]) Next()               {}
func (none[K, V]) Key() (k K, _ error) { return k, nil }
func (none[K, V]) Value() (*V, error)  { return nil, nil }
func (none[K, V]) Close() error        { return nil }
func (none[K, V]) Err() error          { return nil }

// filter is the node of the records of another one that satisfy conditions.
type filter[K, V any] struct {
	base   node[K, V]
	conds  []cond.Cond[V]
	record *V
	err    error
}

func newFilter[K, V any](base node[K, V], conds []cond.Cond[V]) *filter[K, V] {
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

// row is a record that is sorted.
type row[K, V any] struct {
	key    K
	raw    []byte
	record *V
	sort   []sortKey
}

// sortKey is the value of a record for a field that it is sorted by.
type sortKey struct {
	missing bool
	key     []byte
}

// sorter is the node of the records of another one in an order, which it keeps in memory. It
// keeps the first n only, if it is given a number.
type sorter[K, V any] struct {
	base   node[K, V]
	order  []field.Component[V]
	n      int
	rows   []row[K, V]
	pos    int
	err    error
	closed bool
}

// newSorter reads all the records of the base, and keeps the ones that come first.
func newSorter[K, V any](
	base node[K, V],
	order []field.Component[V],
	encode func(K) ([]byte, error),
	n int,
) *sorter[K, V] {
	s := &sorter[K, V]{base: base, order: order, n: n}
	s.read(encode)
	return s
}

func (s *sorter[K, V]) read(encode func(K) ([]byte, error)) {
	defer func() {
		// The records are all read, or it is over: the base is not needed any more.
		s.err = firstError(s.err, s.base.Err())
		s.err = firstError(s.err, s.base.Close())
		s.closed = true
	}()

	var kept *rowHeap[K, V]
	if s.n > 0 {
		kept = &rowHeap[K, V]{order: s.order}
	}

	for ; s.base.Valid(); s.base.Next() {
		key, err := s.base.Key()
		if err != nil {
			s.err = err
			return
		}
		record, err := s.base.Value()
		if err != nil {
			s.err = err
			return
		}
		raw, err := encode(key)
		if err != nil {
			s.err = fmt.Errorf("failed to encode key %v: %w", key, err)
			return
		}

		r := row[K, V]{key: key, raw: raw, record: record}
		for _, f := range s.order {
			keys, err := f.Keys(record)
			if err != nil {
				s.err = err
				return
			}
			if len(keys) == 0 {
				r.sort = append(r.sort, sortKey{missing: true})
			} else {
				r.sort = append(r.sort, sortKey{key: keys[0]})
			}
		}

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
	slices.SortStableFunc(s.rows, func(a, b row[K, V]) int { return compareRows(s.order, a, b) })
}

// compareRows compares the records as they are sorted: by the fields in turn, with the records
// that have no value for a field before the others if it is ascending and after them if it is
// descending, and then by the primary key.
func compareRows[K, V any](order []field.Component[V], a, b row[K, V]) int {
	for i, f := range order {
		ka, kb := a.sort[i], b.sort[i]
		switch {
		case ka.missing && kb.missing:
			continue
		case ka.missing:
			if f.Descending() {
				return 1
			}
			return -1
		case kb.missing:
			if f.Descending() {
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
type rowHeap[K, V any] struct {
	order []field.Component[V]
	rows  []row[K, V]
}

func (h *rowHeap[K, V]) Len() int { return len(h.rows) }
func (h *rowHeap[K, V]) Less(i, j int) bool {
	return compareRows(h.order, h.rows[i], h.rows[j]) > 0
}
func (h *rowHeap[K, V]) Swap(i, j int) { h.rows[i], h.rows[j] = h.rows[j], h.rows[i] }
func (h *rowHeap[K, V]) Push(x any)    { h.rows = append(h.rows, x.(row[K, V])) }
func (h *rowHeap[K, V]) Pop() any {
	last := h.rows[len(h.rows)-1]
	h.rows[len(h.rows)-1] = row[K, V]{}
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
func (s *sorter[K, V]) Valid() bool { return s.err == nil && s.pos < len(s.rows) }

// Next implements the iterutil.Iterator interface.
func (s *sorter[K, V]) Next() {
	if s.Valid() {
		s.pos++
	}
}

// Key implements the iterutil.Iterator interface.
func (s *sorter[K, V]) Key() (key K, err error) {
	if s.err != nil || s.pos >= len(s.rows) {
		return key, s.err
	}
	return s.rows[s.pos].key, nil
}

// Value implements the iterutil.Iterator interface.
func (s *sorter[K, V]) Value() (*V, error) {
	if s.err != nil || s.pos >= len(s.rows) {
		return nil, s.err
	}
	return s.rows[s.pos].record, nil
}

// Err implements the node interface.
func (s *sorter[K, V]) Err() error { return s.err }

// Close implements the iterutil.Iterator interface.
func (s *sorter[K, V]) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.base.Close()
}

// page is the node of the records of another one from the offset on, for as many as the limit.
type page[K, V any] struct {
	base      node[K, V]
	remaining int
	limited   bool
}

func newPage[K, V any](base node[K, V], offset, limit int, limited bool) *page[K, V] {
	for ; offset > 0 && base.Valid(); offset-- {
		base.Next()
	}
	return &page[K, V]{base: base, remaining: limit, limited: limited}
}

// Valid implements the iterutil.Iterator interface.
func (p *page[K, V]) Valid() bool { return (!p.limited || p.remaining > 0) && p.base.Valid() }

// Next implements the iterutil.Iterator interface.
func (p *page[K, V]) Next() {
	if !p.Valid() {
		return
	}
	p.remaining--
	// What comes after the last record is not read.
	if !p.limited || p.remaining > 0 {
		p.base.Next()
	}
}

// Key implements the iterutil.Iterator interface.
func (p *page[K, V]) Key() (K, error) { return p.base.Key() }

// Value implements the iterutil.Iterator interface.
func (p *page[K, V]) Value() (*V, error) { return p.base.Value() }

// Err implements the node interface.
func (p *page[K, V]) Err() error { return p.base.Err() }

// Close implements the iterutil.Iterator interface.
func (p *page[K, V]) Close() error { return p.base.Close() }
