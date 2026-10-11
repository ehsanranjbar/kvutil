package index

import (
	"fmt"

	"github.com/ehsanranjbar/kvutil/kv"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	refstore "github.com/ehsanranjbar/kvutil/store/ref"
)

// Instance is an Index in a transaction, which records are looked up with.
type Instance[K, V any] struct {
	x      *Index[K, V]
	tx     kv.Tx
	refs   *refstore.Instance
	getter extstore.Getter[K, V]
}

// Scan iterates over the entries whose keys are in the range, in ascending order of the keys.
// The iterator must be closed.
func (i *Instance[K, V]) Scan(r KeyRange) (*Iterator[K, V], error) {
	return i.ScanAll([]KeyRange{r}, false)
}

// ReverseScan iterates over the entries whose keys are in the range, in descending order of the keys.
// The iterator must be closed.
func (i *Instance[K, V]) ReverseScan(r KeyRange) (*Iterator[K, V], error) {
	return i.ScanAll([]KeyRange{r}, true)
}

// ScanAll iterates over the entries whose keys are in the ranges, one range after the other in
// the order that they are given. A record is returned once, even if it has an entry in more
// than one range. The iterator must be closed.
func (i *Instance[K, V]) ScanAll(ranges []KeyRange, reverse bool) (*Iterator[K, V], error) {
	it := &Iterator[K, V]{
		ins:     i,
		ranges:  ranges,
		reverse: reverse,
	}
	// A record can only come up twice if it has several entries, or if there are several ranges.
	if i.x.desc.MultiEntry || len(ranges) > 1 {
		it.seen = make(map[string]struct{})
	}

	it.settle()
	if it.err != nil {
		it.Close()
		return nil, it.err
	}
	return it, nil
}

// Lookup iterates over the entries of the records that may satisfy the conditions, if the
// indexer of the index can translate them. The conditions that the index does not enforce are
// returned as the residual, which the records must still be checked against.
func (i *Instance[K, V]) Lookup(conds ...any) (it *Iterator[K, V], residual []any, err error) {
	l, ok := i.x.indexer.(Lookuper)
	if !ok {
		return nil, nil, fmt.Errorf("index %q cannot be looked up with conditions", i.x.name)
	}

	ranges, residual, err := l.Lookup(conds...)
	if err != nil {
		return nil, nil, err
	}

	it, err = i.ScanAll(ranges, false)
	if err != nil {
		return nil, nil, err
	}
	return it, residual, nil
}

// bounds returns the range of stored keys that corresponds to a range of index keys.
func bounds(r KeyRange) (start, end []byte) {
	if low := r.Low(); !low.IsEmpty() {
		start = low.Value()
		if low.Exclusive() {
			// Past all the keys that begin with the bound. If there is nothing past them the
			// range is empty, which a start that is its own end stands for.
			start = kv.PrefixEnd(start)
			if start == nil {
				return []byte{0x00}, []byte{0x00}
			}
		}
	}

	if high := r.High(); !high.IsEmpty() {
		end = high.Value()
		if !high.Exclusive() {
			end = kv.PrefixEnd(end)
		} else if len(end) == 0 {
			return []byte{0x00}, []byte{0x00}
		}
	}
	return start, end
}

// Iterator iterates over the entries of an index. Its keys are the primary keys of the records
// that the entries belong to and its values are the records, which are only read when asked for.
type Iterator[K, V any] struct {
	ins     *Instance[K, V]
	ranges  []KeyRange
	reverse bool

	next int                 // the next range to open
	cur  *refstore.Iterator  // the iterator of the current range
	seen map[string]struct{} // the primary keys that were returned, if they have to be remembered
	err  error
}

// settle moves to the first entry, from where the iterator is, that belongs to a record that
// was not returned yet.
func (it *Iterator[K, V]) settle() {
	for it.err == nil {
		if it.cur == nil {
			if it.next >= len(it.ranges) {
				return
			}

			start, end := bounds(it.ranges[it.next])
			it.next++
			if it.reverse {
				it.cur, it.err = it.ins.refs.ReverseIterator(start, end)
			} else {
				it.cur, it.err = it.ins.refs.Iterator(start, end)
			}
			continue
		}

		if !it.cur.Valid() {
			it.err = it.cur.Close()
			it.cur = nil
			continue
		}
		if it.seen == nil {
			return
		}

		pk, err := it.cur.PrimaryKey()
		if err != nil {
			it.err = err
			return
		}
		if _, ok := it.seen[string(pk)]; !ok {
			it.seen[string(pk)] = struct{}{}
			return
		}
		it.cur.Next()
	}
}

// Valid implements the kvutil.Iterator interface.
func (it *Iterator[K, V]) Valid() bool {
	return it.err == nil && it.cur != nil
}

// Next implements the kvutil.Iterator interface.
func (it *Iterator[K, V]) Next() {
	if !it.Valid() {
		return
	}
	it.cur.Next()
	it.settle()
}

// Err returns the error that stopped the iterator early, if there is one.
func (it *Iterator[K, V]) Err() error {
	return it.err
}

// Key returns the primary key of the record that the current entry belongs to.
func (it *Iterator[K, V]) Key() (key K, err error) {
	pk, err := it.cur.PrimaryKey()
	if err != nil {
		return key, err
	}

	key, err = it.ins.x.store.KeyCodec().Decode(pk)
	if err != nil {
		return key, fmt.Errorf("failed to decode primary key: %w", err)
	}
	return key, nil
}

// Value returns the record that the current entry belongs to, which it gets from the store.
func (it *Iterator[K, V]) Value() (*V, error) {
	key, err := it.Key()
	if err != nil {
		return nil, err
	}

	if it.ins.getter == nil {
		it.ins.getter = it.ins.x.store.Getter(it.ins.tx)
	}
	return it.ins.getter.Get(key)
}

// IndexKey returns the key of the current entry.
// The slice must not be modified and is valid until the iterator moves.
func (it *Iterator[K, V]) IndexKey() ([]byte, error) {
	return it.cur.Key()
}

// RawKey returns the primary key of the record as the store encodes it.
// The slice must not be modified and is valid until the iterator moves.
func (it *Iterator[K, V]) RawKey() ([]byte, error) {
	return it.cur.PrimaryKey()
}

// Covered returns the value that is stored with the current entry.
// The slice must not be modified and is valid until the iterator moves.
func (it *Iterator[K, V]) Covered() ([]byte, error) {
	return it.cur.Value()
}

// Close implements the kvutil.Iterator interface.
func (it *Iterator[K, V]) Close() error {
	if it.cur == nil {
		return nil
	}

	err := it.cur.Close()
	it.cur = nil
	it.next = len(it.ranges)
	return err
}
