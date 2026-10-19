package index

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/ehsanranjbar/kvutil/cond"
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
func (i *Instance[K, V]) Scan(r Bounds) (*Iterator[K, V], error) {
	return i.ScanAll([]Bounds{r}, false)
}

// ReverseScan iterates over the entries whose keys are in the range, in descending order of the keys.
// The iterator must be closed.
func (i *Instance[K, V]) ReverseScan(r Bounds) (*Iterator[K, V], error) {
	return i.ScanAll([]Bounds{r}, true)
}

// ScanAll iterates over the entries whose keys are in the ranges, one range after the other in
// the order that they are given. A record is returned once, even if it has an entry in more
// than one range. The iterator must be closed.
func (i *Instance[K, V]) ScanAll(ranges []Bounds, reverse bool) (*Iterator[K, V], error) {
	return i.scan(ranges, reverse, nil)
}

func (i *Instance[K, V]) scan(ranges []Bounds, reverse bool, filter []cond.Cond[V]) (*Iterator[K, V], error) {
	it := &Iterator[K, V]{
		ins:     i,
		ranges:  ranges,
		reverse: reverse,
		filter:  filter,
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

// Lookup iterates over the records of the index that satisfy all the conditions, in ascending
// order of the keys of the index. The index is used for the conditions that it can serve, and
// the records that it finds are checked against the others, so the result is exact.
// It fails if the indexer of the index cannot translate conditions, and with ErrIncomplete if
// the index may not have a record that satisfies them: if a field that records may have no
// value for is left free by the conditions (see Index.Complete).
func (i *Instance[K, V]) Lookup(conds ...cond.Cond[V]) (*Iterator[K, V], error) {
	return i.lookup(conds, false)
}

// ReverseLookup is like Lookup, in descending order of the keys of the index.
func (i *Instance[K, V]) ReverseLookup(conds ...cond.Cond[V]) (*Iterator[K, V], error) {
	return i.lookup(conds, true)
}

func (i *Instance[K, V]) lookup(conds []cond.Cond[V], reverse bool) (*Iterator[K, V], error) {
	ranges, residual, err := i.Plan(conds...)
	if err != nil {
		return nil, err
	}
	if reverse {
		slices.Reverse(ranges)
	}
	return i.scan(ranges, reverse, residual)
}

// Plan returns how the index would serve conditions: the ranges of keys that it would read and
// the conditions that it would have to check on the records that it finds. It fails with
// ErrIncomplete if the index may not have all the records that satisfy the conditions; see
// Index.Complete.
func (i *Instance[K, V]) Plan(conds ...cond.Cond[V]) (ranges []Bounds, residual []cond.Cond[V], err error) {
	l, ok := i.x.indexer.(RangePlanner[V])
	if !ok {
		return nil, nil, fmt.Errorf("index %q cannot be looked up with conditions", i.x.name)
	}
	if err := i.x.Complete(conds...); err != nil {
		return nil, nil, err
	}
	return l.Ranges(conds...)
}

// bounds returns the range of stored keys that corresponds to a range of index keys.
func bounds(r Bounds) (start, end []byte) {
	if low, exclusive, ok := r.Low(); ok {
		start = low
		if exclusive {
			// Past all the keys that begin with the bound. If there is nothing past them the
			// range is empty, which a start that is its own end stands for.
			start = kv.PrefixEnd(start)
			if start == nil {
				return []byte{0x00}, []byte{0x00}
			}
		}
	}

	if high, exclusive, ok := r.High(); ok {
		end = high
		if !exclusive {
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
	ranges  []Bounds
	reverse bool

	next   int                 // the next range to open
	cur    *refstore.Iterator  // the iterator of the current range
	seen   map[string]struct{} // the primary keys that were returned, if they have to be remembered
	filter []cond.Cond[V]      // the conditions that the records are checked against
	record *V                  // the record of the current entry, once it is read
	err    error
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

		it.record = nil
		if !it.cur.Valid() {
			it.err = it.cur.Close()
			it.cur = nil
			continue
		}

		if it.seen != nil {
			pk, err := it.cur.PrimaryKey()
			if err != nil {
				it.err = err
				return
			}
			if _, ok := it.seen[string(pk)]; ok {
				it.cur.Next()
				continue
			}
			it.seen[string(pk)] = struct{}{}
		}

		ok, err := it.matches()
		if err != nil {
			it.err = err
			return
		}
		if ok {
			return
		}
		it.cur.Next()
	}
}

// matches reports whether the record of the current entry satisfies the conditions that the
// index does not enforce.
func (it *Iterator[K, V]) matches() (bool, error) {
	if len(it.filter) == 0 {
		return true, nil
	}

	record, err := it.Value()
	if err != nil {
		return false, err
	}
	for _, cnd := range it.filter {
		if ok, err := cnd.Match(record); !ok || err != nil {
			return false, err
		}
	}
	return true, nil
}

// Valid implements the iterutil.Iterator interface.
func (it *Iterator[K, V]) Valid() bool {
	return it.err == nil && it.cur != nil
}

// Next implements the iterutil.Iterator interface.
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
	if it.record != nil {
		return it.record, nil
	}

	key, err := it.Key()
	if err != nil {
		return nil, err
	}

	if it.ins.getter == nil {
		it.ins.getter = it.ins.x.store.Getter(it.ins.tx)
	}
	it.record, err = it.ins.getter.Get(key)
	return it.record, err
}

// Contains reports whether a record is one of the records that the iterator returns: whether it
// has an entry in the ranges that the iterator reads, and satisfies the conditions that the
// iterator checks. It does not depend on where the iterator is, and it works on a closed one.
func (it *Iterator[K, V]) Contains(_ []byte, record *V) (bool, error) {
	entries, err := it.ins.x.indexer.Index(record)
	if err != nil {
		return false, err
	}

	found := false
entries:
	for _, e := range entries {
		for _, r := range it.ranges {
			start, end := bounds(r)
			if bytes.Compare(e.Key, start) >= 0 && (len(end) == 0 || bytes.Compare(e.Key, end) < 0) {
				found = true
				break entries
			}
		}
	}
	if !found {
		return false, nil
	}

	for _, cnd := range it.filter {
		if ok, err := cnd.Match(record); !ok || err != nil {
			return false, err
		}
	}
	return true, nil
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

// Close implements the iterutil.Iterator interface.
func (it *Iterator[K, V]) Close() error {
	if it.cur == nil {
		return nil
	}

	err := it.cur.Close()
	it.cur = nil
	it.next = len(it.ranges)
	return err
}
