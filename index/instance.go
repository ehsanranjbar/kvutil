package index

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/ehsanranjbar/kvutil/codec"
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

// partialDecoder returns the decoder of the fields that the conditions read, and no other, of an
// encoded record. It is nil if there are no conditions, the store does not tell its codec, what the
// conditions read is not known, or the codec cannot decode in part.
func (i *Instance[K, V]) partialDecoder(conds []cond.Cond[V]) func([]byte, *V) error {
	if len(conds) == 0 {
		return nil
	}
	coded, ok := i.x.store.(extstore.ValueCoded[V])
	if !ok {
		return nil
	}
	reads, ok := cond.Reads(conds...)
	if !ok {
		return nil
	}
	decode, _ := codec.PartialOf(coded.ValueCodec(), reads)
	return decode
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
	if decode := i.partialDecoder(filter); decode != nil {
		it.decode, it.scratch = decode, new(V)
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
	// decode decodes the fields that the filter reads, if that is known and the codec can do so;
	// they are decoded into the scratch, which is never handed out.
	decode  func([]byte, *V) error
	scratch *V
	pos     position[K, V] // the record of the current entry, once it is read
	viewed  entryView      // what the current entry holds, once it is split
	err     error
}

// Bitmap reads the primary keys of the records that the iterator has left into a bitmap, and
// closes it; see the function Bitmap. No record is read.
func (it *Iterator[K, V]) Bitmap() (*roaring64.Bitmap, bool, error) {
	return Bitmap(it)
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

		it.pos = position[K, V]{}
		it.viewed = entryView{}
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

	var (
		record *V
		err    error
	)
	if it.decode != nil {
		// The fields that the conditions read are decoded; the record in full is decoded by Value,
		// from the same bytes, if it is asked for.
		var st extstore.Stored[K, V]
		if st, err = it.Stored(); err == nil {
			record, err = st.Partial(it.decode, it.scratch)
		}
	} else {
		record, err = it.Value()
	}
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
	if it.pos.record != nil {
		return it.pos.record, nil
	}

	key, err := it.Key()
	if err != nil {
		return nil, err
	}
	return it.pos.value(it.ins.recordGetter(), key)
}

// Stored returns the record that the current entry belongs to as it is stored, which the store
// is asked for once, whether it is decoded in part, with this, or in full, with Value. If the
// store cannot give a record as stored, or the record is read in full already, it is decoded.
// It implements the extstore.StoredIterator interface.
func (it *Iterator[K, V]) Stored() (extstore.Stored[K, V], error) {
	key, err := it.Key()
	if err != nil {
		return extstore.Stored[K, V]{}, err
	}
	return it.pos.getStored(it.ins.recordGetter(), key)
}

// recordGetter returns what gets the records of the store, which is made once.
func (i *Instance[K, V]) recordGetter() extstore.Getter[K, V] {
	if i.getter == nil {
		i.getter = i.x.store.Getter(i.tx)
	}
	return i.getter
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

// position is the record of the position that an iterator is at, read from the store at most
// once: as it was stored if the getter can give that, and decoded in full when it is asked for.
type position[K, V any] struct {
	stored extstore.Stored[K, V]
	read   bool // whether stored is set
	record *V   // the decoded record, once it is known
}

// value returns the whole record of the key, which a getter is asked for once.
func (p *position[K, V]) value(getter extstore.Getter[K, V], key K) (*V, error) {
	if p.record != nil {
		return p.record, nil
	}

	if sg, ok := getter.(extstore.StoredGetter[K, V]); ok {
		st, err := p.get(sg, key)
		if err != nil {
			return nil, err
		}
		if p.record, err = st.Record(); err != nil {
			return nil, err
		}
		return p.record, nil
	}

	record, err := getter.Get(key)
	if err != nil {
		return nil, err
	}
	p.record = record
	return record, nil
}

// get returns the stored record of the key, which a getter is asked for once.
func (p *position[K, V]) get(sg extstore.StoredGetter[K, V], key K) (extstore.Stored[K, V], error) {
	if !p.read {
		st, err := sg.GetStored(key)
		if err != nil {
			return st, err
		}
		p.stored, p.read = st, true
	}
	return p.stored, nil
}

// getStored returns the record of the key as it is stored, or already decoded if the getter
// cannot give it as stored or the record was read in full.
func (p *position[K, V]) getStored(getter extstore.Getter[K, V], key K) (extstore.Stored[K, V], error) {
	if p.record == nil {
		if sg, ok := getter.(extstore.StoredGetter[K, V]); ok {
			return p.get(sg, key)
		}
		if _, err := p.value(getter, key); err != nil {
			return extstore.Stored[K, V]{}, err
		}
	}
	return extstore.Decoded(key, p.record), nil
}
