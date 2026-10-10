package badgerkv

import (
	"bytes"
	"errors"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/kv"
)

// Read wraps a badger transaction that the caller owns as a read-only kv.Tx.
func Read(txn *badger.Txn, opts ...Option) kv.Tx {
	return &tx{txn: txn, prefetch: newOptions(opts).prefetch}
}

// ReadWrite wraps a badger update transaction that the caller owns as a writable kv.Tx.
func ReadWrite(txn *badger.Txn, opts ...Option) kv.Tx {
	return &tx{txn: txn, writable: true, prefetch: newOptions(opts).prefetch}
}

// Txn returns the badger transaction underneath a kv.Tx, looking through its decorators.
// The badger transaction knows nothing of the prefixes, codecs and hooks of the layers above,
// so it is meant for reading and maintenance.
func Txn(t kv.Tx) (*badger.Txn, bool) {
	for {
		switch v := t.(type) {
		case *tx:
			return v.txn, true
		case kv.Unwrapper:
			t = v.Unwrap()
		default:
			return nil, false
		}
	}
}

type tx struct {
	txn      *badger.Txn
	writable bool
	prefetch int
}

// Get implements the kv.Tx interface.
func (t *tx) Get(key []byte) ([]byte, error) {
	item, err := t.txn.Get(key)
	if err != nil {
		return nil, mapError(err)
	}

	value, err := item.ValueCopy(nil)
	return value, mapError(err)
}

// Has implements the kv.Tx interface.
func (t *tx) Has(key []byte) (bool, error) {
	_, err := t.txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, mapError(err)
	}
	return true, nil
}

// Set implements the kv.Tx interface.
func (t *tx) Set(key, value []byte) error {
	if !t.writable {
		return kv.ErrReadOnly
	}
	// Badger keeps the slices until the transaction ends.
	return mapError(t.txn.Set(bytes.Clone(key), bytes.Clone(value)))
}

// SetWithTTL implements the kv.TTLWriter interface.
func (t *tx) SetWithTTL(key, value []byte, ttl time.Duration) error {
	if !t.writable {
		return kv.ErrReadOnly
	}
	e := badger.NewEntry(bytes.Clone(key), bytes.Clone(value)).WithTTL(ttl)
	return mapError(t.txn.SetEntry(e))
}

// Delete implements the kv.Tx interface.
func (t *tx) Delete(key []byte) error {
	if !t.writable {
		return kv.ErrReadOnly
	}
	return mapError(t.txn.Delete(bytes.Clone(key)))
}

// Writable implements the kv.Tx interface.
func (t *tx) Writable() bool {
	return t.writable
}

// Iterator implements the kv.Tx interface.
func (t *tx) Iterator(start, end []byte) (kv.Iterator, error) {
	return t.newIterator(start, end, false), nil
}

// ReverseIterator implements the kv.Tx interface.
func (t *tx) ReverseIterator(start, end []byte) (kv.Iterator, error) {
	return t.newIterator(start, end, true), nil
}

func (t *tx) newIterator(start, end []byte, reverse bool) *iterator {
	opts := badger.IteratorOptions{
		PrefetchValues: t.prefetch > 0,
		PrefetchSize:   t.prefetch,
		Reverse:        reverse,
	}
	// Badger stops at the first key outside of the prefix. A reverse iterator starts by seeking
	// the end of the range, which is outside of it, so the prefix can only be given going forward.
	if !reverse {
		opts.Prefix = rangePrefix(start, end)
	}

	it := &iterator{
		base:    t.txn.NewIterator(opts),
		start:   bytes.Clone(start),
		end:     bytes.Clone(end),
		reverse: reverse,
	}
	it.rewind()
	return it
}

// rangePrefix returns the longest prefix that all the keys in [start, end) have in common,
// which lets badger skip the tables that cannot have any of them.
func rangePrefix(start, end []byte) []byte {
	if len(start) == 0 || len(end) == 0 {
		return nil
	}

	// The greatest key below end is either end without its last zero byte, or has end with its
	// last byte decremented as a prefix.
	upper := end[:len(end)-1]
	if last := end[len(end)-1]; last > 0 {
		upper = append(bytes.Clone(upper), last-1)
	}

	n := 0
	for n < len(start) && n < len(upper) && start[n] == upper[n] {
		n++
	}
	return bytes.Clone(start[:n])
}

type iterator struct {
	base    *badger.Iterator
	start   []byte
	end     []byte
	reverse bool

	// item is the current item if the iterator is on a key in the range. It is loaded once per
	// position, because badger adds a key to the read set of the transaction on each access.
	item *badger.Item
	buf  []byte
}

// rewind moves to the first key of the range in the direction of the iterator.
func (it *iterator) rewind() {
	switch {
	case !it.reverse && len(it.start) > 0:
		it.base.Seek(it.start)
	case it.reverse && len(it.end) > 0:
		it.seekBelowEnd()
	default:
		it.base.Rewind()
	}
	it.load()
}

// seekBelowEnd moves a reverse iterator to the greatest key that is less than the end of the range.
func (it *iterator) seekBelowEnd() {
	it.base.Seek(it.end)
	if it.base.Valid() && bytes.Equal(it.base.Item().Key(), it.end) {
		it.base.Next()
	}
}

// load sets the current item, if the base iterator is on a key in the range.
func (it *iterator) load() {
	it.item = nil
	if !it.base.Valid() {
		return
	}

	item := it.base.Item()
	if it.reverse {
		if len(it.start) > 0 && bytes.Compare(item.Key(), it.start) < 0 {
			return
		}
	} else if len(it.end) > 0 && bytes.Compare(item.Key(), it.end) >= 0 {
		return
	}
	it.item = item
}

// Valid implements the kv.Iterator interface.
func (it *iterator) Valid() bool {
	return it.item != nil
}

// Next implements the kv.Iterator interface.
func (it *iterator) Next() {
	// Badger does not tolerate advancing an iterator that is exhausted.
	if it.item == nil {
		return
	}
	it.base.Next()
	it.load()
}

// Seek implements the kv.Iterator interface.
func (it *iterator) Seek(key []byte) {
	if !it.reverse {
		if bytes.Compare(key, it.start) <= 0 {
			it.rewind()
			return
		}
		it.base.Seek(key)
		it.load()
		return
	}

	switch {
	case len(key) == 0:
		// No key is less than or equal to the empty key.
		it.item = nil
		return
	case len(it.end) > 0 && bytes.Compare(key, it.end) >= 0:
		it.seekBelowEnd()
	default:
		it.base.Seek(key)
	}
	it.load()
}

// Key implements the kv.Iterator interface.
func (it *iterator) Key() []byte {
	return it.item.Key()
}

// Value implements the kv.Iterator interface.
func (it *iterator) Value() ([]byte, error) {
	var err error
	it.buf, err = it.item.ValueCopy(it.buf)
	return it.buf, mapError(err)
}

// Close implements the kv.Iterator interface.
func (it *iterator) Close() error {
	it.item = nil
	it.base.Close()
	return nil
}
