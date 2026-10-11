package iters

import (
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/legacy"
)

// SeverIterator is an iterator that severs (stops) the iteration when the predicate is true.
type SeverIterator[K, V any] struct {
	base    legacy.Iterator[K, V]
	pred    func(k K, v V, item *badger.Item) bool
	severed bool
}

// Sever creates a new sever iterator.
func Sever[K, V any](base legacy.Iterator[K, V], f func(K, V, *badger.Item) bool) *SeverIterator[K, V] {
	return &SeverIterator[K, V]{base: base, pred: f}
}

// Close implements the Iterator interface.
func (it *SeverIterator[K, V]) Close() {
	it.base.Close()
}

// Item implements the Iterator interface.
func (it *SeverIterator[K, V]) Item() *badger.Item {
	return it.base.Item()
}

// Next implements the Iterator interface.
func (it *SeverIterator[K, V]) Next() {
	if it.severed {
		return
	}

	it.base.Next()
	if it.base.Valid() && !it.severed {
		it.checkSevered()
	}
}

func (it *SeverIterator[K, V]) checkSevered() {
	key, err := it.base.Key()
	if err != nil {
		it.severed = true
		return
	}

	value, err := it.base.Value()
	if err != nil || it.pred(key, value, it.base.Item()) {
		it.severed = true
		return
	}

	it.severed = false
}

// Rewind implements the Iterator interface.
func (it *SeverIterator[K, V]) Rewind() {
	it.base.Rewind()
	it.checkSevered()
}

// Seek implements the Iterator interface.
func (it *SeverIterator[K, V]) Seek(key []byte) {
	it.base.Seek(key)
	it.checkSevered()
}

// Valid implements the Iterator interface.
func (it *SeverIterator[K, V]) Valid() bool {
	return it.base.Valid() && !it.severed
}

// Key implements the Iterator interface.
func (it *SeverIterator[K, V]) Key() (K, error) {
	return it.base.Key()
}

// Value implements the Iterator interface.
func (it *SeverIterator[K, V]) Value() (value V, err error) {
	if it.severed {
		return value, fmt.Errorf("unable to return value in a severed iterator")
	}

	return it.base.Value()
}
