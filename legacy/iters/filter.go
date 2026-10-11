package iters

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/legacy"
)

// FilterIterator is an iterator that filters the items based on a predicate.
type FilterIterator[K, V any] struct {
	base legacy.Iterator[K, V]
	f    func(V, *badger.Item) bool
}

// Filter creates a new filter iterator.
func Filter[K, V any](base legacy.Iterator[K, V], f func(V, *badger.Item) bool) *FilterIterator[K, V] {
	return &FilterIterator[K, V]{base: base, f: f}
}

// Close implements the Iterator interface.
func (it *FilterIterator[K, V]) Close() {
	it.base.Close()
}

// Item implements the Iterator interface.
func (it *FilterIterator[K, V]) Item() *badger.Item {
	return it.base.Item()
}

// Next implements the Iterator interface.
func (it *FilterIterator[K, V]) Next() {
	it.base.Next()
	it.findNext()
}

func (it *FilterIterator[K, V]) findNext() {
	for it.base.Valid() {
		v, err := it.Value()
		if err != nil {
			return
		}
		if it.f(v, it.Item()) {
			return
		}
		it.base.Next()
	}
}

// Rewind implements the Iterator interface.
func (it *FilterIterator[K, V]) Rewind() {
	it.base.Rewind()
	it.findNext()
}

// Seek implements the Iterator interface.
func (it *FilterIterator[K, V]) Seek(key []byte) {
	it.base.Seek(key)
	it.findNext()
}

// Valid implements the Iterator interface.
func (it *FilterIterator[K, V]) Valid() bool {
	if !it.base.Valid() {
		return false
	}

	v, err := it.Value()
	if err != nil {
		return false
	}
	return it.f(v, it.Item())
}

// Key implements the Iterator interface.
func (it *FilterIterator[K, V]) Key() (K, error) {
	return it.base.Key()
}

// Value implements the Iterator interface.
func (it *FilterIterator[K, V]) Value() (value V, err error) {
	return it.base.Value()
}
