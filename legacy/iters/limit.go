package iters

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/legacy"
)

// LimitIterator is an iterator that limits the number of items when calling Next.
type LimitIterator[K, V any] struct {
	base legacy.Iterator[K, V]
	n    int
	i    int
}

// Limit creates a new limit iterator.
func Limit[K, V any](base legacy.Iterator[K, V], n int) *LimitIterator[K, V] {
	return &LimitIterator[K, V]{base: base, n: n}
}

// Close implements the Iterator interface.
func (it *LimitIterator[K, V]) Close() {
	it.base.Close()
}

// Item implements the Iterator interface.
func (it *LimitIterator[K, V]) Item() *badger.Item {
	return it.base.Item()
}

// Next implements the Iterator interface.
func (it *LimitIterator[K, V]) Next() {
	if it.i < it.n {
		it.base.Next()
		it.i++
	}
}

// Rewind implements the Iterator interface.
func (it *LimitIterator[K, V]) Rewind() {
	it.base.Rewind()
	it.i = 0
}

// Seek implements the Iterator interface.
func (it *LimitIterator[K, V]) Seek(key []byte) {
	it.base.Seek(key)
	it.i = 0
}

// Valid implements the Iterator interface.
func (it *LimitIterator[K, V]) Valid() bool {
	return it.i < it.n && it.base.Valid()
}

// Key implements the Iterator interface.
func (it *LimitIterator[K, V]) Key() (K, error) {
	return it.base.Key()
}

// Value implements the Iterator interface.
func (it *LimitIterator[K, V]) Value() (value V, err error) {
	return it.base.Value()
}
