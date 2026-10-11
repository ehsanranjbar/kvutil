package iters

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/legacy"
)

// MapIterator is an iterator that maps the value from T to U.
type MapIterator[K, V, U any] struct {
	base legacy.Iterator[K, V]
	f    func(V, *badger.Item) (U, error)
}

// Map creates a new map iterator.
func Map[K, V, U any](base legacy.Iterator[K, V], f func(V, *badger.Item) (U, error)) *MapIterator[K, V, U] {
	return &MapIterator[K, V, U]{base: base, f: f}
}

// Close implements the Iterator interface.
func (it *MapIterator[K, V, U]) Close() {
	it.base.Close()
}

// Item implements the Iterator interface.
func (it *MapIterator[K, V, U]) Item() *badger.Item {
	return it.base.Item()
}

// Next implements the Iterator interface.
func (it *MapIterator[K, V, U]) Next() {
	it.base.Next()
}

// Rewind implements the Iterator interface.
func (it *MapIterator[K, V, U]) Rewind() {
	it.base.Rewind()
}

// Seek implements the Iterator interface.
func (it *MapIterator[K, V, U]) Seek(key []byte) {
	it.base.Seek(key)
}

// Valid implements the Iterator interface.
func (it *MapIterator[K, V, U]) Valid() bool {
	return it.base.Valid()
}

// Key implements the Iterator interface.
func (it *MapIterator[K, V, U]) Key() (K, error) {
	return it.base.Key()
}

// Value implements the Iterator interface.
func (it *MapIterator[K, V, U]) Value() (value U, err error) {
	v, err := it.base.Value()
	if err != nil {
		return value, err
	}
	return it.f(v, it.base.Item())
}
