package iters

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil"
)

// FlattenIterator is an iterator that flattens a set of iterators into a single iterator.
type FlattenIterator[K1, K2, V any] struct {
	base    kvutil.Iterator[K1, kvutil.Iterator[K2, V]]
	current kvutil.Iterator[K2, V]
}

// Flatten creates a new flatten iterator.
func Flatten[K1, K2, V any](base kvutil.Iterator[K1, kvutil.Iterator[K2, V]]) *FlattenIterator[K1, K2, V] {
	return &FlattenIterator[K1, K2, V]{base: base}
}

// Close implements the Iterator interface.
func (it *FlattenIterator[K1, K2, V]) Close() {
	if it.current != nil {
		it.current.Close()
	}
	it.base.Close()
}

// Item implements the Iterator interface.
func (it *FlattenIterator[K1, K2, V]) Item() *badger.Item {
	if it.current == nil || !it.current.Valid() {
		return nil
	}

	return it.current.Item()
}

// Next implements the Iterator interface.
func (it *FlattenIterator[K1, K2, V]) Next() {
	if it.current != nil {
		it.current.Next()
		if it.current.Valid() {
			return
		}
	}

	it.base.Next()
	it.nextCurrent()
}

func (it *FlattenIterator[K1, K2, V]) nextCurrent() {
	for {
		if !it.base.Valid() {
			break
		}

		if it.current != nil {
			it.current.Close()
		}

		var err error
		it.current, err = it.base.Value()
		if err != nil {
			continue
		}

		it.current.Rewind()
		if it.current.Valid() {
			break
		}

		it.base.Next()
	}
}

// Rewind implements the Iterator interface.
func (it *FlattenIterator[K1, K2, V]) Rewind() {
	it.base.Rewind()
	it.nextCurrent()
}

// Seek implements the Iterator interface.
func (it *FlattenIterator[K1, K2, V]) Seek(key []byte) {
	it.base.Seek(key)
	it.nextCurrent()
}

// Valid implements the Iterator interface.
func (it *FlattenIterator[K1, K2, V]) Valid() bool {
	return it.base.Valid() || (it.current != nil && it.current.Valid())
}

// Key implements the Iterator interface.
func (it *FlattenIterator[K1, K2, V]) Key() (key K2, err error) {
	if it.current != nil {
		return it.current.Key()
	}

	return key, nil
}

// Value returns the current value of the iterator.
func (it *FlattenIterator[K1, K2, V]) Value() (value V, err error) {
	if it.current != nil {
		return it.current.Value()
	}

	return value, nil
}
