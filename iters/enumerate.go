package iters

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/internal/constraints"
)

// Enumerator is an iterator that enumerates a counter as the keys for the base value iterator.
type Enumerator[K constraints.Integer, V any] struct {
	it      kvutil.ValueIterator[V]
	counter K
}

// Enumerate creates a new enumerator.
func Enumerate[K constraints.Integer, V any](it kvutil.ValueIterator[V]) *Enumerator[K, V] {
	return &Enumerator[K, V]{
		it:      it,
		counter: 0,
	}
}

// Close implements the Iterator interface.
func (e *Enumerator[K, V]) Close() {
	e.it.Close()
}

// Item implements the Iterator interface.
func (e *Enumerator[K, V]) Item() *badger.Item {
	return e.it.Item()
}

// Next implements the Iterator interface.
func (e *Enumerator[K, V]) Next() {
	e.it.Next()
	e.counter++
}

// Rewind implements the Iterator interface.
func (e *Enumerator[K, V]) Rewind() {
	e.it.Rewind()
	e.counter = 0
}

// Seek implements the Iterator interface.
func (e *Enumerator[K, V]) Seek(key []byte) {
	e.it.Seek(key)
	e.counter = 0
}

// Valid implements the Iterator interface.
func (e *Enumerator[K, V]) Valid() bool {
	return e.it.Valid()
}

// Key returns the current key.
func (e *Enumerator[K, V]) Key() (K, error) {
	return e.counter, nil
}

// Value returns the current value.
func (e *Enumerator[K, V]) Value() (V, error) {
	return e.it.Value()
}
