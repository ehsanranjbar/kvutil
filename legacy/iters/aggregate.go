package iters

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/legacy"
)

// AggregateIterator is an iterator that aggregates the values of base iterator as it iterates without
// interfering with the base iterator.
type AggregateIterator[K, V, S any] struct {
	base  legacy.Iterator[K, V]
	state S
	f     func(S, V, *badger.Item) S
}

// Aggregate creates a new aggregate iterator.
func Aggregate[K, V, S any](base legacy.Iterator[K, V], f func(S, V, *badger.Item) S) *AggregateIterator[K, V, S] {
	return &AggregateIterator[K, V, S]{base: base, f: f}
}

// Close implements the Iterator interface.
func (it *AggregateIterator[K, V, S]) Close() {
	it.base.Close()
}

// Item implements the Iterator interface.
func (it *AggregateIterator[K, V, S]) Item() *badger.Item {
	return it.base.Item()
}

// Next implements the Iterator interface.
func (it *AggregateIterator[K, V, S]) Next() {
	it.base.Next()
	it.update()
}

func (it *AggregateIterator[K, V, S]) update() {
	if !it.base.Valid() {
		return
	}

	v, err := it.Value()
	if err != nil {
		return
	}
	it.state = it.f(it.state, v, it.Item())
}

// Rewind implements the Iterator interface.
func (it *AggregateIterator[K, V, S]) Rewind() {
	it.base.Rewind()
	var s S
	it.state = s
	it.update()
}

// Seek implements the Iterator interface.
func (it *AggregateIterator[K, V, S]) Seek(key []byte) {
	it.base.Seek(key)
	var s S
	it.state = s
	it.update()
}

// Valid implements the Iterator interface.
func (it *AggregateIterator[K, V, S]) Valid() bool {
	return it.base.Valid()
}

func (it *AggregateIterator[K, V, S]) Key() (K, error) {
	return it.base.Key()
}

// Value implements the Iterator interface.
func (it *AggregateIterator[K, V, S]) Value() (value V, err error) {
	return it.base.Value()
}

// Result returns the aggregated result.
func (it *AggregateIterator[K, V, S]) Result() S {
	return it.state
}
