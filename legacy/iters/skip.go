package iters

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/legacy"
)

// SkipIterator is an iterator that skips first items that doesn't satisfy the condition.
type SkipIterator[K, V, S any] struct {
	base  legacy.Iterator[K, V]
	state S
	f     func(S, K, V, *badger.Item) (S, bool)
}

// SkipN creates a new skip iterator that skips the first n items.
func SkipN[K, V any](base legacy.Iterator[K, V], n int) *SkipIterator[K, V, int] {
	return Skip[K, V, int](base, func(s int, k K, v V, item *badger.Item) (int, bool) {
		return s + 1, s < n
	})
}

// Skip creates a new skip iterator.
func Skip[K, V any, S any](base legacy.Iterator[K, V], f func(S, K, V, *badger.Item) (S, bool)) *SkipIterator[K, V, S] {
	return &SkipIterator[K, V, S]{base: base, f: f}
}

// Close implements the Iterator interface.
func (it *SkipIterator[K, V, S]) Close() {
	it.base.Close()
}

// Item implements the Iterator interface.
func (it *SkipIterator[K, V, S]) Item() *badger.Item {
	return it.base.Item()
}

// Next implements the Iterator interface.
func (it *SkipIterator[K, V, S]) Next() {
	it.base.Next()
}

// Rewind implements the Iterator interface.
func (it *SkipIterator[K, V, S]) Rewind() {
	it.base.Rewind()
	it.skip()
}

func (it *SkipIterator[K, V, S]) skip() {
	for it.base.Valid() {
		k, _ := it.base.Key()
		v, _ := it.base.Value()
		s, ok := it.f(it.state, k, v, it.base.Item())
		if !ok {
			break
		}

		it.state = s
		it.base.Next()
	}
}

// Seek implements the Iterator interface.
func (it *SkipIterator[K, V, S]) Seek(key []byte) {
	it.base.Seek(key)
	it.skip()
}

// Valid implements the Iterator interface.
func (it *SkipIterator[K, V, S]) Valid() bool {
	return it.base.Valid()
}

// Key implements the Iterator interface.
func (it *SkipIterator[K, V, S]) Key() (K, error) {
	return it.base.Key()
}

// Value implements the Iterator interface.
func (it *SkipIterator[K, V, S]) Value() (value V, err error) {
	return it.base.Value()
}
