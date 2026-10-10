package iters

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil"
)

// RewindSeekIterator is an iterator that can seeks to the specified key on rewind.
type RewindSeekIterator[K, V any] struct {
	base kvutil.Iterator[K, V]
	key  []byte
}

// RewindSeek creates a new rewind seek iterator.
func RewindSeek[K, V any](base kvutil.Iterator[K, V], key []byte) *RewindSeekIterator[K, V] {
	return &RewindSeekIterator[K, V]{base: base, key: key}
}

// Close implements the Iterator interface.
func (it *RewindSeekIterator[K, V]) Close() {
	it.base.Close()
}

// Item implements the Iterator interface.
func (it *RewindSeekIterator[K, V]) Item() *badger.Item {
	return it.base.Item()
}

// Next implements the Iterator interface.
func (it *RewindSeekIterator[K, V]) Next() {
	it.base.Next()
}

// Rewind implements the Iterator interface.
func (it *RewindSeekIterator[K, V]) Rewind() {
	it.base.Seek(it.key)
}

// Seek implements the Iterator interface.
func (it *RewindSeekIterator[K, V]) Seek(key []byte) {
	it.base.Seek(key)
}

// Valid implements the Iterator interface.
func (it *RewindSeekIterator[K, V]) Valid() bool {
	return it.base.Valid()
}

// Key implements the Iterator interface.
func (it *RewindSeekIterator[K, V]) Key() (K, error) {
	return it.base.Key()
}

// Value implements the Iterator interface.
func (it *RewindSeekIterator[K, V]) Value() (V, error) {
	return it.base.Value()
}
