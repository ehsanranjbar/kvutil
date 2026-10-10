package iters

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil"
)

// First returns the first value of the iterator.
func First[K, V any](iter kvutil.Iterator[K, V]) (value V, err error) {
	iter.Rewind()
	if !iter.Valid() {
		return value, badger.ErrKeyNotFound
	}

	return iter.Value()
}

// FirstItem returns the first item of the iterator.
func FirstItem(iter kvutil.BadgerIterator) (item *badger.Item, err error) {
	iter.Rewind()
	if !iter.Valid() {
		return item, badger.ErrKeyNotFound
	}

	return iter.Item(), nil
}
