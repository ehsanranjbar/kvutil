package iters

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil"
)

// Count returns an aggregate iterator that counts the number of items in the base iterator.
func Count[K, V any](it kvutil.Iterator[K, V]) *AggregateIterator[K, V, uint] {
	return Aggregate(it, func(count uint, _ V, _ *badger.Item) uint {
		return count + 1
	})
}

// ConsumeAndCount consumes the iterator and returns the count of items in the iterator.
func ConsumeAndCount(it NopIterator) uint {
	defer it.Close()

	var count uint
	for it.Rewind(); it.Valid(); it.Next() {
		count++
	}

	return count
}

// NopIterator is an iterator that only implements the Rewind, Valid, and Next methods.
type NopIterator interface {
	Rewind()
	Valid() bool
	Next()
	Close()
}
