// Package kvutil is a toolkit of layered, optional abstractions over ordered key-value stores.
package kvutil

import "github.com/ehsanranjbar/kvutil/kv"

// Instantiator creates the instance of something for a transaction.
// A store, an index and the like are defined once and are instantiated for each transaction.
type Instantiator[I any] interface {
	Instantiate(tx kv.Tx) I
}

// Iterator iterates over typed records.
type Iterator[K, V any] interface {
	// Valid reports whether the iterator is positioned at a record.
	Valid() bool
	// Next moves to the next record.
	Next()
	// Key returns the key of the current record.
	Key() (K, error)
	// Value returns the value of the current record.
	Value() (V, error)
	// Close releases the iterator.
	Close() error
}
