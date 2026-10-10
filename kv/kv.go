// Package kv defines the byte-level store interfaces that every engine adapter implements
// and that the higher layers of the toolkit are built on.
package kv

import (
	"bytes"
	"errors"
)

var (
	// ErrNotFound is returned when a key does not exist.
	ErrNotFound = errors.New("kv: key not found")
	// ErrEmptyKey is returned when an empty key is given to an engine that does not allow it.
	ErrEmptyKey = errors.New("kv: key is empty")
	// ErrReadOnly is returned when writing in a transaction that is not writable.
	ErrReadOnly = errors.New("kv: transaction is read-only")
	// ErrConflict is returned when a transaction conflicts with another one and has to be retried.
	ErrConflict = errors.New("kv: transaction conflict")
	// ErrTxnTooBig is returned when a transaction holds more writes than the engine allows.
	ErrTxnTooBig = errors.New("kv: transaction is too big")
	// ErrUnsupported is returned when the engine does not have an optional capability.
	ErrUnsupported = errors.New("kv: operation is not supported by the engine")
)

// Reader is the read side of a transaction.
type Reader interface {
	// Get returns the value of the key, or ErrNotFound if it does not exist.
	// The returned slice is a copy that the caller owns. An empty value may be returned as nil.
	Get(key []byte) ([]byte, error)
	// Has reports whether the key exists.
	Has(key []byte) (bool, error)
	// Iterator iterates over the keys in [start, end) in ascending order.
	// An empty start or end leaves that side of the range unbounded.
	// The iterator is positioned at its first key and must be closed.
	Iterator(start, end []byte) (Iterator, error)
	// ReverseIterator iterates over the keys in [start, end) in descending order.
	// An empty start or end leaves that side of the range unbounded.
	// The iterator is positioned at its first key and must be closed.
	ReverseIterator(start, end []byte) (Iterator, error)
}

// Tx is a transaction over an ordered key-value store.
// Writes of a transaction are visible to its own reads, but an iterator may not see the writes
// that are made after it was created.
// A transaction must not be used concurrently, nor after the function it was given to returns.
type Tx interface {
	Reader
	// Set sets the value of the key. Neither slice is retained.
	Set(key, value []byte) error
	// Delete deletes the key. Deleting a key that does not exist is not an error.
	// The slice is not retained.
	Delete(key []byte) error
	// Writable reports whether the transaction accepts writes.
	// Set and Delete return ErrReadOnly if it does not.
	Writable() bool
}

// Iterator iterates over a range of keys of a transaction.
type Iterator interface {
	// Valid reports whether the iterator is positioned at a key.
	Valid() bool
	// Next moves to the next key in the direction of the iterator.
	Next()
	// Seek moves to the given key or, if it is not in the range, to the first key after it
	// in the direction of the iterator.
	Seek(key []byte)
	// Key returns the current key. The slice must not be modified and is valid until the next
	// call of Next, Seek or Close.
	Key() []byte
	// Value returns the current value, which is only loaded when this is called.
	// The slice must not be modified and is valid until the next call of Value, Next, Seek or Close.
	Value() ([]byte, error)
	// Close releases the iterator.
	Close() error
}

// DB opens transactions on a store.
type DB interface {
	// View runs the function in a read-only transaction.
	View(fn func(Tx) error) error
	// Update runs the function in a writable transaction and commits it if the function
	// returns no error. The function may run more than once if the engine retries conflicts.
	Update(fn func(Tx) error) error
}

// PrefixEnd returns the smallest key that is greater than every key with the given prefix,
// so that [prefix, PrefixEnd(prefix)) is the range of the keys with the prefix.
// It returns nil if there is no such key, which is when the prefix is empty or all 0xff.
func PrefixEnd(prefix []byte) []byte {
	for i := len(prefix) - 1; i >= 0; i-- {
		if prefix[i] != 0xff {
			end := bytes.Clone(prefix[:i+1])
			end[i]++
			return end
		}
	}
	return nil
}
