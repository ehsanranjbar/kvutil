package legacy

import (
	badger "github.com/dgraph-io/badger/v4"
)

// Instantiator is an interface that creates a new instance of something from a badger transaction.
type Instantiator[I any] interface {
	Instantiate(txn *badger.Txn) I
}

// StoreInstance is a store that works with serialized values.
type StoreInstance[K, G, S any, I BadgerIterator] interface {
	Delete(key K) error
	Get(key K) (v G, err error)
	NewIterator(opts badger.IteratorOptions) I
	Set(key K, value S) error
}

// BadgerStore is an store instance that is compatible with *badger.Txn
type BadgerStore interface {
	StoreInstance[[]byte, *badger.Item, []byte, *badger.Iterator]
	SetEntry(e *badger.Entry) error
}
