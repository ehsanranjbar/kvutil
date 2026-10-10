package ext

import (
	"context"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil"
)

// Extension is an extension to the Store.
// Like stores, extensions are also created using instantiation on a *badger.Txn which is needed
// if the extension needs to access it's own store.
// Some extensions may need to manage records of database outside the current transaction.
// In that case, the extension can get *badger.DB in initialization and use it to manage records off tx.
type Extension[K, V any] interface {
	Instantiate(txn *badger.Txn) ExtensionInstance[K, V]
}

// StoreRegistry determines if an extension needs a private store.
type StoreRegistry interface {
	RegisterStore(kvutil.Instantiator[kvutil.BadgerStore])
}

// ExtensionInstance is an instance of an extension.
// Both OnDelete and OnSet are called before the actual operation is done.
type ExtensionInstance[K, V any] interface {
	OnDelete(ctx context.Context, key K, value *V) error
	OnSet(ctx context.Context, key K, old, new *V, opts ...any) error
}

// ExtOption is an option that is specific to an extension.
type ExtOption struct {
	extName string
	value   any
}

// WithExtOption creates a specific option.
func WithExtOption(name string, value any) ExtOption {
	return ExtOption{
		extName: name,
		value:   value,
	}
}
