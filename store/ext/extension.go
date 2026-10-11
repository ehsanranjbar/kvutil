package ext

import (
	"github.com/ehsanranjbar/kvutil/catalog"
	"github.com/ehsanranjbar/kvutil/kv"
)

// Extension is an extension to the Store, which is told of the records that are set and deleted.
// Like a store, an extension is instantiated for each transaction. It is given the transaction
// as it is, not the keyspace of the store, so that it can work with other stores too.
type Extension[K, V any] interface {
	Instantiate(tx kv.Tx) ExtensionInstance[K, V]
}

// ExtensionInstance is an Extension in a transaction.
// Both OnSet and OnDelete are called before the record is written.
type ExtensionInstance[K, V any] interface {
	// OnSet is called when a record is set. rawKey is the key as the store encodes it,
	// and old is the record that is replaced, or nil if there was none.
	OnSet(key K, rawKey []byte, old, new *V, opts ...any) error
	// OnDelete is called when a record that exists is deleted.
	OnDelete(key K, rawKey []byte, old *V) error
}

// NamespaceUser is implemented by the extensions that keep data of their own.
// They are given a namespace under the namespace of the store when they are added to it.
type NamespaceUser interface {
	UseNamespace(ns *catalog.Namespace)
}

// DeleteHook is called before a record that exists is deleted, with the key as it is given
// and as the store encodes it. Unlike an extension it does not need the type of the records.
type DeleteHook[K any] func(tx kv.Tx, key K, rawKey []byte) error

// DeleteHooker is implemented by the stores that delete hooks can be added to,
// whatever the type of their records is.
type DeleteHooker[K any] interface {
	AddDeleteHook(name string, hook DeleteHook[K])
}

// ExtOption is an option that is specific to an extension.
type ExtOption struct {
	extName string
	value   any
}

// WithExtOption creates an option that only the extension of the given name receives.
func WithExtOption(name string, value any) ExtOption {
	return ExtOption{
		extName: name,
		value:   value,
	}
}
