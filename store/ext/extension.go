package ext

import (
	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/schema"
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
	UseNamespace(ns *schema.Namespace)
}

// DeleteHook is called before a record that exists is deleted, with the key as it is given
// and as the store encodes it. Unlike an extension it does not need the type of the records.
type DeleteHook[K any] func(tx kv.Tx, key K, rawKey []byte) error

// DeleteHooks is implemented by the stores that delete hooks can be added to,
// whatever the type of their records is.
type DeleteHooks[K any] interface {
	AddDeleteHook(name string, hook DeleteHook[K])
}

// Extensible is implemented by the stores that extensions can be added to.
// It is what something that is built as an extension, like an index, needs of its store.
type Extensible[K, V any] interface {
	// AddExtension adds an extension to the store.
	AddExtension(name string, ext Extension[K, V])
	// KeyCodec returns the key codec of the store.
	KeyCodec() codec.KeyCodec[K]
	// Getter returns what gets the records of the store in a transaction.
	Getter(tx kv.Tx) Getter[K, V]
	// Records iterates over the records of the store whose keys are in a range, in a transaction.
	Records(tx kv.Tx, r kvutil.Range[K]) (iterutil.Iterator[K, *V], error)
}

// Getter gets the records of a store by their keys.
type Getter[K, V any] interface {
	// Get returns the record of the key, or kv.ErrNotFound if it does not exist.
	Get(key K) (*V, error)
}

// Option is an option that is specific to an extension.
type Option struct {
	extName string
	value   any
}

// For creates an option that only the extension of the given name receives.
func For(name string, value any) Option {
	return Option{
		extName: name,
		value:   value,
	}
}
