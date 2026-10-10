package indexing

import (
	"context"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	refstore "github.com/ehsanranjbar/kvutil/store/ref"
)

// Extension is an extension for extensible stores that indexes the data with a given indexer.
type Extension[K, V any] struct {
	indexer    Indexer[V]
	descriptor IndexDescriptor
	store      *refstore.Store
}

// NewExtension creates a new Extension.
func NewExtension[K, V any](indexer Indexer[V]) extstore.Extension[K, V] {
	descriptor, _ := indexer.(IndexDescriptor)
	return &Extension[K, V]{
		indexer:    indexer,
		descriptor: descriptor,
	}
}

// Init implements the extensible.Extension interface.
func (e *Extension[K, V]) RegisterStore(store kvutil.Instantiator[kvutil.BadgerStore]) {
	e.store = refstore.New(store)
}

// Instantiate implements the extensible.Extension interface.
func (e *Extension[K, V]) Instantiate(txn *badger.Txn) extstore.ExtensionInstance[K, V] {
	return &ExtensionInstance[K, V]{
		ext:   e,
		store: e.store.Instantiate(txn).(*refstore.Instance),
	}
}

type ExtensionInstance[K, V any] struct {
	ext   *Extension[K, V]
	store *refstore.Instance
}

// OnDelete implements the extensible.Extension interface.
func (e *ExtensionInstance[K, V]) OnDelete(_ context.Context, key K, value *V) error {
	kvs, err := e.ext.indexer.Index(value, false)
	if err != nil {
		return err
	}
	for _, kv := range kvs {
		err := e.store.Delete(kv.Key)
		if err != nil {
			return err
		}
	}

	return nil
}

// OnSet implements the extensible.Extension interface.
func (e *ExtensionInstance[K, V]) OnSet(ctx context.Context, key K, old, new *V, opts ...any) error {
	kbz := extstore.GetKeyBytesFromContext(ctx)
	if old != nil {
		err := e.store.Delete(kbz)
		if err != nil {
			return err
		}
	}

	kvs, err := e.ext.indexer.Index(new, true)
	if err != nil {
		return err
	}
	for _, kv := range kvs {
		err := e.store.Set(kbz, refstore.NewRefEntry(kv.Key).WithValue(kv.Value))
		if err != nil {
			return err
		}
	}

	return nil
}

// Lookup queries the index with the given arguments and returns an iterator of keys.
func (e *ExtensionInstance[K, V]) Lookup(opts badger.IteratorOptions, args ...any) (kvutil.Iterator[[]byte, []byte], error) {
	iter, err := e.ext.indexer.Lookup(args...)
	if err != nil {
		return nil, err
	}

	return LookupChunks(e.store, iter, opts), nil
}

// SupportedQueries returns the supported queries of the index.
func (e *ExtensionInstance[K, V]) SupportedQueries() []string {
	return e.ext.descriptor.SupportedQueries()
}

// SupportedValues returns the supported values of the index.
func (e *ExtensionInstance[K, V]) SupportedValues() []string {
	return e.ext.descriptor.SupportedValues()
}
