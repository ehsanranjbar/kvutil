package prefix

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil"
)

type Store struct {
	base       kvutil.Instantiator[kvutil.BadgerStore]
	basePrefix []byte
	prefix     []byte
}

// New creates a new Store.
func New(base kvutil.Instantiator[kvutil.BadgerStore], prefix []byte) *Store {
	var basePrefix []byte
	if pfx, ok := base.(prefixed); ok {
		basePrefix = pfx.Prefix()
	}

	return &Store{
		base:       base,
		basePrefix: basePrefix,
		prefix:     prefix,
	}
}

type prefixed interface {
	Prefix() []byte
}

// Prefix returns the prefix of the store.
func (s *Store) Prefix() []byte {
	return append(s.basePrefix, s.prefix...)
}

// Instantiate creates a new Instance.
func (s *Store) Instantiate(txn *badger.Txn) kvutil.BadgerStore {
	var base kvutil.BadgerStore = txn
	if s.base != nil {
		base = s.base.Instantiate(txn)
	}

	return &Instance{
		base:       base,
		basePrefix: s.basePrefix,
		prefix:     s.prefix,
	}
}

// Instance is a store that prefixes all keys with a given prefix.
type Instance struct {
	base       kvutil.BadgerStore
	basePrefix []byte
	prefix     []byte
}

// Prefix returns the prefix of the store.
func (s *Instance) Prefix() []byte {
	return append(s.basePrefix, s.prefix...)
}

// Delete deletes the key from the store.
func (s *Instance) Delete(key []byte) error {
	return s.base.Delete(append(s.prefix, key...))
}

// Get gets the key from the store.
func (s *Instance) Get(key []byte) (*badger.Item, error) {
	return s.base.Get(append(s.prefix, key...))
}

// Iterate iterates over the store.
func (s *Instance) NewIterator(opts badger.IteratorOptions) *badger.Iterator {
	return s.base.NewIterator(badger.IteratorOptions{
		Prefix: append(s.prefix, opts.Prefix...),
	})
}

// Set sets the key in the store.
func (s *Instance) Set(key, value []byte) error {
	return s.base.Set(append(s.prefix, key...), value)
}

// SetEntry sets the entry in the store.
func (s *Instance) SetEntry(e *badger.Entry) error {
	e.Key = append(s.prefix, e.Key...)
	return s.base.SetEntry(e)
}
