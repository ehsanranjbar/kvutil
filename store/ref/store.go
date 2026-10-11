// Package ref provides a store of references: entries that lead from a key to the primary key
// of a record. It is what indexes and relations keep their entries in.
package ref

import (
	"bytes"
	"errors"
	"fmt"
	"math"

	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/prefix"
	"github.com/ehsanranjbar/kvutil/schema"
)

var (
	// ErrPrimaryKeyTooLong is returned for a primary key that is longer than 255 bytes.
	ErrPrimaryKeyTooLong = errors.New("ref: primary key is longer than 255 bytes")
	// ErrDuplicate is returned by a unique store when a key already refers to another primary key.
	ErrDuplicate = errors.New("ref: key refers to another primary key")
	// ErrCorrupt is returned for an entry that cannot be read.
	ErrCorrupt = errors.New("ref: corrupt entry")
)

// Store is the definition of a store of references.
//
// Several primary keys can have the same key, so an entry is stored under its key followed by
// its primary key and the length of the primary key, with its value as the value.
// For that to work the keys must mark where they end, or all have the same length: a key must
// not be the beginning of another one.
//
// A unique store has one primary key at most for a key. An entry is stored under its key alone,
// with the primary key in front of its value, so that a key is looked up with a single read.
type Store struct {
	ns     *schema.Namespace
	unique bool
}

// New defines a new Store in the given namespace, which is described by the definition.
func New(ns *schema.Namespace, def schema.Definition) *Store {
	def.Keys = true
	ns.Define(def)
	return &Store{ns: ns}
}

// NewUnique defines a new unique Store in the given namespace, which is described by the definition.
func NewUnique(ns *schema.Namespace, def schema.Definition) *Store {
	s := New(ns, def)
	s.unique = true
	return s
}

// Namespace returns the namespace of the store.
func (s *Store) Namespace() *schema.Namespace {
	return s.ns
}

// Unique reports whether a key of the store refers to one primary key at most.
func (s *Store) Unique() bool {
	return s.unique
}

// Instantiate creates the instance of the store for a transaction.
func (s *Store) Instantiate(tx kv.Tx) *Instance {
	return &Instance{
		store: s,
		tx:    s.ns.Open(tx),
	}
}

// Instance is a Store in a transaction.
type Instance struct {
	store *Store
	tx    *prefix.Tx
}

// entryKey returns the key that a reference is stored under if the store is not unique.
func entryKey(key, primaryKey []byte) []byte {
	k := make([]byte, 0, len(key)+len(primaryKey)+1)
	k = append(k, key...)
	k = append(k, primaryKey...)
	return append(k, byte(len(primaryKey)))
}

// Set makes the key refer to the primary key, with an optional value.
// It returns ErrDuplicate if the store is unique and the key refers to another primary key.
func (i *Instance) Set(key, primaryKey, value []byte) error {
	if len(primaryKey) > math.MaxUint8 {
		return ErrPrimaryKeyTooLong
	}

	if !i.store.unique {
		return i.tx.Set(entryKey(key, primaryKey), value)
	}

	existing, err := i.tx.Get(key)
	if err != nil && !errors.Is(err, kv.ErrNotFound) {
		return err
	}
	if err == nil {
		pk, _, err := splitUnique(existing)
		if err != nil {
			return err
		}
		if !bytes.Equal(pk, primaryKey) {
			return ErrDuplicate
		}
	}

	v := make([]byte, 0, 1+len(primaryKey)+len(value))
	v = append(v, byte(len(primaryKey)))
	v = append(v, primaryKey...)
	return i.tx.Set(key, append(v, value...))
}

// Delete removes the reference of the key to the primary key, if there is one.
func (i *Instance) Delete(key, primaryKey []byte) error {
	if len(primaryKey) > math.MaxUint8 {
		return ErrPrimaryKeyTooLong
	}

	if !i.store.unique {
		return i.tx.Delete(entryKey(key, primaryKey))
	}

	existing, err := i.tx.Get(key)
	if errors.Is(err, kv.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}

	pk, _, err := splitUnique(existing)
	if err != nil {
		return err
	}
	if !bytes.Equal(pk, primaryKey) {
		// The key refers to another record now.
		return nil
	}
	return i.tx.Delete(key)
}

// Get returns the value of the reference of the key to the primary key, or kv.ErrNotFound if
// the key does not refer to it. The value is a slice that the caller owns.
func (i *Instance) Get(key, primaryKey []byte) ([]byte, error) {
	if len(primaryKey) > math.MaxUint8 {
		return nil, ErrPrimaryKeyTooLong
	}

	if !i.store.unique {
		return i.tx.Get(entryKey(key, primaryKey))
	}

	existing, err := i.tx.Get(key)
	if err != nil {
		return nil, err
	}
	pk, value, err := splitUnique(existing)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(pk, primaryKey) {
		return nil, kv.ErrNotFound
	}
	return value, nil
}

// Has reports whether the key refers to the primary key.
func (i *Instance) Has(key, primaryKey []byte) (bool, error) {
	if len(primaryKey) > math.MaxUint8 {
		return false, ErrPrimaryKeyTooLong
	}

	if !i.store.unique {
		return i.tx.Has(entryKey(key, primaryKey))
	}

	existing, err := i.tx.Get(key)
	if errors.Is(err, kv.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	pk, _, err := splitUnique(existing)
	return bytes.Equal(pk, primaryKey), err
}

// Iterator iterates over the references whose keys are in [start, end), in ascending order.
// A bound may be the beginning of a key, and an empty bound leaves that side unbounded.
// The iterator must be closed.
func (i *Instance) Iterator(start, end []byte) (*Iterator, error) {
	base, err := i.tx.Iterator(start, end)
	if err != nil {
		return nil, err
	}
	return &Iterator{base: base, unique: i.store.unique}, nil
}

// ReverseIterator iterates over the references whose keys are in [start, end), in descending order.
// A bound may be the beginning of a key, and an empty bound leaves that side unbounded.
// The iterator must be closed.
func (i *Instance) ReverseIterator(start, end []byte) (*Iterator, error) {
	base, err := i.tx.ReverseIterator(start, end)
	if err != nil {
		return nil, err
	}
	return &Iterator{base: base, unique: i.store.unique}, nil
}

// splitUnique splits the stored value of a unique reference into its primary key and its value.
func splitUnique(stored []byte) (primaryKey, value []byte, err error) {
	if len(stored) < 1 || len(stored) < 1+int(stored[0]) {
		return nil, nil, fmt.Errorf("%w: value of %d bytes", ErrCorrupt, len(stored))
	}
	n := int(stored[0])
	return stored[1 : 1+n], stored[1+n:], nil
}

// splitEntryKey splits the stored key of a reference into its key and its primary key.
func splitEntryKey(stored []byte) (key, primaryKey []byte, err error) {
	if len(stored) < 1 || len(stored) < 1+int(stored[len(stored)-1]) {
		return nil, nil, fmt.Errorf("%w: key of %d bytes", ErrCorrupt, len(stored))
	}
	n := int(stored[len(stored)-1])
	return stored[:len(stored)-1-n], stored[len(stored)-1-n : len(stored)-1], nil
}

// Iterator iterates over the references of a Store.
// The slices that it returns must not be modified and are valid until the iterator moves.
type Iterator struct {
	base   kv.Iterator
	unique bool
}

// Valid reports whether the iterator is positioned at a reference.
func (it *Iterator) Valid() bool {
	return it.base.Valid()
}

// Next moves to the next reference.
func (it *Iterator) Next() {
	it.base.Next()
}

// Seek moves to the first reference whose key is not before the given one,
// in the direction of the iterator.
func (it *Iterator) Seek(key []byte) {
	it.base.Seek(key)
}

// Key returns the key of the current reference.
func (it *Iterator) Key() ([]byte, error) {
	if it.unique {
		return it.base.Key(), nil
	}

	key, _, err := splitEntryKey(it.base.Key())
	return key, err
}

// PrimaryKey returns the primary key that the current reference leads to.
func (it *Iterator) PrimaryKey() ([]byte, error) {
	if !it.unique {
		_, pk, err := splitEntryKey(it.base.Key())
		return pk, err
	}

	stored, err := it.base.Value()
	if err != nil {
		return nil, err
	}
	pk, _, err := splitUnique(stored)
	return pk, err
}

// Value returns the value of the current reference.
func (it *Iterator) Value() ([]byte, error) {
	stored, err := it.base.Value()
	if err != nil || !it.unique {
		return stored, err
	}

	_, value, err := splitUnique(stored)
	return value, err
}

// Close releases the iterator.
func (it *Iterator) Close() error {
	return it.base.Close()
}
