// Package prefix provides a transaction decorator that keeps all of its keys under a prefix.
package prefix

import (
	"bytes"
	"time"

	"github.com/ehsanranjbar/kvutil/kv"
)

// Tx is a transaction that prefixes every key with a given prefix, which gives its user
// a keyspace of their own inside the base transaction.
type Tx struct {
	base   kv.Tx
	prefix []byte
}

// New creates a new Tx over the base transaction.
func New(base kv.Tx, prefix []byte) *Tx {
	return &Tx{
		base:   base,
		prefix: bytes.Clone(prefix),
	}
}

// Prefix returns the prefix of the transaction. The slice must not be modified.
func (t *Tx) Prefix() []byte {
	return t.prefix
}

// Unwrap implements the kv.Unwrapper interface.
func (t *Tx) Unwrap() kv.Tx {
	return t.base
}

// key returns the prefixed key in a slice of its own.
func (t *Tx) key(key []byte) []byte {
	k := make([]byte, 0, len(t.prefix)+len(key))
	k = append(k, t.prefix...)
	return append(k, key...)
}

// bounds returns the range of the base transaction that corresponds to [start, end).
func (t *Tx) bounds(start, end []byte) ([]byte, []byte) {
	if len(end) == 0 {
		return t.key(start), kv.PrefixEnd(t.prefix)
	}
	return t.key(start), t.key(end)
}

// Get implements the kv.Tx interface.
func (t *Tx) Get(key []byte) ([]byte, error) {
	return t.base.Get(t.key(key))
}

// Has implements the kv.Tx interface.
func (t *Tx) Has(key []byte) (bool, error) {
	return t.base.Has(t.key(key))
}

// Set implements the kv.Tx interface.
func (t *Tx) Set(key, value []byte) error {
	return t.base.Set(t.key(key), value)
}

// SetWithTTL implements the kv.TTLWriter interface.
// It returns kv.ErrUnsupported if the base transaction cannot write keys that expire.
func (t *Tx) SetWithTTL(key, value []byte, ttl time.Duration) error {
	return kv.SetWithTTL(t.base, t.key(key), value, ttl)
}

// Delete implements the kv.Tx interface.
func (t *Tx) Delete(key []byte) error {
	return t.base.Delete(t.key(key))
}

// Writable implements the kv.Tx interface.
func (t *Tx) Writable() bool {
	return t.base.Writable()
}

// Iterator implements the kv.Tx interface.
func (t *Tx) Iterator(start, end []byte) (kv.Iterator, error) {
	base, err := t.base.Iterator(t.bounds(start, end))
	if err != nil {
		return nil, err
	}
	return &iterator{Iterator: base, tx: t}, nil
}

// ReverseIterator implements the kv.Tx interface.
func (t *Tx) ReverseIterator(start, end []byte) (kv.Iterator, error) {
	base, err := t.base.ReverseIterator(t.bounds(start, end))
	if err != nil {
		return nil, err
	}
	return &iterator{Iterator: base, tx: t}, nil
}

// iterator trims the prefix from the keys of the base iterator.
type iterator struct {
	kv.Iterator
	tx *Tx
}

// Seek implements the kv.Iterator interface.
func (it *iterator) Seek(key []byte) {
	it.Iterator.Seek(it.tx.key(key))
}

// Key implements the kv.Iterator interface.
func (it *iterator) Key() []byte {
	return it.Iterator.Key()[len(it.tx.prefix):]
}
