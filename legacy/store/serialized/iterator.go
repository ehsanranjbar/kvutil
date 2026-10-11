package serialized

import (
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/legacy"
)

// Iterator is an iterator that unmarshal the value.
type Iterator[K, V any, PV BSP[V]] struct {
	base        legacy.BadgerIterator
	keyCodec    codec.Codec[K]
	keyProvider keyProvider
	cachedKey   *K
	cachedValue *V
}

type keyProvider interface {
	Key() ([]byte, error)
}

// NewIterator creates a new serialized iterator.
func NewIterator[K, V any, PV BSP[V]](base legacy.BadgerIterator, kc codec.Codec[K]) *Iterator[K, V, PV] {
	kp, _ := base.(keyProvider)

	return &Iterator[K, V, PV]{
		base:        base,
		keyCodec:    kc,
		keyProvider: kp,
	}
}

// Close closes the iterator.
func (it *Iterator[K, V, PV]) Close() {
	it.base.Close()
	it.cachedKey = nil
	it.cachedValue = nil
}

// Item returns the current item.
func (it *Iterator[K, V, PV]) Item() *badger.Item {
	return it.base.Item()
}

// Key returns the current key.
func (it *Iterator[K, V, PV]) Next() {
	it.base.Next()
	it.cachedKey = nil
	it.cachedValue = nil
}

// Rewind rewinds the iterator.
func (it *Iterator[K, V, PV]) Rewind() {
	it.base.Rewind()
	it.cachedKey = nil
	it.cachedValue = nil
}

// Seek seeks the key.
func (it *Iterator[K, V, PV]) Seek(key []byte) {
	it.base.Seek(key)
	it.cachedKey = nil
	it.cachedValue = nil
}

// Valid returns if the iterator is valid.
func (it *Iterator[K, V, PV]) Valid() bool {
	return it.base.Valid()
}

// Key returns the current key.
func (it *Iterator[K, V, PV]) Key() (key K, err error) {
	if it.cachedKey != nil {
		return *it.cachedKey, nil
	}

	var kbz []byte
	if it.keyProvider == nil {
		kbz = it.base.Item().Key()
	} else {
		kbz, err = it.keyProvider.Key()
		if err != nil {
			return key, fmt.Errorf("failed to get key: %w", err)
		}
	}

	key, err = it.keyCodec.Decode(kbz)
	if err != nil {
		return key, fmt.Errorf("failed to decode key \"%s\": %w", it.base.Item().Key(), err)
	}
	it.cachedKey = &key

	return key, nil
}

// Value returns the current value unmarshaled as T
func (it *Iterator[K, V, PV]) Value() (value *V, err error) {
	if it.cachedValue != nil {
		return it.cachedValue, nil
	}

	item := it.base.Item()
	if item == nil {
		return nil, nil
	}
	v := PV(new(V))
	err = item.Value(func(val []byte) error {
		if len(val) == 0 {
			return nil
		}
		return v.UnmarshalBinary(val)
	})
	it.cachedValue = v
	return it.cachedValue, err
}
