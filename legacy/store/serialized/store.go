package serialized

import (
	"encoding"
	"fmt"
	"sync"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/legacy"
	pstore "github.com/ehsanranjbar/kvutil/legacy/store/prefix"
)

// BSP is an interface for pointer of T that is binary serializable/deserializable.
type BSP[T any] interface {
	*T
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

type Store[K, V any, PV BSP[V]] struct {
	base        legacy.Instantiator[legacy.BadgerStore]
	prefix      []byte
	keyCodec    codec.Codec[K]
	initialized bool
	init        sync.Once
}

// New creates a new Store.
func New[
	K, V any,
	PV BSP[V],
](base legacy.Instantiator[legacy.BadgerStore]) *Store[K, V, PV] {
	var prefix []byte
	if pfx, ok := base.(prefixed); ok {
		prefix = pfx.Prefix()
	}

	return &Store[K, V, PV]{
		base:   base,
		prefix: prefix,
	}
}

// WithKeyCodec sets the key codec of the store.
func (s *Store[K, V, PV]) WithKeyCodec(codec codec.Codec[K]) *Store[K, V, PV] {
	if s.initialized {
		panic("store is already initialized")
	}

	s.keyCodec = codec
	return s
}

type prefixed interface {
	Prefix() []byte
}

// Prefix returns the prefix of the store.
func (s *Store[K, V, PV]) Prefix() []byte {
	return s.prefix
}

// KeyCodec returns the key codec of the store.
func (s *Store[K, V, PV]) KeyCodec() codec.Codec[K] {
	return s.keyCodec
}

// Instantiate creates a new Instance.
func (s *Store[K, V, PV]) Instantiate(txn *badger.Txn) legacy.StoreInstance[K, *V, *V, legacy.Iterator[K, *V]] {
	// Locking any changes to the store's configuration on first instantiation.
	s.init.Do(func() {
		if s.keyCodec == nil {
			kc, err := codec.CodecFor[K]()
			if err != nil {
				panic(err)
			}
			s.keyCodec = kc
		}

		s.initialized = true
	})

	var base legacy.BadgerStore = txn
	if s.base != nil {
		base = s.base.Instantiate(txn)
	}

	return &Instance[K, V, PV]{
		base:     base,
		prefix:   s.prefix,
		keyCodec: s.keyCodec,
	}
}

// Instance is a store that serializes all keys and values.
type Instance[K, V any, PV BSP[V]] struct {
	base     legacy.BadgerStore
	prefix   []byte
	keyCodec codec.Codec[K]
}

// Prefix returns the prefix of the store.
func (s *Instance[K, V, PV]) Prefix() []byte {
	return s.prefix
}

// Delete deletes the key from the store.
func (s *Instance[K, V, PV]) Delete(key K) error {
	kbz, err := s.keyCodec.Encode(key)
	if err != nil {
		return fmt.Errorf("failed to encode key %v: %w", key, err)
	}
	return s.base.Delete(kbz)
}

// Get gets the value of the key from the store and unmarshal it.
func (s *Instance[K, V, PV]) Get(key K) (value *V, err error) {
	_, value, err = s.GetWithItem(key)
	return value, err
}

// NewIterator creates a new iterator.
func (s *Instance[K, V, PV]) NewIterator(opts badger.IteratorOptions) legacy.Iterator[K, *V] {
	var iter legacy.BadgerIterator = s.base.NewIterator(opts)
	if pfx := s.Prefix(); pfx != nil {
		iter = pstore.NewIterator(iter, pfx)
	}

	return NewIterator[K, V, PV](iter, s.keyCodec)
}

// GetWithItem is similar to Get, but it also returns the badger.Item as well.
func (s *Instance[K, V, PV]) GetWithItem(key K) (item *badger.Item, value *V, err error) {
	kbz, err := s.keyCodec.Encode(key)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to encode key %v: %w", key, err)
	}

	item, err = s.base.Get(kbz)
	if err != nil {
		return nil, nil, err
	}

	v := PV(new(V))
	err = item.Value(func(val []byte) error {
		return v.UnmarshalBinary(val)
	})
	return item, v, err
}

// Set marshals the value as binary and sets it to the key.
func (s *Instance[K, V, PV]) Set(key K, value *V) error {
	kbz, err := s.keyCodec.Encode(key)
	if err != nil {
		return fmt.Errorf("failed to encode key %v: %w", key, err)
	}

	var vbz []byte
	if value != nil {
		vbz, err = PV(value).MarshalBinary()
		if err != nil {
			return fmt.Errorf("failed to marshal value: %w", err)
		}
	}
	return s.base.Set(kbz, vbz)
}
