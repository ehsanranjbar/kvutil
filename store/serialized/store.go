// Package serialized provides a store of typed keys and values over a kv transaction.
package serialized

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/ehsanranjbar/kvutil/catalog"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/expr"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/prefix"
)

// ErrNilValue is returned when a nil value is given to be stored.
var ErrNilValue = errors.New("serialized: value is nil")

// Store is the definition of a store that encodes its keys and values with codecs.
// It is defined once and is instantiated for each transaction.
type Store[K, V any] struct {
	ns         *catalog.Namespace
	keyCodec   codec.KeyCodec[K]
	valueCodec codec.ValueCodec[V]
}

// New defines a new Store in the given namespace, with the codecs that its types have by default.
// The key codec is derived from the type of the keys. The value codec is the type's own
// MarshalBinary and UnmarshalBinary if it has them, and the format of the schema otherwise.
// It panics if the type of the keys has no key codec, in which case the store must be defined
// with NewWithCodecs.
func New[K, V any](ns *catalog.Namespace) *Store[K, V] {
	return NewWithCodecs[K, V](ns, nil, nil)
}

// NewWithCodecs defines a new Store in the given namespace with the given codecs.
// A codec that is nil is the one that New would use.
func NewWithCodecs[K, V any](
	ns *catalog.Namespace,
	keyCodec codec.KeyCodec[K],
	valueCodec codec.ValueCodec[V],
) *Store[K, V] {
	if keyCodec == nil {
		var err error
		keyCodec, err = codec.KeyFor[K]()
		if err != nil {
			// A mistake in the definition, which is made in code when the program starts.
			panic(fmt.Sprintf("store %q: %v; define the store with NewWithCodecs and give it one", ns.Path(), err))
		}
	}
	if valueCodec == nil {
		valueCodec = codec.ValueFor[V](ns.Schema().Format())
	}

	ns.Define(catalog.Definition{
		Kind:      "serialized",
		Keys:      true,
		KeyType:   reflect.TypeFor[K]().String(),
		ValueType: reflect.TypeFor[V]().String(),
		Attributes: func() map[string]string {
			return map[string]string{"key": keyCodec.Name(), "value": valueCodec.Name()}
		},
	})
	return &Store[K, V]{
		ns:         ns,
		keyCodec:   keyCodec,
		valueCodec: valueCodec,
	}
}

// Namespace returns the namespace of the store.
func (s *Store[K, V]) Namespace() *catalog.Namespace {
	return s.ns
}

// KeyCodec returns the key codec of the store.
func (s *Store[K, V]) KeyCodec() codec.KeyCodec[K] {
	return s.keyCodec
}

// ValueCodec returns the value codec of the store.
func (s *Store[K, V]) ValueCodec() codec.ValueCodec[V] {
	return s.valueCodec
}

// Instantiate creates the instance of the store for a transaction.
func (s *Store[K, V]) Instantiate(tx kv.Tx) *Instance[K, V] {
	return &Instance[K, V]{
		store: s,
		tx:    s.ns.Open(tx),
	}
}

// Instance is a Store in a transaction.
type Instance[K, V any] struct {
	store *Store[K, V]
	tx    *prefix.Tx
}

// Writable reports whether the transaction of the instance accepts writes.
func (s *Instance[K, V]) Writable() bool {
	return s.tx.Writable()
}

// EncodeKey returns the key as it is stored in the namespace of the store.
func (s *Instance[K, V]) EncodeKey(key K) ([]byte, error) {
	kbz, err := s.store.keyCodec.Encode(key)
	if err != nil {
		return nil, fmt.Errorf("failed to encode key %v: %w", key, err)
	}
	return kbz, nil
}

// Get returns the value of the key, or kv.ErrNotFound if it does not exist.
func (s *Instance[K, V]) Get(key K) (*V, error) {
	kbz, err := s.EncodeKey(key)
	if err != nil {
		return nil, err
	}

	vbz, err := s.tx.Get(kbz)
	if err != nil {
		return nil, err
	}
	return s.decodeValue(vbz)
}

func (s *Instance[K, V]) decodeValue(vbz []byte) (*V, error) {
	v, err := s.store.valueCodec.Decode(vbz)
	if err != nil {
		return nil, fmt.Errorf("failed to decode value: %w", err)
	}
	return &v, nil
}

// Has reports whether the key exists.
func (s *Instance[K, V]) Has(key K) (bool, error) {
	kbz, err := s.EncodeKey(key)
	if err != nil {
		return false, err
	}
	return s.tx.Has(kbz)
}

// Set sets the value of the key.
func (s *Instance[K, V]) Set(key K, value *V) error {
	kbz, vbz, err := s.encode(key, value)
	if err != nil {
		return err
	}
	return s.tx.Set(kbz, vbz)
}

// SetWithTTL sets the value of the key and makes the key expire after the given duration.
// It returns kv.ErrUnsupported if the engine cannot write keys that expire.
func (s *Instance[K, V]) SetWithTTL(key K, value *V, ttl time.Duration) error {
	kbz, vbz, err := s.encode(key, value)
	if err != nil {
		return err
	}
	return s.tx.SetWithTTL(kbz, vbz, ttl)
}

func (s *Instance[K, V]) encode(key K, value *V) (kbz, vbz []byte, err error) {
	if value == nil {
		return nil, nil, ErrNilValue
	}

	kbz, err = s.EncodeKey(key)
	if err != nil {
		return nil, nil, err
	}

	vbz, err = s.store.valueCodec.Encode(*value)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to encode value: %w", err)
	}
	return kbz, vbz, nil
}

// Delete deletes the key. Deleting a key that does not exist is not an error.
func (s *Instance[K, V]) Delete(key K) error {
	kbz, err := s.EncodeKey(key)
	if err != nil {
		return err
	}
	return s.tx.Delete(kbz)
}

// Iterator iterates over the records whose keys are in the range, in ascending order.
// The zero range has every record in it. The iterator must be closed.
func (s *Instance[K, V]) Iterator(r expr.Range[K]) (*Iterator[K, V], error) {
	return s.iterator(r, false)
}

// ReverseIterator iterates over the records whose keys are in the range, in descending order.
// The zero range has every record in it. The iterator must be closed.
func (s *Instance[K, V]) ReverseIterator(r expr.Range[K]) (*Iterator[K, V], error) {
	return s.iterator(r, true)
}

func (s *Instance[K, V]) iterator(r expr.Range[K], reverse bool) (*Iterator[K, V], error) {
	start, end, err := s.bounds(r)
	if err != nil {
		return nil, err
	}

	var base kv.Iterator
	if reverse {
		base, err = s.tx.ReverseIterator(start, end)
	} else {
		base, err = s.tx.Iterator(start, end)
	}
	if err != nil {
		return nil, err
	}
	return &Iterator[K, V]{base: base, ins: s}, nil
}

// bounds returns the range of encoded keys that corresponds to a range of keys.
func (s *Instance[K, V]) bounds(r expr.Range[K]) (start, end []byte, err error) {
	if low := r.Low(); !low.IsEmpty() {
		start, err = s.EncodeKey(low.Value())
		if err != nil {
			return nil, nil, err
		}
		if low.Exclusive() {
			// The key right after a key is the key with a zero byte after it.
			start = append(start, 0x00)
		}
	}

	if high := r.High(); !high.IsEmpty() {
		end, err = s.EncodeKey(high.Value())
		if err != nil {
			return nil, nil, err
		}
		if !high.Exclusive() {
			end = append(end, 0x00)
		}
		if len(end) == 0 {
			// Nothing is less than the empty key, but an empty end would mean no bound at all.
			return []byte{0x00}, []byte{0x00}, nil
		}
	}
	return start, end, nil
}

// Iterator iterates over the records of a Store.
type Iterator[K, V any] struct {
	base kv.Iterator
	ins  *Instance[K, V]
}

// Valid implements the kvutil.Iterator interface.
func (it *Iterator[K, V]) Valid() bool {
	return it.base.Valid()
}

// Next implements the kvutil.Iterator interface.
func (it *Iterator[K, V]) Next() {
	it.base.Next()
}

// Seek moves to the given key or, if it is not in the range of the iterator, to the first key
// after it in the direction of the iterator.
func (it *Iterator[K, V]) Seek(key K) error {
	kbz, err := it.ins.EncodeKey(key)
	if err != nil {
		return err
	}

	it.base.Seek(kbz)
	return nil
}

// Key implements the kvutil.Iterator interface.
func (it *Iterator[K, V]) Key() (K, error) {
	key, err := it.ins.store.keyCodec.Decode(it.base.Key())
	if err != nil {
		return key, fmt.Errorf("failed to decode key: %w", err)
	}
	return key, nil
}

// Value implements the kvutil.Iterator interface.
func (it *Iterator[K, V]) Value() (*V, error) {
	vbz, err := it.base.Value()
	if err != nil {
		return nil, err
	}
	return it.ins.decodeValue(vbz)
}

// RawKey returns the current key as it is stored in the namespace of the store.
// The slice must not be modified and is valid until the iterator moves.
func (it *Iterator[K, V]) RawKey() []byte {
	return it.base.Key()
}

// RawValue returns the current value as it is stored.
// The slice must not be modified and is valid until the iterator moves.
func (it *Iterator[K, V]) RawValue() ([]byte, error) {
	return it.base.Value()
}

// Close implements the kvutil.Iterator interface.
func (it *Iterator[K, V]) Close() error {
	return it.base.Close()
}
