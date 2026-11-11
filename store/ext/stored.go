package ext

import (
	"fmt"

	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/store/typed"
)

// Stored is a record that was read from a store and is not decoded yet. It is a value, so that
// handing one out allocates nothing. One that comes from an iterator is valid until the iterator
// moves; one that comes from a getter stays valid.
type Stored[K, V any] struct {
	key    K
	bz     []byte
	codec  codec.ValueCodec[V]
	fix    func(v *V, key K)
	record *V
}

// NewStored returns a stored record of a key from its encoded value. fix, if it is not nil, is
// applied to the record after it is decoded, in full or in part: the record store sets the id
// with it.
func NewStored[K, V any](key K, bz []byte, c codec.ValueCodec[V], fix func(v *V, key K)) Stored[K, V] {
	return Stored[K, V]{key: key, bz: bz, codec: c, fix: fix}
}

// Decoded returns a stored record that is decoded already, for whatever cannot give the encoded
// value: Partial returns the record itself.
func Decoded[K, V any](key K, record *V) Stored[K, V] {
	return Stored[K, V]{key: key, record: record}
}

// Fixed returns the stored record with a key and a function that is applied to the record after
// it is decoded; see NewStored. A record that is decoded already is returned as it is.
func (s Stored[K, V]) Fixed(key K, fix func(v *V, key K)) Stored[K, V] {
	s.key, s.fix = key, fix
	return s
}

// Record decodes the whole record. It decodes it each time it is called; whoever calls it twice
// keeps the result.
func (s Stored[K, V]) Record() (*V, error) {
	if s.record != nil {
		return s.record, nil
	}

	v, err := s.codec.Decode(s.bz)
	if err != nil {
		return nil, fmt.Errorf("failed to decode value: %w", err)
	}
	if s.fix != nil {
		s.fix(&v, s.key)
	}
	return &v, nil
}

// Partial decodes the record with a decoder of codec.PartialOf into the given value, which it
// zeroes first, applies fix, and returns it. A stored record that is decoded already returns
// that record instead, which has all the fields.
func (s Stored[K, V]) Partial(decode func([]byte, *V) error, into *V) (*V, error) {
	if s.record != nil {
		return s.record, nil
	}

	var zero V
	*into = zero
	if err := decode(s.bz, into); err != nil {
		return nil, fmt.Errorf("failed to decode value: %w", err)
	}
	if s.fix != nil {
		s.fix(into, s.key)
	}
	return into, nil
}

// StoredGetter is an optional capability of a Getter: getting a record without decoding it.
type StoredGetter[K, V any] interface {
	GetStored(key K) (Stored[K, V], error)
}

// StoredIterator is an optional capability of the iterators of the records of a store (what
// Records returns): the current record without decoding it. It is valid until the iterator moves.
type StoredIterator[K, V any] interface {
	Stored() (Stored[K, V], error)
}

// ValueCoded is implemented by the stores that tell the codec of their values.
type ValueCoded[V any] interface {
	ValueCodec() codec.ValueCodec[V]
}

// GetStored returns the record of the key without decoding it, or kv.ErrNotFound if it does not
// exist. It implements the StoredGetter interface.
func (s *Instance[K, V]) GetStored(key K) (Stored[K, V], error) {
	bz, err := s.data.GetRaw(key)
	if err != nil {
		return Stored[K, V]{}, err
	}
	return NewStored(key, bz, s.store.data.ValueCodec(), nil), nil
}

// records is the iterator that Records returns: the iterator of the serialized store, with the
// current record available without decoding it.
type records[K, V any] struct {
	*typed.Iterator[K, V]
	codec codec.ValueCodec[V]
}

// Stored implements the StoredIterator interface. The key of the stored record is not decoded.
func (it *records[K, V]) Stored() (Stored[K, V], error) {
	bz, err := it.RawValue()
	if err != nil {
		return Stored[K, V]{}, err
	}
	var key K
	return NewStored(key, bz, it.codec, nil), nil
}
