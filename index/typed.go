package index

import (
	"errors"
	"fmt"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/schema"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

// Option is an option of an index that is defined on fields.
type Option[V any] func(indexer Indexer[V]) Indexer[V]

// Where keeps only the records that satisfy a condition in the index, which makes the index
// smaller and cheaper to keep up to date when most of the records are of no interest to it.
// A lookup in such an index only finds the records that satisfy the condition.
func Where[V any](cond schema.Cond[V]) Option[V] {
	return func(indexer Indexer[V]) Indexer[V] {
		return Filter(indexer, cond)
	}
}

// Covering stores a value with every entry of the index, which can be read from the index
// without getting the record. The paths name the fields that the value has, for the
// descriptor of the index. A nil codec is the default one of the type of the value.
func Covering[V, C any](get func(*V) C, c codec.ValueCodec[C], paths ...string) Option[V] {
	return func(indexer Indexer[V]) Indexer[V] {
		return Cover(indexer, get, c, paths...)
	}
}

// Cover wraps an indexer so that a value is stored with every entry that it returns.
// A nil codec is the default one of the type of the value.
func Cover[V, C any](indexer Indexer[V], get func(*V) C, c codec.ValueCodec[C], paths ...string) Indexer[V] {
	if c == nil {
		c = codec.ValueFor[C](nil)
	}
	return &coveringIndexer[V, C]{
		Indexer: indexer,
		get:     get,
		codec:   c,
		paths:   paths,
	}
}

type coveringIndexer[V, C any] struct {
	Indexer[V]
	get   func(*V) C
	codec codec.ValueCodec[C]
	paths []string
}

// Index implements the Indexer interface.
func (c *coveringIndexer[V, C]) Index(v *V) ([]Entry, error) {
	entries, err := c.Indexer.Index(v)
	if err != nil || len(entries) == 0 {
		return entries, err
	}

	value, err := c.codec.Encode(c.get(v))
	if err != nil {
		return nil, fmt.Errorf("failed to encode the covered value: %w", err)
	}
	for i := range entries {
		entries[i].Value = value
	}
	return entries, nil
}

// Descriptor implements the Indexer interface.
func (c *coveringIndexer[V, C]) Descriptor() Descriptor {
	desc := c.Indexer.Descriptor()
	desc.Covered = c.paths
	desc.Encoding += "+" + c.codec.Name()
	return desc
}

// Lookup implements the Lookuper interface, if the wrapped indexer does.
func (c *coveringIndexer[V, C]) Lookup(conds ...schema.Cond[V]) ([]KeyRange, []schema.Cond[V], error) {
	l, ok := c.Indexer.(Lookuper[V])
	if !ok {
		return nil, nil, errors.New("the indexer cannot be looked up with conditions")
	}
	return l.Lookup(conds...)
}

func with[V any](indexer Indexer[V], opts []Option[V]) Indexer[V] {
	for _, opt := range opts {
		indexer = opt(indexer)
	}
	return indexer
}

// Composite defines a new index on a store that finds the records by several fields, which
// are in its keys in the order that they are given in. Any number of records can have the same
// values. The fields can be of different types; schema.Fields makes a list of them.
func Composite[K, V any](
	store extstore.Extensible[K, V],
	name string,
	fields []schema.Component[V],
	opts ...Option[V],
) *Index[K, V] {
	return New(store, name, with[V](ByFields(fields...), opts))
}

// UniqueComposite defines a new index on a store that finds the records by several fields, and
// that lets one record at most have the same values for all of them. Setting a record with the
// values of another record fails with ErrUniqueViolation.
func UniqueComposite[K, V any](
	store extstore.Extensible[K, V],
	name string,
	fields []schema.Component[V],
	opts ...Option[V],
) *Index[K, V] {
	return NewUnique(store, name, with[V](ByFields(fields...), opts))
}

// Typed is an index that finds the records by one field, whose values are of the type F.
type Typed[K, V, F any] struct {
	*Index[K, V]
	field schema.Field[V, F]
}

// Multi defines a new index on a store that finds the records by a field.
// Any number of records can have the same value.
func Multi[K, V, F any](
	store extstore.Extensible[K, V],
	name string,
	by schema.Field[V, F],
	opts ...Option[V],
) *Typed[K, V, F] {
	return &Typed[K, V, F]{
		Index: New(store, name, with[V](ByFields[V](by), opts)),
		field: by,
	}
}

// Unique defines a new index on a store that finds the records by a field, and that lets one
// record at most have a value. Setting a record with a value that another record has fails
// with ErrUniqueViolation.
func Unique[K, V, F any](
	store extstore.Extensible[K, V],
	name string,
	by schema.Field[V, F],
	opts ...Option[V],
) *Typed[K, V, F] {
	return &Typed[K, V, F]{
		Index: NewUnique(store, name, with[V](ByFields[V](by), opts)),
		field: by,
	}
}

// Field returns the field that the index finds the records by.
func (t *Typed[K, V, F]) Field() schema.Field[V, F] {
	return t.field
}

// Instantiate creates the instance of the index for a transaction.
func (t *Typed[K, V, F]) Instantiate(tx kv.Tx) *TypedInstance[K, V, F] {
	return &TypedInstance[K, V, F]{
		Instance: t.Index.Instantiate(tx),
		field:    t.field,
	}
}

// TypedInstance is a Typed index in a transaction.
type TypedInstance[K, V, F any] struct {
	*Instance[K, V]
	field schema.Field[V, F]
}

// Match iterates over the records that have the given value. The iterator must be closed.
func (i *TypedInstance[K, V, F]) Match(value F) (*Iterator[K, V], error) {
	return i.Lookup(i.field.Eq(value))
}

// Range iterates over the records whose values are in the range, in the order that the index
// has them in: from the least value, or from the greatest one if the field is descending.
// The iterator must be closed.
func (i *TypedInstance[K, V, F]) Range(r kvutil.Range[F]) (*Iterator[K, V], error) {
	return i.Lookup(i.field.Range(r))
}

// ReverseRange iterates over the records whose values are in the range, in the order opposite
// to the one that the index has them in. The iterator must be closed.
func (i *TypedInstance[K, V, F]) ReverseRange(r kvutil.Range[F]) (*Iterator[K, V], error) {
	return i.ReverseLookup(i.field.Range(r))
}

// Find returns the primary key of the first record that has the given value, which is the
// only one if the index is unique. It reports whether there is one.
func (i *TypedInstance[K, V, F]) Find(value F) (key K, found bool, err error) {
	it, err := i.Match(value)
	if err != nil {
		return key, false, err
	}
	defer it.Close()

	if !it.Valid() {
		return key, false, it.Err()
	}
	key, err = it.Key()
	return key, err == nil, err
}
