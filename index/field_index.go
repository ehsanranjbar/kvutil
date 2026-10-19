package index

import (
	"errors"
	"fmt"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/kv"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

// Option is an option of an index.
type Option[V any] func(indexer Indexer[V]) Indexer[V]

// Where keeps only the records that satisfy a condition in the index, which makes the index
// smaller and cheaper to keep up to date when most of the records are of no interest to it.
// A lookup in such an index only finds the records that satisfy the condition. A query (package
// query) reads such an index if its own conditions imply the condition of the index; the
// condition of an index that is a function (cond.Func) is implied only by the function of the
// same name, and its name must change when the function does. Versioned says that it did. An
// error of the condition for a record fails the setting of the record.
func Where[V any](cnd cond.Cond[V]) Option[V] {
	return func(indexer Indexer[V]) Indexer[V] {
		return filtered(indexer, cnd)
	}
}

// Versioned gives the index a version, which identifies what the functions that it relies on
// compute, like a condition that is a function (cond.Func) or an SQL function. The index is
// identified by its version as it is by its fields and its condition, so it is built again
// when the version changes. Versions that are given more than once, and the ones of the fields,
// are all a part of the version of the index.
func Versioned[V any](version string) Option[V] {
	return func(indexer Indexer[V]) Indexer[V] {
		return &versionedIndexer[V]{Indexer: indexer, version: version}
	}
}

type versionedIndexer[V any] struct {
	Indexer[V]
	version string
}

// Descriptor implements the Indexer interface.
func (v *versionedIndexer[V]) Descriptor() Descriptor {
	desc := v.Indexer.Descriptor()
	if v.version != "" {
		if desc.Version != "" {
			desc.Version += ";"
		}
		desc.Version += v.version
	}
	return desc
}

// Fields returns the fields of the wrapped indexer, if it is one of fields.
func (v *versionedIndexer[V]) Fields() []field.Component[V] {
	if f, ok := v.Indexer.(fielded[V]); ok {
		return f.Fields()
	}
	return nil
}

// Ranges implements the RangePlanner interface, if the wrapped indexer does.
func (v *versionedIndexer[V]) Ranges(conds ...cond.Cond[V]) ([]Bounds, []cond.Cond[V], error) {
	l, ok := v.Indexer.(RangePlanner[V])
	if !ok {
		return nil, nil, errors.New("the indexer cannot be looked up with conditions")
	}
	return l.Ranges(conds...)
}

// Covering stores a value with every entry of the index, which can be read from the index
// without getting the record. The paths name the fields that the value has, for the
// descriptor of the index. A nil codec is the default one of the type of the value.
func Covering[V, C any](get func(*V) C, c codec.ValueCodec[C], paths ...string) Option[V] {
	return func(indexer Indexer[V]) Indexer[V] {
		return covered(indexer, get, c, paths...)
	}
}

// covered wraps an indexer so that a value is stored with every entry that it returns.
func covered[V, C any](indexer Indexer[V], get func(*V) C, c codec.ValueCodec[C], paths ...string) Indexer[V] {
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

// Fields returns the fields of the wrapped indexer, if it is one of fields.
func (c *coveringIndexer[V, C]) Fields() []field.Component[V] {
	if f, ok := c.Indexer.(fielded[V]); ok {
		return f.Fields()
	}
	return nil
}

// Ranges implements the RangePlanner interface, if the wrapped indexer does.
func (c *coveringIndexer[V, C]) Ranges(conds ...cond.Cond[V]) ([]Bounds, []cond.Cond[V], error) {
	l, ok := c.Indexer.(RangePlanner[V])
	if !ok {
		return nil, nil, errors.New("the indexer cannot be looked up with conditions")
	}
	return l.Ranges(conds...)
}

func with[V any](indexer Indexer[V], opts []Option[V]) Indexer[V] {
	for _, opt := range opts {
		indexer = opt(indexer)
	}
	return indexer
}

// Composite defines a new index on a store that finds the records by several fields, which
// are in its keys in the order that they are given in. Any number of records can have the same
// values. The fields can be of different types; field.List makes a list of them.
func Composite[K, V any](
	store extstore.Extensible[K, V],
	name string,
	fields []field.Component[V],
	opts ...Option[V],
) *Index[K, V] {
	return New[K, V](store, name, ByFields(fields...), opts...)
}

// UniqueComposite defines a new index on a store that finds the records by several fields, and
// that lets one record at most have the same values for all of them. Setting a record with the
// values of another record fails with ErrUniqueViolation.
func UniqueComposite[K, V any](
	store extstore.Extensible[K, V],
	name string,
	fields []field.Component[V],
	opts ...Option[V],
) *Index[K, V] {
	return NewUnique[K, V](store, name, ByFields(fields...), opts...)
}

// FieldIndex is an index that finds the records by one field, whose values are of the type F.
type FieldIndex[K, V, F any] struct {
	*Index[K, V]
	field field.Field[V, F]
}

// On defines a new index on a store that finds the records by a field.
// Any number of records can have the same value.
func On[K, V, F any](
	store extstore.Extensible[K, V],
	name string,
	by field.Field[V, F],
	opts ...Option[V],
) *FieldIndex[K, V, F] {
	return &FieldIndex[K, V, F]{
		Index: New[K, V](store, name, ByFields[V](by), opts...),
		field: by,
	}
}

// Unique defines a new index on a store that finds the records by a field, and that lets one
// record at most have a value. Setting a record with a value that another record has fails
// with ErrUniqueViolation.
func Unique[K, V, F any](
	store extstore.Extensible[K, V],
	name string,
	by field.Field[V, F],
	opts ...Option[V],
) *FieldIndex[K, V, F] {
	return &FieldIndex[K, V, F]{
		Index: NewUnique[K, V](store, name, ByFields[V](by), opts...),
		field: by,
	}
}

// Field returns the field that the index finds the records by.
func (t *FieldIndex[K, V, F]) Field() field.Field[V, F] {
	return t.field
}

// Instantiate creates the instance of the index for a transaction.
func (t *FieldIndex[K, V, F]) Instantiate(tx kv.Tx) *FieldInstance[K, V, F] {
	return &FieldInstance[K, V, F]{
		Instance: t.Index.Instantiate(tx),
		field:    t.field,
	}
}

// FieldInstance is a FieldIndex in a transaction.
type FieldInstance[K, V, F any] struct {
	*Instance[K, V]
	field field.Field[V, F]
}

// Match iterates over the records that have the given value. The iterator must be closed.
func (i *FieldInstance[K, V, F]) Match(value F) (*Iterator[K, V], error) {
	return i.Lookup(i.field.Eq(value))
}

// Range iterates over the records whose values are in the range, in the order that the index
// has them in: from the least value, or from the greatest one if the field is descending.
// The iterator must be closed.
func (i *FieldInstance[K, V, F]) Range(r kvutil.Range[F]) (*Iterator[K, V], error) {
	return i.Lookup(i.field.Range(r))
}

// ReverseRange iterates over the records whose values are in the range, in the order opposite
// to the one that the index has them in. The iterator must be closed.
func (i *FieldInstance[K, V, F]) ReverseRange(r kvutil.Range[F]) (*Iterator[K, V], error) {
	return i.ReverseLookup(i.field.Range(r))
}

// Find returns the primary key of the first record that has the given value, which is the
// only one if the index is unique. It reports whether there is one.
func (i *FieldInstance[K, V, F]) Find(value F) (key K, found bool, err error) {
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
