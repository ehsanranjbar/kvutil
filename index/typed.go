package index

import (
	"fmt"

	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/expr"
	"github.com/ehsanranjbar/kvutil/kv"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

// Extractor says what a typed index finds the records by: a value of the type F that is taken
// from a record, or several of them.
type Extractor[V, F any] struct {
	path  string
	get   func(*V) []F
	multi bool
	codec codec.KeyCodec[F]
}

// Field makes an Extractor of a function that returns the value of a field of a record.
// The path names the field in the descriptor of the index.
func Field[V, F any](path string, get func(*V) F) Extractor[V, F] {
	return Extractor[V, F]{
		path: path,
		get:  func(v *V) []F { return []F{get(v)} },
	}
}

// Each makes an Extractor of a function that returns several values for a record, such as its
// tags. The record is found by each of them, and a record with no value is not in the index.
func Each[V, F any](path string, get func(*V) []F) Extractor[V, F] {
	return Extractor[V, F]{
		path:  path,
		get:   get,
		multi: true,
	}
}

// WithCodec sets the key codec that the values are encoded with, instead of the one that their
// type has by default.
func (e Extractor[V, F]) WithCodec(c codec.KeyCodec[F]) Extractor[V, F] {
	e.codec = c
	return e
}

// Option is an option of a typed index.
type Option[V any] func(indexer Indexer[V]) Indexer[V]

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
func (c *coveringIndexer[V, C]) Lookup(conds ...any) ([]KeyRange, []any, error) {
	l, ok := c.Indexer.(Lookuper)
	if !ok {
		return nil, nil, fmt.Errorf("the indexer cannot be looked up with conditions")
	}
	return l.Lookup(conds...)
}

// extractorIndexer is the indexer of a typed index.
type extractorIndexer[V, F any] struct {
	by     Extractor[V, F]
	unique bool
}

// Index implements the Indexer interface.
func (x *extractorIndexer[V, F]) Index(v *V) ([]Entry, error) {
	values := x.by.get(v)
	entries := make([]Entry, 0, len(values))
	for _, value := range values {
		// The form that marks where the key ends, as the primary key follows it.
		key, err := x.by.codec.Append(nil, value)
		if err != nil {
			return nil, fmt.Errorf("failed to encode %s: %w", x.by.path, err)
		}
		entries = append(entries, Entry{Key: key})
	}
	return entries, nil
}

// Descriptor implements the Indexer interface.
func (x *extractorIndexer[V, F]) Descriptor() Descriptor {
	return Descriptor{
		Fields:     []FieldDescriptor{{Path: x.by.path}},
		Operators:  []Operator{Equal, Range},
		Unique:     x.unique,
		MultiEntry: x.by.multi,
		Encoding:   x.by.codec.Name(),
	}
}

// Typed is an index that finds the records by a value of the type F.
type Typed[K, V, F any] struct {
	*Index[K, V]
	codec codec.KeyCodec[F]
}

// Multi defines a new index on a store that finds the records by what the extractor takes from
// them. Any number of records can have the same value.
// It panics if the type of the values has no key codec and the extractor is not given one.
func Multi[K, V, F any](
	store extstore.Extensible[K, V],
	name string,
	by Extractor[V, F],
	opts ...Option[V],
) *Typed[K, V, F] {
	return newTyped(store, name, by, false, opts)
}

// Unique defines a new index on a store that finds the records by what the extractor takes from
// them, and that lets one record at most have a value. Setting a record with a value that
// another record has fails with ErrUniqueViolation.
// It panics if the type of the values has no key codec and the extractor is not given one.
func Unique[K, V, F any](
	store extstore.Extensible[K, V],
	name string,
	by Extractor[V, F],
	opts ...Option[V],
) *Typed[K, V, F] {
	return newTyped(store, name, by, true, opts)
}

func newTyped[K, V, F any](
	store extstore.Extensible[K, V],
	name string,
	by Extractor[V, F],
	unique bool,
	opts []Option[V],
) *Typed[K, V, F] {
	if by.codec == nil {
		c, err := codec.KeyFor[F]()
		if err != nil {
			// A mistake in the definition, which is made in code when the program starts.
			panic(fmt.Sprintf("index %q: %v; give the extractor one with WithCodec", name, err))
		}
		by.codec = c
	}

	var indexer Indexer[V] = &extractorIndexer[V, F]{by: by, unique: unique}
	for _, opt := range opts {
		indexer = opt(indexer)
	}
	return &Typed[K, V, F]{
		Index: New(store, name, indexer),
		codec: by.codec,
	}
}

// Instantiate creates the instance of the index for a transaction.
func (t *Typed[K, V, F]) Instantiate(tx kv.Tx) *TypedInstance[K, V, F] {
	return &TypedInstance[K, V, F]{
		Instance: t.Index.Instantiate(tx),
		codec:    t.codec,
	}
}

// TypedInstance is a Typed index in a transaction.
type TypedInstance[K, V, F any] struct {
	*Instance[K, V]
	codec codec.KeyCodec[F]
}

// Match iterates over the records that have the given value. The iterator must be closed.
func (i *TypedInstance[K, V, F]) Match(value F) (*Iterator[K, V], error) {
	b := expr.NewBound(value, false)
	return i.Range(expr.NewRange(b, b))
}

// Range iterates over the records whose values are in the range, in ascending order of the
// values. The zero range has every record of the index in it. The iterator must be closed.
func (i *TypedInstance[K, V, F]) Range(r expr.Range[F]) (*Iterator[K, V], error) {
	kr, err := i.keyRange(r)
	if err != nil {
		return nil, err
	}
	return i.Scan(kr)
}

// ReverseRange iterates over the records whose values are in the range, in descending order of
// the values. The zero range has every record of the index in it. The iterator must be closed.
func (i *TypedInstance[K, V, F]) ReverseRange(r expr.Range[F]) (*Iterator[K, V], error) {
	kr, err := i.keyRange(r)
	if err != nil {
		return nil, err
	}
	return i.ReverseScan(kr)
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

func (i *TypedInstance[K, V, F]) keyRange(r expr.Range[F]) (KeyRange, error) {
	encode := func(b *expr.Bound[F]) (*expr.Bound[[]byte], error) {
		if b.IsEmpty() {
			return nil, nil
		}
		key, err := i.codec.Append(nil, b.Value())
		if err != nil {
			return nil, fmt.Errorf("failed to encode the bound: %w", err)
		}
		return expr.NewBound(key, b.Exclusive()), nil
	}

	low, err := encode(r.Low())
	if err != nil {
		return KeyRange{}, err
	}
	high, err := encode(r.High())
	if err != nil {
		return KeyRange{}, err
	}
	return expr.NewRange(low, high), nil
}
