package vector

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strconv"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/prefix"
	"github.com/ehsanranjbar/kvutil/migrate"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

// entriesKey is what the prefix of the entries is made of, after the prefix of the extension.
// An entry is under that and the primary key of its record.
const entriesKey = 'v'

// encoding identifies how a vector is stored, which the index records of itself.
const encoding = "f32le"

// options are what the options of an index set.
type options struct {
	version string
}

// Option is an option of an index.
type Option func(*options)

// Versioned gives the index a version, which identifies what the function that returns the
// vectors computes, like the model that the embeddings come from. The index is built again
// (by migrate) when the version changes. Several versions are joined with ";".
func Versioned(version string) Option {
	return func(o *options) {
		if o.version == "" {
			o.version = version
		} else {
			o.version += ";" + version
		}
	}
}

// Index is a vector index of a store. It is kept up to date as an extension of the store,
// and is instantiated for a transaction to search in.
type Index[K, V any] struct {
	name    string
	store   extstore.Extensible[K, V]
	dims    int
	metric  Metric
	get     func(*V) []float32
	version string
	// prefix is the prefix of the extension, and entries the one that the entries are under.
	// They are set when the index is added to its store.
	prefix  []byte
	entries []byte
}

// On defines a new vector index on a store. get returns the vector of a record; a record for
// which it returns no vector (nil or empty) is not in the index. It panics if dims is not
// positive, the metric is not a defined one, or get is nil.
func On[K, V any](
	store extstore.Extensible[K, V],
	name string,
	dims int,
	metric Metric,
	get func(*V) []float32,
	opts ...Option,
) *Index[K, V] {
	// These are mistakes in the definition, which is made in code when the program starts.
	if dims <= 0 {
		panic(fmt.Sprintf("vector: the index %q must have a positive number of dimensions, not %d", name, dims))
	}
	if !metric.defined() {
		panic(fmt.Sprintf("vector: the index %q has %v, which is not a defined metric", name, metric))
	}
	if get == nil {
		panic(fmt.Sprintf("vector: the index %q has no function to get the vector of a record", name))
	}

	var o options
	for _, opt := range opts {
		opt(&o)
	}
	x := &Index[K, V]{
		name:    name,
		store:   store,
		dims:    dims,
		metric:  metric,
		get:     get,
		version: o.version,
	}
	store.AddExtension(name, maintainer[K, V]{x: x})
	return x
}

// Name returns the name of the index.
func (x *Index[K, V]) Name() string {
	return x.name
}

// Dims returns the number of components of the vectors of the index.
func (x *Index[K, V]) Dims() int {
	return x.dims
}

// Metric returns how the index measures the distance between vectors.
func (x *Index[K, V]) Metric() Metric {
	return x.metric
}

// Prefix returns the prefix that the index keeps its entries under, which is given to it when it
// is added to its store. The slice must not be modified.
func (x *Index[K, V]) Prefix() []byte {
	return x.prefix
}

// Instantiate creates the instance of the index for a transaction.
func (x *Index[K, V]) Instantiate(tx kv.Tx) *Instance[K, V] {
	return &Instance[K, V]{
		x:       x,
		tx:      tx,
		entries: prefix.New(tx, x.entries),
	}
}

// attributes returns what the stored entries of the index depend on.
func (x *Index[K, V]) attributes() map[string]string {
	attrs := map[string]string{
		"dims":     strconv.Itoa(x.dims),
		"metric":   x.metric.String(),
		"encoding": encoding,
	}
	// Only an index that has a version has the attribute, so that the others are what they were.
	if x.version != "" {
		attrs["version"] = x.version
	}
	return attrs
}

// maintainer is the extension that keeps an index up to date.
type maintainer[K, V any] struct {
	x *Index[K, V]
}

// Keep implements the extstore.Keeper interface.
func (m maintainer[K, V]) Keep(prefix []byte) migrate.Part {
	x := m.x
	x.prefix = bytes.Clone(prefix)
	x.entries = append(bytes.Clone(prefix), entriesKey)
	return migrate.Part{
		Kind:       "vector",
		Attributes: x.attributes,
		// An index is derived from the records, so it is built for the ones that are there
		// when it is defined, and again when it or the records have changed.
		Rebuild: func(ctx context.Context, db kv.DB) error {
			return Rebuild(ctx, db, x)
		},
	}
}

// Instantiate implements the extstore.Extension interface.
func (m maintainer[K, V]) Instantiate(tx kv.Tx) extstore.ExtensionInstance[K, V] {
	return &maintenance[K, V]{
		x:       m.x,
		entries: prefix.New(tx, m.x.entries),
	}
}

type maintenance[K, V any] struct {
	x       *Index[K, V]
	entries *prefix.Tx
}

// OnSet implements the extstore.ExtensionInstance interface.
func (m *maintenance[K, V]) OnSet(_ K, rawKey []byte, old, new *V, _ ...any) error {
	vec := m.x.get(new)
	if len(vec) == 0 {
		// The record has no vector now, so the entry of the one it had is to go.
		if old != nil && len(m.x.get(old)) > 0 {
			return m.entries.Delete(rawKey)
		}
		return nil
	}

	// A vector that is not valid fails the write even if it is the one the record had, which
	// can only be if the function that returns the vectors has changed.
	stored, err := m.x.prepare(vec)
	if err != nil {
		return fmt.Errorf("failed to index the record: %w", err)
	}
	if old != nil && slices.Equal(m.x.get(old), vec) {
		return nil
	}
	return m.entries.Set(rawKey, encode(stored))
}

// OnDelete implements the extstore.ExtensionInstance interface.
func (m *maintenance[K, V]) OnDelete(_ K, rawKey []byte, old *V) error {
	if len(m.x.get(old)) == 0 {
		return nil
	}
	return m.entries.Delete(rawKey)
}

// batchSize returns the number of records that Rebuild indexes in a transaction. It is bounded
// by the size of the vectors too, so that a transaction stays small whatever the dimensions.
func (x *Index[K, V]) batchSize() int {
	return min(500, max(16, (1<<20)/(4*x.dims)))
}

// Rebuild builds an index again from the records of its store: it deletes the entries that
// the index has and writes the ones of every record. It is how an index that is defined on a
// store with records in it gets to know of them, and migrate.Run calls it for an index that is
// new, that has changed, or whose store was migrated.
//
// It must not run while records are written to the store. A record with a vector that is not
// valid fails the build, which is left unfinished.
func Rebuild[K, V any](ctx context.Context, db kv.DB, x *Index[K, V]) error {
	start := x.prefix
	if err := kv.DeleteRange(ctx, db, start, kv.PrefixEnd(start)); err != nil {
		return fmt.Errorf("failed to clear the index %q: %w", x.name, err)
	}

	batch := x.batchSize()
	var last *K
	for more := true; more; {
		if err := ctx.Err(); err != nil {
			return err
		}

		// What a batch finds is only kept once its transaction is committed, as the function
		// may run more than once.
		var (
			batchLast *K
			batchSize int
		)
		err := db.Update(func(tx kv.Tx) error {
			batchLast, batchSize = last, 0

			var r kvutil.Range[K]
			if last != nil {
				r = r.After(*last)
			}
			it, err := x.store.Records(tx, r)
			if err != nil {
				return err
			}
			defer it.Close()

			entries := prefix.New(tx, x.entries)
			for ; it.Valid() && batchSize < batch; it.Next() {
				batchSize++
				key, err := it.Key()
				if err != nil {
					return err
				}
				record, err := it.Value()
				if err != nil {
					return err
				}
				if err := x.add(entries, key, record); err != nil {
					return err
				}
				batchLast = &key
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to build the index %q: %w", x.name, err)
		}
		last, more = batchLast, batchSize == batch
	}
	return nil
}

// add writes the entry of a record, if it has a vector.
func (x *Index[K, V]) add(entries *prefix.Tx, key K, record *V) error {
	vec := x.get(record)
	if len(vec) == 0 {
		return nil
	}

	stored, err := x.prepare(vec)
	if err != nil {
		return fmt.Errorf("failed to index the record %v: %w", key, err)
	}
	rawKey, err := x.store.KeyCodec().Encode(key)
	if err != nil {
		return err
	}
	return entries.Set(rawKey, encode(stored))
}
