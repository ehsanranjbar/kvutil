// Package index provides secondary indexes for the stores that extensions can be added to.
package index

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/ehsanranjbar/kvutil/catalog"
	"github.com/ehsanranjbar/kvutil/expr"
	"github.com/ehsanranjbar/kvutil/kv"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	refstore "github.com/ehsanranjbar/kvutil/store/ref"
)

// ErrUniqueViolation is returned when a record is set whose key in a unique index belongs to
// another record.
var ErrUniqueViolation = errors.New("index: unique index violation")

// Entry is an entry of a record in an index.
type Entry struct {
	// Key is what the record is found by. An index key must mark where it ends, or all the keys
	// of the index must have the same length, so that no key is the beginning of another one.
	Key []byte
	// Value is an optional value that is stored with the entry, which can be read from the index
	// without getting the record.
	Value []byte
}

// Indexer computes the entries that a record has in an index.
// The entries must only depend on the record, as they are computed again to remove them.
type Indexer[V any] interface {
	// Index returns the entries of a record. A record may have no entry, or several.
	Index(v *V) ([]Entry, error)
	// Descriptor describes the index.
	Descriptor() Descriptor
}

// KeyRange is a range of index keys. A bound may be the beginning of a key, in which case it
// stands for all the keys that begin with it: an inclusive bound has them in the range and an
// exclusive bound has them out of it. A nil bound leaves that side unbounded.
type KeyRange = expr.Range[[]byte]

// Lookuper is implemented by the indexers that can translate conditions into ranges of keys.
type Lookuper interface {
	// Lookup returns the ranges of index keys that have every record that satisfies the
	// conditions. The conditions that the ranges do not enforce are returned as the residual,
	// which the records must still be checked against.
	Lookup(conds ...any) (ranges []KeyRange, residual []any, err error)
}

// Operator is a kind of condition that an index can serve.
type Operator string

const (
	// Equal is the condition of a field being equal to a value.
	Equal Operator = "="
	// Range is the condition of a field being in a range of values.
	Range Operator = "range"
)

// Descriptor describes an index to whoever plans queries on it.
type Descriptor struct {
	// Fields are the fields that are indexed, in the order that they have in the keys.
	// A field can be used for a lookup if all the fields before it are given exact values.
	Fields []FieldDescriptor
	// Operators are the kinds of conditions that the index can serve.
	Operators []Operator
	// Unique is set if a key of the index belongs to one record at most.
	Unique bool
	// MultiEntry is set if a record may have more than one entry in the index.
	MultiEntry bool
	// Lossy is set if the index may return records that do not satisfy the conditions that it
	// was looked up with, which must then be checked again.
	Lossy bool
	// Covered are the fields that are stored with the entries.
	Covered []string
	// Encoding identifies how the keys and the values of the entries are encoded.
	// An index whose encoding changes must be built again.
	Encoding string
}

// FieldDescriptor describes an indexed field.
type FieldDescriptor struct {
	// Path is the path of the field in the record.
	Path string
	// Descending is set if the keys sort from the greatest value of the field to the least.
	Descending bool
}

// attributes returns what the stored entries of an index with this descriptor depend on.
func (d Descriptor) attributes() map[string]string {
	fields := make([]string, 0, len(d.Fields))
	for _, f := range d.Fields {
		if f.Descending {
			fields = append(fields, f.Path+" desc")
		} else {
			fields = append(fields, f.Path)
		}
	}
	return map[string]string{
		"fields":   strings.Join(fields, ","),
		"unique":   strconv.FormatBool(d.Unique),
		"covered":  strings.Join(d.Covered, ","),
		"encoding": d.Encoding,
	}
}

// Index is an index of a store. It is kept up to date as an extension of the store,
// and is instantiated for a transaction to look records up.
type Index[K, V any] struct {
	name    string
	store   extstore.Extensible[K, V]
	indexer Indexer[V]
	desc    Descriptor
	refs    *refstore.Store
}

// New defines a new index on a store, whose entries are computed by the given indexer.
func New[K, V any](store extstore.Extensible[K, V], name string, indexer Indexer[V]) *Index[K, V] {
	return newIndex(store, name, indexer, indexer.Descriptor())
}

// NewUnique defines a new index on a store, whose entries are computed by the given indexer,
// and that lets one record at most have a key. Setting a record with a key that another record
// has fails with ErrUniqueViolation.
func NewUnique[K, V any](store extstore.Extensible[K, V], name string, indexer Indexer[V]) *Index[K, V] {
	desc := indexer.Descriptor()
	desc.Unique = true
	return newIndex(store, name, indexer, desc)
}

func newIndex[K, V any](
	store extstore.Extensible[K, V],
	name string,
	indexer Indexer[V],
	desc Descriptor,
) *Index[K, V] {
	x := &Index[K, V]{
		name:    name,
		store:   store,
		indexer: indexer,
		desc:    desc,
	}
	store.AddExtension(name, maintainer[K, V]{x: x})
	return x
}

// Name returns the name of the index.
func (x *Index[K, V]) Name() string {
	return x.name
}

// Descriptor returns the descriptor of the index.
func (x *Index[K, V]) Descriptor() Descriptor {
	return x.desc
}

// Indexer returns the indexer of the index.
func (x *Index[K, V]) Indexer() Indexer[V] {
	return x.indexer
}

// Namespace returns the namespace that the index keeps its entries in.
func (x *Index[K, V]) Namespace() *catalog.Namespace {
	return x.refs.Namespace()
}

// Instantiate creates the instance of the index for a transaction.
func (x *Index[K, V]) Instantiate(tx kv.Tx) *Instance[K, V] {
	return &Instance[K, V]{
		x:    x,
		tx:   tx,
		refs: x.refs.Instantiate(tx),
	}
}

// maintainer is the extension that keeps an index up to date.
type maintainer[K, V any] struct {
	x *Index[K, V]
}

// UseNamespace implements the extstore.NamespaceUser interface.
func (m maintainer[K, V]) UseNamespace(ns *catalog.Namespace) {
	x := m.x
	def := catalog.Definition{
		Kind:       "index",
		KeyType:    reflect.TypeFor[K]().String(),
		ValueType:  reflect.TypeFor[V]().String(),
		Attributes: x.desc.attributes,
		Descriptor: x.desc,
	}
	if x.desc.Unique {
		x.refs = refstore.NewUnique(ns, def)
	} else {
		x.refs = refstore.New(ns, def)
	}
}

// Instantiate implements the extstore.Extension interface.
func (m maintainer[K, V]) Instantiate(tx kv.Tx) extstore.ExtensionInstance[K, V] {
	return &maintenance[K, V]{
		x:    m.x,
		refs: m.x.refs.Instantiate(tx),
	}
}

type maintenance[K, V any] struct {
	x    *Index[K, V]
	refs *refstore.Instance
}

// OnSet implements the extstore.ExtensionInstance interface.
func (m *maintenance[K, V]) OnSet(_ K, rawKey []byte, old, new *V, _ ...any) error {
	var oldEntries []Entry
	if old != nil {
		var err error
		oldEntries, err = m.x.indexer.Index(old)
		if err != nil {
			return fmt.Errorf("failed to index the old record: %w", err)
		}
	}
	newEntries, err := m.x.indexer.Index(new)
	if err != nil {
		return fmt.Errorf("failed to index the record: %w", err)
	}

	// Only what has changed is written, and the entries that are gone are removed first so that
	// a unique key can be taken again.
	removed, upserted := Diff(oldEntries, newEntries)
	return m.apply(rawKey, removed, upserted)
}

// OnDelete implements the extstore.ExtensionInstance interface.
func (m *maintenance[K, V]) OnDelete(_ K, rawKey []byte, old *V) error {
	entries, err := m.x.indexer.Index(old)
	if err != nil {
		return fmt.Errorf("failed to index the old record: %w", err)
	}
	return m.apply(rawKey, entries, nil)
}

func (m *maintenance[K, V]) apply(rawKey []byte, removed, upserted []Entry) error {
	for _, e := range removed {
		if err := m.refs.Delete(e.Key, rawKey); err != nil {
			return err
		}
	}
	for _, e := range upserted {
		err := m.refs.Set(e.Key, rawKey, e.Value)
		if errors.Is(err, refstore.ErrDuplicate) {
			return fmt.Errorf("%w: %q", ErrUniqueViolation, m.x.name)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// Diff compares the entries that a record had with the entries that it has now.
// It returns the entries to remove, which are the old ones whose keys are gone, and the entries
// to write, which are the new ones whose keys are new or whose values have changed.
// An entry that is given more than once counts once.
func Diff(old, new []Entry) (removed, upserted []Entry) {
	oldValues := make(map[string][]byte, len(old))
	for _, e := range old {
		oldValues[string(e.Key)] = e.Value
	}

	newKeys := make(map[string]struct{}, len(new))
	for _, e := range new {
		if _, ok := newKeys[string(e.Key)]; ok {
			continue
		}
		newKeys[string(e.Key)] = struct{}{}

		if value, ok := oldValues[string(e.Key)]; !ok || !bytes.Equal(value, e.Value) {
			upserted = append(upserted, e)
		}
	}

	for _, e := range old {
		if _, ok := newKeys[string(e.Key)]; ok {
			continue
		}
		if _, ok := oldValues[string(e.Key)]; ok {
			delete(oldValues, string(e.Key))
			removed = append(removed, e)
		}
	}
	return removed, upserted
}
