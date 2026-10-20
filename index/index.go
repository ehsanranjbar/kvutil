// Package index provides secondary indexes for the stores that extensions can be added to.
package index

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/migrate"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	refstore "github.com/ehsanranjbar/kvutil/store/ref"
)

// ErrUniqueViolation is returned when a record is set whose key in a unique index belongs to
// another record.
var ErrUniqueViolation = errors.New("index: unique index violation")

// ErrIncomplete is returned for a lookup whose conditions leave a field of the index free that a
// record may have no value for, or several. The index has no entry for such a record, or has
// several, so the lookup would not find all the records that satisfy the conditions.
var ErrIncomplete = errors.New("index: the index may not have all the records that satisfy the conditions")

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

// Bounds is a range of index keys. A bound may be the beginning of a key, in which case it
// stands for all the keys that begin with it: a bound that is in the range has them in the
// range and a bound that is out of it has them out of it.
type Bounds = kvutil.Range[[]byte]

// RangePlanner is implemented by the indexers that can translate conditions into ranges of keys.
type RangePlanner[V any] interface {
	// Ranges returns the ranges of index keys that have every record that satisfies all the
	// conditions. The conditions that the ranges do not enforce are returned as the residual,
	// which the records must still be checked against.
	Ranges(conds ...cond.Cond[V]) (ranges []Bounds, residual []cond.Cond[V], err error)
}

// Descriptor describes an index to whoever plans queries on it.
type Descriptor struct {
	// Fields are the fields that are indexed, in the order that they have in the keys.
	// A field can be used for a lookup if all the fields before it are given exact values.
	Fields []FieldDescriptor
	// Operators are the kinds of comparisons that the index can serve.
	Operators []cond.Op
	// Unique is set if a key of the index belongs to one record at most.
	Unique bool
	// MultiEntry is set if a record may have more than one entry in the index.
	MultiEntry bool
	// Lossy is set if the index may return records that do not satisfy the conditions that it
	// was looked up with, which must then be checked again.
	Lossy bool
	// Covered are the fields that are stored with the entries.
	Covered []string
	// Where is the condition that a record must satisfy to be in the index, as text.
	// It is empty if every record is in the index.
	Where string
	// Condition is the condition of Where itself, as a cond.Cond of the type of the records.
	Condition any
	// Encoding identifies how the keys and the values of the entries are encoded.
	// An index whose encoding changes must be built again.
	Encoding string
	// Version identifies what the functions of the index compute, when that cannot be seen in
	// the paths of its fields and in its Where: the versions of its fields (see
	// field.Field.WithVersion) and the one that Versioned gives. It is empty if there are none.
	// An index whose version changes must be built again.
	Version string
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
	attrs := map[string]string{
		"fields":   strings.Join(fields, ","),
		"unique":   strconv.FormatBool(d.Unique),
		"covered":  strings.Join(d.Covered, ","),
		"where":    d.Where,
		"encoding": d.Encoding,
	}
	// Only an index that has a version has the attribute, so that the others are what they were.
	if d.Version != "" {
		attrs["version"] = d.Version
	}
	return attrs
}

// Index is an index of a store. It is kept up to date as an extension of the store,
// and is instantiated for a transaction to look records up.
type Index[K, V any] struct {
	name    string
	store   extstore.Extensible[K, V]
	indexer Indexer[V]
	desc    Descriptor
	refs    *refstore.Store
	// fields are the fields of the keys, which are nil for an indexer that is not one of fields,
	// and slots are what the values of the entries hold.
	fields []field.Component[V]
	slots  []slot[V]
}

// New defines a new index on a store, whose entries are computed by the given indexer.
func New[K, V any](
	store extstore.Extensible[K, V],
	name string,
	indexer Indexer[V],
	opts ...Option[V],
) *Index[K, V] {
	indexer = with(indexer, opts)
	return newIndex(store, name, indexer, indexer.Descriptor())
}

// NewUnique defines a new index on a store, whose entries are computed by the given indexer,
// and that lets one record at most have a key. Setting a record with a key that another record
// has fails with ErrUniqueViolation.
func NewUnique[K, V any](
	store extstore.Extensible[K, V],
	name string,
	indexer Indexer[V],
	opts ...Option[V],
) *Index[K, V] {
	indexer = with(indexer, opts)
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
		slots:   heldBy(indexer),
	}
	if f, ok := indexer.(fielded[V]); ok {
		x.fields = f.Fields()
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

// Fields returns the fields that the index finds the records by, in the order that they have
// in its keys. It returns nil if the indexer of the index is not one of fields.
func (x *Index[K, V]) Fields() []field.Component[V] {
	if f, ok := x.indexer.(fielded[V]); ok {
		return f.Fields()
	}
	return nil
}

// fielded is implemented by the indexers whose keys are made of fields.
type fielded[V any] interface {
	Fields() []field.Component[V]
}

// Complete checks that an index has an entry for every record that satisfies the conditions,
// which it has if each of its fields is either one that every record has exactly one value for
// (field.Always), or one that a condition needs a value of: a comparison of the field, at the
// top of the conditions, with a value or a range. A negation or a disjunction is no such
// condition. It returns ErrIncomplete naming the field if one is left free. An index whose keys
// are not made of fields is not checked.
func (x *Index[K, V]) Complete(conds ...cond.Cond[V]) error {
	fields := x.Fields()
	if fields == nil {
		return nil
	}

	needed := map[string]struct{}{}
	for _, c := range flatten(conds) {
		cmp, ok := c.(*cond.Comparison[V])
		if !ok || cmp.Err() != nil {
			continue
		}
		if cmp.Op == cond.OpRange || len(cmp.Values) > 0 {
			needed[cmp.Path] = struct{}{}
		}
	}

	for _, f := range fields {
		if _, ok := needed[f.Path()]; ok || f.Always() {
			continue
		}
		return fmt.Errorf("%w: the conditions do not need a value of %s, which a record may not have, for the index %q",
			ErrIncomplete, f.Path(), x.name)
	}
	return nil
}

// Prefix returns the prefix that the index keeps its entries under, which is given to it when it
// is added to its store. The slice must not be modified.
func (x *Index[K, V]) Prefix() []byte {
	return x.refs.Prefix()
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

// Keep implements the extstore.Keeper interface.
func (m maintainer[K, V]) Keep(prefix []byte) migrate.Part {
	x := m.x
	if x.desc.Unique {
		x.refs = refstore.NewUnique(prefix)
	} else {
		x.refs = refstore.New(prefix)
	}
	return migrate.Part{
		Kind:       "index",
		Attributes: x.desc.attributes,
		// An index is derived from the records, so it is built for the ones that are there
		// when it is defined, and again when it or the records have changed.
		Rebuild: func(ctx context.Context, db kv.DB) error {
			return Rebuild(ctx, db, x)
		},
	}
}

// Fields returns the fields that the index finds the records by; see Index.Fields. It is how
// a query finds the fields that can be looked up by going through the extensions of a store.
func (m maintainer[K, V]) Fields() []field.Component[V] {
	return m.x.Fields()
}

// Index returns the index that the extension keeps up to date. It is how a query finds the
// indexes of a store by going through its extensions.
func (m maintainer[K, V]) Index() *Index[K, V] {
	return m.x
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
