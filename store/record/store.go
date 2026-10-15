// Package record provides a store of records: values that know their own id, which is their key.
// The id does not have to be part of the stored value, as it is set on every record that is read.
package record

import (
	"errors"
	"fmt"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/schema"
	"github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/ehsanranjbar/kvutil/store/typed"
)

// ErrNoID is returned when a record with the zero id is set in a store that has no id function.
var ErrNoID = errors.New("record: zero id and no id function")

// Record is the pointer of a record type that knows its id.
// The id is the key of the record in the store, so there is no need to serialize it as part of the value.
type Record[I comparable, T any] interface {
	*T
	GetId() I
	SetId(I)
}

// Store is a store of records, which is an extensible store whose values know their own id.
type Store[I comparable, T any] struct {
	base   *ext.Store[I, T]
	getID  func(*T) I
	setID  func(*T, I)
	idFunc func(*T) (I, error)
}

// New defines a new Store in the given namespace, with the codecs that its types have by default.
// It panics if the type of the ids has no key codec, in which case the store must be defined
// with NewWithCodecs.
func New[I comparable, T any, PT Record[I, T]](ns *schema.Namespace) *Store[I, T] {
	return NewWithCodecs[I, T, PT](ns, nil, nil)
}

// NewWithCodecs defines a new Store in the given namespace with the given codecs.
// A codec that is nil is the one that New would use.
func NewWithCodecs[I comparable, T any, PT Record[I, T]](
	ns *schema.Namespace,
	idCodec codec.KeyCodec[I],
	valueCodec codec.ValueCodec[T],
) *Store[I, T] {
	return NewWithAccessors(ns,
		func(v *T) I { return PT(v).GetId() },
		func(v *T, id I) { PT(v).SetId(id) },
		idCodec, valueCodec)
}

// NewWithAccessors defines a new Store in the given namespace for a type of records that does not
// have the methods GetId and SetId, with the functions that get and set the id of a record instead.
// A codec that is nil is the one that New would use. It panics if either function is nil.
func NewWithAccessors[I comparable, T any](
	ns *schema.Namespace,
	getID func(*T) I,
	setID func(*T, I),
	idCodec codec.KeyCodec[I],
	valueCodec codec.ValueCodec[T],
) *Store[I, T] {
	if getID == nil || setID == nil {
		// A mistake in the definition, which is made in code when the program starts.
		panic(fmt.Sprintf("store %q: the functions that get and set the id are required", ns.Path()))
	}

	s := &Store[I, T]{
		base:  ext.NewWithCodecs(ns, idCodec, valueCodec),
		getID: getID,
		setID: setID,
	}
	ns.Definition().Kind = "record"
	return s
}

// WithIDFunc sets the function that gives an id to a record that is set with the zero id.
func (s *Store[I, T]) WithIDFunc(f func(*T) (I, error)) *Store[I, T] {
	s.idFunc = f
	return s
}

// WithExtension adds an extension to the store. Extensions are called in the order that they are added.
func (s *Store[I, T]) WithExtension(name string, e ext.Extension[I, T]) *Store[I, T] {
	s.base.AddExtension(name, e)
	return s
}

// AddExtension implements the ext.Extensible interface. It is WithExtension without a result.
func (s *Store[I, T]) AddExtension(name string, e ext.Extension[I, T]) {
	s.base.AddExtension(name, e)
}

// AddDeleteHook implements the ext.DeleteHooks interface.
func (s *Store[I, T]) AddDeleteHook(name string, hook ext.DeleteHook[I]) {
	s.base.AddDeleteHook(name, hook)
}

// KeyCodec implements the ext.Extensible interface. It is the codec of the ids.
func (s *Store[I, T]) KeyCodec() codec.KeyCodec[I] {
	return s.base.KeyCodec()
}

// Getter implements the ext.Extensible interface. The records that it gets have their ids set.
func (s *Store[I, T]) Getter(tx kv.Tx) ext.Getter[I, T] {
	return s.Instantiate(tx)
}

// IDOf returns the id of a record, which is the zero value for a record that has none yet.
func (s *Store[I, T]) IDOf(v *T) I {
	return s.getID(v)
}

// Contains implements the ext.Referable interface.
func (s *Store[I, T]) Contains(tx kv.Tx, id I) (bool, error) {
	return s.base.Contains(tx, id)
}

// Records implements the ext.Extensible interface. The records that it iterates over have their ids set.
func (s *Store[I, T]) Records(tx kv.Tx, r kvutil.Range[I]) (iterutil.Iterator[I, *T], error) {
	it, err := s.Instantiate(tx).Iterator(r)
	if err != nil {
		return nil, err
	}
	return it, nil
}

// ValueCodec returns the value codec of the store.
func (s *Store[I, T]) ValueCodec() codec.ValueCodec[T] {
	return s.base.ValueCodec()
}

// Namespace returns the namespace of the store.
func (s *Store[I, T]) Namespace() *schema.Namespace {
	return s.base.Namespace()
}

// RetireExtension says that the data of an extension that is not added to the store any more
// is to be deleted from the database. See the RetireExtension of the ext store.
func (s *Store[I, T]) RetireExtension(name string) {
	s.base.RetireExtension(name)
}

// DataNamespace returns the namespace that the records are kept in, under the one of the store.
func (s *Store[I, T]) DataNamespace() *schema.Namespace {
	return s.base.DataNamespace()
}

// Instantiate creates the instance of the store for a transaction.
func (s *Store[I, T]) Instantiate(tx kv.Tx) *Instance[I, T] {
	return &Instance[I, T]{
		store: s,
		base:  s.base.Instantiate(tx),
	}
}

// Instance is a Store in a transaction.
type Instance[I comparable, T any] struct {
	store *Store[I, T]
	base  *ext.Instance[I, T]
}

// Writable reports whether the transaction of the instance accepts writes.
func (s *Instance[I, T]) Writable() bool {
	return s.base.Writable()
}

// Get returns the record of the id, or kv.ErrNotFound if it does not exist.
// The id is set on the record.
func (s *Instance[I, T]) Get(id I) (*T, error) {
	v, err := s.base.Get(id)
	if err != nil {
		return nil, err
	}
	s.store.setID(v, id)
	return v, nil
}

// Has reports whether the id exists.
func (s *Instance[I, T]) Has(id I) (bool, error) {
	return s.base.Has(id)
}

// Set sets the record under its id. A record with the zero id is given one by the id function of
// the store, which is set on it, or ErrNoID is returned if the store has none.
// The options are passed to the extensions, as ext.Instance.SetWithOptions does.
func (s *Instance[I, T]) Set(v *T, opts ...any) error {
	if v == nil {
		return typed.ErrNilValue
	}

	var zero I
	id := s.store.getID(v)
	if id == zero {
		if s.store.idFunc == nil {
			return ErrNoID
		}

		var err error
		id, err = s.store.idFunc(v)
		if err != nil {
			return fmt.Errorf("failed to get id: %w", err)
		}
		s.store.setID(v, id)
	}

	return s.base.SetWithOptions(id, v, opts...)
}

// Delete deletes the record of the id. Deleting an id that does not exist does nothing.
func (s *Instance[I, T]) Delete(id I) error {
	return s.base.Delete(id)
}

// Iterator iterates over the records whose ids are in the range, in ascending order.
// The zero range has every record in it. The iterator must be closed.
func (s *Instance[I, T]) Iterator(r kvutil.Range[I]) (*Iterator[I, T], error) {
	it, err := s.base.Iterator(r)
	if err != nil {
		return nil, err
	}
	return &Iterator[I, T]{base: it, setID: s.store.setID}, nil
}

// ReverseIterator iterates over the records whose ids are in the range, in descending order.
// The zero range has every record in it. The iterator must be closed.
func (s *Instance[I, T]) ReverseIterator(r kvutil.Range[I]) (*Iterator[I, T], error) {
	it, err := s.base.ReverseIterator(r)
	if err != nil {
		return nil, err
	}
	return &Iterator[I, T]{base: it, setID: s.store.setID}, nil
}
