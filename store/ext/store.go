// Package ext provides a store that extensions can be attached to, which are told of every
// record that is set and deleted. Indexes and stored aggregations are built as extensions.
package ext

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/internal/ordmap"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/schema"
	"github.com/ehsanranjbar/kvutil/store/typed"
)

// ErrTTLWithExtensions is returned when a record that expires is set in a store that has
// extensions. A record that expires would not be reported to them as deleted.
var ErrTTLWithExtensions = errors.New("ext: a store with extensions cannot have records that expire")

var (
	dataPrefix = []byte{'d'}
	extPrefix  = []byte{'x'}
)

// Store is a wrapper around a serialized store with an ordered list of extensions
// that are called on set and delete.
type Store[K, V any] struct {
	ns          *schema.Namespace
	data        *typed.Store[K, V]
	extNS       *schema.Namespace
	exts        *ordmap.Map[string, Extension[K, V]]
	deleteHooks *ordmap.Map[string, DeleteHook[K]]
}

// New defines a new Store in the given namespace, with the codecs that its types have by default.
// It panics if the type of the keys has no key codec, in which case the store must be defined
// with NewWithCodecs.
func New[K, V any](ns *schema.Namespace) *Store[K, V] {
	return NewWithCodecs[K, V](ns, nil, nil)
}

// NewWithCodecs defines a new Store in the given namespace with the given codecs.
// A codec that is nil is the one that New would use.
func NewWithCodecs[K, V any](
	ns *schema.Namespace,
	keyCodec codec.KeyCodec[K],
	valueCodec codec.ValueCodec[V],
) *Store[K, V] {
	s := &Store[K, V]{
		ns:          ns,
		exts:        ordmap.New[string, Extension[K, V]](),
		deleteHooks: ordmap.New[string, DeleteHook[K]](),
	}
	ns.Define(schema.Definition{
		Kind:      "ext",
		KeyType:   reflect.TypeFor[K]().String(),
		ValueType: reflect.TypeFor[V]().String(),
	})
	s.data = typed.NewWithCodecs(ns.Sub("data", schema.WithPrefix(dataPrefix)), keyCodec, valueCodec)
	// What the extensions keep depends on the records: an extension that is added to a store
	// that has records must say what it makes of them.
	s.extNS = ns.Sub("ext", schema.WithPrefix(extPrefix), schema.Dependent())
	return s
}

// WithExtension adds an extension to the store. Extensions are called in the order that they
// are added. An extension that is a NamespaceUser is given a namespace of its own.
func (s *Store[K, V]) WithExtension(name string, ext Extension[K, V]) *Store[K, V] {
	s.AddExtension(name, ext)
	return s
}

// RetireExtension says that the data of an extension that is not added to the store any more
// is to be deleted from the database; see schema.Migrate. The name is the one that the
// extension was added with. It is for an extension that keeps data that cannot be built again:
// an index or an aggregation that is not defined any more is deleted without it.
func (s *Store[K, V]) RetireExtension(name string) {
	s.extNS.Retire(name)
}

// AddExtension implements the Extensible interface. It is WithExtension without a result.
func (s *Store[K, V]) AddExtension(name string, ext Extension[K, V]) {
	// A name that is taken is reported by the schema. The extension is given its namespace
	// all the same, so that it is complete, but it is not called by the store.
	ns := s.extNS.Sub(name)
	if nu, ok := ext.(NamespaceUser); ok {
		nu.UseNamespace(ns)
	}
	s.exts.Add(name, ext)
}

// Getter implements the Extensible interface.
func (s *Store[K, V]) Getter(tx kv.Tx) Getter[K, V] {
	return s.Instantiate(tx)
}

// Contains implements the Referable interface.
func (s *Store[K, V]) Contains(tx kv.Tx, key K) (bool, error) {
	return s.data.Instantiate(tx).Has(key)
}

// Records implements the Extensible interface.
func (s *Store[K, V]) Records(tx kv.Tx, r kvutil.Range[K]) (iterutil.Iterator[K, *V], error) {
	return s.data.Instantiate(tx).Iterator(r)
}

// AddDeleteHook implements the DeleteHooks interface. Delete hooks are called after the extensions.
func (s *Store[K, V]) AddDeleteHook(name string, hook DeleteHook[K]) {
	s.deleteHooks.Add(name, hook)
}

// Namespace returns the namespace of the store.
func (s *Store[K, V]) Namespace() *schema.Namespace {
	return s.ns
}

// DataNamespace returns the namespace that the records are kept in, under the one of the store.
func (s *Store[K, V]) DataNamespace() *schema.Namespace {
	return s.data.Namespace()
}

// Data returns the serialized store that holds the records.
func (s *Store[K, V]) Data() *typed.Store[K, V] {
	return s.data
}

// KeyCodec returns the key codec of the store.
func (s *Store[K, V]) KeyCodec() codec.KeyCodec[K] {
	return s.data.KeyCodec()
}

// ValueCodec returns the value codec of the store.
func (s *Store[K, V]) ValueCodec() codec.ValueCodec[V] {
	return s.data.ValueCodec()
}

// Extension returns an extension by name, or nil if there is none.
func (s *Store[K, V]) Extension(name string) Extension[K, V] {
	if ext, ok := s.exts.Get(name); ok {
		return ext
	}
	return nil
}

// Instantiate creates the instance of the store for a transaction.
func (s *Store[K, V]) Instantiate(tx kv.Tx) *Instance[K, V] {
	return &Instance[K, V]{
		store: s,
		tx:    tx,
		data:  s.data.Instantiate(tx),
	}
}

// Instance is a Store in a transaction.
type Instance[K, V any] struct {
	store *Store[K, V]
	tx    kv.Tx
	data  *typed.Instance[K, V]
	exts  *ordmap.Map[string, ExtensionInstance[K, V]]
}

// extensions returns the instances of the extensions, which are created when they are first needed.
func (s *Instance[K, V]) extensions() *ordmap.Map[string, ExtensionInstance[K, V]] {
	if s.exts == nil {
		s.exts = ordmap.New[string, ExtensionInstance[K, V]]()
		for name, ext := range s.store.exts.Iter() {
			s.exts.Add(name, ext.Instantiate(s.tx))
		}
	}
	return s.exts
}

// Extension returns the instance of an extension by name, or nil if there is none.
func (s *Instance[K, V]) Extension(name string) ExtensionInstance[K, V] {
	if ext, ok := s.extensions().Get(name); ok {
		return ext
	}
	return nil
}

// Writable reports whether the transaction of the instance accepts writes.
func (s *Instance[K, V]) Writable() bool {
	return s.tx.Writable()
}

// Get returns the value of the key, or kv.ErrNotFound if it does not exist.
func (s *Instance[K, V]) Get(key K) (*V, error) {
	return s.data.Get(key)
}

// Has reports whether the key exists.
func (s *Instance[K, V]) Has(key K) (bool, error) {
	return s.data.Has(key)
}

// Iterator iterates over the records whose keys are in the range, in ascending order.
func (s *Instance[K, V]) Iterator(r kvutil.Range[K]) (*typed.Iterator[K, V], error) {
	return s.data.Iterator(r)
}

// ReverseIterator iterates over the records whose keys are in the range, in descending order.
func (s *Instance[K, V]) ReverseIterator(r kvutil.Range[K]) (*typed.Iterator[K, V], error) {
	return s.data.ReverseIterator(r)
}

// Set sets the value of the key.
func (s *Instance[K, V]) Set(key K, value *V) error {
	return s.SetWithOptions(key, value)
}

// SetWithOptions is a variant of Set that passes options to the extensions.
// An option that is made by For only goes to the extension that it names.
func (s *Instance[K, V]) SetWithOptions(key K, value *V, opts ...any) error {
	if !s.tx.Writable() {
		return kv.ErrReadOnly
	}
	if value == nil {
		return typed.ErrNilValue
	}

	if s.store.exts.Len() > 0 {
		rawKey, err := s.data.EncodeKey(key)
		if err != nil {
			return err
		}

		old, err := s.data.Get(key)
		if err != nil && !errors.Is(err, kv.ErrNotFound) {
			return fmt.Errorf("failed to get record: %w", err)
		}

		for name, ext := range s.extensions().Iter() {
			err := ext.OnSet(key, rawKey, old, value, filterOptions(name, opts)...)
			if err != nil {
				return fmt.Errorf("extension %q failed on set: %w", name, err)
			}
		}
	}

	return s.data.Set(key, value)
}

func filterOptions(name string, opts []any) []any {
	var extOpts []any
	for _, opt := range opts {
		if eo, ok := opt.(Option); ok {
			if eo.extName == name {
				extOpts = append(extOpts, eo.value)
			}
			continue
		}
		extOpts = append(extOpts, opt)
	}
	return extOpts
}

// SetWithTTL sets the value of the key and makes the key expire after the given duration.
// It returns ErrTTLWithExtensions if the store has extensions or delete hooks,
// and kv.ErrUnsupported if the engine cannot write keys that expire.
func (s *Instance[K, V]) SetWithTTL(key K, value *V, ttl time.Duration) error {
	if s.store.exts.Len() > 0 || s.store.deleteHooks.Len() > 0 {
		return ErrTTLWithExtensions
	}
	return s.data.SetWithTTL(key, value, ttl)
}

// Delete deletes the key. Deleting a key that does not exist does nothing.
func (s *Instance[K, V]) Delete(key K) error {
	if !s.tx.Writable() {
		return kv.ErrReadOnly
	}
	if s.store.exts.Len() == 0 && s.store.deleteHooks.Len() == 0 {
		return s.data.Delete(key)
	}

	rawKey, err := s.data.EncodeKey(key)
	if err != nil {
		return err
	}

	if s.store.exts.Len() > 0 {
		old, err := s.data.Get(key)
		if errors.Is(err, kv.ErrNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to get record: %w", err)
		}

		for name, ext := range s.extensions().Iter() {
			err := ext.OnDelete(key, rawKey, old)
			if err != nil {
				return fmt.Errorf("extension %q failed on delete: %w", name, err)
			}
		}
	} else {
		ok, err := s.data.Has(key)
		if err != nil {
			return fmt.Errorf("failed to get record: %w", err)
		}
		if !ok {
			return nil
		}
	}

	for name, hook := range s.store.deleteHooks.Iter() {
		err := hook(s.tx, key, rawKey)
		if err != nil {
			return fmt.Errorf("delete hook %q failed: %w", name, err)
		}
	}

	return s.data.Delete(key)
}
