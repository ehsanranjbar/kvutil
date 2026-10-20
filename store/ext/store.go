// Package ext provides a store that extensions can be attached to, which are told of every
// record that is set and deleted. Indexes and stored aggregations are built as extensions.
package ext

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/codec/lex"
	"github.com/ehsanranjbar/kvutil/internal/ordmap"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/migrate"
	"github.com/ehsanranjbar/kvutil/store/typed"
)

// ErrTTLWithExtensions is returned when a record that expires is set in a store that has
// extensions. A record that expires would not be reported to them as deleted.
var ErrTTLWithExtensions = errors.New("ext: a store with extensions cannot have records that expire")

// extKey is what the prefix of the data of the extensions is made of, after the prefix of the
// store. The data of an extension is under that and the name of the extension.
const extKey = 'x'

// Store is a wrapper around a serialized store with an ordered list of extensions
// that are called on set and delete.
//
// It keeps its keys under the prefix that it is given, like the serialized store at the same
// prefix does: the records and what the store records of itself, and the data of the extensions
// that keep any under the prefix followed by 'x'.
type Store[K, V any] struct {
	data        *typed.Store[K, V]
	exts        *ordmap.Map[string, Extension[K, V]]
	deleteHooks *ordmap.Map[string, DeleteHook[K]]
}

// New defines a new Store under a prefix, with the codecs that its types have by default.
// It panics if the prefix is empty, and if the type of the keys has no key codec, in which
// case the store must be defined with NewWithCodecs.
func New[K, V any, P ~string | ~[]byte](prefix P) *Store[K, V] {
	return NewWithCodecs[K, V](prefix, nil, nil)
}

// NewWithCodecs defines a new Store under a prefix with the given codecs.
// A codec that is nil is the one that New would use.
func NewWithCodecs[K, V any, P ~string | ~[]byte](
	prefix P,
	keyCodec codec.KeyCodec[K],
	valueCodec codec.ValueCodec[V],
) *Store[K, V] {
	s := &Store[K, V]{
		data:        typed.NewWithCodecs(prefix, keyCodec, valueCodec),
		exts:        ordmap.New[string, Extension[K, V]](),
		deleteHooks: ordmap.New[string, DeleteHook[K]](),
	}
	s.data.Unit().SetKind("ext")
	return s
}

// WithExtension adds an extension to the store. Extensions are called in the order that they
// are added. An extension that is a Keeper is given a prefix of its own.
func (s *Store[K, V]) WithExtension(name string, ext Extension[K, V]) *Store[K, V] {
	s.AddExtension(name, ext)
	return s
}

// WithVersion sets the version of the records of the store, which is 1 by default; see
// typed.Store.WithVersion.
func (s *Store[K, V]) WithVersion(v uint64) *Store[K, V] {
	s.data.Unit().SetVersion(v)
	return s
}

// MigrateFrom adds the migration of the records from a version to the next one: the function
// is given every record in the type it had and returns it as it is to be; see migrate.Records
// and typed.Store.MigrateFrom. The new type is the type of the store in the last migration, and
// the type of the version after it in the ones before.
func (s *Store[K, V]) MigrateFrom[Old, New any](
	from uint64,
	convert func(key K, old *Old) (*New, error),
	opts ...migrate.Option,
) *Store[K, V] {
	migrate.Records(s, from, convert, opts...)
	return s
}

// RetireExtension says that the data of an extension that is not added to the store any more
// is to be deleted from the database; see migrate.Run. The name is the one that the
// extension was added with. It is for an extension that keeps data that cannot be built again:
// an index or an aggregation that is not defined any more is deleted without it.
func (s *Store[K, V]) RetireExtension(name string) {
	s.data.Unit().Retire(name)
}

// AddExtension implements the Extensible interface. It is WithExtension without a result.
// It panics if the name is empty or taken. An extension that is a Keeper is given the prefix
// of its data, and what it records of itself is added to the unit of the store.
func (s *Store[K, V]) AddExtension(name string, ext Extension[K, V]) {
	if name == "" {
		panic(fmt.Sprintf("store %q: the name of an extension must not be empty", migrate.Label(s.Prefix())))
	}
	if _, ok := s.exts.Get(name); ok {
		panic(fmt.Sprintf("store %q: the extension %q is added more than once", migrate.Label(s.Prefix()), name))
	}

	if k, ok := ext.(Keeper); ok {
		prefix := s.extensionPrefix(name)
		part := k.Keep(prefix)
		part.Name = name
		part.Prefix = prefix
		s.data.Unit().AddPart(part)
	}
	s.exts.Add(name, ext)
}

// extensionPrefix returns the prefix of the data of an extension.
func (s *Store[K, V]) extensionPrefix(name string) []byte {
	prefix := append(bytes.Clone(s.Prefix()), extKey)
	return lex.AppendTerminated(prefix, []byte(name))
}

// Extensions returns the extensions of the store, in the order that they were added.
func (s *Store[K, V]) Extensions() []any {
	exts := make([]any, 0, s.exts.Len())
	for _, ext := range s.exts.Iter() {
		exts = append(exts, ext)
	}
	return exts
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
// It panics if the name is taken.
func (s *Store[K, V]) AddDeleteHook(name string, hook DeleteHook[K]) {
	if err := s.deleteHooks.Add(name, hook); err != nil {
		panic(fmt.Sprintf("store %q: the delete hook %q is added more than once", migrate.Label(s.Prefix()), name))
	}
}

// Prefix returns the prefix of the store. The slice must not be modified.
func (s *Store[K, V]) Prefix() []byte {
	return s.data.Prefix()
}

// DataPrefix returns the prefix that the records are kept under. The slice must not be modified.
func (s *Store[K, V]) DataPrefix() []byte {
	return s.data.DataPrefix()
}

// Unit returns what the store records of itself, which is migrated as a whole: the unit of the
// serialized store, with what the extensions that keep data record of themselves as its parts.
func (s *Store[K, V]) Unit() *migrate.Unit {
	return s.data.Unit()
}

// Migrate brings the database to what the store defines; see migrate.Run.
func (s *Store[K, V]) Migrate(ctx context.Context, db kv.DB) (*migrate.Report, error) {
	return migrate.Run(ctx, db, s)
}

// Check compares the database with what the store defines, without changing it; see migrate.Check.
func (s *Store[K, V]) Check(db kv.DB) error {
	return migrate.Check(db, s)
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

// All iterates over every record, in ascending order of the keys; it is Iterator of every key.
// The iterator must be closed.
func (s *Instance[K, V]) All() (*typed.Iterator[K, V], error) {
	return s.data.All()
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
