package ext

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/internal/ordmap"
	pstore "github.com/ehsanranjbar/kvutil/store/prefix"
	sstore "github.com/ehsanranjbar/kvutil/store/serialized"
)

var (
	dataStorePrefix = []byte{'d'}
	extStorePrefix  = []byte{'x'}
)

// Store is a wrapper around a serialized store with an ordered list of extensions
// that can modify data before it is stored or do arbitrary operations on set and delete.
type Store[K, V any, PV sstore.BSP[V]] struct {
	dataStore   *sstore.Store[K, V, PV]
	extStore    *pstore.Store
	exts        *ordmap.Map[string, Extension[K, V]]
	prefix      []byte
	initialized bool
	init        sync.Once
}

// New creates a new Store.
func New[
	K, V any,
	PV sstore.BSP[V],
](base kvutil.Instantiator[kvutil.BadgerStore]) *Store[K, V, PV] {
	var prefix []byte
	if pfx, ok := base.(prefixed); ok {
		prefix = pfx.Prefix()
	}

	store := &Store[K, V, PV]{
		dataStore: sstore.New[K, V, PV](pstore.New(base, dataStorePrefix)),
		extStore:  pstore.New(base, extStorePrefix),
		exts:      ordmap.New[string, Extension[K, V]](),
		prefix:    prefix,
	}

	return store
}

type prefixed interface {
	Prefix() []byte
}

// WithExtension adds an extension to the store.
func (s *Store[K, V, PV]) WithExtension(name string, ext Extension[K, V]) *Store[K, V, PV] {
	if s.initialized {
		panic("store is already initialized")
	}

	if sr, ok := ext.(StoreRegistry); ok {
		sr.RegisterStore(pstore.New(s.extStore, []byte(name)))
	}

	err := s.exts.Add(name, ext)
	if err != nil {
		panic("extension with the same name already exists")
	}

	return s
}

// Instantiate creates a new Instance.
func (s *Store[K, V, PV]) Instantiate(txn *badger.Txn) *Instance[K, V, PV] {
	// Locking any changes to the store's configuration on first instantiation.
	s.init.Do(func() {
		s.initialized = true
	})

	return &Instance[K, V, PV]{
		dataStore: s.dataStore.Instantiate(txn),
		keyCodec:  s.dataStore.KeyCodec(),
		exts:      s.instantiateExts(txn),
		prefix:    s.prefix,
	}
}

func (s *Store[K, V, PV]) instantiateExts(txn *badger.Txn) *ordmap.Map[string, ExtensionInstance[K, V]] {
	exts := ordmap.New[string, ExtensionInstance[K, V]]()
	for name, ext := range s.exts.Iter() {
		exts.Add(name, ext.Instantiate(txn))
	}

	return exts
}

// GetExtension returns an extension by name.
func (s *Store[K, V, PV]) GetExtension(name string) Extension[K, V] {
	if ext, ok := s.exts.Get(name); ok {
		return ext
	}

	return nil
}

func (s *Store[K, V, PV]) Prefix() []byte {
	return s.prefix
}

// Instance is an instance of Store.
type Instance[K, V any, PV sstore.BSP[V]] struct {
	dataStore kvutil.StoreInstance[K, *V, *V, kvutil.Iterator[K, *V]]
	keyCodec  codec.Codec[K]
	exts      *ordmap.Map[string, ExtensionInstance[K, V]]
	prefix    []byte
}

// Prefix returns the prefix of the store.
func (s *Instance[K, V, PV]) Prefix() []byte {
	return s.prefix
}

// Delete implements the kvutil.StoreInstance interface.
func (s *Instance[K, V, PV]) Delete(key K) error {
	err := s.onDelete(key)
	if err != nil {
		return err
	}

	err = s.dataStore.Delete(key)
	if err != nil {
		return fmt.Errorf("failed to delete record: %w", err)
	}

	return nil
}

func (s *Instance[K, V, PV]) onDelete(key K) error {
	if s.exts.Len() == 0 {
		return nil
	}

	data, err := s.dataStore.Get(key)
	if err != nil {
		return err
	}

	ctx := s.newContext(key)
	for _, name := range s.exts.Iter() {
		err := name.OnDelete(ctx, key, data)
		if err != nil {
			return fmt.Errorf("failure in running extension %s OnDelete: %w", name, err)
		}
	}

	return nil
}

func (s *Instance[K, V, PV]) newContext(key K) context.Context {
	ctx := context.Background()

	ctx = context.WithValue(ctx, keyCodecContextKey, s.keyCodec)

	kbz, _ := s.keyCodec.Encode(key)
	ctx = context.WithValue(ctx, keyBytesContextKey, kbz)

	return ctx
}

// Get implements the kvutil.StoreInstance interface.
func (s *Instance[K, V, PV]) Get(key K) (*V, error) {
	return s.dataStore.Get(key)
}

// NewIterator implements the kvutil.StoreInstance interface.
func (s *Instance[K, V, PV]) NewIterator(opts badger.IteratorOptions) kvutil.Iterator[K, *V] {
	return s.dataStore.NewIterator(opts)
}

// Set implements the kvutil.StoreInstance interface.
func (s *Instance[K, V, PV]) Set(key K, v *V) error {
	return s.SetWithOptions(key, v)
}

// SetWithOptions is a variant of Set that allows passing options to extensions.
func (s *Instance[K, V, PV]) SetWithOptions(key K, v *V, opts ...any) error {
	err := s.onSet(key, v, opts...)
	if err != nil {
		return err
	}

	err = s.dataStore.Set(key, v)
	if err != nil {
		return fmt.Errorf("failed to set record: %w", err)
	}

	return nil
}

func (s *Instance[K, V, PV]) onSet(key K, new *V, opts ...any) error {
	if s.exts.Len() == 0 {
		return nil
	}

	old, err := s.dataStore.Get(key)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to get record: %w", err)
	}

	ctx := s.newContext(key)
	for name, ext := range s.exts.Iter() {
		extOpts := filterOptions(name, opts)
		err := ext.OnSet(ctx, key, old, new, extOpts...)
		if err != nil {
			return fmt.Errorf("failure in running extension %s OnSet: %w", name, err)
		}
	}

	return nil
}

func filterOptions(name string, opts []any) []any {
	var extOpts []any
	for _, opt := range opts {
		if so, ok := opt.(ExtOption); ok {
			if so.extName == name {
				extOpts = append(extOpts, so.value)
			} else {
				continue
			}
		}
	}

	return extOpts
}

// GetExtension returns an extension's instance by name.
func (s *Instance[K, V, PV]) GetExtension(name string) ExtensionInstance[K, V] {
	if ext, ok := s.exts.Get(name); ok {
		return ext
	}

	return nil
}
