package rec

import (
	"encoding"
	"fmt"
	"sync"

	"github.com/araddon/qlbridge/expr"
	qlvm "github.com/araddon/qlbridge/vm"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/indexing"
	"github.com/ehsanranjbar/kvutil/internal/constraints"
	"github.com/ehsanranjbar/kvutil/internal/qlutil"
	"github.com/ehsanranjbar/kvutil/iters"
	"github.com/ehsanranjbar/kvutil/schema"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

// Record is a model for something that is serializable and has an unique id assigned to it.
// One thing to consider here is that the id is serialized as key in the store so there's no need to serialize it
// as part of the value.
type Record[I comparable, T any] interface {
	*T
	GetId() I
	SetId(I)
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

// Store is a generic store for entities.
type Store[
	I comparable,
	T any,
	PT Record[I, T],
] struct {
	base          *extstore.Store[I, T, PT]
	idFunc        func(*T) (I, error)
	idCodec       codec.Codec[I]
	indexers      map[string]*indexing.Extension[I, T]
	extractor     schema.PathExtractor[*T]
	flatExtractor schema.PathExtractor[[]byte]
	initialized   bool
	init          sync.Once
}

// New creates a new Store.
func New[
	I comparable,
	T any,
	PT Record[I, T],
](
	base kvutil.Instantiator[kvutil.BadgerStore],
) *Store[I, T, PT] {
	// A missing id codec is reported on first instantiation, unless WithIdCodec sets one.
	idCodec, _ := codec.CodecFor[I]()

	return &Store[I, T, PT]{
		base:      extstore.New[I, T, PT](base),
		idCodec:   idCodec,
		indexers:  map[string]*indexing.Extension[I, T]{},
		extractor: schema.NewReflectPathExtractor[*T](true),
	}
}

// WithIdFunc is an option to set the id function.
func (s *Store[I, T, PT]) WithIdFunc(f func(*T) (I, error)) *Store[I, T, PT] {
	if s.initialized {
		panic("store already initialized")
	}

	s.idFunc = f
	return s
}

// WithSequenceAsIdFunc is an option to set the id function to use a sequence.
func WithSequenceAsIdFunc[
	I constraints.Integer,
	T any,
](
	seq *badger.Sequence,
) func(_ *T) (I, error) {
	return func(_ *T) (I, error) {
		id, err := seq.Next()
		if err != nil {
			return 0, fmt.Errorf("failed to generate id: %w", err)
		}
		// badger sequences start from 0, so we increment it by 1.
		return I(id + 1), nil
	}
}

// WithIdCodec is an option to set the id codec.
func (s *Store[I, T, PT]) WithIdCodec(c codec.Codec[I]) *Store[I, T, PT] {
	if s.initialized {
		panic("store already initialized")
	}

	s.idCodec = c
	return s
}

// WithExtractor is an option to set the extractor.
func (s *Store[I, T, PT]) WithExtractor(e schema.PathExtractor[*T]) *Store[I, T, PT] {
	if s.initialized {
		panic("store already initialized")
	}

	s.extractor = e
	return s
}

// WithFlatExtractor is an option to set the flat extractor.
func (s *Store[I, T, PT]) WithFlatExtractor(e schema.PathExtractor[[]byte]) *Store[I, T, PT] {
	if s.initialized {
		panic("store already initialized")
	}

	s.flatExtractor = e
	return s
}

// WithIndexer adds an indexer to the store.
func (s *Store[I, T, PT]) WithIndexer(name string, idx indexing.Indexer[T]) *Store[I, T, PT] {
	if s.initialized {
		panic("store already initialized")
	}

	if _, ok := s.indexers[name]; ok {
		panic("indexer already exists")
	}

	ext := indexing.NewExtension[I](idx).(*indexing.Extension[I, T])
	s.base.WithExtension(name, ext)
	s.indexers[name] = ext
	return s
}

// WithExtension adds an extension to the store.
func (s *Store[I, T, PT]) WithExtension(name string, ext extstore.Extension[I, T]) *Store[I, T, PT] {
	if s.initialized {
		panic("store already initialized")
	}

	s.base.WithExtension(name, ext)
	return s
}

// Prefix returns the prefix of the store.
func (s *Store[I, T, PT]) Prefix() []byte {
	return s.base.Prefix()
}

// Instantiate implements the kvutil.Instantiator interface.
func (s *Store[I, T, PT]) Instantiate(txn *badger.Txn) *Instance[I, T, PT] {
	// Locking any changes to the store's configuration on first instantiation.
	s.init.Do(func() {
		if s.idCodec == nil {
			panic("no codec for id")
		}

		if s.extractor == nil {
			panic("no extractor")
		}

		s.initialized = true
	})

	return &Instance[I, T, PT]{
		base:      s.base.Instantiate(txn),
		idFunc:    s.idFunc,
		idCodec:   s.idCodec,
		extractor: s.extractor,
	}
}

// IdCodec returns the id codec.
func (s *Store[I, T, PT]) IdCodec() codec.Codec[I] {
	return s.idCodec
}

// Indexer returns the indexer with given name.
func (s *Store[I, T, PT]) Indexer(name string) *indexing.Extension[I, T] {
	idx, ok := s.indexers[name]
	if !ok {
		return nil
	}

	return idx
}

// Instance is an instance of the Store.
type Instance[
	I comparable,
	T any,
	PT Record[I, T],
] struct {
	base      *extstore.Instance[I, T, PT]
	idFunc    func(*T) (I, error)
	idCodec   codec.Codec[I]
	extractor schema.PathExtractor[*T]
}

// Delete implements the kvutil.StoreInstance interface.
func (s *Instance[I, T, PT]) Delete(id I) error {
	return s.base.Delete(id)
}

// Get implements the kvutil.StoreInstance interface.
func (s *Instance[I, T, PT]) Get(id I) (*T, error) {
	r, err := s.base.Get(id)
	if err != nil {
		return nil, err
	}
	PT(r).SetId(id)
	return r, nil
}

// NewIterator implements the kvutil.StoreInstance interface.
func (s *Instance[I, T, PT]) NewIterator(opts badger.IteratorOptions) *Iterator[I, T] {
	return newIterator(s.base.NewIterator(opts), s.idCodec)
}

// Set implements the kvutil.StoreInstance interface.
func (s *Instance[I, T, PT]) Set(v *T, opts ...any) error {
	var zero I
	if PT(v).GetId() == zero {
		if s.idFunc == nil {
			return fmt.Errorf("zero id with no id func")
		}

		id, err := s.idFunc(v)
		if err != nil {
			return fmt.Errorf("failed to get id: %w", err)
		}

		PT(v).SetId(id)
	}

	return s.base.SetWithOptions(PT(v).GetId(), v, opts...)
}

// Query returns the query for the store.
func (s *Instance[I, T, PT]) Query(q string) (kvutil.Iterator[I, *T], error) {
	qe, err := expr.ParseExpression(q)
	if err != nil {
		return nil, err
	}

	iter := iters.Filter(
		s.NewIterator(badger.DefaultIteratorOptions),
		func(r *T, item *badger.Item) bool {
			ctx := qlutil.NewContextWrapper(PT(r).GetId(), r, s.extractor, nil)
			t, _ := qlvm.MatchesExpr(ctx, qe)
			return t
		})
	return iter, nil
}
