package indexing

import (
	"fmt"

	"github.com/ehsanranjbar/kvutil/legacy"
	"github.com/ehsanranjbar/kvutil/legacy/schema"
)

// Indexer is a wrapper around an Indexer that injects custom values to the indexes.
type ValueInjector[T any] struct {
	indexer   Indexer[T]
	describer IndexDescriptor
	retriever ValueRetriever[T]
}

// NewValueInjector creates a new value injector for the given indexer and value retriever
func NewValueInjector[T any](
	indexer Indexer[T],
	retriever ValueRetriever[T],
) *ValueInjector[T] {
	if indexer == nil {
		panic("indexer is required")
	}
	desc, _ := indexer.(IndexDescriptor)
	if retriever == nil {
		panic("retriever is required")
	}

	return &ValueInjector[T]{
		indexer:   indexer,
		describer: desc,
		retriever: retriever,
	}
}

// Index implements the Indexer interface.
func (i *ValueInjector[T]) Index(v *T, set bool) ([]legacy.RawKVPair, error) {
	if v == nil {
		return nil, nil
	}

	idxs, err := i.indexer.Index(v, set)
	if err != nil {
		return nil, err
	}

	if set {
		value, err := i.retriever.RetrieveValue(v)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve value: %v", err)
		}

		for i := range idxs {
			idxs[i].Value = value
		}
	}

	return idxs, nil
}

// Lookup implements the Indexer interface.
func (i *ValueInjector[T]) Lookup(args ...any) (legacy.Iterator[[]byte, Chunk], error) {
	return i.indexer.Lookup(args...)
}

// SupportedQueries implements the IndexDescriber interface.
func (i *ValueInjector[T]) SupportedQueries() []string {
	if i.describer != nil {
		return i.describer.SupportedQueries()
	}
	return nil
}

// SupportedValues implements the IndexDescriber interface.
func (i *ValueInjector[T]) SupportedValues() []string {
	return i.retriever.Paths()
}

// ValueRetriever is an interface that retrieves index custom values for index only scans from the indexed type.
type ValueRetriever[T any] interface {
	RetrieveValue(v *T) ([]byte, error)
	Paths() []string
}

// MapValueRetriever is a value retriever that retrieves the given field paths of struct and encodes them to bytes.
type MapValueRetriever[T any] struct {
	extractor  schema.PathExtractor[T]
	encodeFunc func(any) ([]byte, error)
	paths      []string
}

// NewMapValueRetriever creates a new map value retriever for the given struct type and field paths.
func NewMapValueRetriever[T any](
	extractor schema.PathExtractor[T],
	encodeFunc func(any) ([]byte, error),
	paths ...string,
) *MapValueRetriever[T] {
	if extractor == nil {
		panic("extractor is required")
	}
	if encodeFunc == nil {
		panic("encoder is required")
	}

	return &MapValueRetriever[T]{
		extractor:  extractor,
		encodeFunc: encodeFunc,
		paths:      paths,
	}
}

// RetrieveValue implements the ValueRetriever interface.
func (r *MapValueRetriever[T]) RetrieveValue(v *T) ([]byte, error) {
	m := map[string]any{}
	if v != nil {
		for _, path := range r.paths {
			a, err := r.extractor.ExtractPath(*v, path)
			if err != nil {
				return nil, fmt.Errorf("failed to extract path %s: %v", path, err)
			}
			m[path] = a
		}
	}
	b, err := r.encodeFunc(m)
	if err != nil {
		return nil, fmt.Errorf("failed to encode value: %v", err)
	}
	return b, nil
}

// Paths implements the ValueRetriever interface.
func (r *MapValueRetriever[T]) Paths() []string {
	return r.paths
}
