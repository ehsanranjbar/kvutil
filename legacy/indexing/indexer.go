package indexing

import (
	"github.com/ehsanranjbar/kvutil/legacy"
)

// Indexer is an indexer.
type Indexer[T any] interface {
	Index(v *T, set bool) ([]legacy.RawKVPair, error)
	Lookup(args ...any) (legacy.Iterator[[]byte, Chunk], error)
}

// IndexDescriptor is an index describer.
type IndexDescriptor interface {
	SupportedQueries() []string
	SupportedValues() []string
}
