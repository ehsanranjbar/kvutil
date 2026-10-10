package indexing

import (
	"github.com/ehsanranjbar/kvutil"
)

// Indexer is an indexer.
type Indexer[T any] interface {
	Index(v *T, set bool) ([]kvutil.RawKVPair, error)
	Lookup(args ...any) (kvutil.Iterator[[]byte, Chunk], error)
}

// IndexDescriptor is an index describer.
type IndexDescriptor interface {
	SupportedQueries() []string
	SupportedValues() []string
}
