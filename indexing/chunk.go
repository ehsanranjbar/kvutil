package indexing

import (
	"bytes"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/codec/lex"
	"github.com/ehsanranjbar/kvutil/expr"
	"github.com/ehsanranjbar/kvutil/iters"
	refstore "github.com/ehsanranjbar/kvutil/store/ref"
)

// Chunk represents a range of keys from low to high with optional exclusivity on both ends.
type Chunk = expr.Range[[]byte]

// NewChunk creates a new chunk with the given low and high keys and exclusivity.
func NewChunk(low, high *expr.Bound[[]byte]) Chunk {
	return expr.NewRange(low, high)
}

// LookupChunks returns an iterator that iterates over the keys in the given chunk iterator.
func LookupChunks(
	store *refstore.Instance,
	parts kvutil.Iterator[[]byte, Chunk],
	opts badger.IteratorOptions,
) kvutil.Iterator[[]byte, []byte] {
	return iters.Flatten(
		iters.Map(parts, func(c Chunk, _ *badger.Item) (kvutil.Iterator[[]byte, []byte], error) {
			// Omitting the prefix option.
			iter := store.NewIterator(badger.IteratorOptions{
				PrefetchSize:   opts.PrefetchSize,
				PrefetchValues: opts.PrefetchValues,
				Reverse:        opts.Reverse,
				AllVersions:    opts.AllVersions,
				InternalAccess: opts.InternalAccess,
				SinceTs:        opts.SinceTs,
			})

			if opts.Reverse {
				// Special case for reverse iteration with non-empty high bound that happens with reference stores
				// Because we want to seek and sever base on prefixes instead of the actual keys.
				var s []byte = nil
				if !c.High().IsEmpty() {
					s = c.High().Value()

					if !c.High().Exclusive() {
						s = lex.Increment(bytes.Clone(c.High().Value()))
					}
				}
				iter = iters.RewindSeek(iter, s)

				if !c.High().IsEmpty() {
					iter = iters.Skip(iter, func(_ struct{}, key []byte, _ []byte, _ *badger.Item) (struct{}, bool) {
						if c.High().Exclusive() {
							return struct{}{}, bytes.Compare(key, c.High().Value()) >= 0
						} else {
							return struct{}{}, bytes.Compare(key, c.High().Value()) > 0
						}
					})
				}
			} else {
				var e []byte = nil
				if !c.Low().IsEmpty() {
					e = c.Low().Value()
				}
				iter = iters.RewindSeek(iter, e)

				if !c.Low().IsEmpty() && c.Low().Exclusive() {
					iter = iters.Skip(iter, func(_ struct{}, key []byte, _ []byte, _ *badger.Item) (struct{}, bool) {
						return struct{}{}, bytes.Equal(key, c.Low().Value())
					})
				}
			}

			return iters.Sever(iter, func(key []byte, _ []byte, _ *badger.Item) bool {
				if opts.Reverse {
					if c.Low().IsEmpty() {
						return false
					}

					if c.Low().Exclusive() {
						return bytes.Compare(key, c.Low().Value()) <= 0
					} else {
						return bytes.Compare(key, c.Low().Value()) < 0
					}
				} else {
					if c.High().IsEmpty() {
						return false
					}

					if c.High().Exclusive() {
						return bytes.Compare(key, c.High().Value()) >= 0
					} else {
						return bytes.Compare(key, c.High().Value()) > 0
					}
				}
			}), nil
		}),
	)
}
