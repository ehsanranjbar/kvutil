package index

import (
	"errors"
	"fmt"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

// ErrNotExact is returned when the primary keys of a store cannot be numbers in a bitmap.
var ErrNotExact = errors.New("index: the primary keys are not numbers of 64 bits at most")

// Bitmap reads the primary keys of a source into a bitmap, and closes the source.
// No record is read.
//
// The bitmap is exact if the keys all have the same size, of 8 bytes at most, which is so for
// the keys that are numbers: each key is in the bitmap as a number, and the bitmaps of several
// sources can be combined with And, Or and AndNot and turned back into records with FromBitmap.
// Otherwise the keys are in the bitmap as hashes of 64 bits, and two keys may have the same one.
// Such a bitmap can still be combined with And and Or, and used with InBitmap to find the
// records that may be in it, which must then be checked.
func Bitmap[K, V any](src Keyed[K, V]) (bm *roaring64.Bitmap, exact bool, err error) {
	sets, err := Combiner{lossy: true}.collect([]Keyed[K, V]{src})
	if err != nil {
		return nil, false, err
	}
	return sets[0].bm, sets[0].exact(), nil
}

// keySize returns the size of the encoded primary keys of a store, if they all have the same
// one and can be numbers in a bitmap.
func keySize[K, V any](store extstore.Extensible[K, V]) (int, error) {
	var zero K
	c := store.KeyCodec()
	whole, err := c.Encode(zero)
	if err != nil {
		return 0, err
	}
	// A key of a fixed size is the same whether it has to mark where it ends or not.
	marked, err := c.Append(nil, zero)
	if err != nil {
		return 0, err
	}
	if len(whole) > 8 || len(whole) != len(marked) {
		return 0, fmt.Errorf("%w: %s", ErrNotExact, c.Name())
	}
	return len(whole), nil
}

// FromBitmap returns the records of a store whose primary keys are in an exact bitmap, in the
// order of the keys. It fails with ErrNotExact if the keys of the store cannot be numbers.
// A key of the bitmap that has no record in the store is an error when its record is read.
func FromBitmap[K, V any](store extstore.Extensible[K, V], tx kv.Tx, bm *roaring64.Bitmap) (Keyed[K, V], error) {
	size, err := keySize(store)
	if err != nil {
		return nil, err
	}
	it := &bitmapIterator[K, V]{
		store: store,
		tx:    tx,
		bm:    bm,
		size:  size,
		it:    bm.Iterator(),
	}
	it.Next()
	return it, nil
}

type bitmapIterator[K, V any] struct {
	store  extstore.Extensible[K, V]
	tx     kv.Tx
	getter extstore.Getter[K, V]
	bm     *roaring64.Bitmap
	size   int
	it     roaring64.IntPeekable64
	raw    []byte
}

// Valid implements the iterutil.Iterator interface.
func (it *bitmapIterator[K, V]) Valid() bool {
	return it.raw != nil
}

// Next implements the iterutil.Iterator interface.
func (it *bitmapIterator[K, V]) Next() {
	if !it.it.HasNext() {
		it.raw = nil
		return
	}
	it.raw = keyOf(it.it.Next(), it.size)
}

// Key implements the iterutil.Iterator interface.
func (it *bitmapIterator[K, V]) Key() (K, error) {
	return it.store.KeyCodec().Decode(it.raw)
}

// Value implements the iterutil.Iterator interface.
func (it *bitmapIterator[K, V]) Value() (*V, error) {
	key, err := it.Key()
	if err != nil {
		return nil, err
	}
	if it.getter == nil {
		it.getter = it.store.Getter(it.tx)
	}
	return it.getter.Get(key)
}

// RawKey implements the Keyed interface.
func (it *bitmapIterator[K, V]) RawKey() ([]byte, error) {
	return it.raw, nil
}

// Contains implements the Keyed interface.
func (it *bitmapIterator[K, V]) Contains(rawKey []byte, _ *V) (bool, error) {
	return len(rawKey) == it.size && it.bm.Contains(keyValue(rawKey)), nil
}

// Close implements the iterutil.Iterator interface.
func (it *bitmapIterator[K, V]) Close() error {
	it.raw = nil
	return nil
}

// InBitmap returns the records of a source whose primary keys are in a bitmap that was made by
// Bitmap, in the order of the source. With a bitmap that is exact these are the records of the
// source that the bitmap has. With one that is not, they are the records that it may have:
// a record whose key has the hash of another key is returned too.
func InBitmap[K, V any](src Keyed[K, V], bm *roaring64.Bitmap, exact bool) Keyed[K, V] {
	return newCombined([]Keyed[K, V]{src}, []Keyed[K, V]{src},
		func(_ int, src Keyed[K, V]) (bool, error) {
			raw, err := src.RawKey()
			if err != nil {
				return false, err
			}
			return inBitmap(bm, exact, raw), nil
		},
		func(raw []byte, record *V) (bool, error) {
			if !inBitmap(bm, exact, raw) {
				return false, nil
			}
			return src.Contains(raw, record)
		})
}

func inBitmap(bm *roaring64.Bitmap, exact bool, raw []byte) bool {
	if exact {
		return len(raw) <= 8 && bm.Contains(keyValue(raw))
	}
	return bm.Contains(keyHash(raw))
}

var _ iterutil.Iterator[int, *int] = (*bitmapIterator[int, int])(nil)
