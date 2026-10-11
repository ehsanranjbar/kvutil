package indexing

import (
	"encoding/binary"

	roaring "github.com/RoaringBitmap/roaring/v2"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/ehsanranjbar/kvutil/codec/be"
	"github.com/ehsanranjbar/kvutil/legacy"
)

// PopulateRoaring32 populates a roaring bitmap with 32-bit integers from an iterator of byte slices.
func PopulateRoaring32(bm *roaring.Bitmap, iter legacy.Iterator[[]byte, []byte]) error {
	for iter.Rewind(); iter.Valid(); iter.Next() {
		v, err := iter.Value()
		if err != nil {
			return err
		}
		i := binary.BigEndian.Uint32(be.PadOrTruncLeft(v, 32))
		bm.Add(i)
	}
	return nil
}

// PopulateRoaring64 populates a roaring bitmap with 64-bit integers from an iterator of byte slices.
func PopulateRoaring64(bm *roaring64.Bitmap, iter legacy.Iterator[[]byte, []byte]) error {
	for iter.Rewind(); iter.Valid(); iter.Next() {
		v, err := iter.Value()
		if err != nil {
			return err
		}
		i := binary.BigEndian.Uint64(be.PadOrTruncLeft(v, 64))
		bm.Add(i)
	}
	return nil
}
