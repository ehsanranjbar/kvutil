package index_test

import (
	"testing"

	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/stretchr/testify/require"
)

// TestIteratorBitmap checks that the method gives what the function gives.
func TestIteratorBitmap(t *testing.T) {
	db := memkv.New()
	store := extstore.New[int64, pet]("pets")
	byAge := index.On(store, "by_age", ageField)

	require.NoError(t, db.Update(func(tx kv.Tx) error {
		for id := int64(1); id <= 10; id++ {
			if err := store.Instantiate(tx).Set(id, &pet{Name: "p", Age: int(id % 3)}); err != nil {
				return err
			}
		}
		return nil
	}))

	require.NoError(t, db.View(func(tx kv.Tx) error {
		ins := byAge.Instantiate(tx)

		it, err := ins.Match(1)
		require.NoError(t, err)
		bm, exact, err := it.Bitmap()
		require.NoError(t, err)
		require.True(t, exact)
		// The keys are numbers with the sign bit flipped, as the codec of int64 writes them.
		const sign = 1 << 63
		require.Equal(t, []uint64{sign | 1, sign | 4, sign | 7, sign | 10}, bm.ToArray())

		it, err = ins.Match(1)
		require.NoError(t, err)
		want, wantExact, err := index.Bitmap(it)
		require.NoError(t, err)
		require.Equal(t, want.ToArray(), bm.ToArray())
		require.Equal(t, wantExact, exact)

		it, err = ins.Match(7)
		require.NoError(t, err)
		bm, _, err = it.Bitmap()
		require.NoError(t, err)
		require.True(t, bm.IsEmpty())
		return nil
	}))
}
