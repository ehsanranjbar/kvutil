package record_test

import (
	"testing"

	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/ehsanranjbar/kvutil/store/record"
	"github.com/stretchr/testify/require"
)

func TestInstanceAll(t *testing.T) {
	db := memkv.New()
	store := record.New[int64, pet]("pets")
	require.NoError(t, db.Update(func(tx kv.Tx) error {
		for _, id := range []int64{3, 1, 2} {
			if err := store.Instantiate(tx).Set(&pet{id: id}); err != nil {
				return err
			}
		}
		return nil
	}))

	require.NoError(t, db.View(func(tx kv.Tx) error {
		it, err := store.Instantiate(tx).All()
		require.NoError(t, err)
		got, err := iterutil.Of(it).Keys()
		require.NoError(t, err)
		require.Equal(t, []int64{1, 2, 3}, got)
		return nil
	}))
}
