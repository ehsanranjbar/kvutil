package serialized_test

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/legacy/iters"
	pstore "github.com/ehsanranjbar/kvutil/legacy/store/prefix"
	"github.com/ehsanranjbar/kvutil/legacy/store/serialized"
	"github.com/ehsanranjbar/kvutil/legacy/testutil"
	"github.com/stretchr/testify/require"
)

func TestIterator(t *testing.T) {
	store := serialized.New[[]byte, TestStruct](nil)

	txn := testutil.PrepareTxn(t, true)
	ins := store.Instantiate(txn)

	var (
		keys   = [][]byte{[]byte("foo1"), []byte("foo2")}
		values = []*TestStruct{{A: 1, B: "bar1"}, {A: 2, B: "bar2"}}
	)
	for i, key := range keys {
		err := ins.Set(key, values[i])
		require.NoError(t, err)
	}

	t.Run("Iterate", func(t *testing.T) {
		iter := ins.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		actualKeys, err := iters.CollectKeys(iter)
		require.NoError(t, err)
		require.Equal(t, 2, len(actualKeys))
		require.Equal(t, keys, actualKeys)
		actualValues, err := iters.Collect(iter)
		require.NoError(t, err)
		require.Equal(t, 2, len(actualValues))
		require.Equal(t, values, actualValues)
	})

	t.Run("IteratePrefix", func(t *testing.T) {
		store := serialized.New[[]byte, TestStruct](pstore.New(nil, []byte("foo")))

		ins := store.Instantiate(txn)
		iter := ins.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		actualKeys, err := iters.CollectKeys(iter)
		require.NoError(t, err)
		require.Equal(t, [][]byte{[]byte("1"), []byte("2")}, actualKeys)
		require.Equal(t, 2, len(actualKeys))
		actualValues, err := iters.Collect(iter)
		require.NoError(t, err)
		require.Equal(t, 2, len(actualValues))
		require.Equal(t, values, actualValues)

	})
}
