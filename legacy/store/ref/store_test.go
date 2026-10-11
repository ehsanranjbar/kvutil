package ref_test

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/legacy/iters"
	"github.com/ehsanranjbar/kvutil/legacy/store/ref"
	"github.com/ehsanranjbar/kvutil/legacy/testutil"
	"github.com/stretchr/testify/require"
)

func TestRefStore(t *testing.T) {
	store := ref.New(nil)

	txn := testutil.PrepareTxn(t, true)
	ins := store.Instantiate(txn)

	var (
		prefixes = [][]byte{[]byte("a"), []byte("b"), []byte("c")}
		keys     = [][]byte{{3}, {2}, {1}}
		value    = []byte("foo")
	)

	t.Run("Set", func(t *testing.T) {
		for i, key := range keys {
			err := ins.Set(key, ref.NewRefEntry(prefixes[i]).WithValue(value))
			require.NoError(t, err)
		}
	})

	t.Run("Get", func(t *testing.T) {
		for i, p := range prefixes {
			k, err := ins.Get(p)
			require.NoError(t, err)
			require.Equal(t, keys[i], k)
		}
	})

	t.Run("NotFound", func(t *testing.T) {
		v, err := ins.Get([]byte("d"))
		require.Error(t, err)
		require.Nil(t, v)
	})

	t.Run("Iterate", func(t *testing.T) {
		iter := ins.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		actual, err := iters.Collect(iter)
		require.NoError(t, err)
		require.Len(t, actual, 3)
		require.Equal(t, []byte{3}, actual[0])
		require.Equal(t, []byte{2}, actual[1])
		require.Equal(t, []byte{1}, actual[2])
	})

	t.Run("Delete", func(t *testing.T) {
		err := ins.Delete(prefixes[0])
		require.NoError(t, err)

		v, err := ins.Get(prefixes[0])
		require.Error(t, err)
		require.Nil(t, v)
	})
}
