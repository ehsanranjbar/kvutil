package prefix_test

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/legacy/store/prefix"
	"github.com/ehsanranjbar/kvutil/legacy/testutil"
	"github.com/stretchr/testify/require"
)

func TestPrefixStore(t *testing.T) {
	store := prefix.New(nil, []byte("prefix"))

	txn := testutil.PrepareTxn(t, true)
	ins := store.Instantiate(txn)

	t.Run("Prefix", func(t *testing.T) {
		require.Equal(t, []byte("prefix"), store.Prefix())
		require.Equal(t, []byte("prefixfoo"), prefix.New(store, []byte("foo")).Prefix())
	})

	var (
		key   = []byte("foo")
		value = []byte("bar")
	)

	t.Run("Set", func(t *testing.T) {
		err := ins.Set(key, value)
		require.NoError(t, err)
	})

	t.Run("SetEntry", func(t *testing.T) {
		err := ins.SetEntry(&badger.Entry{Key: key, Value: value})
		require.NoError(t, err)
	})

	t.Run("Get", func(t *testing.T) {
		item, err := ins.Get(key)
		require.NoError(t, err)
		require.NotNil(t, item)
		item, err = txn.Get(append([]byte("prefix"), key...))
		require.NoError(t, err)
		require.NotNil(t, item)
	})

	t.Run("NewIterator", func(t *testing.T) {
		iter := ins.NewIterator(badger.IteratorOptions{Prefix: []byte("foo")})
		defer iter.Close()
		require.NotNil(t, iter)

		for iter.Rewind(); iter.Valid(); iter.Next() {
			iter.Item().Value(func(val []byte) error {
				require.Equal(t, value, val)
				return nil
			})
		}
	})

	t.Run("Delete", func(t *testing.T) {
		err := ins.Delete(key)
		require.NoError(t, err)
	})

	t.Run("Get after Delete", func(t *testing.T) {
		item, err := ins.Get(key)
		require.Error(t, err)
		require.Nil(t, item)
	})
}
