package iters_test

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/legacy/iters"
	sstore "github.com/ehsanranjbar/kvutil/legacy/store/serialized"
	"github.com/ehsanranjbar/kvutil/legacy/testutil"
	"github.com/stretchr/testify/require"
)

func TestLimit(t *testing.T) {
	store := sstore.New[[]byte, StructA](nil)

	txn := testutil.PrepareTxn(t, true)
	ins := store.Instantiate(txn)

	var (
		keys   = [][]byte{[]byte("foo1"), []byte("foo2")}
		values = []*StructA{{A: 1}, {A: 2}}
	)

	for i, key := range keys {
		err := ins.Set(key, values[i])
		require.NoError(t, err)
	}

	iter := iters.Limit(ins.NewIterator(badger.DefaultIteratorOptions), 1)
	defer iter.Close()

	iter.Rewind()
	require.True(t, iter.Valid())
	require.Equal(t, keys[0], iter.Item().Key())
	value, err := iter.Value()
	require.NoError(t, err)
	require.Equal(t, values[0], value)

	iter.Next()
	require.False(t, iter.Valid())
}
