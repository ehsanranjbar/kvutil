package iters_test

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/iters"
	sstore "github.com/ehsanranjbar/kvutil/store/serialized"
	"github.com/ehsanranjbar/kvutil/testutil"
	"github.com/stretchr/testify/require"
)

func TestSkip(t *testing.T) {
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

	iter := iters.SkipN(ins.NewIterator(badger.DefaultIteratorOptions), 1)
	defer iter.Close()

	iter.Rewind()
	require.True(t, iter.Valid())
	require.Equal(t, keys[1], iter.Item().Key())
	value, err := iter.Value()
	require.NoError(t, err)
	require.Equal(t, values[1], value)
}
