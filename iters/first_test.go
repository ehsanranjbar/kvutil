package iters_test

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/iters"
	pstore "github.com/ehsanranjbar/kvutil/store/prefix"
	sstore "github.com/ehsanranjbar/kvutil/store/serialized"
	"github.com/ehsanranjbar/kvutil/testutil"
	"github.com/stretchr/testify/require"
)

func TestFirst(t *testing.T) {
	store := sstore.New[[]byte, StructA](pstore.New(nil, []byte("v")))

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

	iter := ins.NewIterator(badger.IteratorOptions{
		Prefix: []byte("foo"),
	})
	defer iter.Close()

	actual, err := iters.First(iter)
	require.NoError(t, err)
	require.Equal(t, values[0], actual)
}

func TestFirstItem(t *testing.T) {
	txn := testutil.PrepareTxn(t, true)

	var (
		keys   = [][]byte{[]byte("foo1"), []byte("foo2")}
		values = [][]byte{[]byte("bar1"), []byte("bar2")}
	)

	for i, key := range keys {
		err := txn.Set(key, values[i])
		require.NoError(t, err)
	}

	iter := txn.NewIterator(badger.IteratorOptions{
		Prefix: []byte("foo"),
	})
	defer iter.Close()

	actual, err := iters.FirstItem(iter)
	require.NoError(t, err)
	require.Equal(t, keys[0], actual.Key())
	actualValue, err := actual.ValueCopy(nil)
	require.NoError(t, err)
	require.Equal(t, values[0], actualValue)
}
