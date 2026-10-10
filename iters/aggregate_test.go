package iters_test

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/iters"
	sstore "github.com/ehsanranjbar/kvutil/store/serialized"
	"github.com/ehsanranjbar/kvutil/testutil"
	"github.com/stretchr/testify/require"
)

func TestAggregate(t *testing.T) {
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

	iter := iters.Aggregate(ins.NewIterator(badger.DefaultIteratorOptions), func(state *int, value *StructA, item *badger.Item) *int {
		if state == nil {
			state = new(int)
		}

		*state += value.A
		return state
	})
	defer iter.Close()

	_, err := iters.Collect(iter)
	require.NoError(t, err)
	require.Equal(t, 3, *iter.Result())
}
