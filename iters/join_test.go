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

func TestJoin(t *testing.T) {
	aStore := sstore.New[[]byte, StructA](pstore.New(nil, []byte("a")))
	bStore := sstore.New[[]byte, StructB](pstore.New(nil, []byte("b")))

	txn := testutil.PrepareTxn(t, true)
	aIns := aStore.Instantiate(txn)
	bIns := bStore.Instantiate(txn)

	var (
		aKeys   = [][]byte{[]byte("foo1"), []byte("foo2")}
		aValues = []*StructA{{A: 1}, {A: 2}}
		bKeys   = [][]byte{[]byte("foo1"), []byte("foo2"), []byte("foo3")}
		bValues = []*StructB{{B: "1"}, {B: "2"}, {B: "3"}}
	)

	for i, key := range aKeys {
		err := aIns.Set(key, aValues[i])
		require.NoError(t, err)
	}
	for i, key := range bKeys {
		err := bIns.Set(key, bValues[i])
		require.NoError(t, err)
	}

	iter := iters.Join(
		aIns.NewIterator(badger.DefaultIteratorOptions),
		bIns.NewIterator(badger.DefaultIteratorOptions),
		iters.UnionJoinFunc[*StructA, *StructB],
	)
	defer iter.Close()

	result, err := iters.Collect(iter)
	require.NoError(t, err)

	require.Len(t, result, 3)
	require.Equal(t, iters.Union[*StructA, *StructB]{T: &StructA{A: 1}, U: &StructB{B: "1"}}, result[0])
	require.Equal(t, iters.Union[*StructA, *StructB]{T: &StructA{A: 2}, U: &StructB{B: "2"}}, result[1])
	require.Equal(t, iters.Union[*StructA, *StructB]{T: nil, U: &StructB{B: "3"}}, result[2])
}
