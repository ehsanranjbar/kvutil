package iters_test

import (
	"encoding/binary"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/iters"
	"github.com/stretchr/testify/require"
)

func TestSever(t *testing.T) {
	base := iters.Slice([]int{1, 2, 3, 4, 5})
	it := iters.Sever(base, func(_ []byte, value int, _ *badger.Item) bool {
		return value > 3
	})
	it.Close()

	actual, err := iters.Collect(it)
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3}, actual)

	it.Seek(binary.BigEndian.AppendUint64(nil, 2))

	value, err := it.Value()
	require.NoError(t, err)
	require.Equal(t, 3, value)

	it.Next()
	require.False(t, it.Valid())
	value, err = it.Value()
	require.Error(t, err)
	require.Zero(t, value)
}
