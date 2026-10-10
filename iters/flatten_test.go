package iters_test

import (
	"encoding/binary"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/iters"
	pstore "github.com/ehsanranjbar/kvutil/store/prefix"
	sstore "github.com/ehsanranjbar/kvutil/store/serialized"
	"github.com/ehsanranjbar/kvutil/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFlatten(t *testing.T) {
	txn := testutil.PrepareTxn(t, true)

	var its []kvutil.Iterator[[]byte, *StructA]
	for i := 0; i < 3; i++ {
		store := sstore.New[[]byte, StructA](pstore.New(nil, []byte{byte(i)}))
		ins := store.Instantiate(txn)

		for j := 0; j < 3; j++ {
			err := ins.Set([]byte{byte(j)}, &StructA{A: i*3 + j})
			require.NoError(t, err)
		}

		it := ins.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		its = append(its, it)
	}

	flatten := iters.Flatten(iters.Slice(its))
	defer flatten.Close()
	var i, j int
	for flatten.Rewind(); flatten.Valid(); flatten.Next() {
		require.Equal(t, []byte{byte(i), byte(j)}, flatten.Item().Key())

		value, err := flatten.Value()
		require.NoError(t, err)

		assert.Equal(t, &StructA{A: i*3 + j}, value)

		j++
		if j == 3 {
			j = 0
			i++
		}
	}

	flatten.Seek(binary.BigEndian.AppendUint64(nil, 1))
	require.Equal(t, []byte{1, 0}, flatten.Item().Key())
}
