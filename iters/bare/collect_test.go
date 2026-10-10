package bare_test

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/codec/lex"
	"github.com/ehsanranjbar/kvutil/iters/bare"
	"github.com/ehsanranjbar/kvutil/testutil"
	"github.com/stretchr/testify/require"
)

func TestCollectKys(t *testing.T) {
	txn := testutil.PrepareTxn(t, true)

	for i := 0; i < 3; i++ {
		require.NoError(t, txn.Set(lex.EncodeInt8(int8(i)), nil))
	}

	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	collected := bare.CollectKeys(it)
	require.Equal(t, [][]byte{
		lex.EncodeInt8(0),
		lex.EncodeInt8(1),
		lex.EncodeInt8(2),
	}, collected)
}
