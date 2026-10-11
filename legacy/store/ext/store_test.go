package ext_test

import (
	"encoding/json"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/legacy"
	"github.com/ehsanranjbar/kvutil/legacy/indexing"
	"github.com/ehsanranjbar/kvutil/legacy/iters"
	extstore "github.com/ehsanranjbar/kvutil/legacy/store/ext"
	"github.com/ehsanranjbar/kvutil/legacy/testutil"
	"github.com/stretchr/testify/require"
)

type TestStruct struct {
	A int
	B string
}

func (t TestStruct) MarshalBinary() ([]byte, error) {
	return json.Marshal(t)
}

func (t *TestStruct) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, t)
}

type TestIndexer struct{}

func (i TestIndexer) Index(v *TestStruct, update bool) ([]legacy.RawKVPair, error) {
	return []legacy.RawKVPair{}, nil
}

func (i TestIndexer) Lookup(args ...any) (legacy.Iterator[[]byte, indexing.Chunk], error) {
	return nil, nil
}

func TestStore(t *testing.T) {
	store := extstore.New[[]byte, TestStruct](nil).
		WithExtension("test", indexing.NewExtension[[]byte](TestIndexer{}))

	txn := testutil.PrepareTxn(t, true)
	ins := store.Instantiate(txn)

	require.Panics(t, func() {
		store.WithExtension("test", indexing.NewExtension[[]byte](TestIndexer{}))
	})

	var (
		keys   = [][]byte{{1}, {2}, {3}}
		values = []*TestStruct{
			{A: 1, B: "foo"},
			{A: 2, B: "bar"},
			{A: 3, B: "baz"},
		}
	)

	t.Run("NotFound", func(t *testing.T) {
		for _, key := range keys {
			_, err := ins.Get(key)
			require.Error(t, err)
		}
	})

	t.Run("Set", func(t *testing.T) {
		for i, key := range keys {
			err := ins.Set(key, values[i])
			require.NoError(t, err)
		}
	})

	t.Run("Get", func(t *testing.T) {
		for i, key := range keys {
			v, err := ins.Get(key)
			require.NoError(t, err)
			require.Equal(t, values[i], v)
		}
	})

	t.Run("Iterate", func(t *testing.T) {
		iter := ins.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		actual, err := iters.Collect(iter)
		require.NoError(t, err)
		require.Len(t, actual, len(keys))
		require.Equal(t, values, actual)
	})

	t.Run("Delete", func(t *testing.T) {
		for _, key := range keys {
			err := ins.Delete(key)
			require.NoError(t, err)
		}
	})
}
