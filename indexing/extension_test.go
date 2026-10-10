package indexing_test

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/codec/lex"
	"github.com/ehsanranjbar/kvutil/expr"
	"github.com/ehsanranjbar/kvutil/indexing"
	"github.com/ehsanranjbar/kvutil/iters"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/ehsanranjbar/kvutil/testutil"
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

func (i TestIndexer) Index(v *TestStruct, update bool) ([]kvutil.RawKVPair, error) {
	if v == nil {
		return nil, nil
	}

	return []kvutil.RawKVPair{
		kvutil.NewRawKVPair(append([]byte("A_idx"), binary.BigEndian.AppendUint64(nil, uint64(-v.A))...), nil),
		kvutil.NewRawKVPair(append([]byte("B_idx"), []byte(v.B)...), nil),
	}, nil
}

func (i TestIndexer) Lookup(args ...any) (kvutil.Iterator[[]byte, indexing.Chunk], error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("no arguments")
	}

	var low []byte
	switch args[0] {
	case "A":
		low = []byte("A_idx")
		if len(args) > 1 && args[1] != nil {
			low = append(low, binary.BigEndian.AppendUint64(nil, uint64(-args[1].(int)))...)
		}
	case "B":
		low = []byte("B_idx")
		if len(args) > 1 && args[1] != nil {
			low = append(low, []byte(args[1].(string))...)
		}
	default:
		return nil, fmt.Errorf("invalid index: %s", args[0])
	}

	high := lex.Increment(bytes.Clone(low))
	return iters.Slice([]indexing.Chunk{indexing.NewChunk(expr.NewBound(low, false), expr.NewBound(high, true))}), nil
}

func TestStore(t *testing.T) {
	store := extstore.New[[]byte, TestStruct](nil).
		WithExtension("test", indexing.NewExtension[[]byte](TestIndexer{}))

	txn := testutil.PrepareTxn(t, true)
	ins := store.Instantiate(txn)
	extIns := ins.GetExtension("test").(*indexing.ExtensionInstance[[]byte, TestStruct])

	var (
		keys   = [][]byte{{1}, {2}, {3}}
		values = []*TestStruct{
			{A: 1, B: "foo"},
			{A: 2, B: "bar"},
			{A: 3, B: "baz"},
		}
	)

	for i, key := range keys {
		err := ins.Set(key, values[i])
		require.NoError(t, err)
	}

	t.Run("Lookup_A", func(t *testing.T) {
		it, err := extIns.Lookup(badger.DefaultIteratorOptions, "A", nil)
		require.NoError(t, err)
		defer it.Close()

		actual, err := iters.Collect(it)
		require.NoError(t, err)
		require.Len(t, actual, len(keys))
		require.Equal(t, [][]byte{{3}, {2}, {1}}, actual)
	})

	t.Run("Lookup_B", func(t *testing.T) {
		it, err := extIns.Lookup(badger.DefaultIteratorOptions, "B", nil)
		require.NoError(t, err)
		defer it.Close()

		actual, err := iters.Collect(it)
		require.NoError(t, err)
		require.Len(t, actual, len(keys))
		require.Equal(t, [][]byte{{2}, {3}, {1}}, actual)
	})
}
