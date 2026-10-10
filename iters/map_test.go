package iters_test

import (
	"encoding/json"
	"strconv"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/iters"
	sstore "github.com/ehsanranjbar/kvutil/store/serialized"
	"github.com/ehsanranjbar/kvutil/testutil"
	"github.com/stretchr/testify/require"
)

type StructA struct {
	A int
}

func (t StructA) MarshalBinary() ([]byte, error) {
	return json.Marshal(t)
}

func (t *StructA) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, t)
}

type StructB struct {
	B string
}

func (t StructB) MarshalBinary() ([]byte, error) {
	return json.Marshal(t)
}

func (t *StructB) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, t)
}

func TestMap(t *testing.T) {
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

	iter := iters.Map(ins.NewIterator(badger.DefaultIteratorOptions), func(v *StructA, _ *badger.Item) (StructB, error) {
		return StructB{B: strconv.Itoa(v.A)}, nil
	})
	defer iter.Close()

	i := 0
	for iter.Rewind(); iter.Valid(); iter.Next() {
		value, err := iter.Value()
		require.NoError(t, err)
		require.Equal(t, strconv.Itoa(values[i].A), value.B)
		i++
	}
}
