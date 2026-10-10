package serialized_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/ehsanranjbar/kvutil/store/serialized"
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

type FailStruct struct{}

func (f FailStruct) MarshalBinary() ([]byte, error) {
	return nil, fmt.Errorf("failed to marshal")
}

func (f *FailStruct) UnmarshalBinary(data []byte) error {
	return fmt.Errorf("failed to unmarshal")
}

func TestStore(t *testing.T) {
	store := serialized.New[[]byte, TestStruct](nil)
	failStore := serialized.New[[]byte, FailStruct](nil)

	txn := testutil.PrepareTxn(t, true)
	ins := store.Instantiate(txn)
	failIns := failStore.Instantiate(txn)

	var (
		key   = []byte("foo")
		value = &TestStruct{A: 1, B: "bar"}
	)

	t.Run("NotFound", func(t *testing.T) {
		v, err := ins.Get(key)
		require.Error(t, err)
		require.Nil(t, v)
	})

	t.Run("Set", func(t *testing.T) {
		err := ins.Set(key, value)
		require.NoError(t, err)
	})

	t.Run("Get", func(t *testing.T) {
		actual, err := ins.Get(key)
		require.NoError(t, err)
		require.Equal(t, value, actual)
	})

	t.Run("UnmarshalFail", func(t *testing.T) {
		_, err := failIns.Get(key)
		require.Error(t, err)
	})

	t.Run("MarshalFail", func(t *testing.T) {
		err := failIns.Set(key, &FailStruct{})
		require.Error(t, err)
	})

	t.Run("Delete", func(t *testing.T) {
		err := ins.Delete(key)
		require.NoError(t, err)
	})
}
