package rec_test

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/iters"
	recstore "github.com/ehsanranjbar/kvutil/store/rec"
	"github.com/ehsanranjbar/kvutil/testutil"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestStore(t *testing.T) {
	store := recstore.New[uuid.UUID, recstore.Object[uuid.UUID, testutil.SampleStruct]](nil)

	txn := testutil.PrepareTxn(t, true)
	ins := store.Instantiate(txn)

	t.Run("SetWithZeroId", func(t *testing.T) {
		err := ins.Set(recstore.NewObject[uuid.UUID](testutil.SampleStruct{B: "foo"}))
		require.Error(t, err)
	})

	store = recstore.New[uuid.UUID, recstore.Object[uuid.UUID, testutil.SampleStruct]](nil).
		WithIdFunc(func(_ *recstore.Object[uuid.UUID, testutil.SampleStruct]) (uuid.UUID, error) {
			return uuid.New(), nil
		})

	ins = store.Instantiate(txn)
	var (
		e1 = recstore.NewObjectWithId(uuid.New(), testutil.SampleStruct{B: "foo"})
		e2 = recstore.NewObjectWithId(uuid.New(), testutil.SampleStruct{B: "bar"})
		e3 = recstore.NewObject[uuid.UUID](testutil.SampleStruct{B: "baz"})
	)

	t.Run("NotFound", func(t *testing.T) {
		v, err := ins.Get(e1.Id)
		require.Error(t, err)
		require.Nil(t, v)
	})

	t.Run("Set", func(t *testing.T) {
		err := ins.Set(e1)
		require.NoError(t, err)
		err = ins.Set(e2)
		require.NoError(t, err)
		err = ins.Set(e3)
		require.NoError(t, err)
		require.NotZero(t, e3.Id)
	})

	t.Run("Get", func(t *testing.T) {
		v, err := ins.Get(e1.Id)
		require.NoError(t, err)
		require.Equal(t, e1, v)

		v, err = ins.Get(e2.Id)
		require.NoError(t, err)
		require.Equal(t, e2, v)
	})

	t.Run("NewIterator", func(t *testing.T) {
		iter := ins.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		c := iters.ConsumeAndCount(iter)
		require.Equal(t, uint(3), c)
	})

	t.Run("Query", func(t *testing.T) {
		iter, err := ins.Query(`Data.B like "ba*"`)
		require.NoError(t, err)

		c := iters.ConsumeAndCount(iter)
		require.Equal(t, uint(2), c)
	})

	t.Run("Delete", func(t *testing.T) {
		err := ins.Delete(e1.Id)
		require.NoError(t, err)

		_, err = ins.Get(e1.Id)
		require.Error(t, err)
	})
}
