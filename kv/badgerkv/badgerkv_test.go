package badgerkv_test

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/badgerkv"
	"github.com/ehsanranjbar/kvutil/kv/kvtest"
	"github.com/ehsanranjbar/kvutil/kv/prefix"
	"github.com/stretchr/testify/require"
)

func newDB(t *testing.T, opts ...badgerkv.Option) *badgerkv.DB {
	t.Helper()

	db, err := badgerkv.Open(badger.DefaultOptions("").WithInMemory(true).WithLoggingLevel(badger.ERROR), opts...)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestConformance(t *testing.T) {
	kvtest.Run(t, func(t *testing.T) kv.DB { return newDB(t) }, kvtest.Options{})
}

func TestConformanceWithPrefetch(t *testing.T) {
	kvtest.Run(t, func(t *testing.T) kv.DB { return newDB(t, badgerkv.WithPrefetch(10)) }, kvtest.Options{})
}

func TestUpdateRetriesConflicts(t *testing.T) {
	// conflicting runs a transaction that conflicts on its first n attempts.
	conflicting := func(t *testing.T, db *badgerkv.DB, n int) (attempts int, err error) {
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			return tx.Set([]byte("a"), []byte("0"))
		}))

		err = db.Update(func(tx kv.Tx) error {
			attempts++
			if _, err := tx.Get([]byte("a")); err != nil {
				return err
			}
			if attempts <= n {
				// Another transaction commits a write to the key that this one has read.
				require.NoError(t, db.Update(func(other kv.Tx) error {
					return other.Set([]byte("a"), fmt.Append(nil, attempts))
				}))
			}
			return tx.Set([]byte("b"), []byte("1"))
		})
		return attempts, err
	}

	t.Run("Retried", func(t *testing.T) {
		attempts, err := conflicting(t, newDB(t), 2)
		require.NoError(t, err)
		require.Equal(t, 3, attempts)
	})

	t.Run("RetriesExhausted", func(t *testing.T) {
		attempts, err := conflicting(t, newDB(t, badgerkv.WithMaxRetries(1)), 5)
		require.ErrorIs(t, err, kv.ErrConflict)
		require.Equal(t, 2, attempts)
	})

	t.Run("RetriesDisabled", func(t *testing.T) {
		attempts, err := conflicting(t, newDB(t, badgerkv.WithMaxRetries(0)), 1)
		require.ErrorIs(t, err, kv.ErrConflict)
		require.Equal(t, 1, attempts)
	})
}

func TestBulkWriterBeyondTxnLimit(t *testing.T) {
	db := newDB(t)
	value := bytes.Repeat([]byte{0x01}, 1<<19)
	const n = 32

	err := db.Update(func(tx kv.Tx) error {
		for i := range n {
			if err := tx.Set(fmt.Appendf(nil, "tx/%02d", i), value); err != nil {
				return err
			}
		}
		return nil
	})
	require.ErrorIs(t, err, kv.ErrTxnTooBig)

	w := db.NewBulkWriter()
	defer w.Cancel()
	for i := range n {
		require.NoError(t, w.Set(fmt.Appendf(nil, "bulk/%02d", i), value))
	}
	require.NoError(t, w.Flush())

	err = db.View(func(tx kv.Tx) error {
		it, err := tx.Iterator(nil, nil)
		require.NoError(t, err)
		defer it.Close()

		count := 0
		for ; it.Valid(); it.Next() {
			require.True(t, bytes.HasPrefix(it.Key(), []byte("bulk/")))
			count++
		}
		require.Equal(t, n, count)
		return nil
	})
	require.NoError(t, err)
}

func TestTTL(t *testing.T) {
	db := newDB(t)

	err := db.Update(func(tx kv.Tx) error {
		require.NoError(t, kv.SetWithTTL(tx, []byte("a"), []byte("1"), time.Second))
		// Through a decorator.
		require.NoError(t, kv.SetWithTTL(prefix.New(tx, []byte("p/")), []byte("a"), []byte("1"), time.Second))
		return tx.Set([]byte("keep"), []byte("1"))
	})
	require.NoError(t, err)

	keys := func() []string {
		var keys []string
		err := db.View(func(tx kv.Tx) error {
			it, err := tx.Iterator(nil, nil)
			require.NoError(t, err)
			defer it.Close()

			for ; it.Valid(); it.Next() {
				keys = append(keys, string(it.Key()))
			}
			return nil
		})
		require.NoError(t, err)
		return keys
	}

	require.Equal(t, []string{"a", "keep", "p/a"}, keys())
	require.Eventually(t, func() bool {
		return len(keys()) == 1
	}, 5*time.Second, 100*time.Millisecond)
	require.Equal(t, []string{"keep"}, keys())
}

func TestEscapeHatch(t *testing.T) {
	db := newDB(t)
	require.NotNil(t, db.Badger())

	err := db.Update(func(tx kv.Tx) error {
		txn, ok := badgerkv.Unwrap(tx)
		require.True(t, ok)
		require.NotNil(t, txn)

		// Through decorators.
		nested := prefix.New(prefix.New(tx, []byte("a/")), []byte("b/"))
		unwrapped, ok := badgerkv.Unwrap(nested)
		require.True(t, ok)
		require.Same(t, txn, unwrapped)

		// The badger transaction sees the keys as they are stored.
		require.NoError(t, nested.Set([]byte("k"), []byte("1")))
		item, err := txn.Get([]byte("a/b/k"))
		require.NoError(t, err)
		require.Equal(t, []byte("a/b/k"), item.Key())
		return nil
	})
	require.NoError(t, err)
}

func TestWrappedTxn(t *testing.T) {
	db := newDB(t).Badger()

	err := db.Update(func(txn *badger.Txn) error {
		tx := badgerkv.WrapReadWrite(txn)
		require.True(t, tx.Writable())
		return tx.Set([]byte("a"), []byte("1"))
	})
	require.NoError(t, err)

	err = db.View(func(txn *badger.Txn) error {
		tx := badgerkv.WrapRead(txn)
		require.False(t, tx.Writable())
		require.ErrorIs(t, tx.Set([]byte("b"), []byte("2")), kv.ErrReadOnly)

		value, err := tx.Get([]byte("a"))
		require.NoError(t, err)
		require.Equal(t, []byte("1"), value)

		// A read-only badger transaction that is wrapped as writable is still read-only.
		require.ErrorIs(t, badgerkv.WrapReadWrite(txn).Set([]byte("b"), []byte("2")), kv.ErrReadOnly)
		return nil
	})
	require.NoError(t, err)
}

func TestSequenceID(t *testing.T) {
	db := newDB(t).Badger()

	seq, err := db.GetSequence([]byte("seq"), 10)
	require.NoError(t, err)
	defer seq.Release()

	next := badgerkv.SequenceID[int64, struct{}](seq)
	for want := int64(1); want <= 3; want++ {
		id, err := next(nil)
		require.NoError(t, err)
		require.Equal(t, want, id)
	}
}
