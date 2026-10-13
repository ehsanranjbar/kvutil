package kv_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/stretchr/testify/require"
)

func TestPrefixEnd(t *testing.T) {
	tests := []struct {
		prefix []byte
		want   []byte
	}{
		{nil, nil},
		{[]byte{}, nil},
		{[]byte("a"), []byte("b")},
		{[]byte("ab"), []byte("ac")},
		{[]byte{0x01, 0xff}, []byte{0x02}},
		{[]byte{0x01, 0xff, 0xff}, []byte{0x02}},
		{[]byte{0xff}, nil},
		{[]byte{0xff, 0xff}, nil},
	}
	for _, test := range tests {
		require.Equal(t, test.want, kv.PrefixEnd(test.prefix), "PrefixEnd(%v)", test.prefix)
	}

	// The prefix itself is not modified.
	prefix := []byte("ab")
	kv.PrefixEnd(prefix)
	require.Equal(t, []byte("ab"), prefix)
}

func TestDeleteRange(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		// More keys than a batch has, and keys on both sides of the range.
		const n = 2500
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			for i := range n {
				if err := tx.Set(fmt.Appendf(nil, "b/%05d", i), []byte("x")); err != nil {
					return err
				}
			}
			if err := tx.Set([]byte("a/last"), []byte("x")); err != nil {
				return err
			}
			return tx.Set([]byte("c/first"), []byte("x"))
		}))

		require.NoError(t, kv.DeleteRange(context.Background(), db, []byte("b/"), kv.PrefixEnd([]byte("b/"))))

		var keys []string
		require.NoError(t, db.View(func(tx kv.Tx) error {
			it, err := tx.Iterator(nil, nil)
			if err != nil {
				return err
			}
			defer it.Close()
			for ; it.Valid(); it.Next() {
				keys = append(keys, string(it.Key()))
			}
			return nil
		}))
		require.Equal(t, []string{"a/last", "c/first"}, keys)

		// A range without keys, and a context that is done.
		require.NoError(t, kv.DeleteRange(context.Background(), db, []byte("b/"), []byte("b0")))
	})

	// An engine that cannot drop a range stops between the batches.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, kv.DeleteRange(ctx, memkv.New(), []byte("a"), []byte("b")), context.Canceled)
}
