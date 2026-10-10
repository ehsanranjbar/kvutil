package prefix_test

import (
	"bytes"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/badgerkv"
	"github.com/ehsanranjbar/kvutil/kv/kvtest"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/ehsanranjbar/kvutil/kv/prefix"
	"github.com/stretchr/testify/require"
)

// prefixedDB gives every transaction of a DB the same prefix.
type prefixedDB struct {
	base   kv.DB
	prefix []byte
}

func (db prefixedDB) View(fn func(kv.Tx) error) error {
	return db.base.View(func(tx kv.Tx) error {
		return fn(prefix.New(tx, db.prefix))
	})
}

func (db prefixedDB) Update(fn func(kv.Tx) error) error {
	return db.base.Update(func(tx kv.Tx) error {
		return fn(prefix.New(tx, db.prefix))
	})
}

func newBadger(t *testing.T) kv.DB {
	t.Helper()

	db, err := badgerkv.Open(badger.DefaultOptions("").WithInMemory(true).WithLoggingLevel(badger.ERROR))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

// surround writes keys right before, at and right after the keyspace of the prefix,
// which the tests of the prefixed store must never see.
func surround(t *testing.T, db kv.DB, p []byte) kv.DB {
	t.Helper()

	err := db.Update(func(tx kv.Tx) error {
		neighbors := [][]byte{{0x00}}
		if len(p) > 1 {
			before := append(bytes.Clone(p[:len(p)-1]), p[len(p)-1]-1, 0xff)
			neighbors = append(neighbors, p[:len(p)-1], before)
		}
		if end := kv.PrefixEnd(p); end != nil {
			neighbors = append(neighbors, end, append(end, 'a'))
		}
		for _, key := range neighbors {
			if err := tx.Set(key, []byte("outside")); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	return prefixedDB{base: db, prefix: p}
}

func TestConformance(t *testing.T) {
	engines := map[string]func(t *testing.T) kv.DB{
		"Mem":    func(*testing.T) kv.DB { return memkv.New() },
		"Badger": newBadger,
	}
	prefixes := map[string][]byte{
		"Plain":   []byte("p/"),
		"AllOnes": {0xff, 0xff},
		"Nested":  {0x01, 0xff},
	}
	for engine, newEngine := range engines {
		for name, p := range prefixes {
			t.Run(engine+name, func(t *testing.T) {
				kvtest.Run(t, func(t *testing.T) kv.DB {
					return surround(t, newEngine(t), p)
				}, kvtest.Options{AllowEmptyKey: true})
			})
		}

		t.Run(engine+"Empty", func(t *testing.T) {
			kvtest.Run(t, func(t *testing.T) kv.DB {
				return prefixedDB{base: newEngine(t)}
			}, kvtest.Options{})
		})
	}
}

func TestIsolation(t *testing.T) {
	db := memkv.New()

	err := db.Update(func(tx kv.Tx) error {
		a, b := prefix.New(tx, []byte("a")), prefix.New(tx, []byte("b"))
		require.NoError(t, a.Set([]byte("k"), []byte("1")))
		require.NoError(t, b.Set([]byte("k"), []byte("2")))

		// A prefix of another prefix is a store of its own.
		ab := prefix.New(a, []byte("b"))
		require.Equal(t, []byte("b"), ab.Prefix())
		require.NoError(t, ab.Set([]byte("k"), []byte("3")))

		value, err := a.Get([]byte("k"))
		require.NoError(t, err)
		require.Equal(t, []byte("1"), value)

		value, err = b.Get([]byte("k"))
		require.NoError(t, err)
		require.Equal(t, []byte("2"), value)

		it, err := b.Iterator(nil, nil)
		require.NoError(t, err)
		defer it.Close()
		require.True(t, it.Valid())
		require.Equal(t, []byte("k"), it.Key())
		it.Next()
		require.False(t, it.Valid())

		// The keys as they are stored.
		for key, want := range map[string]string{"ak": "1", "bk": "2", "abk": "3"} {
			value, err := tx.Get([]byte(key))
			require.NoError(t, err)
			require.Equal(t, []byte(want), value)
		}
		return nil
	})
	require.NoError(t, err)
}

func TestPrefixIsCopied(t *testing.T) {
	db := memkv.New()

	err := db.Update(func(tx kv.Tx) error {
		p := []byte("a")
		store := prefix.New(tx, p)
		p[0] = 'z'

		require.NoError(t, store.Set([]byte("k"), []byte("1")))
		ok, err := tx.Has([]byte("ak"))
		require.NoError(t, err)
		require.True(t, ok)
		return nil
	})
	require.NoError(t, err)
}

func TestUnwrapAndWritable(t *testing.T) {
	db := memkv.New()

	err := db.View(func(tx kv.Tx) error {
		store := prefix.New(tx, []byte("a"))
		require.Equal(t, tx, store.Unwrap())
		require.False(t, store.Writable())
		require.ErrorIs(t, store.Set([]byte("k"), nil), kv.ErrReadOnly)
		return nil
	})
	require.NoError(t, err)

	err = db.Update(func(tx kv.Tx) error {
		store := prefix.New(tx, []byte("a"))
		require.True(t, store.Writable())
		// The engine has no TTL, which the decorator reports as it is.
		require.ErrorIs(t, kv.SetWithTTL(store, []byte("k"), nil, time.Hour), kv.ErrUnsupported)
		return nil
	})
	require.NoError(t, err)
}
