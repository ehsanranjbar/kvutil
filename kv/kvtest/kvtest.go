// Package kvtest provides the conformance tests that every kv engine and decorator must pass.
package kvtest

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/stretchr/testify/require"
)

// Options adjusts the tests to the store that is tested.
type Options struct {
	// AllowEmptyKey is set for the stores in which the empty key is a valid key.
	AllowEmptyKey bool
}

// Run runs the conformance tests. newDB must return a new empty store on each call.
func Run(t *testing.T, newDB func(t *testing.T) kv.DB, opts Options) {
	tests := []struct {
		name string
		run  func(t *testing.T, db kv.DB)
	}{
		{"GetMissing", testGetMissing},
		{"SetGetDelete", testSetGetDelete},
		{"ReadYourWrites", testReadYourWrites},
		{"EmptyValue", testEmptyValue},
		{"NoRetention", testNoRetention},
		{"ReadOnly", testReadOnly},
		{"Rollback", testRollback},
		{"Snapshot", testSnapshot},
		{"IteratorRanges", testIteratorRanges},
		{"IteratorSeek", testIteratorSeek},
		{"IteratorExhausted", testIteratorExhausted},
		{"IteratorOwnWrites", testIteratorOwnWrites},
		{"IteratorValues", testIteratorValues},
		{"IteratorOrder", testIteratorOrder},
		{"Capabilities", testCapabilities},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.run(t, newDB(t))
		})
	}

	t.Run("EmptyKey", func(t *testing.T) {
		testEmptyKey(t, newDB(t), opts.AllowEmptyKey)
	})
}

// set writes the keys with their own bytes as the values.
func set(t *testing.T, db kv.DB, keys ...string) {
	t.Helper()

	err := db.Update(func(tx kv.Tx) error {
		for _, key := range keys {
			if err := tx.Set([]byte(key), []byte(key)); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
}

// drain collects the remaining keys of the iterator.
func drain(it kv.Iterator) []string {
	keys := []string{}
	for ; it.Valid(); it.Next() {
		keys = append(keys, string(it.Key()))
	}
	return keys
}

// scan collects the keys of a range in a transaction of its own.
func scan(t *testing.T, db kv.DB, start, end string, reverse bool) []string {
	t.Helper()

	var keys []string
	err := db.View(func(tx kv.Tx) error {
		it, err := iterate(tx, start, end, reverse)
		if err != nil {
			return err
		}
		defer it.Close()

		keys = drain(it)
		return nil
	})
	require.NoError(t, err)
	return keys
}

func iterate(r kv.Reader, start, end string, reverse bool) (kv.Iterator, error) {
	if reverse {
		return r.ReverseIterator([]byte(start), []byte(end))
	}
	return r.Iterator([]byte(start), []byte(end))
}

func testGetMissing(t *testing.T, db kv.DB) {
	err := db.View(func(tx kv.Tx) error {
		_, err := tx.Get([]byte("a"))
		require.ErrorIs(t, err, kv.ErrNotFound)

		ok, err := tx.Has([]byte("a"))
		require.NoError(t, err)
		require.False(t, ok)
		return nil
	})
	require.NoError(t, err)
}

func testSetGetDelete(t *testing.T, db kv.DB) {
	err := db.Update(func(tx kv.Tx) error {
		require.True(t, tx.Writable())
		return tx.Set([]byte("a"), []byte("1"))
	})
	require.NoError(t, err)

	err = db.View(func(tx kv.Tx) error {
		value, err := tx.Get([]byte("a"))
		require.NoError(t, err)
		require.Equal(t, []byte("1"), value)

		ok, err := tx.Has([]byte("a"))
		require.NoError(t, err)
		require.True(t, ok)
		return nil
	})
	require.NoError(t, err)

	err = db.Update(func(tx kv.Tx) error {
		require.NoError(t, tx.Delete([]byte("a")))
		// Deleting a key that does not exist is not an error.
		return tx.Delete([]byte("missing"))
	})
	require.NoError(t, err)

	err = db.View(func(tx kv.Tx) error {
		_, err := tx.Get([]byte("a"))
		require.ErrorIs(t, err, kv.ErrNotFound)
		return nil
	})
	require.NoError(t, err)
}

func testReadYourWrites(t *testing.T, db kv.DB) {
	set(t, db, "a")

	err := db.Update(func(tx kv.Tx) error {
		require.NoError(t, tx.Set([]byte("b"), []byte("2")))
		value, err := tx.Get([]byte("b"))
		require.NoError(t, err)
		require.Equal(t, []byte("2"), value)

		require.NoError(t, tx.Set([]byte("a"), []byte("1")))
		value, err = tx.Get([]byte("a"))
		require.NoError(t, err)
		require.Equal(t, []byte("1"), value)

		require.NoError(t, tx.Delete([]byte("a")))
		ok, err := tx.Has([]byte("a"))
		require.NoError(t, err)
		require.False(t, ok)
		_, err = tx.Get([]byte("a"))
		require.ErrorIs(t, err, kv.ErrNotFound)
		return nil
	})
	require.NoError(t, err)
}

func testEmptyValue(t *testing.T, db kv.DB) {
	err := db.Update(func(tx kv.Tx) error {
		return tx.Set([]byte("a"), nil)
	})
	require.NoError(t, err)

	err = db.View(func(tx kv.Tx) error {
		ok, err := tx.Has([]byte("a"))
		require.NoError(t, err)
		require.True(t, ok)

		value, err := tx.Get([]byte("a"))
		require.NoError(t, err)
		require.Empty(t, value)
		return nil
	})
	require.NoError(t, err)
}

func testEmptyKey(t *testing.T, db kv.DB, allowed bool) {
	err := db.Update(func(tx kv.Tx) error {
		return tx.Set(nil, []byte("1"))
	})
	if !allowed {
		require.ErrorIs(t, err, kv.ErrEmptyKey)

		err = db.View(func(tx kv.Tx) error {
			_, err := tx.Get(nil)
			return err
		})
		require.ErrorIs(t, err, kv.ErrEmptyKey)
		return
	}
	require.NoError(t, err)

	set(t, db, "a")
	require.Equal(t, []string{"", "a"}, scan(t, db, "", "", false))
	require.Equal(t, []string{"a", ""}, scan(t, db, "", "", true))

	err = db.Update(func(tx kv.Tx) error {
		value, err := tx.Get(nil)
		require.NoError(t, err)
		require.Equal(t, []byte("1"), value)
		return tx.Delete(nil)
	})
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, scan(t, db, "", "", false))
}

func testNoRetention(t *testing.T, db kv.DB) {
	err := db.Update(func(tx kv.Tx) error {
		key, value := []byte("a"), []byte("1")
		require.NoError(t, tx.Set(key, value))
		// The transaction must not be affected by what happens to the slices afterwards.
		key[0], value[0] = 'z', '9'

		deleted := []byte("b")
		require.NoError(t, tx.Set([]byte("b"), []byte("2")))
		require.NoError(t, tx.Delete(deleted))
		deleted[0] = 'y'
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, scan(t, db, "", "", false))

	err = db.View(func(tx kv.Tx) error {
		value, err := tx.Get([]byte("a"))
		require.NoError(t, err)
		require.Equal(t, []byte("1"), value)

		// The value that is returned belongs to the caller.
		value[0] = '9'
		value, err = tx.Get([]byte("a"))
		require.NoError(t, err)
		require.Equal(t, []byte("1"), value)
		return nil
	})
	require.NoError(t, err)
}

func testReadOnly(t *testing.T, db kv.DB) {
	set(t, db, "a")

	err := db.View(func(tx kv.Tx) error {
		require.False(t, tx.Writable())
		require.ErrorIs(t, tx.Set([]byte("b"), []byte("2")), kv.ErrReadOnly)
		require.ErrorIs(t, tx.Delete([]byte("a")), kv.ErrReadOnly)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, scan(t, db, "", "", false))
}

func testRollback(t *testing.T, db kv.DB) {
	set(t, db, "a")

	failure := errors.New("failure")
	err := db.Update(func(tx kv.Tx) error {
		require.NoError(t, tx.Set([]byte("b"), []byte("2")))
		require.NoError(t, tx.Delete([]byte("a")))
		return failure
	})
	require.ErrorIs(t, err, failure)
	require.Equal(t, []string{"a"}, scan(t, db, "", "", false))
}

func testSnapshot(t *testing.T, db kv.DB) {
	set(t, db, "a")

	err := db.View(func(tx kv.Tx) error {
		// A commit that happens while the transaction is open is not visible to it.
		err := db.Update(func(other kv.Tx) error {
			require.NoError(t, other.Set([]byte("a"), []byte("changed")))
			return other.Set([]byte("b"), []byte("b"))
		})
		require.NoError(t, err)

		value, err := tx.Get([]byte("a"))
		require.NoError(t, err)
		require.Equal(t, []byte("a"), value)

		it, err := tx.Iterator(nil, nil)
		require.NoError(t, err)
		defer it.Close()
		require.Equal(t, []string{"a"}, drain(it))
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, scan(t, db, "", "", false))
}

func testIteratorRanges(t *testing.T, db kv.DB) {
	set(t, db, "a", "b", "c", "d")

	tests := []struct {
		start, end string
		want       []string
	}{
		{"", "", []string{"a", "b", "c", "d"}},
		{"b", "", []string{"b", "c", "d"}},
		{"", "c", []string{"a", "b"}},
		{"b", "d", []string{"b", "c"}},
		{"aa", "cc", []string{"b", "c"}},
		{"b", "c", []string{"b"}},
		{"b", "b", []string{}},
		{"c", "b", []string{}},
		{"e", "", []string{}},
		{"", "a", []string{}},
		{"bb", "bc", []string{}},
	}
	for _, test := range tests {
		got := scan(t, db, test.start, test.end, false)
		require.Equal(t, test.want, got, "forward [%q, %q)", test.start, test.end)

		want := slices.Clone(test.want)
		slices.Reverse(want)
		got = scan(t, db, test.start, test.end, true)
		require.Equal(t, want, got, "reverse [%q, %q)", test.start, test.end)
	}
}

func testIteratorSeek(t *testing.T, db kv.DB) {
	set(t, db, "a", "b", "c", "d", "e")

	type seek struct {
		key  string
		want string // empty if the iterator must not be valid
	}
	check := func(t *testing.T, start, end string, reverse bool, first string, seeks []seek) {
		err := db.View(func(tx kv.Tx) error {
			it, err := iterate(tx, start, end, reverse)
			require.NoError(t, err)
			defer it.Close()

			require.True(t, it.Valid())
			require.Equal(t, first, string(it.Key()))

			for _, s := range seeks {
				it.Seek([]byte(s.key))
				if s.want == "" {
					require.False(t, it.Valid(), "seek %q", s.key)
					continue
				}
				require.True(t, it.Valid(), "seek %q", s.key)
				require.Equal(t, s.want, string(it.Key()), "seek %q", s.key)
			}
			return nil
		})
		require.NoError(t, err)
	}

	t.Run("Forward", func(t *testing.T) {
		check(t, "b", "e", false, "b", []seek{
			{"c", "c"},
			{"bb", "c"},
			{"a", "b"}, // before the range
			{"b", "b"},
			{"d", "d"},
			{"e", ""}, // the end of the range
			{"z", ""},
			{"c", "c"}, // an exhausted iterator can be moved back
		})
	})

	t.Run("ForwardUnbounded", func(t *testing.T) {
		check(t, "", "", false, "a", []seek{
			{"e", "e"},
			{"", "a"},
			{"ee", ""},
		})
	})

	t.Run("Reverse", func(t *testing.T) {
		check(t, "b", "e", true, "d", []seek{
			{"c", "c"},
			{"cc", "c"},
			{"z", "d"}, // after the range
			{"e", "d"}, // the end of the range
			{"d", "d"},
			{"b", "b"},
			{"a", ""},
			{"", ""},
			{"c", "c"}, // an exhausted iterator can be moved back
		})
	})

	t.Run("ReverseUnbounded", func(t *testing.T) {
		check(t, "", "", true, "e", []seek{
			{"a", "a"},
			{"z", "e"},
			{"bb", "b"},
		})
	})

	t.Run("NextAfterSeek", func(t *testing.T) {
		err := db.View(func(tx kv.Tx) error {
			it, err := tx.Iterator([]byte("b"), []byte("e"))
			require.NoError(t, err)
			defer it.Close()

			it.Seek([]byte("c"))
			require.Equal(t, []string{"c", "d"}, drain(it))

			rit, err := tx.ReverseIterator([]byte("b"), []byte("e"))
			require.NoError(t, err)
			defer rit.Close()

			rit.Seek([]byte("c"))
			require.Equal(t, []string{"c", "b"}, drain(rit))
			return nil
		})
		require.NoError(t, err)
	})
}

func testIteratorExhausted(t *testing.T, db kv.DB) {
	set(t, db, "a")

	for _, reverse := range []bool{false, true} {
		err := db.View(func(tx kv.Tx) error {
			it, err := iterate(tx, "", "", reverse)
			require.NoError(t, err)

			require.True(t, it.Valid())
			it.Next()
			require.False(t, it.Valid())
			// Advancing an exhausted iterator does nothing.
			it.Next()
			require.False(t, it.Valid())

			require.NoError(t, it.Close())
			return nil
		})
		require.NoError(t, err)
	}

	// An iterator over nothing.
	err := db.View(func(tx kv.Tx) error {
		it, err := tx.Iterator([]byte("x"), nil)
		require.NoError(t, err)
		defer it.Close()

		require.False(t, it.Valid())
		it.Next()
		require.False(t, it.Valid())
		return nil
	})
	require.NoError(t, err)
}

func testIteratorOwnWrites(t *testing.T, db kv.DB) {
	set(t, db, "a", "b", "c")

	err := db.Update(func(tx kv.Tx) error {
		require.NoError(t, tx.Delete([]byte("a")))
		require.NoError(t, tx.Set([]byte("b"), []byte("changed")))
		require.NoError(t, tx.Set([]byte("bb"), []byte("bb")))
		require.NoError(t, tx.Set([]byte("d"), []byte("d")))

		it, err := tx.Iterator(nil, nil)
		require.NoError(t, err)
		defer it.Close()

		require.True(t, it.Valid())
		require.Equal(t, "b", string(it.Key()))
		value, err := it.Value()
		require.NoError(t, err)
		require.Equal(t, []byte("changed"), value)
		require.Equal(t, []string{"b", "bb", "c", "d"}, drain(it))

		rit, err := tx.ReverseIterator([]byte("b"), []byte("d"))
		require.NoError(t, err)
		defer rit.Close()
		require.Equal(t, []string{"c", "bb", "b"}, drain(rit))
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"b", "bb", "c", "d"}, scan(t, db, "", "", false))
}

func testIteratorValues(t *testing.T, db kv.DB) {
	err := db.Update(func(tx kv.Tx) error {
		require.NoError(t, tx.Set([]byte("a"), []byte("1")))
		require.NoError(t, tx.Set([]byte("b"), nil))
		return tx.Set([]byte("c"), bytes.Repeat([]byte("3"), 4096))
	})
	require.NoError(t, err)

	err = db.View(func(tx kv.Tx) error {
		it, err := tx.Iterator(nil, nil)
		require.NoError(t, err)
		defer it.Close()

		var values [][]byte
		for ; it.Valid(); it.Next() {
			value, err := it.Value()
			require.NoError(t, err)
			// The value is only valid until the iterator moves.
			values = append(values, bytes.Clone(value))
		}
		require.Len(t, values, 3)
		require.Equal(t, []byte("1"), values[0])
		require.Empty(t, values[1])
		require.Equal(t, bytes.Repeat([]byte("3"), 4096), values[2])
		return nil
	})
	require.NoError(t, err)
}

func testIteratorOrder(t *testing.T, db kv.DB) {
	rng := rand.New(rand.NewPCG(1, 2))
	seen := map[string]bool{}
	var keys []string
	for len(keys) < 300 {
		key := make([]byte, 1+rng.IntN(4))
		for i := range key {
			// A small alphabet that has the lowest and the highest byte in it.
			key[i] = []byte{0x00, 0x01, 0x7f, 0xfe, 0xff}[rng.IntN(5)]
		}
		if !seen[string(key)] {
			seen[string(key)] = true
			keys = append(keys, string(key))
		}
	}
	set(t, db, keys...)
	slices.Sort(keys)

	require.Equal(t, keys, scan(t, db, "", "", false))

	// Random ranges against the sorted keys.
	for range 50 {
		start, end := keys[rng.IntN(len(keys))], keys[rng.IntN(len(keys))]
		want := []string{}
		for _, key := range keys {
			if key >= start && key < end {
				want = append(want, key)
			}
		}
		require.Equal(t, want, scan(t, db, start, end, false), "forward [%q, %q)", start, end)

		slices.Reverse(want)
		require.Equal(t, want, scan(t, db, start, end, true), "reverse [%q, %q)", start, end)
	}

	slices.Reverse(keys)
	require.Equal(t, keys, scan(t, db, "", "", true))
}

// testCapabilities checks that an optional capability either works or is reported as unsupported.
func testCapabilities(t *testing.T, db kv.DB) {
	t.Run("TTL", func(t *testing.T) {
		var supported bool
		err := db.Update(func(tx kv.Tx) error {
			err := kv.SetWithTTL(tx, []byte("ttl"), []byte("1"), time.Hour)
			if errors.Is(err, kv.ErrUnsupported) {
				return nil
			}
			supported = true
			return err
		})
		require.NoError(t, err)

		if supported {
			require.Equal(t, []string{"ttl"}, scan(t, db, "ttl", "ttl\xff", false))
		}

		err = db.View(func(tx kv.Tx) error {
			err := kv.SetWithTTL(tx, []byte("ttl"), []byte("1"), time.Hour)
			if supported {
				require.ErrorIs(t, err, kv.ErrReadOnly)
			} else {
				require.ErrorIs(t, err, kv.ErrUnsupported)
			}
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("BulkWriter", func(t *testing.T) {
		w, err := kv.NewBulkWriter(db)
		if errors.Is(err, kv.ErrUnsupported) {
			return
		}
		require.NoError(t, err)
		defer w.Cancel()

		set(t, db, "bulk1")
		require.NoError(t, w.Set([]byte("bulk2"), []byte("2")))
		require.NoError(t, w.Delete([]byte("bulk1")))
		require.NoError(t, w.Flush())
		require.Equal(t, []string{"bulk2"}, scan(t, db, "bulk", "bulk\xff", false))
	})

	t.Run("DropRange", func(t *testing.T) {
		set(t, db, "drop/a", "drop/b", "drop/c", "dropped", "dropz")

		err := kv.DropRange(db, []byte("drop/a"), []byte("drop/c"))
		if errors.Is(err, kv.ErrUnsupported) {
			return
		}
		require.NoError(t, err)
		require.Equal(t, []string{"drop/c", "dropped", "dropz"}, scan(t, db, "drop", "drop\xff", false))

		// The range of a prefix.
		prefix := []byte("drop/")
		require.NoError(t, kv.DropRange(db, prefix, kv.PrefixEnd(prefix)))
		require.Equal(t, []string{"dropped", "dropz"}, scan(t, db, "drop", "drop\xff", false))
	})
}
