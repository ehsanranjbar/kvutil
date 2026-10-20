package typed_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/ehsanranjbar/kvutil/kv/prefix"
	"github.com/ehsanranjbar/kvutil/migrate"
	"github.com/ehsanranjbar/kvutil/store/typed"
	"github.com/stretchr/testify/require"
)

type pet struct {
	Name string
	Age  int
}

// tagged encodes itself.
type tagged struct {
	Tag string
}

func (v tagged) MarshalBinary() ([]byte, error) {
	return []byte("tag:" + v.Tag), nil
}

func (v *tagged) UnmarshalBinary(data []byte) error {
	if len(data) < 4 {
		return errors.New("not a tag")
	}
	v.Tag = string(data[4:])
	return nil
}

var (
	_ kvutil.Instantiator[*typed.Instance[int64, pet]] = (*typed.Store[int64, pet])(nil)
	_ iterutil.Iterator[int64, *pet]                   = (*typed.Iterator[int64, pet])(nil)
)

// scan collects the keys of a range of a store.
func scan[K, V any](t *testing.T, ins *typed.Instance[K, V], r kvutil.Range[K], reverse bool) []K {
	t.Helper()

	iterate := ins.Iterator
	if reverse {
		iterate = ins.ReverseIterator
	}
	it, err := iterate(r)
	require.NoError(t, err)
	defer it.Close()

	result := []K{}
	for ; it.Valid(); it.Next() {
		key, err := it.Key()
		require.NoError(t, err)
		result = append(result, key)
	}
	return result
}

type point struct{ X, Y int }

// pointKey is a key codec that a store cannot derive by itself.
type pointKey struct {
	base codec.KeyCodec[codec.Pair[int, int]]
}

func (c pointKey) Name() string { return "point" }

func (c pointKey) Encode(p point) ([]byte, error) {
	return c.base.Encode(codec.NewPair(p.X, p.Y))
}

func (c pointKey) Decode(bz []byte) (point, error) {
	pair, err := c.base.Decode(bz)
	return point{X: pair.First, Y: pair.Second}, err
}

func (c pointKey) Append(dst []byte, p point) ([]byte, error) {
	return c.base.Append(dst, codec.NewPair(p.X, p.Y))
}

func (c pointKey) Read(bz []byte) (point, int, error) {
	pair, n, err := c.base.Read(bz)
	return point{X: pair.First, Y: pair.Second}, n, err
}

func TestStore(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		store := typed.New[int64, pet]("pets")
		require.NoError(t, store.Check(db))
		require.Equal(t, "int64", store.KeyCodec().Name())
		require.Equal(t, "msgpack", store.ValueCodec().Name())
		require.Equal(t, []byte("pets"), store.Prefix())
		require.Equal(t, []byte("petsd"), store.DataPrefix())
		require.Equal(t, "typed", store.Unit().Kind())
		require.Equal(t, "pets", store.Unit().Name())

		err := db.Update(func(tx kv.Tx) error {
			ins := store.Instantiate(tx)
			require.True(t, ins.Writable())

			_, err := ins.Get(1)
			require.ErrorIs(t, err, kv.ErrNotFound)
			ok, err := ins.Has(1)
			require.NoError(t, err)
			require.False(t, ok)

			require.NoError(t, ins.Set(1, &pet{Name: "rex", Age: 3}))
			require.NoError(t, ins.Set(2, &pet{Name: "tom"}))
			require.NoError(t, ins.Set(-1, &pet{Name: "old"}))
			require.ErrorIs(t, ins.Set(3, nil), typed.ErrNilValue)

			got, err := ins.Get(1)
			require.NoError(t, err)
			require.Equal(t, &pet{Name: "rex", Age: 3}, got)
			ok, err = ins.Has(1)
			require.NoError(t, err)
			require.True(t, ok)

			require.NoError(t, ins.Delete(2))
			require.NoError(t, ins.Delete(2))
			_, err = ins.Get(2)
			require.ErrorIs(t, err, kv.ErrNotFound)
			return nil
		})
		require.NoError(t, err)

		err = db.View(func(tx kv.Tx) error {
			ins := store.Instantiate(tx)
			require.False(t, ins.Writable())
			require.ErrorIs(t, ins.Set(4, &pet{}), kv.ErrReadOnly)
			require.ErrorIs(t, ins.Delete(1), kv.ErrReadOnly)

			require.Equal(t, []int64{-1, 1}, scan(t, ins, kvutil.All[int64](), false))

			// The store keeps its keys under its own prefix: the record of the store, and the records.
			raw, err := tx.Iterator(nil, nil)
			require.NoError(t, err)
			defer raw.Close()

			var keys []string
			for ; raw.Valid(); raw.Next() {
				keys = append(keys, string(raw.Key()[:5]))
			}
			require.Equal(t, []string{"petsd", "petsd", "petsm"}, keys)
			return nil
		})
		require.NoError(t, err)
	})
}

func TestIterator(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		store := typed.New[int64, pet]("pets")

		err := db.Update(func(tx kv.Tx) error {
			ins := store.Instantiate(tx)
			for _, id := range []int64{-2, -1, 0, 1, 2, 3} {
				require.NoError(t, ins.Set(id, &pet{Age: int(id)}))
			}
			return nil
		})
		require.NoError(t, err)

		tests := []struct {
			name string
			r    kvutil.Range[int64]
			want []int64
		}{
			{"All", kvutil.All[int64](), []int64{-2, -1, 0, 1, 2, 3}},
			{"Zero", kvutil.Range[int64]{}, []int64{-2, -1, 0, 1, 2, 3}},
			{"Inclusive", kvutil.Between[int64](-1, 2), []int64{-1, 0, 1, 2}},
			{"Exclusive", kvutil.After[int64](-1).Until(2), []int64{0, 1}},
			{"LowOnly", kvutil.From[int64](1), []int64{1, 2, 3}},
			{"LowOnlyExclusive", kvutil.After[int64](1), []int64{2, 3}},
			{"HighOnly", kvutil.Through[int64](-1), []int64{-2, -1}},
			{"HighOnlyExclusive", kvutil.Until[int64](-1), []int64{-2}},
			{"OneKey", kvutil.Only[int64](1), []int64{1}},
			{"MissingBounds", kvutil.Between[int64](-10, 10), []int64{-2, -1, 0, 1, 2, 3}},
			{"Empty", kvutil.After[int64](1).Until(1), []int64{}},
			{"Inverted", kvutil.Between[int64](2, 1), []int64{}},
		}

		err = db.View(func(tx kv.Tx) error {
			ins := store.Instantiate(tx)
			for _, test := range tests {
				require.Equal(t, test.want, scan(t, ins, test.r, false), test.name)

				reversed := []int64{}
				for i := len(test.want) - 1; i >= 0; i-- {
					reversed = append(reversed, test.want[i])
				}
				require.Equal(t, reversed, scan(t, ins, test.r, true), test.name+" reversed")
			}

			it, err := ins.All()
			require.NoError(t, err)
			defer it.Close()

			require.NoError(t, it.Seek(1))
			require.True(t, it.Valid())
			value, err := it.Value()
			require.NoError(t, err)
			require.Equal(t, &pet{Age: 1}, value)

			// The record as it is stored.
			require.Equal(t, []byte{0x80, 0, 0, 0, 0, 0, 0, 1}, it.RawKey())
			raw, err := it.RawValue()
			require.NoError(t, err)
			decoded, err := store.ValueCodec().Decode(raw)
			require.NoError(t, err)
			require.Equal(t, pet{Age: 1}, decoded)
			return nil
		})
		require.NoError(t, err)
	})
}

func TestEmptyStringKey(t *testing.T) {
	db := memkv.New()
	store := typed.New[string, pet]("pets")

	err := db.Update(func(tx kv.Tx) error {
		ins := store.Instantiate(tx)
		for _, key := range []string{"", "a", "b"} {
			require.NoError(t, ins.Set(key, &pet{Name: key}))
		}

		require.Equal(t, []string{"", "a", "b"}, scan(t, ins, kvutil.All[string](), false))
		require.Equal(t, []string{""}, scan(t, ins, kvutil.Through(""), false))
		// Nothing is before the empty key.
		require.Equal(t, []string{}, scan(t, ins, kvutil.Until(""), false))
		require.Equal(t, []string{}, scan(t, ins, kvutil.Until(""), true))
		require.Equal(t, []string{"a", "b"}, scan(t, ins, kvutil.After(""), false))
		return nil
	})
	require.NoError(t, err)
}

func TestCodecs(t *testing.T) {
	db := memkv.New()
	stored := func(t *testing.T, dataPrefix []byte) (key, value []byte) {
		err := db.View(func(tx kv.Tx) error {
			it, err := prefix.New(tx, dataPrefix).Iterator(nil, nil)
			require.NoError(t, err)
			defer it.Close()

			require.True(t, it.Valid())
			key = append([]byte(nil), it.Key()...)
			v, err := it.Value()
			require.NoError(t, err)
			value = append([]byte(nil), v...)
			return nil
		})
		require.NoError(t, err)
		return key, value
	}
	set := func(t *testing.T, write func(tx kv.Tx) error) {
		require.NoError(t, db.Update(write))
	}

	t.Run("DefaultFormat", func(t *testing.T) {
		store := typed.New[string, pet]("msgpack")
		set(t, func(tx kv.Tx) error { return store.Instantiate(tx).Set("rex", &pet{Name: "rex", Age: 3}) })

		key, value := stored(t, store.DataPrefix())
		require.Equal(t, []byte("rex"), key)
		var got pet
		require.NoError(t, codec.Msgpack.Unmarshal(value, &got))
		require.Equal(t, pet{Name: "rex", Age: 3}, got)
	})

	t.Run("SelfEncodingValue", func(t *testing.T) {
		store := typed.New[string, tagged]("tagged")
		require.Equal(t, "binary", store.ValueCodec().Name())
		set(t, func(tx kv.Tx) error { return store.Instantiate(tx).Set("a", &tagged{Tag: "x"}) })

		_, value := stored(t, store.DataPrefix())
		require.Equal(t, []byte("tag:x"), value)

		err := db.View(func(tx kv.Tx) error {
			got, err := store.Instantiate(tx).Get("a")
			require.NoError(t, err)
			require.Equal(t, &tagged{Tag: "x"}, got)
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("Explicit", func(t *testing.T) {
		store := typed.NewWithCodecs([]byte("explicit"),
			codec.StringKey[string](), codec.Formatted[tagged](codec.JSON))
		set(t, func(tx kv.Tx) error { return store.Instantiate(tx).Set("a", &tagged{Tag: "x"}) })

		_, value := stored(t, store.DataPrefix())
		require.JSONEq(t, `{"Tag":"x"}`, string(value))
	})

	t.Run("ExplicitValueOnly", func(t *testing.T) {
		store := typed.NewWithCodecs[int64, tagged]("value",
			nil, codec.Formatted[tagged](codec.JSON))
		require.Equal(t, "int64", store.KeyCodec().Name())
		require.Equal(t, "json", store.ValueCodec().Name())
	})

	t.Run("UndecodableValue", func(t *testing.T) {
		store := typed.NewWithCodecs[string, pet]("broken", nil, codec.Formatted[pet](codec.JSON))
		set(t, func(tx kv.Tx) error {
			require.NoError(t, prefix.New(tx, store.DataPrefix()).Set([]byte("a"), []byte("not json")))

			_, err := store.Instantiate(tx).Get("a")
			require.Error(t, err)
			require.NotErrorIs(t, err, kv.ErrNotFound)
			return nil
		})
	})
}

func TestMissingKeyCodec(t *testing.T) {
	// A key type that has no codec is a mistake in the definition, which panics right there.
	require.PanicsWithValue(t,
		`store "points": no key codec for type typed_test.point; define the store with NewWithCodecs and give it one`,
		func() { typed.New[point, pet]("points") })

	// A key codec that is given makes the definition complete.
	db := memkv.New()
	store := typed.NewWithCodecs[point, pet]("points",
		pointKey{base: codec.PairKey(codec.IntKey[int](), codec.IntKey[int]())}, nil)
	require.NoError(t, store.Check(db))
	require.Equal(t, "point", store.KeyCodec().Name())
	require.Equal(t, "msgpack", store.ValueCodec().Name())

	err := db.Update(func(tx kv.Tx) error {
		ins := store.Instantiate(tx)
		for _, p := range []point{{1, 2}, {1, -1}, {-1, 5}} {
			require.NoError(t, ins.Set(p, &pet{}))
		}
		require.Equal(t, []point{{-1, 5}, {1, -1}, {1, 2}}, scan(t, ins, kvutil.All[point](), false))
		return nil
	})
	require.NoError(t, err)
}

func TestCheckDetectsCodecChange(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		require.NoError(t, typed.New[int64, pet]("pets").Check(db))
		require.NoError(t, typed.New[int64, pet]("pets").Check(db))

		otherFormat := typed.NewWithCodecs[int64, pet]("pets", nil, codec.Formatted[pet](codec.JSON))
		require.ErrorIs(t, otherFormat.Check(db), migrate.ErrChanged)

		require.ErrorIs(t, typed.New[int32, pet]("pets").Check(db), migrate.ErrChanged)

		// A type that is renamed is the same store.
		type id int64
		require.NoError(t, typed.New[id, pet]("pets").Check(db))

		// Another codec with another version, and the migration to it, is the store from then on.
		next := otherFormat.WithVersion(2)
		migrate.Custom(next, 1, func(context.Context, *migrate.Migrator) error { return nil })
		report, err := next.Migrate(context.Background(), db)
		require.NoError(t, err)
		require.Equal(t, `migrated typed "pets" from version 1 to 2`, report.String())
		require.NoError(t, typed.NewWithCodecs[int64, pet]("pets", nil, codec.Formatted[pet](codec.JSON)).
			WithVersion(2).Check(db))
	})
}

func TestPrefix(t *testing.T) {
	// A prefix is a string or bytes, and the store keeps a copy of it.
	raw := []byte("raw")
	fromBytes := typed.New[int64, pet](raw)
	raw[0] = 'x'
	require.Equal(t, []byte("raw"), fromBytes.Prefix())
	require.Equal(t, []byte("rawd"), fromBytes.DataPrefix())

	type name string
	require.Equal(t, []byte("named"), typed.New[int64, pet](name("named")).Prefix())

	// Whatever is not text is named by its bytes.
	odd := typed.New[int64, pet]([]byte{0x01})
	require.Equal(t, "0x01", odd.Unit().Name())
	require.Equal(t, migrate.MetaKey([]byte{0x01}), []byte{0x01, 'm'})

	require.Panics(t, func() { typed.New[int64, pet]("") })
	require.Panics(t, func() { typed.New[int64, pet]([]byte(nil)) })
}

func TestTTL(t *testing.T) {
	store := typed.New[string, pet]("pets")

	t.Run("Unsupported", func(t *testing.T) {
		err := testkv.NewMem(t).Update(func(tx kv.Tx) error {
			return store.Instantiate(tx).SetWithTTL("a", &pet{}, time.Hour)
		})
		require.ErrorIs(t, err, kv.ErrUnsupported)
	})

	t.Run("Badger", func(t *testing.T) {
		db := testkv.NewBadger(t)
		err := db.Update(func(tx kv.Tx) error {
			ins := store.Instantiate(tx)
			require.ErrorIs(t, ins.SetWithTTL("nil", nil, time.Hour), typed.ErrNilValue)
			require.NoError(t, ins.Set("keep", &pet{}))
			return ins.SetWithTTL("a", &pet{Name: "a"}, time.Second)
		})
		require.NoError(t, err)

		has := func() bool {
			var ok bool
			err := db.View(func(tx kv.Tx) error {
				var err error
				ok, err = store.Instantiate(tx).Has("a")
				return err
			})
			require.NoError(t, err)
			return ok
		}
		require.True(t, has())
		require.Eventually(t, func() bool { return !has() }, 5*time.Second, 100*time.Millisecond)

		err = db.View(func(tx kv.Tx) error {
			require.Equal(t, []string{"keep"}, scan(t, store.Instantiate(tx), kvutil.All[string](), false))
			return nil
		})
		require.NoError(t, err)
	})
}
