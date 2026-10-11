package serialized_test

import (
	"errors"
	"testing"
	"time"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/catalog"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/expr"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	sstore "github.com/ehsanranjbar/kvutil/store/serialized"
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
	_ kvutil.Instantiator[*sstore.Instance[int64, pet]] = (*sstore.Store[int64, pet])(nil)
	_ kvutil.Iterator[int64, *pet]                      = (*sstore.Iterator[int64, pet])(nil)
)

// scan collects the keys of a range of a store.
func scan[K, V any](t *testing.T, ins *sstore.Instance[K, V], r expr.Range[K], reverse bool) []K {
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
		schema := catalog.NewSchema()
		store := sstore.New[int64, pet](schema.Namespace("pets"))
		require.NoError(t, schema.Check(db))
		require.Equal(t, "int64", store.KeyCodec().Name())
		require.Equal(t, "msgpack", store.ValueCodec().Name())

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
			require.ErrorIs(t, ins.Set(3, nil), sstore.ErrNilValue)

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

			require.Equal(t, []int64{-1, 1}, scan(t, ins, expr.Range[int64]{}, false))

			// The store keeps its keys in its own namespace.
			raw, err := schema.Namespace("other").Open(tx).Iterator(nil, nil)
			require.NoError(t, err)
			defer raw.Close()
			require.False(t, raw.Valid())
			return nil
		})
		require.NoError(t, err)
	})
}

func TestIterator(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		store := sstore.New[int64, pet](catalog.NewSchema().Namespace("pets"))

		err := db.Update(func(tx kv.Tx) error {
			ins := store.Instantiate(tx)
			for _, id := range []int64{-2, -1, 0, 1, 2, 3} {
				require.NoError(t, ins.Set(id, &pet{Age: int(id)}))
			}
			return nil
		})
		require.NoError(t, err)

		bound := func(v int64, exclusive bool) *expr.Bound[int64] { return expr.NewBound(v, exclusive) }
		tests := []struct {
			name string
			r    expr.Range[int64]
			want []int64
		}{
			{"All", expr.Range[int64]{}, []int64{-2, -1, 0, 1, 2, 3}},
			{"Inclusive", expr.NewRange(bound(-1, false), bound(2, false)), []int64{-1, 0, 1, 2}},
			{"Exclusive", expr.NewRange(bound(-1, true), bound(2, true)), []int64{0, 1}},
			{"LowOnly", expr.NewRange(bound(1, false), nil), []int64{1, 2, 3}},
			{"LowOnlyExclusive", expr.NewRange(bound(1, true), nil), []int64{2, 3}},
			{"HighOnly", expr.NewRange(nil, bound(-1, false)), []int64{-2, -1}},
			{"HighOnlyExclusive", expr.NewRange(nil, bound(-1, true)), []int64{-2}},
			{"OneKey", expr.NewRange(bound(1, false), bound(1, false)), []int64{1}},
			{"MissingBounds", expr.NewRange(bound(-10, false), bound(10, false)), []int64{-2, -1, 0, 1, 2, 3}},
			{"Empty", expr.NewRange(bound(1, true), bound(1, true)), []int64{}},
			{"Inverted", expr.NewRange(bound(2, false), bound(1, false)), []int64{}},
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

			it, err := ins.Iterator(expr.Range[int64]{})
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
	store := sstore.New[string, pet](catalog.NewSchema().Namespace("pets"))

	err := db.Update(func(tx kv.Tx) error {
		ins := store.Instantiate(tx)
		for _, key := range []string{"", "a", "b"} {
			require.NoError(t, ins.Set(key, &pet{Name: key}))
		}

		require.Equal(t, []string{"", "a", "b"}, scan(t, ins, expr.Range[string]{}, false))
		require.Equal(t, []string{""}, scan(t, ins, expr.NewRange(nil, expr.NewBound("", false)), false))
		// Nothing is before the empty key.
		require.Equal(t, []string{}, scan(t, ins, expr.NewRange(nil, expr.NewBound("", true)), false))
		require.Equal(t, []string{}, scan(t, ins, expr.NewRange(nil, expr.NewBound("", true)), true))
		require.Equal(t, []string{"a", "b"}, scan(t, ins, expr.NewRange(expr.NewBound("", true), nil), false))
		return nil
	})
	require.NoError(t, err)
}

func TestCodecs(t *testing.T) {
	db := memkv.New()
	stored := func(t *testing.T, ns *catalog.Namespace) (key, value []byte) {
		err := db.View(func(tx kv.Tx) error {
			it, err := ns.Open(tx).Iterator(nil, nil)
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

	t.Run("SchemaFormat", func(t *testing.T) {
		schema := catalog.NewSchema(catalog.WithFormat(codec.JSON))
		store := sstore.New[string, pet](schema.Namespace("json"))
		set(t, func(tx kv.Tx) error { return store.Instantiate(tx).Set("rex", &pet{Name: "rex", Age: 3}) })

		key, value := stored(t, store.Namespace())
		require.Equal(t, []byte("rex"), key)
		require.JSONEq(t, `{"Name":"rex","Age":3}`, string(value))
	})

	t.Run("SelfEncodingValue", func(t *testing.T) {
		schema := catalog.NewSchema(catalog.WithFormat(codec.JSON))
		store := sstore.New[string, tagged](schema.Namespace("tagged"))
		require.Equal(t, "binary", store.ValueCodec().Name())
		set(t, func(tx kv.Tx) error { return store.Instantiate(tx).Set("a", &tagged{Tag: "x"}) })

		_, value := stored(t, store.Namespace())
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
		store := sstore.New[string, tagged](catalog.NewSchema().Namespace("explicit")).
			WithKeyCodec(codec.StringKey[string]()).
			WithValueCodec(codec.From[tagged](codec.JSON))
		set(t, func(tx kv.Tx) error { return store.Instantiate(tx).Set("a", &tagged{Tag: "x"}) })

		_, value := stored(t, store.Namespace())
		require.JSONEq(t, `{"Tag":"x"}`, string(value))
	})

	t.Run("ConfiguredAfterUse", func(t *testing.T) {
		store := sstore.New[string, pet](catalog.NewSchema().Namespace("late"))
		set(t, func(tx kv.Tx) error { return store.Instantiate(tx).Set("a", &pet{}) })

		require.Panics(t, func() { store.WithKeyCodec(codec.StringKey[string]()) })
		require.Panics(t, func() { store.WithValueCodec(codec.From[pet](codec.JSON)) })
	})

	t.Run("UndecodableValue", func(t *testing.T) {
		store := sstore.New[string, pet](catalog.NewSchema(catalog.WithFormat(codec.JSON)).Namespace("broken"))
		set(t, func(tx kv.Tx) error {
			require.NoError(t, store.Namespace().Open(tx).Set([]byte("a"), []byte("not json")))

			_, err := store.Instantiate(tx).Get("a")
			require.Error(t, err)
			require.NotErrorIs(t, err, kv.ErrNotFound)
			return nil
		})
	})
}

func TestMissingKeyCodec(t *testing.T) {
	db := memkv.New()
	schema := catalog.NewSchema()
	store := sstore.New[point, pet](schema.Namespace("points"))

	// The problem is reported once by the schema, and nothing panics.
	require.ErrorContains(t, schema.Validate(), `store "points"`)
	require.Error(t, schema.Check(db))
	require.Error(t, store.Validate())
	require.Nil(t, store.KeyCodec())

	err := db.Update(func(tx kv.Tx) error {
		ins := store.Instantiate(tx)
		require.Error(t, ins.Set(point{}, &pet{}))
		_, err := ins.Get(point{})
		require.Error(t, err)
		_, err = ins.Has(point{})
		require.Error(t, err)
		require.Error(t, ins.Delete(point{}))
		_, err = ins.Iterator(expr.Range[point]{})
		require.Error(t, err)
		_, err = ins.ReverseIterator(expr.NewRange(expr.NewBound(point{}, false), nil))
		require.Error(t, err)
		return nil
	})
	require.NoError(t, err)

	// A key codec that is given makes the store complete.
	schema = catalog.NewSchema()
	store = sstore.New[point, pet](schema.Namespace("points")).
		WithKeyCodec(pointKey{base: codec.PairKey(codec.IntKey[int](), codec.IntKey[int]())})
	require.NoError(t, schema.Check(db))

	err = db.Update(func(tx kv.Tx) error {
		ins := store.Instantiate(tx)
		for _, p := range []point{{1, 2}, {1, -1}, {-1, 5}} {
			require.NoError(t, ins.Set(p, &pet{}))
		}
		require.Equal(t, []point{{-1, 5}, {1, -1}, {1, 2}}, scan(t, ins, expr.Range[point]{}, false))
		return nil
	})
	require.NoError(t, err)
}

func TestCheckDetectsCodecChange(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		schema := catalog.NewSchema()
		sstore.New[int64, pet](schema.Namespace("pets"))
		require.NoError(t, schema.Check(db))

		same := catalog.NewSchema()
		sstore.New[int64, pet](same.Namespace("pets"))
		require.NoError(t, same.Check(db))

		otherFormat := catalog.NewSchema(catalog.WithFormat(codec.JSON))
		sstore.New[int64, pet](otherFormat.Namespace("pets"))
		require.ErrorIs(t, otherFormat.Check(db), catalog.ErrChanged)

		otherKey := catalog.NewSchema()
		sstore.New[int32, pet](otherKey.Namespace("pets"))
		require.ErrorIs(t, otherKey.Check(db), catalog.ErrChanged)

		// A type that is renamed is the same store.
		type id int64
		renamed := catalog.NewSchema()
		sstore.New[id, pet](renamed.Namespace("pets"))
		require.NoError(t, renamed.Check(db))
	})
}

func TestTTL(t *testing.T) {
	store := sstore.New[string, pet](catalog.NewSchema().Namespace("pets"))

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
			require.ErrorIs(t, ins.SetWithTTL("nil", nil, time.Hour), sstore.ErrNilValue)
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
			require.Equal(t, []string{"keep"}, scan(t, store.Instantiate(tx), expr.Range[string]{}, false))
			return nil
		})
		require.NoError(t, err)
	})
}
