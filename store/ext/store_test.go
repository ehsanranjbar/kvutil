package ext_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/catalog"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	sstore "github.com/ehsanranjbar/kvutil/store/serialized"
	"github.com/stretchr/testify/require"
)

type pet struct {
	Name string
}

var (
	_ kvutil.Instantiator[*extstore.Instance[int64, pet]] = (*extstore.Store[int64, pet])(nil)
	_ extstore.DeleteHooker[int64]                        = (*extstore.Store[int64, pet])(nil)
)

// recorder is an extension that records what it is told, and that keeps the names of the
// records in a namespace of its own.
type recorder struct {
	name   string
	events *[]string
	ns     *catalog.Namespace
	fail   error
}

func (r *recorder) UseNamespace(ns *catalog.Namespace) {
	r.ns = ns
}

func (r *recorder) Instantiate(tx kv.Tx) extstore.ExtensionInstance[int64, pet] {
	return &recorderInstance{recorder: r, tx: tx}
}

type recorderInstance struct {
	*recorder
	tx kv.Tx
}

func name(p *pet) string {
	if p == nil {
		return "nil"
	}
	return p.Name
}

func (r *recorderInstance) OnSet(key int64, rawKey []byte, old, new *pet, opts ...any) error {
	*r.events = append(*r.events, fmt.Sprintf("%s set %d %x %s->%s %v", r.name, key, rawKey, name(old), name(new), opts))
	if r.fail != nil {
		return r.fail
	}
	return r.ns.Open(r.tx).Set(rawKey, []byte(new.Name))
}

func (r *recorderInstance) OnDelete(key int64, rawKey []byte, old *pet) error {
	*r.events = append(*r.events, fmt.Sprintf("%s delete %d %x %s", r.name, key, rawKey, name(old)))
	if r.fail != nil {
		return r.fail
	}
	return r.ns.Open(r.tx).Delete(rawKey)
}

func TestStoreWithoutExtensions(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		schema := catalog.NewSchema()
		store := extstore.New[int64, pet](schema.Namespace("pets"))
		require.NoError(t, schema.Check(db))

		err := db.Update(func(tx kv.Tx) error {
			ins := store.Instantiate(tx)
			require.True(t, ins.Writable())
			require.NoError(t, ins.Set(1, &pet{Name: "rex"}))
			require.NoError(t, ins.Set(2, &pet{Name: "tom"}))
			require.ErrorIs(t, ins.Set(3, nil), sstore.ErrNilValue)
			require.NoError(t, ins.Delete(2))
			require.NoError(t, ins.Delete(2))

			got, err := ins.Get(1)
			require.NoError(t, err)
			require.Equal(t, &pet{Name: "rex"}, got)
			ok, err := ins.Has(2)
			require.NoError(t, err)
			require.False(t, ok)

			it, err := ins.Iterator(kvutil.All[int64]())
			require.NoError(t, err)
			defer it.Close()
			require.True(t, it.Valid())
			key, err := it.Key()
			require.NoError(t, err)
			require.Equal(t, int64(1), key)
			it.Next()
			require.False(t, it.Valid())

			rit, err := ins.ReverseIterator(kvutil.All[int64]())
			require.NoError(t, err)
			defer rit.Close()
			require.True(t, rit.Valid())
			return nil
		})
		require.NoError(t, err)

		// The records are under the data part of the namespace of the store.
		err = db.View(func(tx kv.Tx) error {
			value, err := store.Data().Namespace().Open(tx).Get([]byte{0x80, 0, 0, 0, 0, 0, 0, 1})
			require.NoError(t, err)
			decoded, err := store.ValueCodec().Decode(value)
			require.NoError(t, err)
			require.Equal(t, pet{Name: "rex"}, decoded)
			require.Equal(t, "pets/data", store.Data().Namespace().Path())
			require.Equal(t, "pets", store.Namespace().Path())
			return nil
		})
		require.NoError(t, err)
	})
}

func TestExtensions(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		var events []string
		first := &recorder{name: "first", events: &events}
		second := &recorder{name: "second", events: &events}

		schema := catalog.NewSchema()
		store := extstore.New[int64, pet](schema.Namespace("pets")).
			WithExtension("first", first).
			WithExtension("second", second)
		require.NoError(t, schema.Check(db))
		require.Same(t, first, store.Extension("first"))
		require.Nil(t, store.Extension("missing"))
		require.Equal(t, "pets/ext/first", first.ns.Path())

		err := db.Update(func(tx kv.Tx) error {
			ins := store.Instantiate(tx)
			require.NotNil(t, ins.Extension("first"))
			require.Nil(t, ins.Extension("missing"))

			require.NoError(t, ins.Set(1, &pet{Name: "rex"}))
			require.NoError(t, ins.SetWithOptions(1, &pet{Name: "max"},
				"for all",
				extstore.WithExtOption("second", "for second"),
				extstore.WithExtOption("third", "for nobody"),
			))
			// Deleting a record that does not exist is not reported.
			require.NoError(t, ins.Delete(2))
			require.NoError(t, ins.Delete(1))
			require.NoError(t, ins.Delete(1))
			return nil
		})
		require.NoError(t, err)

		require.Equal(t, []string{
			"first set 1 8000000000000001 nil->rex []",
			"second set 1 8000000000000001 nil->rex []",
			"first set 1 8000000000000001 rex->max [for all]",
			"second set 1 8000000000000001 rex->max [for all for second]",
			"first delete 1 8000000000000001 max",
			"second delete 1 8000000000000001 max",
		}, events)
	})
}

func TestExtensionNamespace(t *testing.T) {
	db := memkv.New()
	var events []string
	names := &recorder{name: "names", events: &events}
	store := extstore.New[int64, pet](catalog.NewSchema().Namespace("pets")).WithExtension("names", names)

	err := db.Update(func(tx kv.Tx) error {
		ins := store.Instantiate(tx)
		require.NoError(t, ins.Set(1, &pet{Name: "rex"}))
		require.NoError(t, ins.Set(2, &pet{Name: "tom"}))
		return ins.Delete(1)
	})
	require.NoError(t, err)

	err = db.View(func(tx kv.Tx) error {
		// What the extension wrote is in its own namespace, in the same transaction as the records.
		it, err := names.ns.Open(tx).Iterator(nil, nil)
		require.NoError(t, err)
		defer it.Close()

		require.True(t, it.Valid())
		require.Equal(t, []byte{0x80, 0, 0, 0, 0, 0, 0, 2}, it.Key())
		value, err := it.Value()
		require.NoError(t, err)
		require.Equal(t, []byte("tom"), value)
		it.Next()
		require.False(t, it.Valid())

		// The records and the data of the extensions are both in the range of the store.
		footprint := store.Namespace().Footprint()
		require.Len(t, footprint, 1)
		all, err := tx.Iterator(footprint[0].Start, footprint[0].End)
		require.NoError(t, err)
		defer all.Close()
		count := 0
		for ; all.Valid(); all.Next() {
			count++
		}
		require.Equal(t, 2, count)
		return nil
	})
	require.NoError(t, err)
}

func TestExtensionFailure(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		var events []string
		failure := errors.New("failure")
		ok := &recorder{name: "ok", events: &events}
		failing := &recorder{name: "failing", events: &events}
		store := extstore.New[int64, pet](catalog.NewSchema().Namespace("pets")).
			WithExtension("ok", ok).
			WithExtension("failing", failing)

		require.NoError(t, db.Update(func(tx kv.Tx) error {
			return store.Instantiate(tx).Set(1, &pet{Name: "rex"})
		}))

		// A failing extension stops the write, and what the others did is rolled back with it.
		failing.fail = failure
		err := db.Update(func(tx kv.Tx) error {
			return store.Instantiate(tx).Set(1, &pet{Name: "max"})
		})
		require.ErrorIs(t, err, failure)
		require.ErrorContains(t, err, `extension "failing"`)

		err = db.Update(func(tx kv.Tx) error {
			return store.Instantiate(tx).Delete(1)
		})
		require.ErrorIs(t, err, failure)

		err = db.View(func(tx kv.Tx) error {
			got, err := store.Instantiate(tx).Get(1)
			require.NoError(t, err)
			require.Equal(t, &pet{Name: "rex"}, got)

			name, err := ok.ns.Open(tx).Get([]byte{0x80, 0, 0, 0, 0, 0, 0, 1})
			require.NoError(t, err)
			require.Equal(t, []byte("rex"), name)
			return nil
		})
		require.NoError(t, err)
	})
}

func TestDeleteHooks(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		var events []string
		hook := func(name string) extstore.DeleteHook[int64] {
			return func(tx kv.Tx, key int64, rawKey []byte) error {
				require.True(t, tx.Writable())
				events = append(events, fmt.Sprintf("%s %d %x", name, key, rawKey))
				return nil
			}
		}

		// Hooks alone, which do not need the record to be read.
		store := extstore.New[int64, pet](catalog.NewSchema().Namespace("pets"))
		store.AddDeleteHook("a", hook("a"))
		store.AddDeleteHook("b", hook("b"))

		err := db.Update(func(tx kv.Tx) error {
			ins := store.Instantiate(tx)
			require.NoError(t, ins.Set(1, &pet{Name: "rex"}))
			require.NoError(t, ins.Delete(2))
			require.NoError(t, ins.Delete(1))
			require.NoError(t, ins.Delete(1))

			ok, err := ins.Has(1)
			require.NoError(t, err)
			require.False(t, ok)
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, []string{"a 1 8000000000000001", "b 1 8000000000000001"}, events)

		// With an extension, which is called first.
		events = nil
		store = extstore.New[int64, pet](catalog.NewSchema().Namespace("pets")).
			WithExtension("ext", &recorder{name: "ext", events: &events})
		store.AddDeleteHook("a", hook("a"))

		err = db.Update(func(tx kv.Tx) error {
			ins := store.Instantiate(tx)
			require.NoError(t, ins.Set(1, &pet{Name: "rex"}))
			return ins.Delete(1)
		})
		require.NoError(t, err)
		require.Equal(t, []string{
			"ext set 1 8000000000000001 nil->rex []",
			"ext delete 1 8000000000000001 rex",
			"a 1 8000000000000001",
		}, events)

		// A failing hook stops the delete.
		failure := errors.New("failure")
		store = extstore.New[int64, pet](catalog.NewSchema().Namespace("pets"))
		store.AddDeleteHook("failing", func(kv.Tx, int64, []byte) error { return failure })
		err = db.Update(func(tx kv.Tx) error {
			ins := store.Instantiate(tx)
			require.NoError(t, ins.Set(1, &pet{Name: "rex"}))
			require.ErrorIs(t, ins.Delete(1), failure)

			ok, err := ins.Has(1)
			require.NoError(t, err)
			require.True(t, ok)
			return nil
		})
		require.NoError(t, err)
	})
}

func TestReadOnly(t *testing.T) {
	db := memkv.New()
	var events []string
	store := extstore.New[int64, pet](catalog.NewSchema().Namespace("pets")).
		WithExtension("ext", &recorder{name: "ext", events: &events})

	require.NoError(t, db.Update(func(tx kv.Tx) error {
		return store.Instantiate(tx).Set(1, &pet{Name: "rex"})
	}))
	events = nil

	// A write in a read-only transaction fails before any extension is called.
	err := db.View(func(tx kv.Tx) error {
		ins := store.Instantiate(tx)
		require.False(t, ins.Writable())
		require.ErrorIs(t, ins.Set(1, &pet{Name: "max"}), kv.ErrReadOnly)
		require.ErrorIs(t, ins.Delete(1), kv.ErrReadOnly)

		got, err := ins.Get(1)
		require.NoError(t, err)
		require.Equal(t, &pet{Name: "rex"}, got)
		return nil
	})
	require.NoError(t, err)
	require.Empty(t, events)
}

func TestTTL(t *testing.T) {
	db := testkv.NewBadger(t)
	var events []string

	plain := extstore.New[int64, pet](catalog.NewSchema().Namespace("plain"))
	extended := extstore.New[int64, pet](catalog.NewSchema().Namespace("extended")).
		WithExtension("ext", &recorder{name: "ext", events: &events})
	hooked := extstore.New[int64, pet](catalog.NewSchema().Namespace("hooked"))
	hooked.AddDeleteHook("hook", func(kv.Tx, int64, []byte) error { return nil })

	err := db.Update(func(tx kv.Tx) error {
		require.NoError(t, plain.Instantiate(tx).SetWithTTL(1, &pet{Name: "rex"}, time.Hour))
		// Expiry would not be reported to the extensions and the hooks.
		require.ErrorIs(t, extended.Instantiate(tx).SetWithTTL(1, &pet{Name: "rex"}, time.Hour), extstore.ErrTTLWithExtensions)
		require.ErrorIs(t, hooked.Instantiate(tx).SetWithTTL(1, &pet{Name: "rex"}, time.Hour), extstore.ErrTTLWithExtensions)

		ok, err := plain.Instantiate(tx).Has(1)
		require.NoError(t, err)
		require.True(t, ok)
		return nil
	})
	require.NoError(t, err)
	require.Empty(t, events)
}

func TestDefinitionProblems(t *testing.T) {
	t.Run("ExtensionNameTwice", func(t *testing.T) {
		var events []string
		schema := catalog.NewSchema()
		first := &recorder{name: "first", events: &events}
		store := extstore.New[int64, pet](schema.Namespace("pets")).
			WithExtension("same", first).
			WithExtension("same", &recorder{name: "second", events: &events})

		require.Error(t, schema.Validate())
		// The first one stays.
		require.Same(t, first, store.Extension("same"))
	})

	t.Run("MissingKeyCodec", func(t *testing.T) {
		type point struct{ X, Y int }

		require.Panics(t, func() { extstore.New[point, pet](catalog.NewSchema().Namespace("points")) })
	})

	t.Run("Codecs", func(t *testing.T) {
		schema := catalog.NewSchema()
		store := extstore.NewWithCodecs(schema.Namespace("pets"),
			codec.StringKey[string](), codec.From[pet](codec.JSON))
		require.NoError(t, schema.Validate())
		require.Equal(t, "string", store.KeyCodec().Name())
		require.Equal(t, "json", store.ValueCodec().Name())
	})
}
