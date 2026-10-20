package ext_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/codec/lex"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/ehsanranjbar/kvutil/kv/prefix"
	"github.com/ehsanranjbar/kvutil/migrate"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/ehsanranjbar/kvutil/store/typed"
	"github.com/stretchr/testify/require"
)

type pet struct {
	Name string
}

var (
	_ kvutil.Instantiator[*extstore.Instance[int64, pet]] = (*extstore.Store[int64, pet])(nil)
	_ extstore.DeleteHooks[int64]                         = (*extstore.Store[int64, pet])(nil)
	_ extstore.Referable[int64]                           = (*extstore.Store[int64, pet])(nil)
	_ migrate.Migratable                                  = (*extstore.Store[int64, pet])(nil)
	_ migrate.Store[int64, pet]                           = (*extstore.Store[int64, pet])(nil)
)

// recorder is an extension that records what it is told, and that keeps the names of the
// records under a prefix of its own.
type recorder struct {
	name   string
	events *[]string
	prefix []byte
	fail   error
}

func (r *recorder) Keep(prefix []byte) migrate.Part {
	r.prefix = prefix
	return migrate.Part{Kind: "recorder", StartEmpty: true}
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
	return prefix.New(r.tx, r.prefix).Set(rawKey, []byte(new.Name))
}

func (r *recorderInstance) OnDelete(key int64, rawKey []byte, old *pet) error {
	*r.events = append(*r.events, fmt.Sprintf("%s delete %d %x %s", r.name, key, rawKey, name(old)))
	if r.fail != nil {
		return r.fail
	}
	return prefix.New(r.tx, r.prefix).Delete(rawKey)
}

// extPrefix returns the prefix of the data of an extension of a store.
func extPrefix(store string, ext string) []byte {
	return lex.AppendTerminated([]byte(store+"x"), []byte(ext))
}

func TestStoreWithoutExtensions(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		store := extstore.New[int64, pet]("pets")
		require.NoError(t, store.Check(db))

		err := db.Update(func(tx kv.Tx) error {
			ins := store.Instantiate(tx)
			require.True(t, ins.Writable())
			require.NoError(t, ins.Set(1, &pet{Name: "rex"}))
			require.NoError(t, ins.Set(2, &pet{Name: "tom"}))
			require.ErrorIs(t, ins.Set(3, nil), typed.ErrNilValue)
			require.NoError(t, ins.Delete(2))
			require.NoError(t, ins.Delete(2))

			got, err := ins.Get(1)
			require.NoError(t, err)
			require.Equal(t, &pet{Name: "rex"}, got)
			ok, err := ins.Has(2)
			require.NoError(t, err)
			require.False(t, ok)
			ok, err = store.Contains(tx, 1)
			require.NoError(t, err)
			require.True(t, ok)
			ok, err = store.Contains(tx, 2)
			require.NoError(t, err)
			require.False(t, ok)

			it, err := ins.All()
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

		// The records are under the data prefix of the store, which is the one of the typed store
		// at the same prefix, and the store is a unit of the kind "ext".
		require.Equal(t, []byte("pets"), store.Prefix())
		require.Equal(t, []byte("petsd"), store.DataPrefix())
		require.Equal(t, store.DataPrefix(), store.Data().DataPrefix())
		require.Same(t, store.Data().Unit(), store.Unit())
		require.Equal(t, "ext", store.Unit().Kind())
		require.Equal(t, "pets", store.Unit().Name())
		require.Empty(t, store.Extensions())

		err = db.View(func(tx kv.Tx) error {
			value, err := prefix.New(tx, store.DataPrefix()).Get([]byte{0x80, 0, 0, 0, 0, 0, 0, 1})
			require.NoError(t, err)
			decoded, err := store.ValueCodec().Decode(value)
			require.NoError(t, err)
			require.Equal(t, pet{Name: "rex"}, decoded)
			return nil
		})
		require.NoError(t, err)

		// A typed store at the same prefix is the same unit, but another kind of it.
		require.ErrorIs(t, typed.New[int64, pet]("pets").Check(db), migrate.ErrChanged)
	})
}

func TestExtensions(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		var events []string
		first := &recorder{name: "first", events: &events}
		second := &recorder{name: "second", events: &events}

		store := extstore.New[int64, pet]("pets").
			WithExtension("first", first).
			WithExtension("second", second)
		require.NoError(t, store.Check(db))
		require.Same(t, first, store.Extension("first"))
		require.Nil(t, store.Extension("missing"))
		require.Equal(t, []any{first, second}, store.Extensions())
		require.Equal(t, extPrefix("pets", "first"), first.prefix)
		require.Equal(t, extPrefix("pets", "second"), second.prefix)

		// What the extensions keep is what the unit of the store has as its parts.
		parts := store.Unit().Parts()
		require.Len(t, parts, 2)
		require.Equal(t, "first", parts[0].Name)
		require.Equal(t, "recorder", parts[0].Kind)
		require.Equal(t, first.prefix, parts[0].Prefix)
		require.Equal(t, "second", parts[1].Name)

		err := db.Update(func(tx kv.Tx) error {
			ins := store.Instantiate(tx)
			require.NotNil(t, ins.Extension("first"))
			require.Nil(t, ins.Extension("missing"))

			require.NoError(t, ins.Set(1, &pet{Name: "rex"}))
			require.NoError(t, ins.SetWithOptions(1, &pet{Name: "max"},
				"for all",
				extstore.For("second", "for second"),
				extstore.For("third", "for nobody"),
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

func TestExtensionPrefix(t *testing.T) {
	db := memkv.New()
	var events []string
	names := &recorder{name: "names", events: &events}
	store := extstore.New[int64, pet]("pets").WithExtension("names", names)

	err := db.Update(func(tx kv.Tx) error {
		ins := store.Instantiate(tx)
		require.NoError(t, ins.Set(1, &pet{Name: "rex"}))
		require.NoError(t, ins.Set(2, &pet{Name: "tom"}))
		return ins.Delete(1)
	})
	require.NoError(t, err)

	err = db.View(func(tx kv.Tx) error {
		// What the extension wrote is under its own prefix, in the same transaction as the records.
		it, err := prefix.New(tx, names.prefix).Iterator(nil, nil)
		require.NoError(t, err)
		defer it.Close()

		require.True(t, it.Valid())
		require.Equal(t, []byte{0x80, 0, 0, 0, 0, 0, 0, 2}, it.Key())
		value, err := it.Value()
		require.NoError(t, err)
		require.Equal(t, []byte("tom"), value)
		it.Next()
		require.False(t, it.Valid())

		// The records and the data of the extensions are both under the prefix of the store.
		all, err := tx.Iterator([]byte("pets"), kv.PrefixEnd([]byte("pets")))
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
		store := extstore.New[int64, pet]("pets").
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

			name, err := prefix.New(tx, ok.prefix).Get([]byte{0x80, 0, 0, 0, 0, 0, 0, 1})
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
		store := extstore.New[int64, pet]("pets")
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

		// A name that is taken is a mistake of the definition.
		require.Panics(t, func() { store.AddDeleteHook("a", hook("again")) })

		// With an extension, which is called first.
		events = nil
		store = extstore.New[int64, pet]("pets").
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
		store = extstore.New[int64, pet]("pets")
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
	store := extstore.New[int64, pet]("pets").
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

	plain := extstore.New[int64, pet]("plain")
	extended := extstore.New[int64, pet]("extended").
		WithExtension("ext", &recorder{name: "ext", events: &events})
	hooked := extstore.New[int64, pet]("hooked")
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
		first := &recorder{name: "first", events: &events}
		store := extstore.New[int64, pet]("pets").WithExtension("same", first)

		require.Panics(t, func() {
			store.AddExtension("same", &recorder{name: "second", events: &events})
		})
		// The first one stays.
		require.Same(t, first, store.Extension("same"))
		require.Len(t, store.Unit().Parts(), 1)
	})

	t.Run("ExtensionWithoutAName", func(t *testing.T) {
		var events []string
		store := extstore.New[int64, pet]("pets")
		require.Panics(t, func() {
			store.AddExtension("", &recorder{name: "nameless", events: &events})
		})
	})

	t.Run("MissingKeyCodec", func(t *testing.T) {
		type point struct{ X, Y int }

		require.Panics(t, func() { extstore.New[point, pet]("points") })
	})

	t.Run("EmptyPrefix", func(t *testing.T) {
		require.Panics(t, func() { extstore.New[int64, pet]("") })
	})

	t.Run("Codecs", func(t *testing.T) {
		store := extstore.NewWithCodecs[string, pet]("pets",
			codec.StringKey[string](), codec.Formatted[pet](codec.JSON))
		require.NoError(t, store.Check(memkv.New()))
		require.Equal(t, "string", store.KeyCodec().Name())
		require.Equal(t, "json", store.ValueCodec().Name())
	})
}

// TestOverlappingStores checks that stores whose prefixes overlap are not migrated together.
func TestOverlappingStores(t *testing.T) {
	db := memkv.New()
	pets := extstore.New[int64, pet]("pets")
	petsToo := extstore.New[int64, pet]("pets2")
	other := extstore.New[int64, pet]("owners")

	_, err := migrate.Run(context.Background(), db, pets, other, petsToo)
	require.ErrorContains(t, err, "overlaps")
	require.Error(t, migrate.Check(db, pets, petsToo))

	// A store that is given twice is one, and a store and the typed store under it are one unit.
	report, err := migrate.Run(context.Background(), db, pets, other, pets, pets.Data())
	require.NoError(t, err)
	require.Equal(t, "recorded ext \"pets\"\nrecorded ext \"owners\"", report.String())
}

// names is an extension that keeps the names of the pets under a prefix that it is given, and
// that says what it makes of the records of its store, as the test wants it to.
type names struct {
	recorder
	define func(names *names, part *migrate.Part)
	store  *extstore.Store[int64, pet]
}

func (n *names) Keep(prefix []byte) migrate.Part {
	n.prefix = prefix
	part := migrate.Part{Kind: "names"}
	if n.define != nil {
		n.define(n, &part)
	}
	return part
}

// rebuild writes the names of the pets that the store has.
func (n *names) rebuild(ctx context.Context, db kv.DB) error {
	return db.Update(func(tx kv.Tx) error {
		it, err := n.store.Records(tx, kvutil.All[int64]())
		if err != nil {
			return err
		}
		defer it.Close()

		for ; it.Valid(); it.Next() {
			key, err := it.Key()
			if err != nil {
				return err
			}
			p, err := it.Value()
			if err != nil {
				return err
			}
			rawKey, err := n.store.KeyCodec().Encode(key)
			if err != nil {
				return err
			}
			if err := prefix.New(tx, n.prefix).Set(rawKey, []byte(p.Name)); err != nil {
				return err
			}
		}
		return nil
	})
}

// TestExtensionOfAStoreWithRecords checks that an extension that keeps data, and that is added
// to a store that has records, must say what it makes of them.
func TestExtensionOfAStoreWithRecords(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		ctx := context.Background()
		var events []string
		program := func(define func(*names, *migrate.Part)) (*extstore.Store[int64, pet], *names) {
			store := extstore.New[int64, pet]("pets")
			if define == nil {
				return store, nil
			}
			n := &names{recorder: recorder{name: "names", events: &events}, define: define, store: store}
			store.AddExtension("names", n)
			return store, n
		}
		count := func(n *names) (count int) {
			require.NoError(t, db.View(func(tx kv.Tx) error {
				it, err := prefix.New(tx, n.prefix).Iterator(nil, nil)
				require.NoError(t, err)
				defer it.Close()
				for ; it.Valid(); it.Next() {
					count++
				}
				return nil
			}))
			return count
		}

		store, _ := program(nil)
		_, err := store.Migrate(ctx, db)
		require.NoError(t, err)
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			if err := store.Instantiate(tx).Set(1, &pet{Name: "rex"}); err != nil {
				return err
			}
			return store.Instantiate(tx).Set(2, &pet{Name: "tom"})
		}))

		// It says nothing.
		plain := func(*names, *migrate.Part) {}
		store, _ = program(plain)
		_, err = store.Migrate(ctx, db)
		require.ErrorIs(t, err, migrate.ErrCannotBuild)
		require.ErrorContains(t, err, `names "pets/names"`)
		require.ErrorIs(t, store.Check(db), migrate.ErrCannotBuild)

		// It is built from the records, like an index.
		store, n := program(func(n *names, part *migrate.Part) { part.Rebuild = n.rebuild })
		require.ErrorIs(t, store.Check(db), migrate.ErrNeedsMigration)
		report, err := store.Migrate(ctx, db)
		require.NoError(t, err)
		require.Equal(t, `built names "pets/names"`, report.String())
		require.Equal(t, 2, count(n))

		// Or it has nothing to know of the records that were there before it, in a database
		// that has not seen it.
		db2 := testkv.NewMem(t)
		store, _ = program(nil)
		_, err = store.Migrate(ctx, db2)
		require.NoError(t, err)
		require.NoError(t, db2.Update(func(tx kv.Tx) error {
			return store.Instantiate(tx).Set(1, &pet{Name: "rex"})
		}))
		store, n = program(func(_ *names, part *migrate.Part) { part.StartEmpty = true })
		report, err = store.Migrate(ctx, db2)
		require.NoError(t, err)
		require.Equal(t, `recorded names "pets/names"`, report.String())
		require.NoError(t, db2.Update(func(tx kv.Tx) error {
			return store.Instantiate(tx).Set(2, &pet{Name: "tom"})
		}))
		require.NoError(t, db2.View(func(tx kv.Tx) error {
			it, err := prefix.New(tx, n.prefix).Iterator(nil, nil)
			require.NoError(t, err)
			defer it.Close()
			require.True(t, it.Valid())
			value, err := it.Value()
			require.NoError(t, err)
			require.Equal(t, "tom", string(value))
			it.Next()
			require.False(t, it.Valid())
			return nil
		}))

		// With a store that is new, it needs to say nothing.
		store, _ = program(plain)
		require.NoError(t, store.Check(testkv.NewMem(t)))
	})
}

// TestRetireExtension checks that the data of an extension that cannot be built again, and that
// is not added to its store any more, is retired by the name of the extension.
func TestRetireExtension(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		ctx := context.Background()
		var events []string

		store := extstore.New[int64, pet]("pets")
		n := &names{recorder: recorder{name: "names", events: &events}}
		store.AddExtension("names", n)
		_, err := store.Migrate(ctx, db)
		require.NoError(t, err)
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			return store.Instantiate(tx).Set(1, &pet{Name: "rex"})
		}))

		// Without the extension, its data is not left without a word, which says what deletes it.
		without := func(retire bool) *extstore.Store[int64, pet] {
			store := extstore.New[int64, pet]("pets")
			if retire {
				store.RetireExtension("names")
			}
			return store
		}
		_, err = without(false).Migrate(ctx, db)
		require.ErrorIs(t, err, migrate.ErrUnknown)
		require.ErrorContains(t, err, `names "pets/names" is in the database and is not defined: retire it to delete it`)

		require.ErrorIs(t, without(true).Check(db), migrate.ErrNeedsMigration)
		report, err := without(true).Migrate(ctx, db)
		require.NoError(t, err)
		require.Equal(t, `retired names "pets/names"`, report.String())
		require.NoError(t, db.View(func(tx kv.Tx) error {
			it, err := prefix.New(tx, n.prefix).Iterator(nil, nil)
			require.NoError(t, err)
			defer it.Close()
			require.False(t, it.Valid())

			// The records are where they were.
			ok, err := store.Instantiate(tx).Has(1)
			require.NoError(t, err)
			require.True(t, ok)
			return nil
		}))
		require.NoError(t, without(false).Check(db))

		// An extension that is added cannot be retired.
		both := extstore.New[int64, pet]("pets")
		both.AddExtension("names", &names{recorder: recorder{name: "names", events: &events}})
		both.RetireExtension("names")
		require.ErrorContains(t, both.Check(db), "retired and defined")
	})
}
