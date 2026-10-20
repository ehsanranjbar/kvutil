package record_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"uuid"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/badgerkv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/ehsanranjbar/kvutil/migrate"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/ehsanranjbar/kvutil/store/record"
	"github.com/ehsanranjbar/kvutil/store/typed"
	"github.com/stretchr/testify/require"
)

// pet is a record whose id is not serialized, as it is the key.
type pet struct {
	id   int64
	Name string
	Age  int
}

func (p *pet) GetId() int64 { return p.id }

func (p *pet) SetId(id int64) { p.id = id }

// sample is the data of the objects of the tests, which serializes itself.
type sample struct {
	A int    `json:"a,omitempty"`
	B string `json:"b,omitempty"`
}

func (s sample) MarshalBinary() ([]byte, error) { return json.Marshal(s) }

func (s *sample) UnmarshalBinary(data []byte) error { return json.Unmarshal(data, s) }

// card is a record type that does not have the methods of a record.
type card struct {
	Number int64 `json:"-"`
	Owner  string
}

var (
	_ kvutil.Instantiator[*record.Instance[int64, pet]] = (*record.Store[int64, pet])(nil)
	_ extstore.Extensible[int64, pet]                   = (*record.Store[int64, pet])(nil)
	_ extstore.DeleteHooks[int64]                       = (*record.Store[int64, pet])(nil)
	_ extstore.Referable[int64]                         = (*record.Store[int64, pet])(nil)
	_ extstore.Getter[int64, pet]                       = (*record.Instance[int64, pet])(nil)
	_ iterutil.Iterator[int64, *pet]                    = (*record.Iterator[int64, pet])(nil)
)

// must returns the value of a call that must not fail.
func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// idsOf returns the ids that the records of an iterator have, which is closed.
func idsOf(t *testing.T, it iterutil.Iterator[int64, *pet]) []int64 {
	t.Helper()
	defer it.Close()

	ids := []int64{}
	for ; it.Valid(); it.Next() {
		key, err := it.Key()
		require.NoError(t, err)
		p, err := it.Value()
		require.NoError(t, err)
		// The id of a record is its key, though it is not part of its value.
		require.Equal(t, key, p.GetId())
		ids = append(ids, p.GetId())
	}
	return ids
}

func TestStore(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		type object = record.Object[uuid.UUID, sample]

		noIDFunc := record.New[uuid.UUID, object]("without")
		store := record.New[uuid.UUID, object]("objects").
			WithIDFunc(func(*object) (uuid.UUID, error) { return uuid.New(), nil })
		require.NoError(t, migrate.Check(db, noIDFunc, store))

		err := db.Update(func(tx kv.Tx) error {
			ins := noIDFunc.Instantiate(tx)
			require.True(t, ins.Writable())
			err := ins.Set(record.NewObject[uuid.UUID](sample{B: "foo"}))
			require.ErrorIs(t, err, record.ErrNoID)

			ins = store.Instantiate(tx)
			var (
				e1 = record.NewObjectWithId(uuid.New(), sample{B: "foo"})
				e2 = record.NewObjectWithId(uuid.New(), sample{B: "bar"})
				e3 = record.NewObject[uuid.UUID](sample{B: "baz"})
			)

			// NotFound
			v, err := ins.Get(e1.Id)
			require.ErrorIs(t, err, kv.ErrNotFound)
			require.Nil(t, v)

			// Set
			require.NoError(t, ins.Set(e1))
			require.NoError(t, ins.Set(e2))
			require.NoError(t, ins.Set(e3))
			require.NotZero(t, e3.Id)

			// Get
			v, err = ins.Get(e1.Id)
			require.NoError(t, err)
			require.Equal(t, e1, v)
			v, err = ins.Get(e2.Id)
			require.NoError(t, err)
			require.Equal(t, e2, v)
			v, err = ins.Get(e3.Id)
			require.NoError(t, err)
			require.Equal(t, e3, v)

			// Iterator
			it, err := ins.All()
			require.NoError(t, err)
			ids, err := iterutil.Of(it).Keys()
			require.NoError(t, err)
			require.Len(t, ids, 3)

			// Delete
			require.NoError(t, ins.Delete(e1.Id))
			_, err = ins.Get(e1.Id)
			require.ErrorIs(t, err, kv.ErrNotFound)
			return nil
		})
		require.NoError(t, err)
	})
}

func TestSetGetHasDelete(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		store := record.New[int64, pet]("pets")
		require.NoError(t, store.Check(db))

		err := db.Update(func(tx kv.Tx) error {
			ins := store.Instantiate(tx)
			require.NoError(t, ins.Set(&pet{id: 1, Name: "rex", Age: 3}))
			require.NoError(t, ins.Set(&pet{id: 2, Name: "tom", Age: 5}))

			got, err := ins.Get(1)
			require.NoError(t, err)
			require.Equal(t, &pet{id: 1, Name: "rex", Age: 3}, got)

			ok, err := ins.Has(2)
			require.NoError(t, err)
			require.True(t, ok)
			ok, err = ins.Has(3)
			require.NoError(t, err)
			require.False(t, ok)

			// A record that is set again is replaced.
			require.NoError(t, ins.Set(&pet{id: 1, Name: "max", Age: 4}))
			got, err = ins.Get(1)
			require.NoError(t, err)
			require.Equal(t, &pet{id: 1, Name: "max", Age: 4}, got)

			require.ErrorIs(t, ins.Set(nil), typed.ErrNilValue)

			require.NoError(t, ins.Delete(2))
			// A record that does not exist is not an error to delete.
			require.NoError(t, ins.Delete(2))
			require.NoError(t, ins.Delete(99))
			ok, err = ins.Has(2)
			require.NoError(t, err)
			require.False(t, ok)
			_, err = ins.Get(2)
			require.True(t, errors.Is(err, kv.ErrNotFound))
			return nil
		})
		require.NoError(t, err)

		// The value does not have the id, which is the key.
		err = db.View(func(tx kv.Tx) error {
			ins := store.Instantiate(tx)
			require.False(t, ins.Writable())
			require.ErrorIs(t, ins.Set(&pet{id: 5}), kv.ErrReadOnly)
			require.ErrorIs(t, ins.Delete(1), kv.ErrReadOnly)

			raw, err := tx.Get(append([]byte("petsd"), 0x80, 0, 0, 0, 0, 0, 0, 1))
			require.NoError(t, err)
			decoded, err := store.ValueCodec().Decode(raw)
			require.NoError(t, err)
			require.Equal(t, pet{Name: "max", Age: 4}, decoded)
			return nil
		})
		require.NoError(t, err)
	})
}

func TestIterators(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		store := record.New[int64, pet]("pets")

		err := db.Update(func(tx kv.Tx) error {
			ins := store.Instantiate(tx)
			for id := int64(1); id <= 5; id++ {
				require.NoError(t, ins.Set(&pet{id: id, Name: fmt.Sprint("pet", id)}))
			}

			forward := func(r kvutil.Range[int64]) []int64 { return idsOf(t, must(ins.Iterator(r))) }
			reverse := func(r kvutil.Range[int64]) []int64 { return idsOf(t, must(ins.ReverseIterator(r))) }

			require.Equal(t, []int64{1, 2, 3, 4, 5}, forward(kvutil.All[int64]()))
			require.Equal(t, []int64{5, 4, 3, 2, 1}, reverse(kvutil.All[int64]()))
			require.Equal(t, []int64{2, 3}, forward(kvutil.From[int64](2).Until(4)))
			require.Equal(t, []int64{3, 4, 5}, forward(kvutil.After[int64](2)))
			require.Equal(t, []int64{4, 3, 2}, reverse(kvutil.After[int64](1).Through(4)))
			require.Equal(t, []int64{1, 2}, forward(kvutil.Through[int64](2)))
			require.Equal(t, []int64{3}, forward(kvutil.Only[int64](3)))
			require.Equal(t, []int64{}, forward(kvutil.From[int64](9)))

			// Seeking.
			it := must(ins.All())
			defer it.Close()
			require.NoError(t, it.Seek(3))
			require.True(t, it.Valid())
			p, err := it.Value()
			require.NoError(t, err)
			require.Equal(t, &pet{id: 3, Name: "pet3"}, p)
			require.Len(t, it.RawKey(), 8)
			it.Next()
			key, err := it.Key()
			require.NoError(t, err)
			require.Equal(t, int64(4), key)

			rit := must(ins.ReverseIterator(kvutil.All[int64]()))
			defer rit.Close()
			require.NoError(t, rit.Seek(3))
			key, err = rit.Key()
			require.NoError(t, err)
			require.Equal(t, int64(3), key)
			return nil
		})
		require.NoError(t, err)
	})
}

func TestGetterAndRecords(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		store := record.New[int64, pet]("pets")

		err := db.Update(func(tx kv.Tx) error {
			ins := store.Instantiate(tx)
			require.NoError(t, ins.Set(&pet{id: 1, Name: "rex"}))
			require.NoError(t, ins.Set(&pet{id: 2, Name: "tom"}))

			var ext extstore.Extensible[int64, pet] = store
			getter := ext.Getter(tx)
			got, err := getter.Get(2)
			require.NoError(t, err)
			require.Equal(t, &pet{id: 2, Name: "tom"}, got)
			_, err = getter.Get(3)
			require.ErrorIs(t, err, kv.ErrNotFound)

			it, err := ext.Records(tx, kvutil.From[int64](2))
			require.NoError(t, err)
			require.Equal(t, []int64{2}, idsOf(t, it))
			it, err = ext.Records(tx, kvutil.All[int64]())
			require.NoError(t, err)
			require.Equal(t, []int64{1, 2}, idsOf(t, it))

			require.Equal(t, "int64", store.KeyCodec().Name())
			return nil
		})
		require.NoError(t, err)
	})
}

func TestIDFunc(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		t.Run("NoIDFunc", func(t *testing.T) {
			store := record.New[int64, pet]("pets")
			err := db.Update(func(tx kv.Tx) error {
				ins := store.Instantiate(tx)
				p := &pet{Name: "rex"}
				require.ErrorIs(t, ins.Set(p), record.ErrNoID)
				require.Zero(t, p.id)

				ok, err := ins.Has(0)
				require.NoError(t, err)
				require.False(t, ok)
				return nil
			})
			require.NoError(t, err)
		})

		t.Run("IDFunc", func(t *testing.T) {
			next := int64(10)
			store := record.New[int64, pet]("generated").
				WithIDFunc(func(*pet) (int64, error) {
					next++
					return next, nil
				})
			err := db.Update(func(tx kv.Tx) error {
				ins := store.Instantiate(tx)
				a, b := &pet{Name: "rex"}, &pet{Name: "tom"}
				require.NoError(t, ins.Set(a))
				require.NoError(t, ins.Set(b))
				require.Equal(t, int64(11), a.id)
				require.Equal(t, int64(12), b.id)

				// A record that has an id keeps it.
				c := &pet{id: 5, Name: "max"}
				require.NoError(t, ins.Set(c))
				require.Equal(t, int64(5), c.id)
				require.Equal(t, int64(12), next)

				got, err := ins.Get(11)
				require.NoError(t, err)
				require.Equal(t, a, got)
				return nil
			})
			require.NoError(t, err)
		})

		t.Run("IDFuncFails", func(t *testing.T) {
			failure := errors.New("failure")
			store := record.New[int64, pet]("failing").
				WithIDFunc(func(*pet) (int64, error) { return 0, failure })
			err := db.Update(func(tx kv.Tx) error {
				p := &pet{Name: "rex"}
				require.ErrorIs(t, store.Instantiate(tx).Set(p), failure)
				require.Zero(t, p.id)
				return nil
			})
			require.NoError(t, err)
		})
	})
}

func TestSequenceID(t *testing.T) {
	db, err := badgerkv.Open(badger.DefaultOptions("").WithInMemory(true).WithLoggingLevel(badger.ERROR))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	seq, err := db.Badger().GetSequence([]byte("pets"), 10)
	require.NoError(t, err)
	defer seq.Release()

	store := record.New[int64, pet]("pets").WithIDFunc(badgerkv.SequenceID[int64, pet](seq))
	require.NoError(t, store.Check(db))

	err = db.Update(func(tx kv.Tx) error {
		ins := store.Instantiate(tx)
		for want := int64(1); want <= 3; want++ {
			p := &pet{Name: "pet"}
			require.NoError(t, ins.Set(p))
			require.Equal(t, want, p.id)
		}
		return nil
	})
	require.NoError(t, err)

	err = db.View(func(tx kv.Tx) error {
		require.Equal(t, []int64{1, 2, 3}, idsOf(t, must(store.Instantiate(tx).All())))
		return nil
	})
	require.NoError(t, err)
}

func TestAccessors(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		next := int64(100)
		store := record.NewWithAccessors("cards",
			func(c *card) int64 { return c.Number },
			func(c *card, id int64) { c.Number = id },
			nil, nil).
			WithIDFunc(func(*card) (int64, error) {
				next++
				return next, nil
			})
		require.NoError(t, store.Check(db))
		require.Equal(t, "record", store.Unit().Kind())

		err := db.Update(func(tx kv.Tx) error {
			ins := store.Instantiate(tx)
			a, b := &card{Owner: "ann"}, &card{Number: 7, Owner: "bob"}
			require.NoError(t, ins.Set(a))
			require.NoError(t, ins.Set(b))
			require.Equal(t, int64(101), a.Number)

			got, err := ins.Get(101)
			require.NoError(t, err)
			require.Equal(t, &card{Number: 101, Owner: "ann"}, got)
			got, err = ins.Get(7)
			require.NoError(t, err)
			require.Equal(t, &card{Number: 7, Owner: "bob"}, got)

			it, err := ins.All()
			require.NoError(t, err)
			defer it.Close()
			numbers := []int64{}
			for ; it.Valid(); it.Next() {
				c, err := it.Value()
				require.NoError(t, err)
				numbers = append(numbers, c.Number)
			}
			require.Equal(t, []int64{7, 101}, numbers)
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("NoAccessors", func(t *testing.T) {
		require.Panics(t, func() {
			record.NewWithAccessors[int64, card]("cards", nil, func(*card, int64) {}, nil, nil)
		})
	})
}

func TestDocs(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		store := record.New[int64, record.Doc[int64]]("docs").
			WithIDFunc(func(*record.Doc[int64]) (int64, error) { return 42, nil })

		err := db.Update(func(tx kv.Tx) error {
			ins := store.Instantiate(tx)
			doc := record.Doc[int64]{"a": "b"}
			require.NoError(t, ins.Set(&doc))
			require.Equal(t, int64(42), doc.GetId())

			got, err := ins.Get(42)
			require.NoError(t, err)
			require.Equal(t, record.Doc[int64]{"_id": int64(42), "a": "b"}, *got)
			return nil
		})
		require.NoError(t, err)
	})
}

// recorder is an extension that records what it is told.
type recorder struct {
	events *[]string
}

func (r recorder) Instantiate(kv.Tx) extstore.ExtensionInstance[int64, pet] {
	return recorderInstance(r)
}

type recorderInstance struct {
	events *[]string
}

func (r recorderInstance) OnSet(key int64, _ []byte, old, new *pet, opts ...any) error {
	was := "nil"
	if old != nil {
		was = old.Name
	}
	*r.events = append(*r.events, fmt.Sprintf("set %d %s->%s %v", key, was, new.Name, opts))
	return nil
}

func (r recorderInstance) OnDelete(key int64, _ []byte, old *pet) error {
	*r.events = append(*r.events, fmt.Sprintf("delete %d %s", key, old.Name))
	return nil
}

func TestExtensionsAndDeleteHooks(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		var events []string
		hook := func(name string) extstore.DeleteHook[int64] {
			return func(tx kv.Tx, key int64, rawKey []byte) error {
				require.True(t, tx.Writable())
				events = append(events, fmt.Sprintf("%s %d %x", name, key, rawKey))
				return nil
			}
		}

		store := record.New[int64, pet]("pets").
			WithExtension("first", recorder{events: &events})
		store.AddExtension("second", recorder{events: &events})
		store.AddDeleteHook("hook", hook("hook"))

		err := db.Update(func(tx kv.Tx) error {
			ins := store.Instantiate(tx)
			require.NoError(t, ins.Set(&pet{id: 1, Name: "rex"}))
			require.NoError(t, ins.Set(&pet{id: 1, Name: "max"}, "all", extstore.For("second", "second")))
			require.NoError(t, ins.Delete(2))
			require.NoError(t, ins.Delete(1))
			return nil
		})
		require.NoError(t, err)

		require.Equal(t, []string{
			"set 1 nil->rex []",
			"set 1 nil->rex []",
			"set 1 rex->max [all]",
			"set 1 rex->max [all second]",
			"delete 1 max",
			"delete 1 max",
			"hook 1 8000000000000001",
		}, events)
	})

	t.Run("Failure", func(t *testing.T) {
		failure := errors.New("failure")
		store := record.New[int64, pet]("pets")
		store.AddDeleteHook("failing", func(kv.Tx, int64, []byte) error { return failure })

		err := testkv.NewMem(t).Update(func(tx kv.Tx) error {
			ins := store.Instantiate(tx)
			require.NoError(t, ins.Set(&pet{id: 1, Name: "rex"}))
			require.ErrorIs(t, ins.Delete(1), failure)

			ok, err := ins.Has(1)
			require.NoError(t, err)
			require.True(t, ok)
			return nil
		})
		require.NoError(t, err)
	})
}

func TestUnit(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		store := record.New[int64, pet]("pets")
		require.Equal(t, "record", store.Unit().Kind())
		require.Equal(t, "pets", store.Unit().Name())
		require.Equal(t, []byte("pets"), store.Prefix())
		require.Equal(t, []byte("petsd"), store.DataPrefix())
		require.Equal(t, uint64(1), store.Unit().Version())
		require.NoError(t, store.Check(db))

		require.NoError(t, record.New[int64, pet]("pets").Check(db))

		otherFormat := record.NewWithCodecs[int64, pet]("pets", nil, codec.Formatted[pet](codec.JSON))
		require.ErrorIs(t, otherFormat.Check(db), migrate.ErrChanged)

		require.ErrorIs(t, record.New[int32, pet32]("pets").Check(db), migrate.ErrChanged)

		// A store that is not a record store is not the same.
		require.ErrorIs(t, extstore.New[int64, pet]("pets").Check(db), migrate.ErrChanged)
		require.ErrorIs(t, typed.New[int64, pet]("pets").Check(db), migrate.ErrChanged)

		// A version comes with a migration, and then the store is what it is defined as.
		next := record.NewWithCodecs[int64, pet]("pets", nil, codec.Formatted[pet](codec.JSON)).WithVersion(2)
		require.Equal(t, uint64(2), next.Unit().Version())
		migrate.Custom(next, 1, func(context.Context, *migrate.Migrator) error { return nil })
		report, err := next.Migrate(context.Background(), db)
		require.NoError(t, err)
		require.Equal(t, `migrated record "pets" from version 1 to 2`, report.String())
		require.NoError(t, next.Check(db))
	})
}

func TestExtensionsOfTheStore(t *testing.T) {
	var events []string
	first, second := recorder{events: &events}, recorder{events: &events}
	store := record.New[int64, pet]("pets").WithExtension("first", first)
	store.AddExtension("second", second)
	require.Equal(t, []any{first, second}, store.Extensions())

	// A name that is taken is a mistake of the definition, and an extension that keeps nothing
	// has nothing to retire, but a name can be retired all the same.
	require.Panics(t, func() { store.AddExtension("first", recorder{events: &events}) })
	require.Empty(t, store.Unit().Parts())
	store.RetireExtension("gone")
	require.NoError(t, store.Check(memkv.New()))
}

// pet32 is a pet with a smaller id.
type pet32 struct {
	id   int32
	Name string
	Age  int
}

func (p *pet32) GetId() int32 { return p.id }

func (p *pet32) SetId(id int32) { p.id = id }

func TestDefinitionProblems(t *testing.T) {
	type point struct{ X, Y int }
	type pin struct {
		id point
	}

	t.Run("MissingKeyCodec", func(t *testing.T) {
		accessors := func(*pin) point { return point{} }
		require.Panics(t, func() {
			record.NewWithAccessors("pins", accessors, func(*pin, point) {}, nil, nil)
		})
	})

	t.Run("Codecs", func(t *testing.T) {
		store := record.NewWithCodecs[int64, pet]("pets",
			codec.IntKey[int64](), codec.Formatted[pet](codec.JSON))
		require.NoError(t, store.Check(memkv.New()))
		require.Equal(t, "json", store.ValueCodec().Name())
	})
}

// isRecord compiles only for a pointer of a record type.
func isRecord[I comparable, T any, PT record.Record[I, T]]() {}

func TestRecordTypes(t *testing.T) {
	isRecord[int64, pet]()
	isRecord[uuid.UUID, record.Object[uuid.UUID, sample]]()
	isRecord[string, record.Doc[string]]()
}

func TestIDOf(t *testing.T) {
	store := record.New[int64, pet]("pets")

	p := &pet{}
	require.Zero(t, store.IDOf(p))
	p.SetId(7)
	require.Equal(t, int64(7), store.IDOf(p))
}
