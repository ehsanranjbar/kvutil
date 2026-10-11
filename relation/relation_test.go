package relation_test

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/relation"
	"github.com/ehsanranjbar/kvutil/schema"
	"github.com/ehsanranjbar/kvutil/store/record"
	"github.com/stretchr/testify/require"
)

// entity is a record of any id type, whose id is the key it is stored under.
type entity[I comparable] struct {
	id   I
	Name string
}

func (e *entity[I]) GetId() I { return e.id }

func (e *entity[I]) SetId(id I) { e.id = id }

// membership is the data of the pairs of users and groups.
type membership struct {
	Role string
}

// must returns the value of a call that must not fail.
func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// newStore defines a store of entities.
func newStore[I comparable](sch *schema.Schema, name string) *record.Store[I, entity[I]] {
	return record.New[I, entity[I]](sch.Namespace(name))
}

// add sets the entities of the ids.
func add[I comparable](t *testing.T, tx kv.Tx, store *record.Store[I, entity[I]], ids ...I) {
	t.Helper()

	for _, id := range ids {
		require.NoError(t, store.Instantiate(tx).Set(&entity[I]{id: id, Name: fmt.Sprint(id)}))
	}
}

// remove deletes the entities of the ids.
func remove[I comparable](t *testing.T, tx kv.Tx, store *record.Store[I, entity[I]], ids ...I) {
	t.Helper()

	for _, id := range ids {
		require.NoError(t, store.Instantiate(tx).Delete(id))
	}
}

// entry is a key of an iterator with its value.
type entry[K, D any] struct {
	Key  K
	Data D
}

// drain collects what an iterator has and closes it. It is never nil.
func drain[K, D any](t *testing.T, it iterutil.Iterator[K, *D]) []entry[K, D] {
	t.Helper()
	defer it.Close()

	entries := []entry[K, D]{}
	for ; it.Valid(); it.Next() {
		key, err := it.Key()
		require.NoError(t, err)
		data, err := it.Value()
		require.NoError(t, err)
		entries = append(entries, entry[K, D]{Key: key, Data: *data})
	}
	return entries
}

// keysOf returns the keys of entries.
func keysOf[K, D any](entries []entry[K, D]) []K {
	keys := make([]K, 0, len(entries))
	for _, e := range entries {
		keys = append(keys, e.Key)
	}
	return keys
}

// rawCount counts the keys that a namespace has in a transaction.
func rawCount(t *testing.T, tx kv.Tx, ns *schema.Namespace) int {
	t.Helper()

	it, err := ns.Open(tx).Iterator(nil, nil)
	require.NoError(t, err)
	defer it.Close()

	n := 0
	for ; it.Valid(); it.Next() {
		n++
	}
	return n
}

// rawCountUnder counts the keys of a namespace that have a prefix, which is the namespace of a
// direction of a relation.
func rawCountUnder(t *testing.T, tx kv.Tx, ns *schema.Namespace, prefix byte) int {
	t.Helper()

	it, err := ns.Open(tx).Iterator([]byte{prefix}, []byte{prefix + 1})
	require.NoError(t, err)
	defer it.Close()

	n := 0
	for ; it.Valid(); it.Next() {
		n++
	}
	return n
}

// plainCodec encodes a string as it is.
type plainCodec struct{}

func (plainCodec) Name() string { return "plain" }

func (plainCodec) Encode(v string) ([]byte, error) { return []byte(v), nil }

func (plainCodec) Decode(bz []byte) (string, error) { return string(bz), nil }

var _ codec.ValueCodec[string] = plainCodec{}

func TestRelation(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		sch := schema.New()
		users := newStore[int64](sch, "users")
		groups := newStore[string](sch, "groups")
		memberships := relation.New[membership](sch.Namespace("memberships"), users, groups)
		require.NoError(t, sch.Check(db))
		require.Equal(t, "memberships", memberships.Name())

		err := db.Update(func(tx kv.Tx) error {
			add(t, tx, users, 1, 2)
			add(t, tx, groups, "admins", "staff")
			rel := memberships.Instantiate(tx)

			// Nothing is related yet.
			_, err := rel.Get(1, "admins")
			require.ErrorIs(t, err, kv.ErrNotFound)
			require.False(t, must(rel.Has(1, "admins")))
			require.NoError(t, rel.Delete(1, "admins"))

			// Both records have to exist.
			err = rel.Set(3, "admins", nil)
			require.ErrorIs(t, err, relation.ErrMissingRecord)
			err = rel.Set(1, "nobody", nil)
			require.ErrorIs(t, err, relation.ErrMissingRecord)
			require.False(t, must(rel.Has(3, "admins")))
			require.False(t, must(rel.Has(1, "nobody")))

			// The data comes back as it was set.
			require.NoError(t, rel.Set(1, "admins", &membership{Role: "owner"}))
			require.True(t, must(rel.Has(1, "admins")))
			require.False(t, must(rel.Has(1, "staff")))
			require.False(t, must(rel.Has(2, "admins")))
			require.Equal(t, &membership{Role: "owner"}, must(rel.Get(1, "admins")))

			// A pair without data has the zero data.
			require.NoError(t, rel.Set(2, "staff", nil))
			require.Equal(t, &membership{}, must(rel.Get(2, "staff")))

			// Setting a pair again replaces its data.
			require.NoError(t, rel.Set(1, "admins", &membership{Role: "member"}))
			require.Equal(t, &membership{Role: "member"}, must(rel.Get(1, "admins")))
			require.Len(t, drain(t, must(rel.All())), 2)

			// Deleting removes the pair and only the pair.
			require.NoError(t, rel.Delete(1, "admins"))
			require.False(t, must(rel.Has(1, "admins")))
			_, err = rel.Get(1, "admins")
			require.ErrorIs(t, err, kv.ErrNotFound)
			require.True(t, must(rel.Has(2, "staff")))
			require.Empty(t, drain(t, must(rel.Lefts("admins"))))
			require.Empty(t, drain(t, must(rel.Rights(1))))
			return nil
		})
		require.NoError(t, err)

		// A transaction that cannot write cannot relate.
		err = db.View(func(tx kv.Tx) error {
			return memberships.Instantiate(tx).Set(1, "admins", nil)
		})
		require.ErrorIs(t, err, kv.ErrReadOnly)
	})
}

func TestRelationIteration(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		sch := schema.New()
		users := newStore[int64](sch, "users")
		groups := newStore[string](sch, "groups")
		memberships := relation.New[membership](sch.Namespace("memberships"), users, groups)
		require.NoError(t, sch.Check(db))

		err := db.Update(func(tx kv.Tx) error {
			add(t, tx, users, 1, 2, 3, 4)
			add(t, tx, groups, "dev", "ops", "qa")
			rel := memberships.Instantiate(tx)

			// They are set out of order.
			for _, p := range []struct {
				user  int64
				group string
				role  string
			}{
				{3, "qa", "c"}, {1, "ops", "b"}, {2, "dev", "d"}, {1, "dev", "a"}, {3, "dev", "e"}, {1, "qa", "f"},
			} {
				require.NoError(t, rel.Set(p.user, p.group, &membership{Role: p.role}))
			}

			require.Equal(t, []entry[codec.Pair[int64, string], membership]{
				{codec.NewPair(int64(1), "dev"), membership{"a"}},
				{codec.NewPair(int64(1), "ops"), membership{"b"}},
				{codec.NewPair(int64(1), "qa"), membership{"f"}},
				{codec.NewPair(int64(2), "dev"), membership{"d"}},
				{codec.NewPair(int64(3), "dev"), membership{"e"}},
				{codec.NewPair(int64(3), "qa"), membership{"c"}},
			}, drain(t, must(rel.All())))

			require.Equal(t, []entry[string, membership]{
				{"dev", membership{"a"}}, {"ops", membership{"b"}}, {"qa", membership{"f"}},
			}, drain(t, must(rel.Rights(1))))
			require.Empty(t, drain(t, must(rel.Rights(4))))
			require.Empty(t, drain(t, must(rel.Rights(9))))

			require.Equal(t, []entry[int64, membership]{
				{1, membership{"a"}}, {2, membership{"d"}}, {3, membership{"e"}},
			}, drain(t, must(rel.Lefts("dev"))))
			require.Equal(t, []entry[int64, membership]{{1, membership{"b"}}}, drain(t, must(rel.Lefts("ops"))))

			// Deleting a pair, and changing the data of another, shows in both directions.
			require.NoError(t, rel.Delete(1, "dev"))
			require.NoError(t, rel.Set(3, "dev", &membership{Role: "z"}))
			require.Equal(t, []int64{2, 3}, keysOf(drain(t, must(rel.Lefts("dev")))))
			require.Equal(t, []entry[string, membership]{
				{"dev", membership{"z"}}, {"qa", membership{"c"}},
			}, drain(t, must(rel.Rights(3))))
			require.Len(t, drain(t, must(rel.All())), 5)
			return nil
		})
		require.NoError(t, err)

		// What was written is there in a later transaction.
		err = db.View(func(tx kv.Tx) error {
			rel := memberships.Instantiate(tx)
			require.Equal(t, []string{"ops", "qa"}, keysOf(drain(t, must(rel.Rights(1)))))
			return nil
		})
		require.NoError(t, err)
	})
}

func TestRelationCascade(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		sch := schema.New()
		users := newStore[int64](sch, "users")
		groups := newStore[string](sch, "groups")
		memberships := relation.New[membership](sch.Namespace("memberships"), users, groups)
		require.NoError(t, sch.Check(db))

		err := db.Update(func(tx kv.Tx) error {
			add(t, tx, users, 1, 2, 3)
			add(t, tx, groups, "dev", "ops")
			rel := memberships.Instantiate(tx)
			for _, u := range []int64{1, 2, 3} {
				for _, g := range []string{"dev", "ops"} {
					require.NoError(t, rel.Set(u, g, &membership{Role: fmt.Sprint(u, g)}))
				}
			}
			require.Equal(t, 6, rawCountUnder(t, tx, memberships.Namespace(), 'l'))
			require.Equal(t, 6, rawCountUnder(t, tx, memberships.Namespace(), 'r'))

			// Deleting a left record removes its pairs, in both directions.
			remove(t, tx, users, 2)
			require.Equal(t, 4, len(drain(t, must(rel.All()))))
			require.Empty(t, drain(t, must(rel.Rights(2))))
			require.Equal(t, []int64{1, 3}, keysOf(drain(t, must(rel.Lefts("dev")))))
			require.Equal(t, []int64{1, 3}, keysOf(drain(t, must(rel.Lefts("ops")))))
			require.Equal(t, 4, rawCountUnder(t, tx, memberships.Namespace(), 'l'))
			require.Equal(t, 4, rawCountUnder(t, tx, memberships.Namespace(), 'r'))

			// Likewise a right record.
			remove(t, tx, groups, "dev")
			require.Equal(t, []string{"ops"}, keysOf(drain(t, must(rel.Rights(1)))))
			require.Equal(t, []string{"ops"}, keysOf(drain(t, must(rel.Rights(3)))))
			require.Empty(t, drain(t, must(rel.Lefts("dev"))))
			require.Equal(t, 2, rawCountUnder(t, tx, memberships.Namespace(), 'l'))
			require.Equal(t, 2, rawCountUnder(t, tx, memberships.Namespace(), 'r'))

			// The pairs that are left are untouched, and so are the records.
			require.Equal(t, &membership{Role: "1ops"}, must(rel.Get(1, "ops")))
			require.Equal(t, &membership{Role: "3ops"}, must(rel.Get(3, "ops")))
			require.True(t, must(users.Contains(tx, 1)))
			require.True(t, must(groups.Contains(tx, "ops")))

			// Deleting a record that has no pairs, or that does not exist, is fine.
			remove(t, tx, users, 2, 9)
			require.Len(t, drain(t, must(rel.All())), 2)

			// A record that is created again starts without pairs.
			add(t, tx, groups, "dev")
			require.Empty(t, drain(t, must(rel.Lefts("dev"))))
			return nil
		})
		require.NoError(t, err)
	})
}

func TestRelationKeysThatBeginOthers(t *testing.T) {
	t.Run("strings", func(t *testing.T) {
		testkv.Each(t, func(t *testing.T, db kv.DB) {
			sch := schema.New()
			lefts := newStore[string](sch, "lefts")
			rights := newStore[string](sch, "rights")
			rel := relation.New[int](sch.Namespace("rel"), lefts, rights)
			require.NoError(t, sch.Check(db))

			keys := []string{"a", "ab", "abc", "b"}
			err := db.Update(func(tx kv.Tx) error {
				add(t, tx, lefts, keys...)
				add(t, tx, rights, keys...)
				ins := rel.Instantiate(tx)

				// Every left is related to the rights that are not shorter than it.
				for i, l := range keys {
					for j, r := range keys {
						if len(r) >= len(l) {
							d := 10*i + j
							require.NoError(t, ins.Set(l, r, &d))
						}
					}
				}

				for i, l := range keys {
					var want []entry[string, int]
					for j, r := range keys {
						if len(r) >= len(l) {
							want = append(want, entry[string, int]{r, 10*i + j})
						}
					}
					require.Equal(t, want, drain(t, must(ins.Rights(l))), "rights of %q", l)
				}
				for j, r := range keys {
					var want []entry[string, int]
					for i, l := range keys {
						if len(r) >= len(l) {
							want = append(want, entry[string, int]{l, 10*i + j})
						}
					}
					require.Equal(t, want, drain(t, must(ins.Lefts(r))), "lefts of %q", r)
				}

				// A pair is only related to itself.
				require.True(t, must(ins.Has("a", "ab")))
				require.False(t, must(ins.Has("ab", "a")))
				require.Equal(t, 12, *must(ins.Get("ab", "abc")))
				require.Equal(t, 2, *must(ins.Get("a", "abc")))
				require.Equal(t, 1, *must(ins.Get("a", "ab")))

				// Deleting a key leaves the keys that begin with it, and the ones it begins.
				require.NoError(t, ins.Delete("a", "ab"))
				require.False(t, must(ins.Has("a", "ab")))
				require.True(t, must(ins.Has("a", "a")))
				require.True(t, must(ins.Has("a", "abc")))
				require.True(t, must(ins.Has("ab", "ab")))
				remove(t, tx, lefts, "ab")
				require.Equal(t, []string{"a", "abc", "b"}, keysOf(drain(t, must(ins.Lefts("abc")))))
				require.Equal(t, []string{"a", "b"}, keysOf(drain(t, must(ins.Lefts("b")))))
				require.Equal(t, []string{"a", "abc", "b"}, keysOf(drain(t, must(ins.Rights("a")))))
				require.Equal(t, []string{"abc"}, keysOf(drain(t, must(ins.Rights("abc")))))
				require.Empty(t, drain(t, must(ins.Rights("ab"))))
				return nil
			})
			require.NoError(t, err)
		})
	})

	t.Run("integers", func(t *testing.T) {
		testkv.Each(t, func(t *testing.T, db kv.DB) {
			sch := schema.New()
			lefts := newStore[int64](sch, "lefts")
			rights := newStore[int64](sch, "rights")
			rel := relation.New[struct{}](sch.Namespace("rel"), lefts, rights)
			require.NoError(t, sch.Check(db))

			keys := []int64{-300, -2, -1, 1, 2, 255, 256, 1 << 40}
			err := db.Update(func(tx kv.Tx) error {
				add(t, tx, lefts, keys...)
				add(t, tx, rights, keys...)
				ins := rel.Instantiate(tx)

				for _, l := range keys {
					for _, r := range keys {
						if l <= r {
							require.NoError(t, ins.Set(l, r, nil))
						}
					}
				}
				for i, l := range keys {
					require.Equal(t, keys[i:], keysOf(drain(t, must(ins.Rights(l)))), "rights of %d", l)
				}
				for j, r := range keys {
					require.Equal(t, keys[:j+1], keysOf(drain(t, must(ins.Lefts(r)))), "lefts of %d", r)
				}
				all := drain(t, must(ins.All()))
				require.Len(t, all, len(keys)*(len(keys)+1)/2)
				require.True(t, slices.IsSortedFunc(all, func(a, b entry[codec.Pair[int64, int64], struct{}]) int {
					if a.Key.First != b.Key.First {
						return int(a.Key.First - b.Key.First)
					}
					return int(a.Key.Second - b.Key.Second)
				}))

				remove(t, tx, lefts, -1)
				require.False(t, must(ins.Has(-1, 1)))
				require.True(t, must(ins.Has(1, 1)))
				require.Equal(t, []int64{-300, -2}, keysOf(drain(t, must(ins.Lefts(-2)))))
				return nil
			})
			require.NoError(t, err)
		})
	})
}

func TestRelationWithItself(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		sch := schema.New()
		users := newStore[int64](sch, "users")
		follows := relation.New[struct{}](sch.Namespace("follows"), users, users)
		require.NoError(t, sch.Check(db))

		err := db.Update(func(tx kv.Tx) error {
			add(t, tx, users, 1, 2, 3)
			rel := follows.Instantiate(tx)
			for _, p := range [][2]int64{{1, 2}, {1, 3}, {2, 1}, {3, 1}, {2, 2}, {3, 2}} {
				require.NoError(t, rel.Set(p[0], p[1], nil))
			}
			require.Equal(t, []int64{2, 3}, keysOf(drain(t, must(rel.Rights(1)))))
			require.Equal(t, []int64{1, 2, 3}, keysOf(drain(t, must(rel.Lefts(2)))))
			require.True(t, must(rel.Has(2, 2)))
			require.False(t, must(rel.Has(2, 3)))

			// A user is removed from both sides.
			remove(t, tx, users, 2)
			require.Equal(t, []int64{3}, keysOf(drain(t, must(rel.Rights(1)))))
			require.Equal(t, []int64{1}, keysOf(drain(t, must(rel.Rights(3)))))
			require.Empty(t, drain(t, must(rel.Lefts(2))))
			require.Empty(t, drain(t, must(rel.Rights(2))))
			require.Len(t, drain(t, must(rel.All())), 2)
			require.Equal(t, 2, rawCountUnder(t, tx, follows.Namespace(), 'l'))
			require.Equal(t, 2, rawCountUnder(t, tx, follows.Namespace(), 'r'))

			remove(t, tx, users, 1)
			require.Empty(t, drain(t, must(rel.All())))
			require.Zero(t, rawCount(t, tx, follows.Namespace()))
			return nil
		})
		require.NoError(t, err)
	})
}

func TestRelationData(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		sch := schema.New()
		users := newStore[int64](sch, "users")
		groups := newStore[int64](sch, "groups")
		plain := relation.New[struct{}](sch.Namespace("plain"), users, groups)
		roles := relation.NewWithCodec(sch.Namespace("roles"), users, groups, codec.ValueCodec[string](plainCodec{}))
		require.NoError(t, sch.Check(db))

		err := db.Update(func(tx kv.Tx) error {
			add(t, tx, users, 1, 2)
			add(t, tx, groups, 10)

			// A pair without data.
			p := plain.Instantiate(tx)
			require.NoError(t, p.Set(1, 10, nil))
			require.NoError(t, p.Set(2, 10, &struct{}{}))
			require.Equal(t, &struct{}{}, must(p.Get(1, 10)))
			require.Equal(t, []int64{1, 2}, keysOf(drain(t, must(p.Lefts(10)))))

			// A data codec of its own.
			r := roles.Instantiate(tx)
			admin, nobody := "admin", ""
			require.NoError(t, r.Set(1, 10, &admin))
			require.NoError(t, r.Set(2, 10, &nobody))
			require.Equal(t, &admin, must(r.Get(1, 10)))
			require.Equal(t, &nobody, must(r.Get(2, 10)))
			require.Equal(t, []entry[int64, string]{{1, "admin"}, {2, ""}}, drain(t, must(r.Lefts(10))))

			// The data is stored as the codec encodes it.
			it, err := roles.Namespace().Open(tx).Iterator([]byte{'l'}, []byte{'m'})
			require.NoError(t, err)
			defer it.Close()
			require.True(t, it.Valid())
			value, err := it.Value()
			require.NoError(t, err)
			require.True(t, bytes.Equal([]byte("admin"), value), "%q", value)
			return nil
		})
		require.NoError(t, err)
	})
}

func TestRelationDefinitionPanics(t *testing.T) {
	sch := schema.New()
	users := newStore[int64](sch, "users")
	groups := newStore[int64](sch, "groups")

	require.PanicsWithValue(t, "relation: the namespace is required", func() {
		relation.New[struct{}](nil, users, groups)
	})
	require.PanicsWithValue(t, `relation "a": the left store is required`, func() {
		relation.New[struct{}, int64, int64](sch.Namespace("a"), nil, groups)
	})
	require.PanicsWithValue(t, `relation "b": the right store is required`, func() {
		relation.New[struct{}, int64, int64](sch.Namespace("b"), users, nil)
	})
	require.PanicsWithValue(t, `relation "c": the left store is required`, func() {
		var none *record.Store[int64, entity[int64]]
		relation.New[struct{}](sch.Namespace("c"), none, groups)
	})
}

func TestRelationSchema(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		define := func() (*schema.Schema, *relation.Relation[int64, string, string]) {
			sch := schema.New()
			users := newStore[int64](sch, "users")
			groups := newStore[string](sch, "groups")
			return sch, relation.NewWithCodec(sch.Namespace("roles"), users, groups, codec.ValueCodec[string](plainCodec{}))
		}

		sch, rel := define()
		require.NoError(t, sch.Validate())
		def := rel.Namespace().Definition()
		require.Equal(t, "relation", def.Kind)
		require.Equal(t, map[string]string{"left": "int64", "right": "string", "data": "plain"}, def.Attributes())
		require.NoError(t, sch.Check(db))

		// The footprint of the stores includes the relation.
		for _, name := range []string{"users", "groups"} {
			var ns *schema.Namespace
			for _, n := range sch.Namespaces() {
				if n.Path() == name {
					ns = n
				}
			}
			require.NotNil(t, ns)
			require.Contains(t, ns.Footprint(), rel.Namespace().Range())
		}

		// The same definition is accepted again.
		sch, _ = define()
		require.NoError(t, sch.Check(db))

		// A definition with another data codec is not.
		other := schema.New()
		users := newStore[int64](other, "users")
		groups := newStore[string](other, "groups")
		relation.New[string](other.Namespace("roles"), users, groups)
		require.ErrorIs(t, other.Check(db), schema.ErrChanged)
	})
}

// pairKey is a pair of keys in the model of the property test.
type pairKey struct {
	left  int64
	right string
}

func TestRelationRandom(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		ops := 600
		if t.Name() != "TestRelationRandom/Mem" {
			ops = 150
		}

		sch := schema.New()
		users := newStore[int64](sch, "users")
		groups := newStore[string](sch, "groups")
		rel := relation.New[membership](sch.Namespace("memberships"), users, groups)
		require.NoError(t, sch.Check(db))

		lefts := []int64{-3, -1, 1, 2, 7, 300}
		rights := []string{"a", "ab", "abc", "b", "ba", "c"}

		var (
			rng        = rand.New(rand.NewPCG(7, 11))
			model      = map[pairKey]membership{}
			haveLeft   = map[int64]bool{}
			haveRight  = map[string]bool{}
			randLeft   = func() int64 { return lefts[rng.IntN(len(lefts))] }
			randRight  = func() string { return rights[rng.IntN(len(rights))] }
			compareAll func(tx kv.Tx)
			largest    int
		)

		compareAll = func(tx kv.Tx) {
			t.Helper()
			ins := rel.Instantiate(tx)

			var want []entry[codec.Pair[int64, string], membership]
			for _, l := range slices.Sorted(slices.Values(lefts)) {
				for _, r := range slices.Sorted(slices.Values(rights)) {
					if data, ok := model[pairKey{l, r}]; ok {
						want = append(want, entry[codec.Pair[int64, string], membership]{codec.NewPair(l, r), data})
					}
				}
			}
			got := drain(t, must(ins.All()))
			if len(want) == 0 {
				require.Empty(t, got)
			} else {
				require.Equal(t, want, got)
			}

			for _, l := range lefts {
				var wantRights []entry[string, membership]
				for _, r := range slices.Sorted(slices.Values(rights)) {
					if data, ok := model[pairKey{l, r}]; ok {
						wantRights = append(wantRights, entry[string, membership]{r, data})
					}
				}
				got := drain(t, must(ins.Rights(l)))
				if len(wantRights) == 0 {
					require.Empty(t, got, "rights of %d", l)
				} else {
					require.Equal(t, wantRights, got, "rights of %d", l)
				}
			}
			for _, r := range rights {
				var wantLefts []entry[int64, membership]
				for _, l := range slices.Sorted(slices.Values(lefts)) {
					if data, ok := model[pairKey{l, r}]; ok {
						wantLefts = append(wantLefts, entry[int64, membership]{l, data})
					}
				}
				got := drain(t, must(ins.Lefts(r)))
				if len(wantLefts) == 0 {
					require.Empty(t, got, "lefts of %q", r)
				} else {
					require.Equal(t, wantLefts, got, "lefts of %q", r)
				}
			}
			require.Equal(t, len(model), rawCountUnder(t, tx, rel.Namespace(), 'l'))
			require.Equal(t, len(model), rawCountUnder(t, tx, rel.Namespace(), 'r'))
		}

		err := db.Update(func(tx kv.Tx) error {
			add(t, tx, users, lefts...)
			add(t, tx, groups, rights...)
			for _, l := range lefts {
				haveLeft[l] = true
			}
			for _, r := range rights {
				haveRight[r] = true
			}

			ins := rel.Instantiate(tx)
			for n := 1; n <= ops; n++ {
				l, r := randLeft(), randRight()
				switch op := rng.IntN(20); {
				case op < 10:
					data := membership{Role: fmt.Sprint("role", rng.IntN(100))}
					err := ins.Set(l, r, &data)
					if haveLeft[l] && haveRight[r] {
						require.NoError(t, err)
						model[pairKey{l, r}] = data
					} else {
						require.True(t, errors.Is(err, relation.ErrMissingRecord), "%v", err)
					}
				case op < 13:
					require.NoError(t, ins.Delete(l, r))
					delete(model, pairKey{l, r})
				case op < 14:
					remove(t, tx, users, l)
					haveLeft[l] = false
					for k := range model {
						if k.left == l {
							delete(model, k)
						}
					}
				case op < 15:
					remove(t, tx, groups, r)
					haveRight[r] = false
					for k := range model {
						if k.right == r {
							delete(model, k)
						}
					}
				default:
					// Creating a record again, which does not change its pairs.
					if rng.IntN(2) == 0 {
						add(t, tx, users, l)
						haveLeft[l] = true
					} else {
						add(t, tx, groups, r)
						haveRight[r] = true
					}
				}

				if n%25 == 0 {
					compareAll(tx)
					largest = max(largest, len(model))
				}
			}
			compareAll(tx)
			// The walk has to have had pairs to compare.
			require.Greater(t, largest, 5)
			return nil
		})
		require.NoError(t, err)

		// What was left is the same in a transaction of its own.
		err = db.View(func(tx kv.Tx) error {
			compareAll(tx)
			return nil
		})
		require.NoError(t, err)
	})
}
