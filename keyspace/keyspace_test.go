package keyspace_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/keyspace"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/ehsanranjbar/kvutil/kv/prefix"
	"github.com/ehsanranjbar/kvutil/migrate"
	"github.com/ehsanranjbar/kvutil/store/typed"
	"github.com/stretchr/testify/require"
)

type pet struct {
	Name string
}

func open(t *testing.T, db kv.DB, opts ...keyspace.Option) *keyspace.Keyspace {
	t.Helper()

	ks, err := keyspace.Open(db, opts...)
	require.NoError(t, err)
	return ks
}

func TestPrefixes(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		ks := open(t, db)

		pre := ks.Prefix("foo")
		require.Equal(t, []byte{0x01}, pre)
		require.Equal(t, []byte{0x02}, ks.Prefix("bar"))
		require.Equal(t, []byte{0x01}, ks.Prefix("foo"))

		// The prefix that is returned belongs to the caller.
		pre[0] = 0xff
		require.Equal(t, []byte{0x01}, ks.Prefix("foo"))

		name, ok := ks.Name([]byte{0x02})
		require.True(t, ok)
		require.Equal(t, "bar", name)
		_, ok = ks.Name([]byte{0x09})
		require.False(t, ok)

		t.Run("Reopen", func(t *testing.T) {
			ks := open(t, db)
			require.Equal(t, []byte{0x02}, ks.Prefix("bar"))
			require.Equal(t, []byte{0x03}, ks.Prefix("baz"))
			require.Equal(t, []byte{0x01}, ks.Prefix("foo"))
		})

		t.Run("RegistryKey", func(t *testing.T) {
			sub := open(t, db, keyspace.WithRegistryKey([]byte{0x7f}))
			require.Equal(t, []byte{0x01}, sub.Prefix("foo"))
			require.Equal(t, []byte{0x02}, sub.Prefix("bar"))
		})

		t.Run("Keys", func(t *testing.T) {
			err := db.View(func(tx kv.Tx) error {
				it, err := tx.Iterator(nil, nil)
				require.NoError(t, err)
				defer it.Close()

				var keys [][]byte
				for ; it.Valid(); it.Next() {
					keys = append(keys, append([]byte(nil), it.Key()...))
				}
				require.Equal(t, [][]byte{{0x00}, {0x7f}}, keys)
				return nil
			})
			require.NoError(t, err)
		})
	})

	t.Run("Full", func(t *testing.T) {
		ks := open(t, memkv.New())

		for i := range 255 {
			require.NotPanics(t, func() { ks.Prefix(string(rune('a' + i))) })
		}
		require.Panics(t, func() { ks.Prefix("one too many") })
		// What it has is still there.
		require.Equal(t, []byte{0x01}, ks.Prefix("a"))
	})

	t.Run("KeyLen", func(t *testing.T) {
		ks := open(t, memkv.New(), keyspace.WithKeyLen(2))
		require.Equal(t, []byte{0x00, 0x01}, ks.Prefix("foo"))
		require.Equal(t, []byte{0x00, 0x02}, ks.Prefix("bar"))

		_, err := keyspace.Open(memkv.New(), keyspace.WithKeyLen(0))
		require.Error(t, err)
	})

	t.Run("RegistryKeyIsNotAPrefix", func(t *testing.T) {
		// The key of the registry is not given to a name, nor is a prefix that it is under.
		ks := open(t, memkv.New(), keyspace.WithRegistryKey([]byte{0x01}))
		require.Equal(t, []byte{0x02}, ks.Prefix("foo"))

		ks = open(t, memkv.New(), keyspace.WithRegistryKey([]byte{0x01, 0x05}))
		require.Equal(t, []byte{0x02}, ks.Prefix("foo"))
	})

	t.Run("EmptyName", func(t *testing.T) {
		ks := open(t, memkv.New())
		require.Panics(t, func() { ks.Prefix("") })
		require.Panics(t, func() { ks.Retire("") })
	})

	t.Run("Concurrent", func(t *testing.T) {
		ks := open(t, memkv.New())

		var wg sync.WaitGroup
		prefixes := make([][]byte, 40)
		for i := range prefixes {
			wg.Add(1)
			go func() {
				defer wg.Done()
				prefixes[i] = ks.Prefix(fmt.Sprint("name", i%20))
				_, _ = ks.Name(prefixes[i])
			}()
		}
		wg.Wait()

		for i := range 20 {
			require.Equal(t, prefixes[i], prefixes[i+20])
		}
	})
}

// failingDB fails every update.
type failingDB struct {
	kv.DB
}

func (failingDB) Update(func(kv.Tx) error) error {
	return errors.New("failure")
}

func TestPrefixFailedPersist(t *testing.T) {
	ks := open(t, failingDB{DB: memkv.New()})

	// A name that could not be persisted must not be handed out from memory afterwards.
	for range 2 {
		require.PanicsWithValue(t, `keyspace: the prefix of "foo": failed to update name registry: failure`,
			func() { ks.Prefix("foo") })
	}
}

func TestUnitsAreNamed(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		ks := open(t, db)
		pets := typed.New[int64, pet](ks.Prefix("pets"))
		owners := typed.New[int64, pet](ks.Prefix("owners"))
		byHand := typed.New[int64, pet]("by_hand")

		// Without the keyspace a unit goes by its prefix.
		require.Equal(t, "0x01", pets.Unit().Name())

		report, err := ks.Migrate(context.Background(), pets, owners, byHand)
		require.NoError(t, err)
		require.Equal(t, `recorded typed "pets"
recorded typed "owners"
recorded typed "by_hand"`, report.String())
		require.Equal(t, "pets", pets.Unit().Name())
		require.Equal(t, "owners", owners.Unit().Name())
		require.Equal(t, "by_hand", byHand.Unit().Name())

		// And errors say them.
		again := open(t, db)
		changed := typed.New[int32, pet](again.Prefix("pets"))
		err = again.Check(changed, typed.New[int64, pet](again.Prefix("owners")))
		require.ErrorIs(t, err, migrate.ErrChanged)
		require.ErrorContains(t, err, `typed "pets"`)

		_, err = again.Migrate(context.Background(), changed)
		require.ErrorIs(t, err, migrate.ErrChanged)
		require.ErrorContains(t, err, `typed "pets"`)
	})
}

// store defines the stores of a program that claims the names it is given.
func store(ks *keyspace.Keyspace, name string) *typed.Store[int64, pet] {
	return typed.New[int64, pet](ks.Prefix(name))
}

func TestUnclaimedNames(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		ks := open(t, db)
		pets, toys := store(ks, "pets"), store(ks, "toys")
		_, err := ks.Migrate(context.Background(), pets, toys)
		require.NoError(t, err)
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			return toys.Instantiate(tx).Set(1, &pet{Name: "ball"})
		}))

		// A program that does not claim a name that the database has is told of it.
		ks = open(t, db)
		pets = store(ks, "pets")
		wantErr := `"toys" is in the database and is not claimed: Retire("toys") deletes it`
		_, err = ks.Migrate(context.Background(), pets)
		require.ErrorIs(t, err, migrate.ErrUnknown)
		require.ErrorContains(t, err, wantErr)
		err = ks.Check(pets)
		require.ErrorIs(t, err, migrate.ErrUnknown)
		require.ErrorContains(t, err, wantErr)

		// And nothing is done: not even for what is fine.
		pre := toys.DataPrefix()
		require.NoError(t, db.View(func(tx kv.Tx) error {
			ok, err := prefix.New(tx, pre).Has(toysKey(t, toys, 1))
			require.NoError(t, err)
			require.True(t, ok)
			return nil
		}))
		_, ok := ks.Name([]byte{0x02})
		require.True(t, ok)
	})
}

// toysKey returns the key that a store has for an id.
func toysKey(t *testing.T, s *typed.Store[int64, pet], id int64) []byte {
	t.Helper()

	key, err := s.KeyCodec().Encode(id)
	require.NoError(t, err)
	return key
}

func TestRetire(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		ks := open(t, db)
		pets, toys := store(ks, "pets"), store(ks, "toys")
		_, err := ks.Migrate(context.Background(), pets, toys)
		require.NoError(t, err)
		toysPrefix := ks.Prefix("toys")
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			if err := pets.Instantiate(tx).Set(1, &pet{Name: "rex"}); err != nil {
				return err
			}
			if err := toys.Instantiate(tx).Set(1, &pet{Name: "ball"}); err != nil {
				return err
			}
			return toys.Instantiate(tx).Set(2, &pet{Name: "bone"})
		}))

		// The program without the toys retires them.
		program := func() (*keyspace.Keyspace, *typed.Store[int64, pet]) {
			ks := open(t, db)
			pets := store(ks, "pets")
			ks.Retire("toys")
			return ks, pets
		}
		ks, pets = program()
		err = ks.Check(pets)
		require.ErrorIs(t, err, migrate.ErrNeedsMigration)
		require.ErrorContains(t, err, "toys")
		require.Equal(t, 3, count(t, db, toysPrefix))
		_, ok := ks.Name(toysPrefix)
		require.True(t, ok)

		report, err := ks.Migrate(context.Background(), pets)
		require.NoError(t, err)
		require.Equal(t, `retired unit "toys"`, report.String())

		// Its data is gone, with the record of its unit, and the name is forgotten.
		require.Zero(t, count(t, db, toysPrefix))
		_, ok = ks.Name(toysPrefix)
		require.False(t, ok)
		require.Equal(t, 2, count(t, db, ks.Prefix("pets")), "the record and the pet are kept")

		// It is persisted, and there is nothing to do the next time, whichever way is asked.
		ks, pets = program()
		_, ok = ks.Name(toysPrefix)
		require.False(t, ok)
		require.NoError(t, ks.Check(pets))
		report, err = ks.Migrate(context.Background(), pets)
		require.NoError(t, err)
		require.Empty(t, report.Actions)

		// The prefix of what is forgotten is not given to another name, which would meet the
		// data of the one before it if there was any left.
		require.Equal(t, []byte{0x03}, ks.Prefix("cats"))

		// And the program can go on to retire it for as long as it likes, or forget it.
		forgot := open(t, db)
		require.NoError(t, forgot.Check(store(forgot, "pets"), store(forgot, "cats")))
	})
}

func TestRetiredAndClaimed(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		ks := open(t, db)
		pets := store(ks, "pets")
		_, err := ks.Migrate(context.Background(), pets)
		require.NoError(t, err)
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			return pets.Instantiate(tx).Set(1, &pet{Name: "rex"})
		}))

		ks = open(t, db)
		pets = store(ks, "pets")
		ks.Retire("pets")
		_, err = ks.Migrate(context.Background(), pets)
		require.ErrorContains(t, err, `"pets" is retired and claimed`)
		require.ErrorContains(t, ks.Check(pets), `"pets" is retired and claimed`)

		// Nothing was deleted.
		require.Equal(t, 2, count(t, db, ks.Prefix("pets")))
		_, ok := ks.Name([]byte{0x01})
		require.True(t, ok)
	})
}

func TestRetireNeverDeletesTheRegistry(t *testing.T) {
	db := memkv.New()
	elsewhere := open(t, db, keyspace.WithRegistryKey([]byte{0x7f}))
	require.Equal(t, []byte{0x01}, elsewhere.Prefix("a"))

	// A registry that is kept under the prefix of a name that is retired.
	var raw []byte
	require.NoError(t, db.Update(func(tx kv.Tx) error {
		var err error
		raw, err = tx.Get([]byte{0x7f})
		if err != nil {
			return err
		}
		return tx.Set([]byte{0x01, 0xff}, raw)
	}))

	ks := open(t, db, keyspace.WithRegistryKey([]byte{0x01, 0xff}))
	ks.Retire("a")
	_, err := ks.Migrate(context.Background())
	require.ErrorContains(t, err, "holds the registry itself")
	require.ErrorContains(t, ks.Check(), "holds the registry itself")

	// The registry is where it was.
	require.Equal(t, 1, count(t, db, []byte{0x01, 0xff}))
	require.Equal(t, []byte{0x01}, open(t, db, keyspace.WithRegistryKey([]byte{0x01, 0xff})).Prefix("a"))
}

func TestCheckAndMigrate(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		// Check records what is new, as migrate.Check does, and Migrate has nothing to do then.
		ks := open(t, db)
		pets := store(ks, "pets")
		require.NoError(t, ks.Check(pets))
		_, ok := readState(t, db, ks.Prefix("pets"))
		require.True(t, ok)

		report, err := ks.Migrate(context.Background(), pets)
		require.NoError(t, err)
		require.Empty(t, report.Actions)

		// What needs Migrate is reported by Check, which then writes nothing, also of the unit
		// that is new next to it.
		next := open(t, db)
		v2 := typed.New[int64, pet](next.Prefix("pets")).WithVersion(2)
		migrate.Custom(v2, 1, func(context.Context, *migrate.Migrator) error { return nil })
		owners := store(next, "owners")
		require.ErrorIs(t, next.Check(v2, owners), migrate.ErrNeedsMigration)
		_, ok = readState(t, db, next.Prefix("owners"))
		require.False(t, ok)

		report, err = next.Migrate(context.Background(), v2, owners)
		require.NoError(t, err)
		// What is only recorded is done right away, and the migration after it.
		require.Equal(t, `recorded typed "owners"
migrated typed "pets" from version 1 to 2`, report.String())
		require.NoError(t, next.Check(v2, owners))
	})
}

func TestRetiringAndMigratingTogether(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		ks := open(t, db)
		_, err := ks.Migrate(context.Background(), store(ks, "pets"), store(ks, "toys"))
		require.NoError(t, err)

		next := open(t, db)
		v2 := typed.New[int64, pet](next.Prefix("pets")).WithVersion(2)
		migrate.Custom(v2, 1, func(context.Context, *migrate.Migrator) error { return nil })
		next.Retire("toys")

		report, err := next.Migrate(context.Background(), v2)
		require.NoError(t, err)
		require.Equal(t, `retired unit "toys"
migrated typed "pets" from version 1 to 2`, report.String())
	})
}

// count returns the number of keys under a prefix.
func count(t *testing.T, db kv.DB, pre []byte) (n int) {
	t.Helper()

	require.NoError(t, db.View(func(tx kv.Tx) error {
		it, err := tx.Iterator(pre, kv.PrefixEnd(pre))
		if err != nil {
			return err
		}
		defer it.Close()

		for ; it.Valid(); it.Next() {
			n++
		}
		return nil
	}))
	return n
}

// readState tells whether the record of the unit under a prefix is there.
func readState(t *testing.T, db kv.DB, pre []byte) (bz []byte, ok bool) {
	t.Helper()

	err := db.View(func(tx kv.Tx) error {
		var err error
		bz, err = tx.Get(migrate.MetaKey(pre))
		if errors.Is(err, kv.ErrNotFound) {
			return nil
		}
		ok = err == nil
		return err
	})
	require.NoError(t, err)
	return bz, ok
}
