package schema

import (
	"context"
	"testing"

	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/stretchr/testify/require"
)

// TestLegacyFingerprints checks that a database that was checked before there were versions,
// when only a fingerprint was recorded of a definition, is of the first version.
func TestLegacyFingerprints(t *testing.T) {
	builds := 0
	var pets *Namespace
	define := func(version uint64, codec string) *Schema {
		sch := New()
		pets = sch.Namespace("pets", Version(version))
		pets.Sub("data").Define(Definition{
			Kind:       "data",
			Keys:       true,
			Attributes: func() map[string]string { return map[string]string{"codec": codec} },
		})
		pets.Sub("ext", Dependent()).Sub("by_name").Define(Definition{
			Kind:    "index",
			Keys:    true,
			Rebuild: func(context.Context, kv.DB) error { builds++; return nil },
		})
		return sch
	}

	// The database as it was recorded then.
	old := define(1, "v1")
	legacy := func() kv.DB {
		db := memkv.New()
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			for _, def := range old.Definitions() {
				err := old.meta.Open(tx).Set(append([]byte("f/"), def.ns.Path()...), def.fingerprint())
				if err != nil {
					return err
				}
			}
			return nil
		}))
		return db
	}
	db := legacy()

	// It is what the same program defines, with nothing to build.
	require.ErrorIs(t, define(1, "v2").Check(db), ErrChanged)
	require.NoError(t, define(1, "v1").Check(db))
	require.Zero(t, builds)

	// The records are the ones of today from then on, with all that is recorded of them.
	require.NoError(t, db.View(func(tx kv.Tx) error {
		stored, err := old.load(tx)
		require.NoError(t, err)
		require.Empty(t, stored.legacy)
		require.Equal(t, unitState{Version: 1}, stored.units["pets"])
		require.Equal(t, old.readyState(old.Definitions()[1]), stored.defs["pets/data"])
		index := stored.defs["pets/ext/by_name"]
		require.True(t, index.Derived)
		require.Equal(t, stateReady, index.State)
		require.NotEmpty(t, index.Prefix)
		return nil
	}))

	// And it is migrated as a database of the first version is.
	next := define(2, "v2")
	ran := false
	pets.AddMigration(1, func(context.Context, *Migrator) error { ran = true; return nil })
	_, err := next.Migrate(context.Background(), db)
	require.NoError(t, err)
	require.True(t, ran)
	require.Equal(t, 1, builds)
	require.NoError(t, define(2, "v2").Check(db))

	// Also when the first program that sees it is of a newer version.
	db, builds, ran = legacy(), 0, false
	next = define(2, "v2")
	pets.AddMigration(1, func(context.Context, *Migrator) error { ran = true; return nil })
	require.ErrorIs(t, next.Check(db), ErrNeedsMigration)
	_, err = next.Migrate(context.Background(), db)
	require.NoError(t, err)
	require.True(t, ran)
	require.Equal(t, 1, builds)
}
