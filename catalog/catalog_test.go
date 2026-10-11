package catalog_test

import (
	"errors"
	"testing"

	"github.com/ehsanranjbar/kvutil/catalog"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/stretchr/testify/require"
)

func TestNamespaces(t *testing.T) {
	schema := catalog.NewSchema()
	pets := schema.Namespace("pets")
	data := pets.Sub("data", catalog.WithPrefix([]byte{'d'}))
	owners := schema.Namespace("owners", catalog.WithPrefix([]byte{0x01}))
	require.NoError(t, schema.Validate())

	require.Equal(t, "pets", pets.Name())
	require.Equal(t, "pets", pets.Path())
	require.Equal(t, "pets/data", data.Path())
	require.Same(t, schema, data.Schema())

	// A name is its own prefix by default, in a form that marks where it ends.
	require.Equal(t, []byte("pets\x00\x01"), pets.Prefix())
	require.Equal(t, []byte("pets\x00\x01d"), data.Prefix())
	require.Equal(t, []byte{0x01}, owners.Prefix())

	require.Equal(t, catalog.Range{Start: []byte{0x01}, End: []byte{0x02}}, owners.Range())

	var paths []string
	for _, ns := range schema.Namespaces() {
		paths = append(paths, ns.Path())
	}
	require.Equal(t, []string{"_catalog", "pets", "pets/data", "owners"}, paths)
}

func TestNamespaceOpen(t *testing.T) {
	schema := catalog.NewSchema()
	pets, owners := schema.Namespace("pets"), schema.Namespace("owners")
	db := memkv.New()

	err := db.Update(func(tx kv.Tx) error {
		require.NoError(t, pets.Open(tx).Set([]byte("k"), []byte("1")))
		require.NoError(t, owners.Open(tx).Set([]byte("k"), []byte("2")))

		value, err := pets.Open(tx).Get([]byte("k"))
		require.NoError(t, err)
		require.Equal(t, []byte("1"), value)

		value, err = tx.Get([]byte("owners\x00\x01k"))
		require.NoError(t, err)
		require.Equal(t, []byte("2"), value)
		return nil
	})
	require.NoError(t, err)
}

func TestNamespaceProblems(t *testing.T) {
	tests := []struct {
		name   string
		define func(schema *catalog.Schema)
	}{
		{"SameNameTwice", func(schema *catalog.Schema) {
			schema.Namespace("pets")
			schema.Namespace("pets", catalog.WithPrefix([]byte{0x01}))
		}},
		{"SamePrefix", func(schema *catalog.Schema) {
			schema.Namespace("a", catalog.WithPrefix([]byte{0x01}))
			schema.Namespace("b", catalog.WithPrefix([]byte{0x01}))
		}},
		{"PrefixOfAnother", func(schema *catalog.Schema) {
			schema.Namespace("a", catalog.WithPrefix([]byte{0x01, 0x02}))
			schema.Namespace("b", catalog.WithPrefix([]byte{0x01}))
		}},
		{"PrefixUnderAnother", func(schema *catalog.Schema) {
			schema.Namespace("a", catalog.WithPrefix([]byte{0x01}))
			schema.Namespace("b", catalog.WithPrefix([]byte{0x01, 0x02}))
		}},
		{"EmptyName", func(schema *catalog.Schema) {
			schema.Namespace("")
		}},
		{"SlashInName", func(schema *catalog.Schema) {
			schema.Namespace("a/b")
		}},
		{"EmptyPrefix", func(schema *catalog.Schema) {
			schema.Namespace("a", catalog.WithPrefix([]byte{}))
		}},
		{"ReservedName", func(schema *catalog.Schema) {
			schema.Namespace("_catalog")
		}},
		{"UsedTwice", func(schema *catalog.Schema) {
			ns := schema.Namespace("a")
			ns.Define(catalog.Definition{Kind: "one"})
			ns.Define(catalog.Definition{Kind: "other"})
		}},
		{"NamespaceUnderKeys", func(schema *catalog.Schema) {
			ns := schema.Namespace("a")
			ns.Define(catalog.Definition{Kind: "store", Keys: true})
			ns.Sub("b")
		}},
		{"KeysOverNamespace", func(schema *catalog.Schema) {
			ns := schema.Namespace("a")
			ns.Sub("b")
			ns.Define(catalog.Definition{Kind: "store", Keys: true})
		}},
		{"InvalidDefinition", func(schema *catalog.Schema) {
			schema.Namespace("a").Define(catalog.Definition{
				Kind:     "store",
				Validate: func() error { return errors.New("incomplete") },
			})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schema := catalog.NewSchema()
			test.define(schema)
			require.Error(t, schema.Validate())
			// A schema with a problem is not written to the database.
			require.Error(t, schema.Check(memkv.New()))
		})
	}

	t.Run("NoProblem", func(t *testing.T) {
		schema := catalog.NewSchema()
		a := schema.Namespace("a")
		a.Define(catalog.Definition{Kind: "parent"})
		a.Sub("b").Define(catalog.Definition{Kind: "store", Keys: true})
		a.Sub("c", catalog.WithPrefix([]byte{'c'}))
		schema.Namespace("ab")
		require.NoError(t, schema.Validate())
	})
}

func TestFootprint(t *testing.T) {
	schema := catalog.NewSchema()
	pets, owners, logs := schema.Namespace("pets"), schema.Namespace("owners"), schema.Namespace("logs")
	visits := schema.Namespace("visits")

	require.Equal(t, []catalog.Range{owners.Range()}, owners.Footprint())

	// Deleting an owner deletes their pets, which deletes the visits of the pets.
	owners.Sub("ext").Cascade(pets)
	pets.Cascade(visits)
	pets.Cascade(visits)
	// A cycle.
	visits.Cascade(owners)

	require.Equal(t, []catalog.Range{owners.Range(), pets.Range(), visits.Range()}, owners.Footprint())
	require.Equal(t, []catalog.Range{pets.Range(), visits.Range(), owners.Range()}, pets.Footprint())
	require.Equal(t, []catalog.Range{logs.Range()}, logs.Footprint())
}

// define builds a schema with one definition whose attributes are given.
func define(attrs map[string]string, opts ...catalog.NamespaceOption) *catalog.Schema {
	schema := catalog.NewSchema()
	schema.Namespace("pets", opts...).Define(catalog.Definition{
		Kind:       "store",
		Keys:       true,
		Attributes: func() map[string]string { return attrs },
	})
	return schema
}

func TestCheck(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		attrs := map[string]string{"key": "int64", "value": "msgpack"}

		// The first check records the definitions and the next ones compare with them.
		require.NoError(t, define(attrs).Check(db))
		require.NoError(t, define(attrs).Check(db))
		require.NoError(t, define(map[string]string{"value": "msgpack", "key": "int64"}).Check(db))

		err := define(map[string]string{"key": "int64", "value": "json"}).Check(db)
		require.ErrorIs(t, err, catalog.ErrChanged)
		require.ErrorContains(t, err, `store "pets"`)

		err = define(attrs, catalog.WithPrefix([]byte{0x01})).Check(db)
		require.ErrorIs(t, err, catalog.ErrChanged)

		// A failed check does not replace what is recorded.
		require.NoError(t, define(attrs).Check(db))

		// A definition that is new to the database is recorded next to the others.
		schema := define(attrs)
		schema.Namespace("owners").Define(catalog.Definition{Kind: "store", Keys: true})
		require.NoError(t, schema.Check(db))

		changed := define(attrs)
		changed.Namespace("owners").Define(catalog.Definition{Kind: "index", Keys: true})
		require.ErrorIs(t, changed.Check(db), catalog.ErrChanged)

		// A definition that is gone from the code is not a problem.
		require.NoError(t, define(attrs).Check(db))
	})
}

func TestFormat(t *testing.T) {
	require.Nil(t, catalog.NewSchema().Format())
	require.Equal(t, codec.JSON, catalog.NewSchema(catalog.WithFormat(codec.JSON)).Format())
}

func TestNameRegistry(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		nreg, err := catalog.NewNameRegistry(db)
		require.NoError(t, err)

		prefix, err := nreg.Name("foo")
		require.NoError(t, err)
		require.Equal(t, []byte{0x01}, prefix)
		require.Equal(t, []byte{0x02}, nreg.MustName("bar"))
		require.Equal(t, []byte{0x01}, nreg.MustName("foo"))

		// The key that is returned belongs to the caller.
		prefix[0] = 0xff
		require.Equal(t, []byte{0x01}, nreg.MustName("foo"))

		t.Run("Reload", func(t *testing.T) {
			nreg2, err := catalog.NewNameRegistry(db)
			require.NoError(t, err)
			require.Equal(t, []byte{0x02}, nreg2.MustName("bar"))
			require.Equal(t, []byte{0x03}, nreg2.MustName("baz"))
		})

		t.Run("SubRegistry", func(t *testing.T) {
			sub, err := catalog.NewNameRegistry(db, catalog.WithRegistryPrefix([]byte{0x7f}))
			require.NoError(t, err)
			require.Equal(t, []byte{0x01}, sub.MustName("foo"))
			require.Equal(t, []byte{0x02}, sub.MustName("bar"))
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
		nreg, err := catalog.NewNameRegistry(memkv.New())
		require.NoError(t, err)

		for i := range 255 {
			_, err := nreg.Name(string(rune('a' + i)))
			require.NoError(t, err)
		}
		_, err = nreg.Name("one too many")
		require.Error(t, err)
	})

	t.Run("KeyLen", func(t *testing.T) {
		nreg, err := catalog.NewNameRegistry(memkv.New(), catalog.WithRegistryKeyLen(2))
		require.NoError(t, err)
		require.Equal(t, []byte{0x00, 0x01}, nreg.MustName("foo"))
	})
}

// failingDB fails every update.
type failingDB struct {
	kv.DB
}

func (failingDB) Update(func(kv.Tx) error) error {
	return errors.New("failure")
}

func TestNameRegistryFailedPersist(t *testing.T) {
	nreg, err := catalog.NewNameRegistry(failingDB{DB: memkv.New()})
	require.NoError(t, err)

	// A name that could not be persisted must not be handed out from memory afterwards.
	for range 2 {
		_, err = nreg.Name("foo")
		require.Error(t, err)
	}
}

func TestPrefixAllocator(t *testing.T) {
	db := memkv.New()
	nreg, err := catalog.NewNameRegistry(db)
	require.NoError(t, err)

	schema := catalog.NewSchema(catalog.WithPrefixAllocator(nreg))
	pets := schema.Namespace("pets")
	owners := schema.Namespace("owners")
	explicit := schema.Namespace("explicit", catalog.WithPrefix([]byte{0x70}))
	require.NoError(t, schema.Validate())

	// The first name went to the namespace of the schema itself.
	require.Equal(t, []byte{0x02}, pets.Prefix())
	require.Equal(t, []byte{0x03}, owners.Prefix())
	require.Equal(t, []byte{0x70}, explicit.Prefix())
	// Only the namespaces at the root are given their prefixes.
	require.Equal(t, []byte("\x02data\x00\x01"), pets.Sub("data").Prefix())

	// The same names get the same prefixes the next time.
	nreg, err = catalog.NewNameRegistry(db)
	require.NoError(t, err)
	schema = catalog.NewSchema(catalog.WithPrefixAllocator(nreg))
	require.Equal(t, []byte{0x03}, schema.Namespace("owners").Prefix())
	require.Equal(t, []byte{0x02}, schema.Namespace("pets").Prefix())

	t.Run("Failure", func(t *testing.T) {
		nreg, err := catalog.NewNameRegistry(failingDB{DB: memkv.New()})
		require.NoError(t, err)

		schema := catalog.NewSchema(catalog.WithPrefixAllocator(nreg))
		require.Error(t, schema.Validate())
	})
}
