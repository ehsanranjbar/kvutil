package schema_test

import (
	"errors"
	"testing"

	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/ehsanranjbar/kvutil/schema"
	"github.com/stretchr/testify/require"
)

func TestNamespaces(t *testing.T) {
	sch := schema.New()
	pets := sch.Namespace("pets")
	data := pets.Sub("data", schema.WithPrefix([]byte{'d'}))
	owners := sch.Namespace("owners", schema.WithPrefix([]byte{0x01}))
	require.NoError(t, sch.Validate())

	require.Equal(t, "pets", pets.Name())
	require.Equal(t, "pets", pets.Path())
	require.Equal(t, "pets/data", data.Path())
	require.Same(t, sch, data.Schema())

	// A name is its own prefix by default, in a form that marks where it ends.
	require.Equal(t, []byte("pets\x00\x01"), pets.Prefix())
	require.Equal(t, []byte("pets\x00\x01d"), data.Prefix())
	require.Equal(t, []byte{0x01}, owners.Prefix())

	require.Equal(t, kv.KeyRange{Start: []byte{0x01}, End: []byte{0x02}}, owners.Range())

	var paths []string
	for _, ns := range sch.Namespaces() {
		paths = append(paths, ns.Path())
	}
	require.Equal(t, []string{"_schema", "pets", "pets/data", "owners"}, paths)
}

func TestNamespaceOpen(t *testing.T) {
	sch := schema.New()
	pets, owners := sch.Namespace("pets"), sch.Namespace("owners")
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
		define func(sch *schema.Schema)
	}{
		{"SameNameTwice", func(sch *schema.Schema) {
			sch.Namespace("pets")
			sch.Namespace("pets", schema.WithPrefix([]byte{0x01}))
		}},
		{"SamePrefix", func(sch *schema.Schema) {
			sch.Namespace("a", schema.WithPrefix([]byte{0x01}))
			sch.Namespace("b", schema.WithPrefix([]byte{0x01}))
		}},
		{"PrefixOfAnother", func(sch *schema.Schema) {
			sch.Namespace("a", schema.WithPrefix([]byte{0x01, 0x02}))
			sch.Namespace("b", schema.WithPrefix([]byte{0x01}))
		}},
		{"PrefixUnderAnother", func(sch *schema.Schema) {
			sch.Namespace("a", schema.WithPrefix([]byte{0x01}))
			sch.Namespace("b", schema.WithPrefix([]byte{0x01, 0x02}))
		}},
		{"EmptyName", func(sch *schema.Schema) {
			sch.Namespace("")
		}},
		{"SlashInName", func(sch *schema.Schema) {
			sch.Namespace("a/b")
		}},
		{"EmptyPrefix", func(sch *schema.Schema) {
			sch.Namespace("a", schema.WithPrefix([]byte{}))
		}},
		{"ReservedName", func(sch *schema.Schema) {
			sch.Namespace("_schema")
		}},
		{"UsedTwice", func(sch *schema.Schema) {
			ns := sch.Namespace("a")
			ns.Define(schema.Definition{Kind: "one"})
			ns.Define(schema.Definition{Kind: "other"})
		}},
		{"NamespaceUnderKeys", func(sch *schema.Schema) {
			ns := sch.Namespace("a")
			ns.Define(schema.Definition{Kind: "store", Keys: true})
			ns.Sub("b")
		}},
		{"KeysOverNamespace", func(sch *schema.Schema) {
			ns := sch.Namespace("a")
			ns.Sub("b")
			ns.Define(schema.Definition{Kind: "store", Keys: true})
		}},
		{"InvalidDefinition", func(sch *schema.Schema) {
			sch.Namespace("a").Define(schema.Definition{
				Kind:     "store",
				Validate: func() error { return errors.New("incomplete") },
			})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sch := schema.New()
			test.define(sch)
			require.Error(t, sch.Validate())
			// A schema with a problem is not written to the database.
			require.Error(t, sch.Check(memkv.New()))
		})
	}

	t.Run("NoProblem", func(t *testing.T) {
		sch := schema.New()
		a := sch.Namespace("a")
		a.Define(schema.Definition{Kind: "parent"})
		a.Sub("b").Define(schema.Definition{Kind: "store", Keys: true})
		a.Sub("c", schema.WithPrefix([]byte{'c'}))
		sch.Namespace("ab")
		require.NoError(t, sch.Validate())
	})
}

func TestFootprint(t *testing.T) {
	sch := schema.New()
	pets, owners, logs := sch.Namespace("pets"), sch.Namespace("owners"), sch.Namespace("logs")
	visits := sch.Namespace("visits")

	require.Equal(t, []kv.KeyRange{owners.Range()}, owners.Footprint())

	// Deleting an owner deletes their pets, which deletes the visits of the pets.
	owners.Sub("ext").Cascade(pets)
	pets.Cascade(visits)
	pets.Cascade(visits)
	// A cycle.
	visits.Cascade(owners)

	require.Equal(t, []kv.KeyRange{owners.Range(), pets.Range(), visits.Range()}, owners.Footprint())
	require.Equal(t, []kv.KeyRange{pets.Range(), visits.Range(), owners.Range()}, pets.Footprint())
	require.Equal(t, []kv.KeyRange{logs.Range()}, logs.Footprint())
}

// define builds a schema with one definition whose attributes are given.
func define(attrs map[string]string, opts ...schema.NamespaceOption) *schema.Schema {
	sch := schema.New()
	sch.Namespace("pets", opts...).Define(schema.Definition{
		Kind:       "store",
		Keys:       true,
		Attributes: func() map[string]string { return attrs },
	})
	return sch
}

func TestCheck(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		attrs := map[string]string{"key": "int64", "value": "msgpack"}

		// The first check records the definitions and the next ones compare with them.
		require.NoError(t, define(attrs).Check(db))
		require.NoError(t, define(attrs).Check(db))
		require.NoError(t, define(map[string]string{"value": "msgpack", "key": "int64"}).Check(db))

		err := define(map[string]string{"key": "int64", "value": "json"}).Check(db)
		require.ErrorIs(t, err, schema.ErrChanged)
		require.ErrorContains(t, err, `store "pets"`)

		err = define(attrs, schema.WithPrefix([]byte{0x01})).Check(db)
		require.ErrorIs(t, err, schema.ErrChanged)

		// A failed check does not replace what is recorded.
		require.NoError(t, define(attrs).Check(db))

		// A definition that is new to the database is recorded next to the others.
		sch := define(attrs)
		sch.Namespace("owners").Define(schema.Definition{Kind: "store", Keys: true})
		require.NoError(t, sch.Check(db))

		changed := define(attrs)
		changed.Namespace("owners").Define(schema.Definition{Kind: "index", Keys: true})
		require.ErrorIs(t, changed.Check(db), schema.ErrChanged)

		// Data that the program does not define any more is not left without a word.
		require.ErrorIs(t, define(attrs).Check(db), schema.ErrUnknown)
	})
}

func TestFormat(t *testing.T) {
	require.Nil(t, schema.New().Format())
	require.Equal(t, codec.JSON, schema.New(schema.WithFormat(codec.JSON)).Format())
}

func TestNameRegistry(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		nreg, err := schema.NewNameRegistry(db)
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
			nreg2, err := schema.NewNameRegistry(db)
			require.NoError(t, err)
			require.Equal(t, []byte{0x02}, nreg2.MustName("bar"))
			require.Equal(t, []byte{0x03}, nreg2.MustName("baz"))
		})

		t.Run("SubRegistry", func(t *testing.T) {
			sub, err := schema.NewNameRegistry(db, schema.WithRegistryPrefix([]byte{0x7f}))
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
		nreg, err := schema.NewNameRegistry(memkv.New())
		require.NoError(t, err)

		for i := range 255 {
			_, err := nreg.Name(string(rune('a' + i)))
			require.NoError(t, err)
		}
		_, err = nreg.Name("one too many")
		require.Error(t, err)
	})

	t.Run("KeyLen", func(t *testing.T) {
		nreg, err := schema.NewNameRegistry(memkv.New(), schema.WithRegistryKeyLen(2))
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
	nreg, err := schema.NewNameRegistry(failingDB{DB: memkv.New()})
	require.NoError(t, err)

	// A name that could not be persisted must not be handed out from memory afterwards.
	for range 2 {
		_, err = nreg.Name("foo")
		require.Error(t, err)
	}
}

func TestPrefixAllocator(t *testing.T) {
	db := memkv.New()
	nreg, err := schema.NewNameRegistry(db)
	require.NoError(t, err)

	sch := schema.New(schema.WithPrefixAllocator(nreg))
	pets := sch.Namespace("pets")
	owners := sch.Namespace("owners")
	explicit := sch.Namespace("explicit", schema.WithPrefix([]byte{0x70}))
	require.NoError(t, sch.Validate())

	// The first name went to the namespace of the schema itself.
	require.Equal(t, []byte{0x02}, pets.Prefix())
	require.Equal(t, []byte{0x03}, owners.Prefix())
	require.Equal(t, []byte{0x70}, explicit.Prefix())
	// Only the namespaces at the root are given their prefixes.
	require.Equal(t, []byte("\x02data\x00\x01"), pets.Sub("data").Prefix())

	// The same names get the same prefixes the next time.
	nreg, err = schema.NewNameRegistry(db)
	require.NoError(t, err)
	sch = schema.New(schema.WithPrefixAllocator(nreg))
	require.Equal(t, []byte{0x03}, sch.Namespace("owners").Prefix())
	require.Equal(t, []byte{0x02}, sch.Namespace("pets").Prefix())

	t.Run("Failure", func(t *testing.T) {
		nreg, err := schema.NewNameRegistry(failingDB{DB: memkv.New()})
		require.NoError(t, err)

		sch := schema.New(schema.WithPrefixAllocator(nreg))
		require.Error(t, sch.Validate())
	})
}

func TestNamespaceDefinitions(t *testing.T) {
	sch := schema.New()
	pets := sch.Namespace("pets")
	pets.Define(schema.Definition{Kind: "store"})
	pets.Sub("data").Define(schema.Definition{Kind: "data", Keys: true})
	exts := pets.Sub("ext")
	exts.Sub("by_name").Define(schema.Definition{Kind: "index", Keys: true})
	exts.Sub("by_age").Define(schema.Definition{Kind: "index", Keys: true})
	sch.Namespace("owners").Define(schema.Definition{Kind: "store"})
	require.NoError(t, sch.Validate())

	paths := func(defs []*schema.Definition) []string {
		var paths []string
		for _, def := range defs {
			paths = append(paths, def.Kind+" "+def.Namespace().Path())
		}
		return paths
	}

	// What is defined in a namespace and under it, and nothing of the others.
	require.Equal(t,
		[]string{"store pets", "data pets/data", "index pets/ext/by_name", "index pets/ext/by_age"},
		paths(pets.Definitions()))
	require.Equal(t,
		[]string{"index pets/ext/by_name", "index pets/ext/by_age"},
		paths(exts.Definitions()))
	require.Empty(t, sch.Namespace("empty").Definitions())
}
