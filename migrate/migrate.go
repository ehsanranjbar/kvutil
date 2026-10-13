// Package migrate has the migrations of the records of stores: what converts the records
// that an older version of a program wrote into the ones that the program reads now.
//
// A migration is a part of the program of the version that it leads to, like the type of the
// records of the version before, which is kept next to it for as long as a database of that
// version may be met:
//
//	pets := record.New[int64, Pet](sch.Namespace("pets", schema.Version(2)))
//
//	type PetV1 struct { ... }
//
//	migrate.Records(pets, 1, func(id int64, old *PetV1) (*Pet, error) {
//		return &Pet{Name: old.Name, Species: old.Kind}, nil
//	})
//
// The Migrate of the schema runs the migrations that a database needs when the program starts.
// A value that only gets fields that it did not have needs no migration in the formats that
// leave out what they do not know, like the default one.
package migrate

import (
	"bytes"
	"context"
	"fmt"

	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/schema"
)

// Migrator is what a migration works with.
type Migrator = schema.Migrator

// Store is a store whose records are migrated. It is implemented by the stores of the
// packages typed, ext and record.
type Store[K, V any] interface {
	// KeyCodec returns the key codec of the store.
	KeyCodec() codec.KeyCodec[K]
	// ValueCodec returns the value codec of the store.
	ValueCodec() codec.ValueCodec[V]
	// DataNamespace returns the namespace that the records are kept in.
	DataNamespace() *schema.Namespace
}

// Namespaced is anything that is defined in a namespace, like a store or a relation.
type Namespaced interface {
	Namespace() *schema.Namespace
}

type options struct {
	batch    int
	bytes    int
	oldCodec any
}

// Option is an option of a migration of records.
type Option func(*options)

// WithBatchSize sets the number of records that are converted in a transaction, which is 256
// by default. A transaction is also ended once it has 4 MB of records.
func WithBatchSize(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.batch = n
		}
	}
}

// WithOldCodec sets the codec that the records were written with, for a store whose codec
// has changed too. By default it is the codec that the type of the old records has in a
// store that is not given one.
func WithOldCodec[Old any](c codec.ValueCodec[Old]) Option {
	return func(o *options) {
		o.oldCodec = c
	}
}

// Records adds the migration of the records of a store from a version to the next one: a
// function that is given every record as it was written, in the type that it had, and returns
// it as it is to be. A record that the function returns nil for is deleted.
//
// The records are converted in place, under the keys that they have, a batch at a time. The
// migration goes on from where it stopped when it is interrupted, and no record is converted
// twice. The indexes and the aggregations of the store are built again from the new records
// once it is done, so the function has nothing to do for them.
//
// It panics if the options are not for the type of the old records.
func Records[K, Old, New any](
	store Store[K, New],
	from uint64,
	convert func(key K, old *Old) (*New, error),
	opts ...Option,
) {
	// The options are checked when the migration is defined, as a mistake in them is one of
	// the program.
	oldCodec[K, Old](store, opts, new(options))

	store.DataNamespace().AddMigration(from, func(ctx context.Context, m *Migrator) error {
		return Convert(ctx, m, store, convert, opts...)
	})
}

// Custom adds a migration of the unit of something from a version to the next one that is
// any function, for what is not the conversion of one record into one record. The function
// is given the database, and saves how far it has come with the steps of the Migrator if it
// is to go on from there when it is interrupted. It can use Convert for a store.
func Custom(of Namespaced, from uint64, m schema.Migration) {
	of.Namespace().AddMigration(from, m)
}

func oldCodec[K, Old, New any](store Store[K, New], opts []Option, o *options) codec.ValueCodec[Old] {
	*o = options{batch: 256, bytes: 4 << 20}
	for _, opt := range opts {
		opt(o)
	}
	if o.oldCodec == nil {
		return codec.ValueFor[Old](store.DataNamespace().Schema().Format())
	}

	c, ok := o.oldCodec.(codec.ValueCodec[Old])
	if !ok {
		panic(fmt.Sprintf("migrate: the old codec of %q is a %T, not a codec of %T",
			store.DataNamespace().Path(), o.oldCodec, *new(Old)))
	}
	return c
}

// Convert converts the records of a store inside a migration, as Records does. It is for a
// custom migration that has more to do than that. It uses the cursor of the Migrator, so a
// migration converts one store with it, as the last thing that it does.
func Convert[K, Old, New any](
	ctx context.Context,
	m *Migrator,
	store Store[K, New],
	convert func(key K, old *Old) (*New, error),
	opts ...Option,
) error {
	var o options
	old := oldCodec[K, Old](store, opts, &o)
	ns := store.DataNamespace()

	type change struct {
		key   []byte
		value []byte
		drop  bool
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		// A batch is read and converted first, and written in a step that saves its last key.
		// Nothing else writes to the store while it is migrated.
		var batch []change
		err := m.DB().View(func(tx kv.Tx) error {
			var start []byte
			if cursor := m.Cursor(); cursor != nil {
				// The first key after the one that was converted last.
				start = append(bytes.Clone(cursor), 0x00)
			}
			it, err := ns.Open(tx).Iterator(start, nil)
			if err != nil {
				return err
			}
			defer it.Close()

			size := 0
			for ; it.Valid() && len(batch) < o.batch && size < o.bytes; it.Next() {
				rawKey := bytes.Clone(it.Key())
				c, err := convertOne(store, old, convert, rawKey, it)
				if err != nil {
					return fmt.Errorf("record %x of %q: %w", rawKey, ns.Path(), err)
				}
				batch = append(batch, change{key: rawKey, value: c, drop: c == nil})
				size += len(rawKey) + len(c)
			}
			return nil
		})
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}

		err = m.Step(batch[len(batch)-1].key, func(tx kv.Tx) error {
			records := ns.Open(tx)
			for _, c := range batch {
				var err error
				if c.drop {
					err = records.Delete(c.key)
				} else {
					err = records.Set(c.key, c.value)
				}
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
}

// convertOne returns the new encoded value of the record that an iterator is at, or nil if
// the record is to be deleted.
func convertOne[K, Old, New any](
	store Store[K, New],
	old codec.ValueCodec[Old],
	convert func(key K, old *Old) (*New, error),
	rawKey []byte,
	it kv.Iterator,
) ([]byte, error) {
	key, err := store.KeyCodec().Decode(rawKey)
	if err != nil {
		return nil, fmt.Errorf("failed to decode the key: %w", err)
	}
	value, err := it.Value()
	if err != nil {
		return nil, err
	}
	was, err := old.Decode(value)
	if err != nil {
		return nil, fmt.Errorf("failed to decode the record as it was: %w", err)
	}

	is, err := convert(key, &was)
	if err != nil || is == nil {
		return nil, err
	}
	bz, err := store.ValueCodec().Encode(*is)
	if err != nil {
		return nil, fmt.Errorf("failed to encode the record: %w", err)
	}
	if bz == nil {
		// An empty value is a value, and is not a record that is deleted.
		bz = []byte{}
	}
	return bz, nil
}
