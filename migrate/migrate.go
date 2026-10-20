// Package migrate keeps what a store records of itself and brings a database to what a program
// defines: versions, migrations of the records, and the rebuilding of what is derived from them.
//
// Every store keeps a record of itself under its own prefix: its version, a fingerprint of what
// its data depends on, like the codecs, and the parts that depend on its data, like its
// indexes. It is a Unit. Run compares the units of a program with the records in the database
// when the program starts, and brings the database to them; Check only compares.
//
// A migration converts the records that an older version of a program wrote into the ones that
// the program reads now. It is a part of the program of the version that it leads to, like the
// type of the records of the version before, which is kept next to it for as long as a database
// of that version may be met:
//
//	pets := record.New[int64, Pet]("pets").WithVersion(2)
//
//	type PetV1 struct { ... }
//
//	migrate.Records(pets, 1, func(id int64, old *PetV1) (*Pet, error) {
//		return &Pet{Name: old.Name, Species: old.Kind}, nil
//	})
//
//	report, err := pets.Migrate(ctx, db)
//
// A store that went through more than one version has a migration for each of them, and the
// types of the versions in between stay next to the migrations. Every step is given the type
// that the version before wrote and returns the type of the next one, and the last step
// returns the type of the store:
//
//	pets := record.New[int64, Pet]("pets").WithVersion(3).
//		MigrateFrom(1, func(id int64, old *PetV1) (*PetV2, error) { ... }).
//		MigrateFrom(2, func(id int64, old *PetV2) (*Pet, error) { ... })
//
// A database of version 1 goes through both steps, and one of version 2 through the second.
// The records of a version that is not the last one are read and written with the default
// codec of their type, which is how a program of that version wrote them unless it gave the
// store another codec. For a program that did, the codec is given to the step that reads the
// type, with WithOldCodec, and to the step that writes it, with WithNewCodec. The type of the
// store is always read and written with the codec of the store.
//
// A value that only gets fields that it did not have needs no migration in the formats that
// leave out what they do not know, like the default one.
package migrate

import (
	"bytes"
	"context"
	"fmt"
	"reflect"

	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/prefix"
)

// Store is a store whose records are migrated. It is implemented by the stores of the
// packages typed, ext and record.
type Store[K, V any] interface {
	// KeyCodec returns the key codec of the store.
	KeyCodec() codec.KeyCodec[K]
	// ValueCodec returns the value codec of the store.
	ValueCodec() codec.ValueCodec[V]
	// DataPrefix returns the prefix that the records are kept under.
	DataPrefix() []byte
	// Unit returns the unit of the store, which the migrations are added to.
	Unit() *Unit
}

type options struct {
	batch    int
	bytes    int
	oldCodec any
	newCodec any
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
// has changed too, or for a type of an earlier version that its program wrote with another
// codec than the default one. Without it the records are read with the codec of the store if
// their type is the type of the store, and with the default codec of their type otherwise;
// see codec.ValueFor.
func WithOldCodec[Old any](c codec.ValueCodec[Old]) Option {
	return func(o *options) {
		o.oldCodec = c
	}
}

// WithNewCodec sets the codec that the converted records are written with, for a type of a
// version in between, which is not the type of the store. Without it they are written with the
// default codec of their type; see codec.ValueFor. The step that reads them later is given the
// same codec, with WithOldCodec. It has no effect on a migration to the type of the store,
// which is always written with the codec of the store.
func WithNewCodec[New any](c codec.ValueCodec[New]) Option {
	return func(o *options) {
		o.newCodec = c
	}
}

// Records adds the migration of the records of a store from a version to the next one: a
// function that is given every record as it was written, in the type that it had, and returns
// it as it is to be. A record that the function returns nil for is deleted.
//
// The types of the old and the new records are independent of the type of the store, so a store
// that went through several versions is migrated through the types of the versions in between,
// one step for each. The new type of the last step, the one that leads to the version of the
// store, must be the type of the store; it is an error when the migration runs otherwise, before
// any record is converted. The records of a type other than the one of the store are read and
// written with the default codec of the type unless WithOldCodec and WithNewCodec say
// otherwise.
//
// The records are converted in place, under the keys that they have, a batch at a time. The
// migration goes on from where it stopped when it is interrupted, and no record is converted
// twice. The indexes and the aggregations of the store are built again from the new records
// once it is done, so the function has nothing to do for them.
//
// It panics if the options are not for the types of the old and the new records.
func Records[K, V, Old, New any](
	store Store[K, V],
	from uint64,
	convert func(key K, old *Old) (*New, error),
	opts ...Option,
) {
	// The options are checked when the migration is defined, as a mistake in them is one of
	// the program.
	var o options
	resolve[K, V, Old, New](store, opts, &o)

	store.Unit().AddMigration(from, func(ctx context.Context, m *Migrator) error {
		return Convert(ctx, m, store, convert, opts...)
	})
}

// Custom adds a migration of the unit of something from a version to the next one that is
// any function, for what is not the conversion of one record into one record. The function
// is given the database, and saves how far it has come with the steps of the Migrator if it
// is to go on from there when it is interrupted. It can use Convert for a store.
func Custom(of Migratable, from uint64, m Migration) {
	of.Unit().AddMigration(from, m)
}

// resolve applies the options and returns the codecs that the old records are read with and the
// new ones are written with. It panics if a codec of the options is not for its type.
func resolve[K, V, Old, New any](
	store Store[K, V],
	opts []Option,
	o *options,
) (old codec.ValueCodec[Old], is codec.ValueCodec[New]) {
	*o = options{batch: 256, bytes: 4 << 20}
	for _, opt := range opts {
		opt(o)
	}
	name := store.Unit().Name()

	// Reading: the option, the codec of the store for the type of the store, or the default.
	switch c := o.oldCodec.(type) {
	case nil:
		if reflect.TypeFor[Old]() == reflect.TypeFor[V]() {
			old = any(store.ValueCodec()).(codec.ValueCodec[Old])
		} else {
			old = codec.ValueFor[Old](nil)
		}
	case codec.ValueCodec[Old]:
		old = c
	default:
		panic(fmt.Sprintf("migrate: the old codec of %q is a %T, not a codec of %v",
			name, o.oldCodec, reflect.TypeFor[Old]()))
	}

	// Writing: the codec of the store for the type of the store, otherwise the option or
	// the default. The option is checked even where it has no effect.
	var given codec.ValueCodec[New]
	if o.newCodec != nil {
		var ok bool
		if given, ok = o.newCodec.(codec.ValueCodec[New]); !ok {
			panic(fmt.Sprintf("migrate: the new codec of %q is a %T, not a codec of %v",
				name, o.newCodec, reflect.TypeFor[New]()))
		}
	}
	switch {
	case reflect.TypeFor[New]() == reflect.TypeFor[V]():
		is = any(store.ValueCodec()).(codec.ValueCodec[New])
	case given != nil:
		is = given
	default:
		is = codec.ValueFor[New](nil)
	}
	return old, is
}

// Convert converts the records of a store inside a migration, as Records does, with the same
// types and codecs. It is for a custom migration that has more to do than that. It uses the
// cursor of the Migrator, so a migration converts one store with it, as the last thing that it
// does.
//
// It fails before it converts anything if the migration is the last one of the unit, the one
// from the version before its version, and the new type is not the type of the store.
func Convert[K, V, Old, New any](
	ctx context.Context,
	m *Migrator,
	store Store[K, V],
	convert func(key K, old *Old) (*New, error),
	opts ...Option,
) error {
	var o options
	old, is := resolve[K, V, Old, New](store, opts, &o)
	dataPrefix := store.DataPrefix()
	name := store.Unit().Name()

	// The records of the version of the store are of the type of the store.
	if m.From()+1 == store.Unit().Version() && reflect.TypeFor[New]() != reflect.TypeFor[V]() {
		return fmt.Errorf("the last migration of %q, from version %d to %d, gives %v, not %v, which is the type of the store",
			name, m.From(), m.From()+1, reflect.TypeFor[New](), reflect.TypeFor[V]())
	}

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
			it, err := prefix.New(tx, dataPrefix).Iterator(start, nil)
			if err != nil {
				return err
			}
			defer it.Close()

			size := 0
			for ; it.Valid() && len(batch) < o.batch && size < o.bytes; it.Next() {
				rawKey := bytes.Clone(it.Key())
				c, err := convertOne(store.KeyCodec(), old, is, convert, rawKey, it)
				if err != nil {
					return fmt.Errorf("record %x of %q: %w", rawKey, name, err)
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
			records := prefix.New(tx, dataPrefix)
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
	keys codec.KeyCodec[K],
	old codec.ValueCodec[Old],
	is codec.ValueCodec[New],
	convert func(key K, old *Old) (*New, error),
	rawKey []byte,
	it kv.Iterator,
) ([]byte, error) {
	key, err := keys.Decode(rawKey)
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

	converted, err := convert(key, &was)
	if err != nil || converted == nil {
		return nil, err
	}
	bz, err := is.Encode(*converted)
	if err != nil {
		return nil, fmt.Errorf("failed to encode the record: %w", err)
	}
	if bz == nil {
		// An empty value is a value, and is not a record that is deleted.
		bz = []byte{}
	}
	return bz, nil
}
