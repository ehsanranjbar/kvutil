// Package keyspace hands out short prefixes by name, so that the stores of a database can be
// given long and readable names and still keep short keys. It also finds the stores whose names
// are in the database and are no longer claimed by the program, and retires them.
//
// It is optional: a store takes any prefix, and the prefixes of a program can as well be
// written by hand.
//
//	ks, err := keyspace.Open(db)
//	pets := record.New[int64, Pet](ks.Prefix("pets"))
//	owners := record.New[int64, Owner](ks.Prefix("owners"))
//	ks.Retire("toys") // the store of the toys is gone from the program
//
//	report, err := ks.Migrate(ctx, pets, owners)
package keyspace

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/migrate"
)

// Keyspace gives names their prefixes, which are the same every time a database is opened.
// It is safe for concurrent use.
type Keyspace struct {
	db  kv.DB
	reg *registry

	mu      sync.Mutex
	claimed map[string]bool
	retired map[string]bool
}

type options struct {
	keyLen int
	key    []byte
}

// Option is an option of a Keyspace.
type Option func(*options)

// WithKeyLen sets the length of the prefixes that are handed out, which is 1 by default.
// A keyspace can hand out 255 prefixes of a length of 1.
func WithKeyLen(n int) Option {
	return func(o *options) {
		o.keyLen = n
	}
}

// WithRegistryKey sets the key that the registry of the names is stored at. By default it is
// the key of zeros that has the length of the prefixes.
func WithRegistryKey(key []byte) Option {
	return func(o *options) {
		o.key = bytes.Clone(key)
	}
}

// Open creates a Keyspace and loads the names that the database has.
func Open(db kv.DB, opts ...Option) (*Keyspace, error) {
	o := options{keyLen: 1}
	for _, opt := range opts {
		opt(&o)
	}

	reg, err := newRegistry(db, o.keyLen, o.key)
	if err != nil {
		return nil, fmt.Errorf("keyspace: %w", err)
	}
	return &Keyspace{
		db:      db,
		reg:     reg,
		claimed: map[string]bool{},
		retired: map[string]bool{},
	}, nil
}

// Prefix returns the prefix of a name, which it allocates if the name has none yet, and marks
// the name as claimed by the program. It panics on an empty name, and if the registry cannot
// give or remember the prefix: that is a mistake of the definitions, which are made in code
// when the program starts, like the one of a regular expression that does not compile.
func (k *Keyspace) Prefix(name string) []byte {
	if name == "" {
		panic("keyspace: a name must not be empty")
	}

	prefix, err := k.reg.name(name)
	if err != nil {
		panic(fmt.Sprintf("keyspace: the prefix of %q: %v", name, err))
	}

	k.mu.Lock()
	k.claimed[name] = true
	k.mu.Unlock()
	return prefix
}

// Retire says that the data of a name, which the program has no store for any more, is to be
// deleted by Migrate, and the name forgotten. Without it, a name that the database has and the
// program does not claim is migrate.ErrUnknown. A name that the database does not have is
// nothing to do, so the call can stay for as long as a database that had the name may be met.
// A name that is claimed too is an error. It panics on an empty name.
func (k *Keyspace) Retire(name string) {
	if name == "" {
		panic("keyspace: a name must not be empty")
	}

	k.mu.Lock()
	defer k.mu.Unlock()

	k.retired[name] = true
}

// Name returns the name that a prefix was handed out for.
func (k *Keyspace) Name(prefix []byte) (string, bool) {
	return k.reg.nameOf(prefix)
}

// Migrate brings the database to what the program defines. It deletes the data of the names that
// were retired, and forgets them, and then runs the units; see migrate.Run. The units whose
// prefixes were handed out by the keyspace go by the names they were handed out for in the
// report and in errors. A name that the database has and the program neither claims nor
// retires is migrate.ErrUnknown, and nothing is changed then.
func (k *Keyspace) Migrate(ctx context.Context, units ...migrate.Migratable) (*migrate.Report, error) {
	retire, err := k.compare(units)
	if err != nil {
		return &migrate.Report{}, err
	}

	report := &migrate.Report{}
	for _, name := range retire {
		key, ok := k.reg.lookup(name)
		if !ok {
			continue
		}
		if err := kv.DeleteRange(ctx, k.db, key, kv.PrefixEnd(key)); err != nil {
			return report, fmt.Errorf("keyspace: failed to delete the data of %q: %w", name, err)
		}
		// The name goes last, so that a deletion that is interrupted is done again.
		if err := k.reg.forget(name); err != nil {
			return report, fmt.Errorf("keyspace: failed to forget %q: %w", name, err)
		}
		report.Actions = append(report.Actions, migrate.Action{What: migrate.Retired, Kind: "unit", Name: name})
	}

	rest, err := migrate.Run(ctx, k.db, units...)
	if rest != nil {
		report.Actions = append(report.Actions, rest.Actions...)
	}
	return report, err
}

// Check compares the database with what the program defines, without changing it; see
// migrate.Check. A name that is to be retired and is still in the database is
// migrate.ErrNeedsMigration.
func (k *Keyspace) Check(units ...migrate.Migratable) error {
	retire, err := k.compare(units)
	if err != nil {
		return err
	}
	if len(retire) > 0 {
		return fmt.Errorf("%w: %q is to be retired", migrate.ErrNeedsMigration, retire)
	}
	return migrate.Check(k.db, units...)
}

// compare names the units, and finds the names in the database that are to be retired. It
// fails for what Migrate and Check fail for.
func (k *Keyspace) compare(units []migrate.Migratable) (retire []string, err error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	var errs []error
	for _, m := range units {
		if m == nil || m.Unit() == nil {
			continue
		}
		u := m.Unit()
		name, ok := k.reg.nameOf(u.Prefix())
		if !ok {
			continue
		}
		u.SetName(name)
		if k.retired[name] {
			errs = append(errs, fmt.Errorf("keyspace: %q is retired and has a unit", name))
		}
	}

	for _, name := range k.reg.names() {
		switch {
		case k.retired[name] && k.claimed[name]:
			errs = append(errs, fmt.Errorf("keyspace: %q is retired and claimed", name))
		case k.retired[name]:
			if key, _ := k.reg.lookup(name); bytes.HasPrefix(k.reg.key, key) {
				errs = append(errs, fmt.Errorf("keyspace: the prefix of %q holds the registry itself", name))
				continue
			}
			retire = append(retire, name)
		case !k.claimed[name]:
			errs = append(errs, fmt.Errorf("%w: %q is in the database and is not claimed: Retire(%q) deletes it",
				migrate.ErrUnknown, name, name))
		}
	}

	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	slices.Sort(retire)
	return retire, nil
}
