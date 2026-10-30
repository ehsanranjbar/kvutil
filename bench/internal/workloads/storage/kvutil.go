package storage

import (
	"context"
	"fmt"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/dockerapi"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/kvsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/wl"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/badgerkv"
	"github.com/ehsanranjbar/kvutil/migrate"
	"github.com/ehsanranjbar/kvutil/store/record"
	"github.com/ehsanranjbar/kvutil/store/typed"
)

type order = dataset.Order

var (
	fCustomer = field.Path[order, string]("Customer")
	fCode     = field.Path[order, string]("Code")
	fAt       = field.Path[order, time.Time]("At")
	fTags     = field.Path[order, string]("Tags")
)

// kvDriver is kvutil on a database of its own.
type kvDriver struct {
	sys   *kvsys.System
	db    *kvsys.DB
	meter *dockerapi.Meter

	store *record.Store[int64, order]
	kinds []string
	// prefixes of the indexes, to add up their bytes.
	prefixes map[string][]byte
	// physical bytes: the disk after the load, and what each index added to it.
	tableDisk  int64
	indexDisks map[string]int64

	plain, ttl *typed.Store[int64, order]
}

func openKV(ctx context.Context, sys *kvsys.System, v *variant, meter *dockerapi.Meter) (driver, error) {
	t := &kvsys.Tuning{}
	if v.what == "footprint" {
		// The garbage of the value log is collected by the workload, when it says so.
		t.KV = append(t.KV, badgerkv.WithoutValueLogGC())
	}
	if v.vlog {
		t.Badger = func(o badger.Options) badger.Options {
			return o.WithValueThreshold(64).WithValueLogFileSize(16 << 20)
		}
	}
	db, err := sys.OpenWith("storage-"+v.name, 0, t)
	if err != nil {
		return nil, err
	}
	d := &kvDriver{sys: sys, db: db, meter: meter, indexDisks: map[string]int64{}}
	if err := d.define(ctx, nil); err != nil {
		db.Destroy()
		return nil, err
	}
	return d, nil
}

// define makes the store with the indexes of the kinds and migrates the database to it.
func (d *kvDriver) define(ctx context.Context, kinds []string) error {
	st := record.New[int64, order]("rec")
	prefixes := map[string][]byte{}
	for _, kind := range kinds {
		name := "by_" + kind
		switch kind {
		case "customer":
			prefixes[kind] = index.On(st, name, fCustomer).Prefix()
		case "code":
			prefixes[kind] = index.Unique(st, name, fCode).Prefix()
		case "composite":
			prefixes[kind] = index.Composite(st, name, field.List[order](fCustomer, fAt)).Prefix()
		case "tags":
			prefixes[kind] = index.On(st, name, fTags).Prefix()
		default:
			panic("unknown index " + kind)
		}
	}
	report, err := st.Migrate(ctx, d.db)
	if err != nil {
		return err
	}
	built := 0
	for _, a := range report.Actions {
		if a.What == migrate.Built {
			built++
		}
	}
	if want := len(kinds) - len(d.kinds); built != want {
		return fmt.Errorf("the migration built %d indexes, not %d: %s", built, want, report)
	}
	d.store, d.kinds, d.prefixes = st, kinds, prefixes
	return nil
}

func (d *kvDriver) disk() int64 {
	n, _ := d.db.DiskBytes()
	return n
}

func (d *kvDriver) Load(_ context.Context, spec dataset.Spec, n int64) error {
	_, err := wl.BulkLoad(d.db, 1, n+1, 1, func() func(kv.BulkWriter, int64) error {
		o := &order{}
		return func(w kv.BulkWriter, id int64) error {
			spec.GenInto(o, id, 0)
			k, err := d.store.KeyCodec().Encode(id)
			if err != nil {
				return err
			}
			v, err := d.store.ValueCodec().Encode(*o)
			if err != nil {
				return err
			}
			return w.Set(append(append([]byte(nil), d.store.DataPrefix()...), k...), v)
		}
	})
	if err != nil {
		return err
	}
	if err := d.Flush(context.Background()); err != nil {
		return err
	}
	d.tableDisk = d.disk()
	return nil
}

// AddIndex defines the store with one more index and migrates, which builds it over the records.
func (d *kvDriver) AddIndex(ctx context.Context, kind string) error {
	if err := d.Flush(ctx); err != nil {
		return err
	}
	before := d.disk()
	if err := d.define(ctx, append(append([]string(nil), d.kinds...), kind)); err != nil {
		return err
	}
	if err := d.Flush(ctx); err != nil {
		return err
	}
	d.indexDisks[kind] = d.disk() - before
	return nil
}

// logicalBytes adds up the lengths of the keys and values under a prefix: what is stored before
// Badger compresses and lays it out.
func (d *kvDriver) logicalBytes(prefix []byte) (float64, error) {
	var total int64
	err := d.db.View(func(tx kv.Tx) error {
		it, err := tx.Iterator(prefix, kv.PrefixEnd(prefix))
		if err != nil {
			return err
		}
		defer it.Close()
		for ; it.Valid(); it.Next() {
			v, err := it.Value()
			if err != nil {
				return err
			}
			total += int64(len(it.Key()) + len(v))
		}
		return nil
	})
	return float64(total), err
}

// Components: the disk the load took and what each index added to it (approximate, as Badger
// compacts in the background), and the logical bytes under each prefix.
func (d *kvDriver) Components(ctx context.Context) (map[string]float64, error) {
	out := map[string]float64{"table_bytes": float64(d.tableDisk), "total_bytes": float64(d.disk())}
	table, err := d.logicalBytes(d.store.DataPrefix())
	if err != nil {
		return nil, err
	}
	out["table_logical_bytes"] = table
	logical := table
	for _, kind := range d.kinds {
		out["index_"+kind+"_bytes"] = float64(d.indexDisks[kind])
		b, err := d.logicalBytes(d.prefixes[kind])
		if err != nil {
			return nil, err
		}
		out["index_"+kind+"_logical_bytes"] = b
		logical += b
	}
	out["total_logical_bytes"] = logical
	return out, nil
}

func (d *kvDriver) Rewrite(ctx context.Context, spec dataset.Spec, n int64, version uint32) error {
	_, err := wl.TxLoad(ctx, d.db, 1, n+1, 500, 1, func() func(kv.Tx, int64) error {
		o := &order{}
		return func(tx kv.Tx, id int64) error {
			spec.GenInto(o, id, version)
			return d.store.Instantiate(tx).Set(o)
		}
	})
	return err
}

func (d *kvDriver) DeleteOdd(ctx context.Context, n int64) error {
	_, err := wl.TxLoad(ctx, d.db, 1, n+1, 500, 1, func() func(kv.Tx, int64) error {
		return func(tx kv.Tx, id int64) error {
			if id%2 == 0 {
				return nil
			}
			return d.store.Instantiate(tx).Delete(id)
		}
	})
	return err
}

func (d *kvDriver) Count(context.Context) (int64, error) {
	var n int64
	err := d.db.View(func(tx kv.Tx) error {
		it, err := d.store.Instantiate(tx).All()
		if err != nil {
			return err
		}
		defer it.Close()
		for ; it.Valid(); it.Next() {
			n++
		}
		return nil
	})
	return n, err
}

// Reclaims: the collection of the value log, which rewrites the files of which enough is
// garbage; Flatten, which compacts the tree into its last level (dropping the old versions and the
// deleted keys), and the collection again, which can use what the compaction found to be garbage.
func (d *kvDriver) Reclaims() []reclaim {
	return []reclaim{
		{"value-log-gc", func(context.Context) error { return d.db.RunValueLogGC() }},
		{"flatten", func(context.Context) error { return d.db.Flatten(4) }},
		{"value-log-gc-after-flatten", func(context.Context) error { return d.db.RunValueLogGC() }},
	}
}

func (d *kvDriver) ExpiryPrepare(ctx context.Context) error {
	d.plain = typed.New[int64, order]("plain")
	d.ttl = typed.New[int64, order]("ttl")
	_, err := migrate.Run(ctx, d.db, d.plain, d.ttl)
	return err
}

func (d *kvDriver) ExpiryInsert(_ context.Context, o *dataset.Order, ttl time.Duration) error {
	return d.db.Update(func(tx kv.Tx) error {
		if ttl > 0 {
			return d.ttl.Instantiate(tx).SetWithTTL(o.ID, o, ttl)
		}
		return d.plain.Instantiate(tx).Set(o.ID, o)
	})
}

// ExpiryStartJob: kvutil has no job; Badger drops an expired key when it compacts the tables
// that hold it, and a read does not see it from the moment it expired.
func (d *kvDriver) ExpiryStartJob(context.Context, time.Duration) func() map[string]float64 {
	return func() map[string]float64 { return nil }
}

func (d *kvDriver) ExpiryLive(context.Context) (plain, expiring int64, err error) {
	count := func(st *typed.Store[int64, order]) (n int64, err error) {
		err = d.db.View(func(tx kv.Tx) error {
			it, err := st.Instantiate(tx).All()
			if err != nil {
				return err
			}
			defer it.Close()
			for ; it.Valid(); it.Next() {
				n++
			}
			return nil
		})
		return
	}
	if plain, err = count(d.plain); err != nil {
		return
	}
	expiring, err = count(d.ttl)
	return
}

// Flush closes the database and opens it again: the memtable is written to a table of the
// tree, and everything is synced.
func (d *kvDriver) Flush(context.Context) error { return d.db.Reopen() }

func (d *kvDriver) Sizes(context.Context) (int64, int64, error) {
	n, err := d.db.DiskBytes()
	return n, 0, err
}

func (d *kvDriver) Meter() *dockerapi.Meter { return d.meter }

func (d *kvDriver) Counters(context.Context) map[string]float64 { return nil }

func (d *kvDriver) Close(context.Context) error { return d.db.Destroy() }
