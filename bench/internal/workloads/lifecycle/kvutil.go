package lifecycle

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/kvsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/wl"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/migrate"
	"github.com/ehsanranjbar/kvutil/store/record"
)

// orderV2 is the record after the value change: the fields of dataset.Order and a priority
// that is derived from the total.
type orderV2 struct {
	ID       int64 `msgpack:"-"`
	Code     string
	Customer string
	Name     string
	Status   string
	Total    int64
	Qty      int32
	At       time.Time
	Note     *string
	Tags     []string
	Payload  []byte
	Priority int32
}

func (o *orderV2) GetId() int64    { return o.ID }
func (o *orderV2) SetId(id int64)  { o.ID = id }
func (o *orderV2) priority() int32 { return o.Priority }

// orderV3Flat is orderV2 with one more field, which an old record does not have: the change that
// needs no migration, as the codec leaves out what it does not know.
type orderV3Flat struct {
	ID       int64 `msgpack:"-"`
	Code     string
	Customer string
	Name     string
	Status   string
	Total    int64
	Qty      int32
	At       time.Time
	Note     *string
	Tags     []string
	Payload  []byte
	Priority int32
	Extra    string
}

func (o *orderV3Flat) GetId() int64    { return o.ID }
func (o *orderV3Flat) SetId(id int64)  { o.ID = id }
func (o *orderV3Flat) priority() int32 { return o.Priority }

// priorityOf is the priority that the value change gives a record: its total in tens of
// thousands, 0 to 9.
func priorityOf(total int64) int32 { return int32(total / 10000) }

// lookup answers a probe through an index.
type lookup func(tx kv.Tx, p probe) ([]int64, error)

type idIterator interface {
	Valid() bool
	Next()
	Key() (int64, error)
	Err() error
	Close() error
}

func collect(it idIterator, err error) ([]int64, error) {
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var ids []int64
	for ; it.Valid(); it.Next() {
		id, err := it.Key()
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, it.Err()
}

func seconds(s int64) time.Time { return time.Unix(s, 0).UTC() }

// defineIndexes adds the indexes of the kinds to a store of any version of the record (the fields
// are found by name) and returns how each answers a probe.
func defineIndexes[V any](st *record.Store[int64, V], kinds []string) map[string]lookup {
	fCustomer := field.Path[V, string]("Customer")
	fStatus := field.Path[V, string]("Status")
	fTotal := field.Path[V, int64]("Total")
	fAt := field.Path[V, time.Time]("At")
	fCode := field.Path[V, string]("Code")
	fTags := field.Path[V, string]("Tags")

	look := map[string]lookup{}
	for _, kind := range kinds {
		name := "by_" + kind
		switch kind {
		case "customer":
			x := index.On(st, name, fCustomer)
			look[kind] = func(tx kv.Tx, p probe) ([]int64, error) { return collect(x.Instantiate(tx).Match(p.S)) }
		case "status":
			x := index.On(st, name, fStatus)
			look[kind] = func(tx kv.Tx, p probe) ([]int64, error) { return collect(x.Instantiate(tx).Match(p.S)) }
		case "total":
			x := index.On(st, name, fTotal)
			look[kind] = func(tx kv.Tx, p probe) ([]int64, error) {
				return collect(x.Instantiate(tx).Range(kvutil.Between(p.Lo, p.Hi)))
			}
		case "code":
			x := index.Unique(st, name, fCode)
			look[kind] = func(tx kv.Tx, p probe) ([]int64, error) { return collect(x.Instantiate(tx).Match(p.S)) }
		case "composite":
			x := index.Composite(st, name, field.List[V](fCustomer, fAt))
			look[kind] = func(tx kv.Tx, p probe) ([]int64, error) {
				return collect(x.Instantiate(tx).Lookup(fCustomer.Eq(p.S), fAt.Between(seconds(p.Lo), seconds(p.Hi))))
			}
		case "tags":
			x := index.On(st, name, fTags)
			look[kind] = func(tx kv.Tx, p probe) ([]int64, error) { return collect(x.Instantiate(tx).Match(p.S)) }
		default:
			panic("unknown index kind " + kind)
		}
	}
	return look
}

// stage is the store of a version of the record with a set of indexes, as a program of that
// version defines it.
type stage interface {
	migrate(ctx context.Context, db kv.DB) (*migrate.Report, error)
	lookups() map[string]lookup
	// each reads every record: its id and its priority (0 before the value change).
	each(tx kv.Tx, fn func(id int64, priority int32)) error
	// has reports whether a record is there.
	has(tx kv.Tx, id int64) (bool, error)
	keyCodec() func(id int64) ([]byte, error)
	dataPrefix() []byte
	encode(o *dataset.Order) ([]byte, error)
}

type prioritized interface{ priority() int32 }

type stageOf[V any, PV record.Record[int64, V]] struct {
	st   *record.Store[int64, V]
	look map[string]lookup
	conv func(o *dataset.Order) *V
}

func newStage[V any, PV record.Record[int64, V]](
	version uint64, kinds []string, conv func(*dataset.Order) *V, configure func(*record.Store[int64, V]),
) *stageOf[V, PV] {
	st := record.New[int64, V, PV]("rec").WithVersion(version)
	if configure != nil {
		configure(st)
	}
	return &stageOf[V, PV]{st: st, look: defineIndexes(st, kinds), conv: conv}
}

func (s *stageOf[V, PV]) migrate(ctx context.Context, db kv.DB) (*migrate.Report, error) {
	return s.st.Migrate(ctx, db)
}
func (s *stageOf[V, PV]) lookups() map[string]lookup { return s.look }
func (s *stageOf[V, PV]) dataPrefix() []byte         { return s.st.DataPrefix() }
func (s *stageOf[V, PV]) keyCodec() func(int64) ([]byte, error) {
	return s.st.KeyCodec().Encode
}
func (s *stageOf[V, PV]) encode(o *dataset.Order) ([]byte, error) {
	return s.st.ValueCodec().Encode(*s.conv(o))
}
func (s *stageOf[V, PV]) has(tx kv.Tx, id int64) (bool, error) { return s.st.Instantiate(tx).Has(id) }
func (s *stageOf[V, PV]) each(tx kv.Tx, fn func(int64, int32)) error {
	it, err := s.st.Instantiate(tx).All()
	if err != nil {
		return err
	}
	defer it.Close()
	for ; it.Valid(); it.Next() {
		id, err := it.Key()
		if err != nil {
			return err
		}
		v, err := it.Value()
		if err != nil {
			return err
		}
		var prio int32
		if p, ok := any(v).(prioritized); ok {
			prio = p.priority()
		}
		fn(id, prio)
	}
	return nil
}

func stageV1(kinds []string) stage {
	return newStage[dataset.Order, *dataset.Order](1, kinds, func(o *dataset.Order) *dataset.Order { return o }, nil)
}

func orderToV2(o *dataset.Order) *orderV2 {
	return &orderV2{
		ID: o.ID, Code: o.Code, Customer: o.Customer, Name: o.Name, Status: o.Status, Total: o.Total,
		Qty: o.Qty, At: o.At, Note: o.Note, Tags: o.Tags, Payload: o.Payload, Priority: priorityOf(o.Total),
	}
}

// stageV2 is the store after the value change: version 2, with the migration from version 1 that
// computes the priority of every record.
func stageV2(kinds []string) stage {
	return newStage[orderV2, *orderV2](2, kinds, orderToV2, func(st *record.Store[int64, orderV2]) {
		st.MigrateFrom(1, func(id int64, old *dataset.Order) (*orderV2, error) {
			return orderToV2(old), nil
		})
	})
}

// stageV3 is the store with a field added to the record, in the same version: no migration.
func stageV3(kinds []string) stage {
	return newStage[orderV3Flat, *orderV3Flat](2, kinds, func(o *dataset.Order) *orderV3Flat {
		v := orderToV2(o)
		return &orderV3Flat{ID: v.ID, Code: v.Code, Customer: v.Customer, Name: v.Name, Status: v.Status,
			Total: v.Total, Qty: v.Qty, At: v.At, Note: v.Note, Tags: v.Tags, Payload: v.Payload, Priority: v.Priority}
	}, func(st *record.Store[int64, orderV3Flat]) {
		// The migration of the version before is still in the program, as a program that is
		// older than its database keeps it.
		st.MigrateFrom(1, func(id int64, old *dataset.Order) (*orderV3Flat, error) {
			v := orderToV2(old)
			return &orderV3Flat{ID: v.ID, Priority: v.Priority, Code: v.Code, Customer: v.Customer, Name: v.Name,
				Status: v.Status, Total: v.Total, Qty: v.Qty, At: v.At, Note: v.Note, Tags: v.Tags, Payload: v.Payload}, nil
		})
	})
}

// kvDriver is kvutil on a database of its own.
type kvDriver struct {
	sys   *kvsys.System
	db    *kvsys.DB
	cur   stage
	kinds []string
	// version is 1, 2 or 3: which store is the current one.
	version int
}

func openKVDriver(ctx context.Context, sys *kvsys.System) (driver, error) {
	db, err := sys.Open("lifecycle")
	if err != nil {
		return nil, err
	}
	d := &kvDriver{sys: sys, db: db, version: 1}
	d.cur = d.stageFor(nil)
	if _, err := d.cur.migrate(ctx, db); err != nil {
		db.Destroy()
		return nil, err
	}
	return d, nil
}

func (d *kvDriver) stageFor(kinds []string) stage {
	switch d.version {
	case 2:
		return stageV2(kinds)
	case 3:
		return stageV3(kinds)
	}
	return stageV1(kinds)
}

func (d *kvDriver) Load(_ context.Context, spec dataset.Spec, n int64) error {
	_, err := wl.BulkLoad(d.db, 1, n+1, 1, func() func(kv.BulkWriter, int64) error {
		o := &dataset.Order{}
		return func(w kv.BulkWriter, id int64) error {
			spec.GenInto(o, id, 0)
			k, err := d.cur.keyCodec()(id)
			if err != nil {
				return err
			}
			v, err := d.cur.encode(o)
			if err != nil {
				return err
			}
			return w.Set(append(bytes.Clone(d.cur.dataPrefix()), k...), v)
		}
	})
	return err
}

func count(r *migrate.Report, what string) int {
	n := 0
	for _, a := range r.Actions {
		if a.What == what {
			n++
		}
	}
	return n
}

// AddIndexes defines the indexes on the store and migrates: the database has the records and
// not the indexes, so Migrate builds them.
func (d *kvDriver) AddIndexes(ctx context.Context, kinds []string) error {
	next := append(append([]string(nil), d.kinds...), kinds...)
	st := d.stageFor(next)
	report, err := st.migrate(ctx, d.db)
	if err != nil {
		return err
	}
	if got := count(report, migrate.Built); got != len(kinds) {
		return fmt.Errorf("the migration built %d indexes, not %d: %s", got, len(kinds), report)
	}
	d.cur, d.kinds = st, next
	return nil
}

// DropIndexes defines the store without the indexes and migrates, which deletes their entries.
func (d *kvDriver) DropIndexes(ctx context.Context, kinds []string) error {
	var next []string
	for _, k := range d.kinds {
		keep := true
		for _, drop := range kinds {
			if k == drop {
				keep = false
			}
		}
		if keep {
			next = append(next, k)
		}
	}
	st := d.stageFor(next)
	report, err := st.migrate(ctx, d.db)
	if err != nil {
		return err
	}
	if got := count(report, migrate.Dropped); got != len(kinds) {
		return fmt.Errorf("the migration dropped %d indexes, not %d: %s", got, len(kinds), report)
	}
	d.cur, d.kinds = st, next
	return nil
}

// CreateConcurrently has no counterpart: Migrate must be alone with the data.
func (d *kvDriver) CreateConcurrently(context.Context, string) error { return errUnsupported }

func (d *kvDriver) Probe(_ context.Context, kind string, p probe) ([]int64, error) {
	look, ok := d.cur.lookups()[kind]
	if !ok {
		return nil, fmt.Errorf("no index %q", kind)
	}
	var ids []int64
	err := d.db.View(func(tx kv.Tx) error {
		var err error
		ids, err = look(tx, p)
		return err
	})
	return ids, err
}

// Convert raises the version of the store, with the migration that computes the priority of
// every record; with indexes on the store they are built again from the new records.
func (d *kvDriver) Convert(ctx context.Context, _ stepFunc) error {
	d.version = 2
	st := d.stageFor(d.kinds)
	report, err := st.migrate(ctx, d.db)
	if err != nil {
		return err
	}
	if count(report, migrate.Migrated) != 1 || count(report, migrate.Built) != len(d.kinds) {
		return fmt.Errorf("the migration did not convert the records and build %d indexes: %s", len(d.kinds), report)
	}
	d.cur = st
	return nil
}

func (d *kvDriver) Priority(_ context.Context, k int32) ([]int64, error) {
	var ids []int64
	err := d.db.View(func(tx kv.Tx) error {
		return d.cur.each(tx, func(id int64, prio int32) {
			if prio == k {
				ids = append(ids, id)
			}
		})
	})
	return ids, err
}

// AddField uses a store with one more field in the record and no new version: Migrate finds
// nothing to do.
func (d *kvDriver) AddField(ctx context.Context) error {
	d.version = 3
	st := d.stageFor(d.kinds)
	report, err := st.migrate(ctx, d.db)
	if err != nil {
		return err
	}
	if len(report.Actions) != 0 {
		return fmt.Errorf("adding a field needed a migration: %s", report)
	}
	d.cur = st
	return nil
}

func (d *kvDriver) Exists(_ context.Context, id int64) (bool, error) {
	var ok bool
	err := d.db.View(func(tx kv.Tx) error {
		var err error
		ok, err = d.cur.has(tx, id)
		return err
	})
	return ok, err
}

func (d *kvDriver) Settle(context.Context) error { return d.db.Sync() }

func (d *kvDriver) Close(context.Context) error { return d.db.Destroy() }

// ready is what a program does to be ready to use a database: define its stores and migrate
// (nothing to do), and read one record.
func (d *kvDriver) ready(ctx context.Context) error {
	st := d.stageFor(d.kinds)
	report, err := st.migrate(ctx, d.db)
	if err != nil {
		return err
	}
	if len(report.Actions) != 0 {
		return fmt.Errorf("the migration of a database that is up to date did something: %s", report)
	}
	d.cur = st
	_, err = d.Exists(ctx, 1)
	return err
}

// FreshOpen opens a new database with nothing in it, and is ready when the stores are recorded
// and a first read is answered.
func (d *kvDriver) FreshOpen(ctx context.Context) (time.Duration, error) {
	t0 := time.Now()
	db, err := d.sys.Open("lifecycle-fresh")
	if err != nil {
		return 0, err
	}
	fresh := &kvDriver{sys: d.sys, db: db, version: d.version, kinds: d.kinds}
	err = errFirst(
		func() error { _, err := fresh.stageFor(d.kinds).migrate(ctx, db); return err },
		func() error { fresh.cur = fresh.stageFor(d.kinds); _, err := fresh.Exists(ctx, 1); return err },
	)
	elapsed := time.Since(t0)
	db.Destroy()
	return elapsed, err
}

func errFirst(fns ...func() error) error {
	for _, fn := range fns {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}

// Reopen closes the database and opens it from its files, then is ready as above. It returns the
// times of the three parts.
func (d *kvDriver) Reopen(ctx context.Context) (closeD, openD, readyD time.Duration, err error) {
	t0 := time.Now()
	if err = d.db.Shutdown(); err != nil {
		return
	}
	closeD = time.Since(t0)
	t1 := time.Now()
	if err = d.db.Start(); err != nil {
		return
	}
	openD = time.Since(t1)
	t2 := time.Now()
	err = d.ready(ctx)
	readyD = time.Since(t2)
	return
}

func (d *kvDriver) Restart(ctx context.Context) (time.Duration, string, error) {
	closeD, openD, readyD, err := d.Reopen(ctx)
	return closeD + openD + readyD, "close and open of the database in the process", err
}

func (d *kvDriver) Sizes(context.Context) (int64, int64, error) {
	n, err := d.db.DiskBytes()
	return n, 0, err
}
