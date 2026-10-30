package aggs

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"github.com/ehsanranjbar/kvutil/agg"
	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/kvsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/wl"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/migrate"
	"github.com/ehsanranjbar/kvutil/store/record"
)

type event = dataset.Event

type eventStore = record.Store[int64, event]

// kvAggs is the record store of the events and the three aggregations that are defined on it:
//
//	amount_sum   agg.Sum of Amount for each customer, by Daily, Weekly, Monthly and AllTime
//	event_count  agg.Count of the events of each customer, by the same buckets
//	amount_max   agg.Max of Amount for each customer, by Daily, Weekly and Monthly: a recomputed
//	             aggregation with WithMerge, so that only the days are stored and the weeks and
//	             months are put together from them when they are read
type kvAggs struct {
	store *eventStore
	sum   *agg.Aggregation[int64, event, string, int64, int64]
	count *agg.Aggregation[int64, event, string, int64, int64]
	max   *agg.Aggregation[int64, event, string, int64, int64]
}

// newKVStore returns the store of the events with its aggregations, or without them.
func newKVStore(withAggs bool) *kvAggs {
	st := record.New[int64, event]("events")
	a := &kvAggs{store: st}
	if !withAggs {
		return a
	}
	at := func(e *event) time.Time { return e.At }
	amount := func(e *event) (string, int64, bool) { return e.Customer, e.Amount, true }
	a.sum = agg.Sum(st, "amount_sum", amount,
		agg.ByTime(at, agg.Daily, agg.Weekly, agg.Monthly, agg.AllTime))
	a.count = agg.Count(st, "event_count", func(e *event) (string, bool) { return e.Customer, true },
		agg.ByTime(at, agg.Daily, agg.Weekly, agg.Monthly, agg.AllTime))
	a.max = agg.Max(st, "amount_max", amount,
		agg.ByTime(at, agg.Daily, agg.Weekly, agg.Monthly))
	return a
}

// bucket returns the bucket of kvutil for a unit.
func bucket(u unit) agg.Calendar {
	switch u {
	case week:
		return agg.Weekly
	case month:
		return agg.Monthly
	}
	return agg.Daily
}

// countedDB counts the transactions that the collector had to run again: its conflicts with the
// writers, which the counter of the system (the writers' own) does not include.
type countedDB struct {
	kv.DB
	retries atomic.Int64
}

func (d *countedDB) Update(fn func(kv.Tx) error) error {
	calls := 0
	err := d.DB.Update(func(tx kv.Tx) error {
		calls++
		return fn(tx)
	})
	if calls > 1 {
		d.retries.Add(int64(calls - 1))
	}
	return err
}

type kvEnv struct {
	s     setup
	db    *kvsys.DB
	cdb   *countedDB
	a     *kvAggs
	exact bool // reads take the notes that are not collected into account

	coll     *agg.Collector
	collErrs atomic.Int64
	plain    bool
}

func openKV(ctx context.Context, sys *kvsys.System, name string, s setup, schema string, plain bool) (env, error) {
	db, err := sys.Open(schema + "-" + name)
	if err != nil {
		return nil, err
	}
	e := &kvEnv{s: s, db: db, cdb: &countedDB{DB: db.DB}, exact: name != kvCollected, plain: plain}
	e.a = newKVStore(!plain)
	// A store records itself in the database before it is used.
	if _, err := e.a.store.Migrate(ctx, db); err != nil {
		db.Destroy()
		return nil, err
	}
	e.makeCollector()
	return e, nil
}

func (e *kvEnv) makeCollector() {
	if e.a.sum == nil {
		return
	}
	e.coll = agg.NewCollector(e.cdb, []agg.Source{e.a.store},
		agg.WithInterval(e.s.interval),
		agg.WithErrorHandler(func(error) { e.collErrs.Add(1) }))
}

func (e *kvEnv) Load(ctx context.Context, n int64) error {
	_, err := wl.TxLoad(ctx, e.db, 1, n+1, 500, e.s.workers, func() func(kv.Tx, int64) error {
		ev := &event{}
		return func(tx kv.Tx, id int64) error {
			e.s.spec.GenInto(ev, id, 0)
			return e.a.store.Instantiate(tx).Set(ev)
		}
	})
	return err
}

// LoadPlain writes the events with the store that has no aggregation, as one write batch.
func (e *kvEnv) LoadPlain(ctx context.Context, n int64) error {
	st := e.a.store
	_, err := wl.BulkLoad(e.db, 1, n+1, e.s.workers, func() func(kv.BulkWriter, int64) error {
		ev := &event{}
		return func(w kv.BulkWriter, id int64) error {
			e.s.spec.GenInto(ev, id, 0)
			k, err := st.KeyCodec().Encode(id)
			if err != nil {
				return err
			}
			v, err := st.ValueCodec().Encode(*ev)
			if err != nil {
				return err
			}
			return w.Set(append(bytes.Clone(st.DataPrefix()), k...), v)
		}
	})
	return err
}

// Build defines the aggregations on the store of the events and migrates the database to it,
// which builds them from the records that are there (store.Migrate); then it builds each of
// them again alone (agg.Rebuild). Each is a step.
func (e *kvEnv) Build(ctx context.Context, step func(name string, fn func(context.Context) error) error) error {
	st := newKVStore(true)
	err := step("migrate", func(ctx context.Context) error {
		report, err := st.store.Migrate(ctx, e.db)
		if err != nil {
			return err
		}
		built := 0
		for _, a := range report.Actions {
			if a.What == migrate.Built {
				built++
			}
		}
		if built != 3 {
			return fmt.Errorf("the migration built %d aggregations, not 3: %s", built, report)
		}
		return nil
	})
	if err != nil {
		return err
	}
	e.a, e.plain = st, false
	e.makeCollector()
	if err := step("rebuild-sum", func(ctx context.Context) error { return agg.Rebuild(ctx, e.db, st.sum) }); err != nil {
		return err
	}
	if err := step("rebuild-count", func(ctx context.Context) error { return agg.Rebuild(ctx, e.db, st.count) }); err != nil {
		return err
	}
	return step("rebuild-max", func(ctx context.Context) error { return agg.Rebuild(ctx, e.db, st.max) })
}

func (e *kvEnv) CatchUp(ctx context.Context) error {
	if e.coll == nil {
		return nil
	}
	return e.coll.Collect(ctx)
}

func (e *kvEnv) Background(ctx context.Context, on bool) error {
	if e.coll == nil {
		return nil
	}
	if on {
		e.coll.Spawn(context.Background())
		return nil
	}
	return e.coll.Shutdown(ctx)
}

func (e *kvEnv) write(es []*event, f func(st *eventStore, tx kv.Tx, ev *event) error) error {
	return e.db.Update(func(tx kv.Tx) error {
		for _, ev := range es {
			if err := f(e.a.store, tx, ev); err != nil {
				return err
			}
		}
		return nil
	})
}

func set(st *eventStore, tx kv.Tx, ev *event) error { return st.Instantiate(tx).Set(ev) }

func (e *kvEnv) Insert(_ context.Context, ev *event) error {
	return e.write([]*event{ev}, set)
}

func (e *kvEnv) InsertBatch(_ context.Context, es []*event) error {
	for chunk := range slices.Chunk(es, 500) {
		if err := e.write(chunk, set); err != nil {
			return err
		}
	}
	return nil
}

func (e *kvEnv) Update(_ context.Context, _, ev *event) error {
	return e.write([]*event{ev}, set)
}

func (e *kvEnv) Delete(_ context.Context, old *event) error {
	return e.write([]*event{old}, func(st *eventStore, tx kv.Tx, ev *event) error {
		return st.Instantiate(tx).Delete(ev.ID)
	})
}

func (e *kvEnv) Read(_ context.Context, q query) ([]row, error) { return e.read(q, e.exact) }

func (e *kvEnv) Truth(_ context.Context, q query) ([]row, error) { return e.read(q, true) }

func (e *kvEnv) read(q query, exact bool) ([]row, error) {
	if e.a.sum == nil {
		return nil, fmt.Errorf("the aggregations are not defined yet")
	}
	var rows []row
	err := e.db.View(func(tx kv.Tx) error {
		var err error
		switch q.Kind {
		case qSeries:
			rows, err = e.series(tx, q, exact)
		case qTotal:
			rows, err = e.total(tx, q, exact)
		default:
			rows, err = e.top(tx, q, exact)
		}
		return err
	})
	return rows, err
}

func (e *kvEnv) series(tx kv.Tx, q query, exact bool) ([]row, error) {
	b := bucket(q.Unit)
	read := func(a *agg.Aggregation[int64, event, string, int64, int64]) ([]agg.Point[int64], error) {
		if exact {
			return a.Instantiate(tx).Series(b, q.Customer, q.From, q.To)
		}
		return a.Instantiate(tx).SeriesCollected(b, q.Customer, q.From, q.To)
	}
	sums, err := read(e.a.sum)
	if err != nil {
		return nil, err
	}
	counts, err := read(e.a.count)
	if err != nil {
		return nil, err
	}
	maxs, err := read(e.a.max)
	if err != nil {
		return nil, err
	}

	// The three have the spans that have a record in them, and the collected ones may differ
	// while the collector is between two of them.
	byStart := map[int64]*row{}
	at := func(t time.Time) *row {
		r, ok := byStart[t.UnixNano()]
		if !ok {
			r = &row{Bucket: t.UTC()}
			byStart[t.UnixNano()] = r
		}
		return r
	}
	for _, p := range sums {
		at(p.Start).Sum = p.State
	}
	for _, p := range counts {
		at(p.Start).Count = p.State
	}
	for _, p := range maxs {
		at(p.Start).Max = p.State
	}
	rows := make([]row, 0, len(byStart))
	for _, r := range byStart {
		rows = append(rows, *r)
	}
	sortRows(rows)
	return rows, nil
}

func (e *kvEnv) total(tx kv.Tx, q query, exact bool) ([]row, error) {
	get := func(a *agg.Aggregation[int64, event, string, int64, int64]) (int64, bool, error) {
		if exact {
			return a.Instantiate(tx).Get(q.Customer)
		}
		return a.Instantiate(tx).GetCollected(q.Customer)
	}
	sum, _, err := get(e.a.sum)
	if err != nil {
		return nil, err
	}
	count, _, err := get(e.a.count)
	if err != nil {
		return nil, err
	}
	return []row{{Sum: sum, Count: count}}, nil
}

// top reads the month of every customer, as the aggregations have no way to read all the groups at
// once, and keeps the 10 greatest.
func (e *kvEnv) top(tx kv.Tx, q query, exact bool) ([]row, error) {
	in := e.a.sum.Instantiate(tx)
	var rows []row
	for rank := range e.s.spec.NumCustomers() {
		name := dataset.CustomerName(rank)
		var (
			sum int64
			ok  bool
			err error
		)
		if exact {
			sum, ok, err = in.At(agg.Monthly, name, q.From)
		} else {
			sum, ok, err = in.AtCollected(agg.Monthly, name, q.From)
		}
		if err != nil {
			return nil, err
		}
		if ok {
			rows = append(rows, row{Customer: name, Sum: sum})
		}
	}
	return topRows(rows), nil
}

func (e *kvEnv) Sizes(context.Context) (harness.Sizes, error) {
	if err := e.db.Sync(); err != nil {
		return harness.Sizes{}, err
	}
	n, err := e.db.DiskBytes()
	return harness.Sizes{Data: n}, err
}

// bytesUnder is the sum of the lengths of the keys and values under a prefix.
func (e *kvEnv) bytesUnder(prefix []byte) (int64, error) {
	var n int64
	err := e.db.View(func(tx kv.Tx) error {
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
			n += int64(len(it.Key()) + len(v))
		}
		return nil
	})
	return n, err
}

func (e *kvEnv) Storage(context.Context) (storage, error) {
	st := storage{Logical: true}
	var err error
	if st.Events, err = e.bytesUnder(e.a.store.DataPrefix()); err != nil {
		return st, err
	}
	if e.a.sum == nil {
		return st, nil
	}
	for _, p := range [][]byte{e.a.sum.Prefix(), e.a.count.Prefix(), e.a.max.Prefix()} {
		n, err := e.bytesUnder(p)
		if err != nil {
			return st, err
		}
		st.Aggs += n
	}
	return st, nil
}

func (e *kvEnv) Stats() map[string]float64 {
	return map[string]float64{
		"collector_retries": float64(e.cdb.retries.Load()),
		"collector_errors":  float64(e.collErrs.Load()),
	}
}

func (e *kvEnv) Close(ctx context.Context) error {
	if e.coll != nil {
		e.coll.Shutdown(context.WithoutCancel(ctx))
	}
	return e.db.Destroy()
}
