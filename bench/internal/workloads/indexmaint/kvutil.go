package indexmaint

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/kvsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/wl"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/migrate"
	sqlq "github.com/ehsanranjbar/kvutil/query/sql"
	"github.com/ehsanranjbar/kvutil/store/record"
)

type order = dataset.Order

var (
	fCustomer = field.Path[order, string]("Customer")
	fStatus   = field.Path[order, string]("Status")
	fTotal    = field.Path[order, int64]("Total")
	fAt       = field.Path[order, time.Time]("At")
	fName     = field.Path[order, string]("Name")
	fCode     = field.Path[order, string]("Code")
	fTags     = field.Path[order, string]("Tags")
	fLower    = sqlq.Expr[order, string](`lower(Name)`)
)

type recordStore = record.Store[int64, order]

func newStore() *recordStore { return record.New[int64, order]("rec") }

// kvIndexes are the indexes that a configuration adds to a store, and how they answer the probes.
type kvIndexes struct {
	names []string
	// lookups answer the probes by their kind.
	lookups map[string]func(tx kv.Tx, p probe) ([]int64, error)
}

// addIndexes defines the indexes of the configuration on the store.
func addIndexes(st *recordStore, config string) *kvIndexes {
	x := &kvIndexes{lookups: map[string]func(kv.Tx, probe) ([]int64, error){}}
	add := func(name string) string {
		x.names = append(x.names, name)
		return name
	}

	switch config {
	case "none":
		x.lookups["all"] = func(tx kv.Tx, _ probe) ([]int64, error) {
			it, err := st.Instantiate(tx).All()
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
			return ids, nil
		}

	case "n1", "n3", "n5":
		n := map[string]int{"n1": 1, "n3": 3, "n5": 5}[config]
		byCustomer := index.On(st, add("by_customer"), fCustomer)
		x.lookups["customer"] = func(tx kv.Tx, p probe) ([]int64, error) {
			return wl.CollectIDs(byCustomer.Instantiate(tx).Match(p.S))
		}
		if n >= 3 {
			byStatus := index.On(st, add("by_status"), fStatus)
			byTotal := index.On(st, add("by_total"), fTotal)
			x.lookups["status"] = func(tx kv.Tx, p probe) ([]int64, error) {
				return wl.CollectIDs(byStatus.Instantiate(tx).Match(p.S))
			}
			x.lookups["total"] = func(tx kv.Tx, p probe) ([]int64, error) {
				return wl.CollectIDs(byTotal.Instantiate(tx).Range(kvutil.Between(p.Lo, p.Hi)))
			}
		}
		if n >= 5 {
			byCreated := index.On(st, add("by_created"), fAt)
			byName := index.On(st, add("by_name"), fName)
			x.lookups["created"] = func(tx kv.Tx, p probe) ([]int64, error) {
				return wl.CollectIDs(byCreated.Instantiate(tx).Range(kvutil.Between(wl.Seconds(p.Lo), wl.Seconds(p.Hi))))
			}
			x.lookups["name"] = func(tx kv.Tx, p probe) ([]int64, error) {
				return wl.CollectIDs(byName.Instantiate(tx).Match(p.S))
			}
		}

	case "unique":
		byCode := index.Unique(st, add("by_code"), fCode)
		x.lookups["code"] = func(tx kv.Tx, p probe) ([]int64, error) {
			return wl.CollectIDs(byCode.Instantiate(tx).Match(p.S))
		}

	case "composite":
		byCustomerAt := index.Composite(st, add("by_customer_at"), field.List[order](fCustomer, fAt))
		x.lookups["composite"] = func(tx kv.Tx, p probe) ([]int64, error) {
			return wl.CollectIDs(byCustomerAt.Instantiate(tx).Lookup(fCustomer.Eq(p.S), fAt.Between(wl.Seconds(p.Lo), wl.Seconds(p.Hi))))
		}

	case "multi":
		byTag := index.On(st, add("by_tag"), fTags)
		x.lookups["tag"] = func(tx kv.Tx, p probe) ([]int64, error) {
			return wl.CollectIDs(byTag.Instantiate(tx).Match(p.S))
		}

	case "expr":
		byLower := index.On(st, add("by_name_lower"), fLower)
		x.lookups["lower"] = func(tx kv.Tx, p probe) ([]int64, error) {
			return wl.CollectIDs(byLower.Instantiate(tx).Match(p.S))
		}

	case "conditional":
		big := index.On(st, add("big_by_customer"), fCustomer, index.Where[order](fTotal.Gte(dataset.BigTotal)))
		x.lookups["conditional"] = func(tx kv.Tx, p probe) ([]int64, error) {
			return wl.CollectIDs(big.Instantiate(tx).Lookup(fCustomer.Eq(p.S), fTotal.Gte(dataset.BigTotal)))
		}

	case "covering":
		covering := index.On(st, add("by_customer_total"), fCustomer, index.Including[order](fTotal))
		x.lookups["customer"] = func(tx kv.Tx, p probe) ([]int64, error) {
			return wl.CollectIDs(covering.Instantiate(tx).Match(p.S))
		}

	default:
		panic("unknown configuration " + config)
	}
	return x
}

type kvTarget struct {
	config string
	db     *kvsys.DB
	// base is the store without indexes, which loads the records; cur is the one that writes.
	base, cur *recordStore
	idx       *kvIndexes
}

func openKV(ctx context.Context, sys harness.System, config string) (target, error) {
	db, err := sys.(*kvsys.System).Open("indexmaint-" + config)
	if err != nil {
		return nil, err
	}
	base := newStore()
	if _, err := base.Migrate(ctx, db); err != nil {
		db.Destroy()
		return nil, err
	}
	t := &kvTarget{config: config, db: db, base: base, cur: base}
	if config == "none" {
		t.idx = addIndexes(base, config)
	}
	return t, nil
}

func (t *kvTarget) Load(_ context.Context, spec dataset.Spec, n int64) error {
	_, err := wl.BulkLoad(t.db, 1, n+1, 1, func() func(kv.BulkWriter, int64) error {
		o := &order{}
		return func(w kv.BulkWriter, id int64) error {
			spec.GenInto(o, id, 0)
			k, err := t.base.KeyCodec().Encode(id)
			if err != nil {
				return err
			}
			v, err := t.base.ValueCodec().Encode(*o)
			if err != nil {
				return err
			}
			return w.Set(append(bytes.Clone(t.base.DataPrefix()), k...), v)
		}
	})
	return err
}

// Build defines the indexes on a store that has the same prefix as the one that loaded the
// records, and migrates the database to it, which builds them.
func (t *kvTarget) Build(ctx context.Context) ([]string, error) {
	if t.config == "none" {
		return nil, nil
	}
	st := newStore()
	idx := addIndexes(st, t.config)
	report, err := st.Migrate(ctx, t.db)
	if err != nil {
		return nil, err
	}
	built := 0
	for _, a := range report.Actions {
		if a.What == migrate.Built {
			built++
		}
	}
	if built != len(idx.names) {
		return nil, fmt.Errorf("the migration built %d of %d indexes: %s", built, len(idx.names), report)
	}
	t.cur, t.idx = st, idx
	return idx.names, nil
}

func (t *kvTarget) Add(ctx context.Context, spec dataset.Spec, from, to int64) error {
	_, err := wl.TxLoad(ctx, t.db, from, to, 500, 1, func() func(kv.Tx, int64) error {
		o := &order{}
		return func(tx kv.Tx, id int64) error {
			spec.GenInto(o, id, 0)
			return t.cur.Instantiate(tx).Set(o)
		}
	})
	return err
}

func (t *kvTarget) write(o *order) error {
	return t.db.Update(func(tx kv.Tx) error { return t.cur.Instantiate(tx).Set(o) })
}

func (t *kvTarget) Insert(_ context.Context, o *order) error { return t.write(o) }
func (t *kvTarget) Update(_ context.Context, o *order) error { return t.write(o) }

func (t *kvTarget) Delete(_ context.Context, id int64) error {
	return t.db.Update(func(tx kv.Tx) error { return t.cur.Instantiate(tx).Delete(id) })
}

func (t *kvTarget) Probe(_ context.Context, p probe) ([]int64, error) {
	if t.idx == nil {
		return nil, fmt.Errorf("the indexes are not built")
	}
	look, ok := t.idx.lookups[p.Kind]
	if !ok {
		return nil, fmt.Errorf("no index for the probe %q", p.Kind)
	}
	var ids []int64
	err := t.db.View(func(tx kv.Tx) error {
		var err error
		ids, err = look(tx, p)
		return err
	})
	return ids, err
}

func (t *kvTarget) Settle(context.Context) error { return t.db.Sync() }

func (t *kvTarget) Sizes(context.Context) (harness.Sizes, error) {
	n, err := t.db.DiskBytes()
	return harness.Sizes{Data: n}, err
}

func (t *kvTarget) Close(context.Context) error { return t.db.Destroy() }
