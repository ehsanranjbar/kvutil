package relations

import (
	"context"
	"errors"
	"fmt"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/metrics"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/kvsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/wl"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/migrate"
	"github.com/ehsanranjbar/kvutil/relation"
	"github.com/ehsanranjbar/kvutil/store/record"
)

// kvStores are the stores and relations of the workload, as a program defines them.
type kvStores struct {
	parents  *record.Store[int64, parent]
	children *record.Store[int64, child]
	// owned makes a child belong to a parent: it checks the parent when a child is set, keeps
	// the references of both directions, and deletes the children of a parent that is deleted.
	owned   *relation.Association[int64, parent, int64, child]
	lefts   *record.Store[int64, entity]
	rights  *record.Store[int64, entity]
	members *relation.Relation[int64, int64, pairData]
}

func newKVStores() *kvStores {
	s := &kvStores{
		parents:  record.New[int64, parent]("parents"),
		children: record.New[int64, child]("children"),
		lefts:    record.New[int64, entity]("lefts"),
		rights:   record.New[int64, entity]("rights"),
	}
	s.owned = relation.Associate("owned", s.parents, s.children)
	s.members = relation.New[pairData]("members", s.lefts, s.rights)
	return s
}

func (s *kvStores) migrate(ctx context.Context, db kv.DB) error {
	_, err := migrate.Run(ctx, db, s.parents, s.children, s.owned, s.lefts, s.rights, s.members)
	return err
}

type kvTarget struct {
	db *kvsys.DB
	*kvStores
}

func openKV(ctx context.Context, sys *kvsys.System) (target, error) {
	db, err := sys.Open("relations")
	if err != nil {
		return nil, err
	}
	t := &kvTarget{db: db, kvStores: newKVStores()}
	if err := t.migrate(ctx, db); err != nil {
		db.Destroy()
		return nil, err
	}
	return t, nil
}

const loadBatch = 500

func (t *kvTarget) LoadParents(ctx context.Context, m *model, workers int) error {
	_, err := wl.TxLoad(ctx, t.db, 1, m.parents+1, loadBatch, workers, func() func(kv.Tx, int64) error {
		p := &parent{}
		return func(tx kv.Tx, id int64) error {
			m.genParent(id, p)
			return t.parents.Instantiate(tx).Set(p)
		}
	})
	return err
}

// LoadChildren sets each child under its parent: the parent is looked up, and the references are
// written, besides the record.
func (t *kvTarget) LoadChildren(ctx context.Context, m *model, workers int) error {
	_, err := wl.TxLoad(ctx, t.db, 1, m.children+1, loadBatch, workers, func() func(kv.Tx, int64) error {
		c := &child{}
		return func(tx kv.Tx, id int64) error {
			m.genChild(id, c)
			return t.children.Instantiate(tx).Set(c, t.owned.Under(m.parentOf(id)))
		}
	})
	return err
}

func (t *kvTarget) LoadEntities(ctx context.Context, m *model, workers int) error {
	for _, side := range []struct {
		name  string
		n     int64
		store *record.Store[int64, entity]
	}{{"left", m.lefts, t.lefts}, {"right", m.rights, t.rights}} {
		_, err := wl.TxLoad(ctx, t.db, 1, side.n+1, loadBatch, workers, func() func(kv.Tx, int64) error {
			e := &entity{}
			return func(tx kv.Tx, id int64) error {
				m.genEntity(side.name, id, e)
				return side.store.Instantiate(tx).Set(e)
			}
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// LoadPairs relates the records: each pair checks that both records exist and writes the
// references of both directions.
func (t *kvTarget) LoadPairs(ctx context.Context, m *model, workers int) error {
	_, err := wl.TxLoad(ctx, t.db, 1, m.lefts+1, 50, workers, func() func(kv.Tx, int64) error {
		return func(tx kv.Tx, l int64) error {
			rel := t.members.Instantiate(tx)
			for j := int64(0); j < int64(m.leftCount[l]); j++ {
				r := m.rightOf(l, j)
				d := m.data(l, r)
				if err := rel.Set(l, r, &d); err != nil {
					return err
				}
			}
			return nil
		}
	})
	return err
}

func (t *kvTarget) InsertChild(_ context.Context, c *child, parent int64) error {
	return t.db.Update(func(tx kv.Tx) error {
		return t.children.Instantiate(tx).Set(c, t.owned.Under(parent))
	})
}

func (t *kvTarget) Children(_ context.Context, parent int64, fn func(*child)) error {
	return t.db.View(func(tx kv.Tx) error {
		it, err := t.owned.Instantiate(tx).Children(parent)
		if err != nil {
			return err
		}
		defer it.Close()
		for ; it.Valid(); it.Next() {
			c, err := it.Value()
			if err != nil {
				return err
			}
			fn(c)
		}
		return nil
	})
}

func (t *kvTarget) ChildKeys(_ context.Context, parent int64, fn func(id int64)) error {
	return t.db.View(func(tx kv.Tx) error {
		it, err := t.owned.Instantiate(tx).Children(parent)
		if err != nil {
			return err
		}
		defer it.Close()
		for ; it.Valid(); it.Next() {
			id, err := it.Key()
			if err != nil {
				return err
			}
			fn(id)
		}
		return nil
	})
}

func (t *kvTarget) ParentOf(_ context.Context, child int64) (*parent, error) {
	var p *parent
	err := t.db.View(func(tx kv.Tx) error {
		var err error
		p, err = t.owned.Instantiate(tx).Parent(child)
		return err
	})
	if errors.Is(err, kv.ErrNotFound) {
		return nil, nil
	}
	return p, err
}

func (t *kvTarget) DeleteParent(_ context.Context, id int64) error {
	return t.db.Update(func(tx kv.Tx) error { return t.parents.Instantiate(tx).Delete(id) })
}

func (t *kvTarget) SetPair(_ context.Context, left, right int64, d *pairData) error {
	return t.db.Update(func(tx kv.Tx) error { return t.members.Instantiate(tx).Set(left, right, d) })
}

func (t *kvTarget) GetPair(_ context.Context, left, right int64) (*pairData, error) {
	var d *pairData
	err := t.db.View(func(tx kv.Tx) error {
		var err error
		d, err = t.members.Instantiate(tx).Get(left, right)
		return err
	})
	if errors.Is(err, kv.ErrNotFound) {
		return nil, nil
	}
	return d, err
}

// list reads a list of related keys, with the data of the pairs and the related records as the
// mode says. The records are read with a Get for each key, as the relation holds keys only.
func list(
	mode listMode,
	it interface {
		Valid() bool
		Next()
		Key() (int64, error)
		Value() (*pairData, error)
		Close() error
	},
	other func(id int64) (*entity, error),
	fn func(int64, *pairData, *entity),
) error {
	defer it.Close()
	for ; it.Valid(); it.Next() {
		id, err := it.Key()
		if err != nil {
			return err
		}
		var d *pairData
		var rec *entity
		if mode != modeKeys {
			if d, err = it.Value(); err != nil {
				return err
			}
		}
		if mode == modeRecords {
			if rec, err = other(id); err != nil {
				return err
			}
		}
		fn(id, d, rec)
	}
	return nil
}

func (t *kvTarget) Rights(_ context.Context, left int64, mode listMode, fn func(int64, *pairData, *entity)) error {
	return t.db.View(func(tx kv.Tx) error {
		it, err := t.members.Instantiate(tx).Rights(left)
		if err != nil {
			return err
		}
		rights := t.rights.Instantiate(tx)
		return list(mode, it, rights.Get, fn)
	})
}

func (t *kvTarget) Lefts(_ context.Context, right int64, mode listMode, fn func(int64, *pairData, *entity)) error {
	return t.db.View(func(tx kv.Tx) error {
		it, err := t.members.Instantiate(tx).Lefts(right)
		if err != nil {
			return err
		}
		lefts := t.lefts.Instantiate(tx)
		return list(mode, it, lefts.Get, fn)
	})
}

func (t *kvTarget) DeleteLeft(_ context.Context, id int64) error {
	return t.db.Update(func(tx kv.Tx) error { return t.lefts.Instantiate(tx).Delete(id) })
}

func (t *kvTarget) DeleteRight(_ context.Context, id int64) error {
	return t.db.Update(func(tx kv.Tx) error { return t.rights.Instantiate(tx).Delete(id) })
}

func (t *kvTarget) Count(_ context.Context, what string) (int64, error) {
	var n int64
	err := t.db.View(func(tx kv.Tx) error {
		var iter interface {
			Valid() bool
			Next()
			Close() error
		}
		var err error
		switch what {
		case "parents":
			iter, err = t.parents.Instantiate(tx).All()
		case "children":
			iter, err = t.children.Instantiate(tx).All()
		case "lefts":
			iter, err = t.lefts.Instantiate(tx).All()
		case "rights":
			iter, err = t.rights.Instantiate(tx).All()
		case "pairs":
			iter, err = t.members.Instantiate(tx).All()
		default:
			return fmt.Errorf("unknown kind %q", what)
		}
		if err != nil {
			return err
		}
		defer iter.Close()
		for ; iter.Valid(); iter.Next() {
			n++
		}
		return nil
	})
	return n, err
}

func (t *kvTarget) Settle(context.Context) error { return t.db.Sync() }

func (t *kvTarget) Sizes(context.Context) (harness.Sizes, error) {
	n, err := t.db.DiskBytes()
	return harness.Sizes{Data: n}, err
}

func (t *kvTarget) Close(context.Context) error { return t.db.Destroy() }

// cascadeLimit finds how many children a parent can have for deleting it to fit in one transaction:
// the deletion of a parent is a transaction that deletes each child with its references, and a
// transaction holds a limited number of writes. Parents of 1,024 children, then 2,048 and so on
// are deleted one after the other until one fails.
func (t *kvTarget) cascadeLimit(ctx context.Context, rc *harness.RunContext) error {
	db, err := rc.Sys.(*kvsys.System).Open("relations-cascade")
	if err != nil {
		return err
	}
	defer db.Destroy()
	s := newKVStores()
	if err := s.migrate(ctx, db); err != nil {
		return err
	}

	start := time.Now()
	var maxOK, firstFail int64
	var nextChild int64
	extra := map[string]float64{}
	sizes := cascadeSizes(rc.Params.Scale)
	for i, size := range sizes {
		pid := int64(i + 1)
		err := db.Update(func(tx kv.Tx) error { return s.parents.Instantiate(tx).Set(&parent{ID: pid, Name: "p"}) })
		if err != nil {
			return err
		}
		for lo := int64(0); lo < size; lo += loadBatch {
			err := db.Update(func(tx kv.Tx) error {
				for i := lo; i < min(lo+loadBatch, size); i++ {
					nextChild++
					if err := s.children.Instantiate(tx).Set(&child{ID: nextChild, Title: "c"}, s.owned.Under(pid)); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		t0 := time.Now()
		err = db.Update(func(tx kv.Tx) error { return s.parents.Instantiate(tx).Delete(pid) })
		took := time.Since(t0)
		if errors.Is(err, kv.ErrTxnTooBig) || errors.Is(err, badger.ErrTxnTooBig) {
			firstFail = size
			extra[fmt.Sprintf("failed_after_ms_%d", size)] = took.Seconds() * 1000
			break
		}
		if err != nil {
			return fmt.Errorf("deleting a parent of %d children: %w", size, err)
		}
		maxOK = size
		extra[fmt.Sprintf("delete_ms_%d", size)] = took.Seconds() * 1000
	}
	extra["largest_cascade_that_fit_children"] = float64(maxOK)
	extra["first_cascade_that_failed_children"] = float64(firstFail)
	return rc.Report(ctx, harness.Case{Name: "cascade-limit", Concurrency: 1, Params: map[string]any{"sizes": sizes}},
		start, time.Now(), metrics.Metrics{ElapsedSec: time.Since(start).Seconds(), Extra: extra})
}

// cascadeSizes are the numbers of children of the parents that are deleted to find the limit. The
// time of a cascade grows faster than its size (about four times for twice the children: 0.13 s
// for 1,024, 11 s for 8,192), so the full range is only walked on a run of size: a smoke run
// stops at 8,192 children, short of the limit, and a tiny one at 2,048.
func cascadeSizes(scale int) []int64 {
	sizes := []int64{1024, 2048}
	if scale >= 5_000 {
		sizes = append(sizes, 4096, 8192)
	}
	if scale >= 100_000 {
		sizes = append(sizes, 16384, 24576, 32768)
	}
	return sizes
}
