package bulkload

import (
	"bytes"
	"context"
	"fmt"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/kvsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/wl"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/store/typed"
)

type kvTarget struct {
	db *kvsys.DB
	st *typed.Store[int64, dataset.Order]
}

func openKV(ctx context.Context, sys harness.System, _ harness.Params) (target, error) {
	db, err := sys.(*kvsys.System).Open("bulkload")
	if err != nil {
		return nil, err
	}
	st := typed.New[int64, dataset.Order]("kv")
	if _, err := st.Migrate(ctx, db); err != nil {
		db.Destroy()
		return nil, err
	}
	return &kvTarget{db: db, st: st}, nil
}

func (t *kvTarget) Check(_ context.Context, digest bool) (int64, string, error) {
	d := newDigester()
	var count int64
	err := t.db.View(func(tx kv.Tx) error {
		count = 0
		it, err := t.st.Instantiate(tx).All()
		if err != nil {
			return err
		}
		defer it.Close()
		for ; it.Valid(); it.Next() {
			count++
			if !digest {
				continue
			}
			id, err := it.Key()
			if err != nil {
				return err
			}
			o, err := it.Value()
			if err != nil {
				return err
			}
			o.ID = id
			d.add(o)
		}
		return nil
	})
	return count, d.sum(), err
}

func (t *kvTarget) Sizes(context.Context) (harness.Sizes, error) {
	if err := t.db.Sync(); err != nil {
		return harness.Sizes{}, err
	}
	n, err := t.db.DiskBytes()
	return harness.Sizes{Data: n}, err
}

func (t *kvTarget) Close(context.Context) error { return t.db.Destroy() }

func kvStrategies() []strategy {
	writeBatch := strategy{
		name: "writebatch",
		load: func(_ context.Context, t target, spec dataset.Spec, n int64, workers int) error {
			kt := t.(*kvTarget)
			_, err := wl.BulkLoad(kt.db, 0, n, workers, func() func(kv.BulkWriter, int64) error {
				o := &dataset.Order{}
				return func(w kv.BulkWriter, id int64) error {
					spec.GenInto(o, id, 0)
					k, err := kt.st.KeyCodec().Encode(id)
					if err != nil {
						return err
					}
					v, err := kt.st.ValueCodec().Encode(*o)
					if err != nil {
						return err
					}
					return w.Set(append(bytes.Clone(kt.st.DataPrefix()), k...), v)
				}
			})
			return err
		},
	}

	txn := func(batch int) strategy {
		return strategy{
			name:    fmt.Sprintf("txn-b%d", batch),
			params:  map[string]any{"batch": batch},
			singles: batch == 1,
			load: func(ctx context.Context, t target, spec dataset.Spec, n int64, workers int) error {
				kt := t.(*kvTarget)
				_, err := wl.TxLoad(ctx, kt.db, 0, n, batch, workers, func() func(kv.Tx, int64) error {
					o := &dataset.Order{}
					return func(tx kv.Tx, id int64) error {
						spec.GenInto(o, id, 0)
						return kt.st.Instantiate(tx).Set(id, o)
					}
				})
				return err
			},
		}
	}
	return []strategy{writeBatch, txn(1000), txn(100), txn(1)}
}
