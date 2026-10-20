package codecs

import (
	"context"
	"errors"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/kvsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/wl"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/store/record"
)

// recordStore is what the three record stores of kvutil share, whatever the type of their
// records is: the functions that convert to and from the record of the dataset.
type recordStore[T any] struct {
	db    *kvsys.DB
	st    *record.Store[int64, T]
	from  func(*dataset.Order) *T
	to    func(*T) *dataset.Order
	codec codec.ValueCodec[T]
}

const loadBatch = 1000

func (s *recordStore[T]) Load(ctx context.Context, spec dataset.Spec, n int64) error {
	o := &dataset.Order{}
	_, err := wl.TxLoad(ctx, s.db, 1, n+1, loadBatch, 1, func() func(kv.Tx, int64) error {
		return func(tx kv.Tx, id int64) error {
			spec.GenInto(o, id, 0)
			return s.st.Instantiate(tx).Set(s.from(o))
		}
	})
	return err
}

func (s *recordStore[T]) Get(_ context.Context, id int64) (*dataset.Order, error) {
	var o *dataset.Order
	err := s.db.View(func(tx kv.Tx) error {
		r, err := s.st.Instantiate(tx).Get(id)
		if errors.Is(err, kv.ErrNotFound) {
			o = nil
			return nil
		}
		if err != nil {
			return err
		}
		o = s.to(r)
		return nil
	})
	return o, err
}

func (s *recordStore[T]) Set(_ context.Context, o *dataset.Order) error {
	return s.db.Update(func(tx kv.Tx) error { return s.st.Instantiate(tx).Set(s.from(o)) })
}

func (s *recordStore[T]) AvgBytes(_ context.Context, spec dataset.Spec, n int64) (float64, error) {
	const sample = 1000
	var total, count int
	for i := int64(0); i < sample; i++ {
		o := spec.Gen(1+i*max(n/sample, 1)%n, 0)
		bz, err := s.codec.Encode(*s.from(o))
		if err != nil {
			return 0, err
		}
		total += len(bz)
		count++
	}
	return float64(total) / float64(count), nil
}

func (s *recordStore[T]) Settle(context.Context) error { return s.db.Sync() }

func (s *recordStore[T]) Sizes(context.Context) (harness.Sizes, error) {
	n, err := s.db.DiskBytes()
	return harness.Sizes{Data: n}, err
}

func (s *recordStore[T]) Close(context.Context) error { return s.db.Destroy() }

func openMsgpack(ctx context.Context, sys harness.System, _ harness.Params) (store, error) {
	st := record.New[int64, dataset.Order]("rec")
	return openRecords(ctx, sys, "codecs-msgpack", st, st.ValueCodec(),
		func(o *dataset.Order) *dataset.Order { return o },
		func(o *dataset.Order) *dataset.Order { return o })
}

func openJSON(ctx context.Context, sys harness.System, _ harness.Params) (store, error) {
	st := record.NewWithCodecs[int64, dataset.Order]("rec", nil, codec.Formatted[dataset.Order](codec.JSON))
	return openRecords(ctx, sys, "codecs-json", st, st.ValueCodec(),
		func(o *dataset.Order) *dataset.Order { return o },
		func(o *dataset.Order) *dataset.Order { return o })
}

func openBinary(ctx context.Context, sys harness.System, _ harness.Params) (store, error) {
	st := record.New[int64, OrderBin]("rec")
	return openRecords(ctx, sys, "codecs-binary", st, st.ValueCodec(),
		func(o *dataset.Order) *OrderBin { return (*OrderBin)(o) },
		func(o *OrderBin) *dataset.Order { return (*dataset.Order)(o) })
}

func openRecords[T any](
	ctx context.Context,
	sys harness.System,
	name string,
	st *record.Store[int64, T],
	vc codec.ValueCodec[T],
	from func(*dataset.Order) *T,
	to func(*T) *dataset.Order,
) (store, error) {
	db, err := sys.(*kvsys.System).Open(name)
	if err != nil {
		return nil, err
	}
	if _, err := st.Migrate(ctx, db); err != nil {
		db.Destroy()
		return nil, err
	}
	return &recordStore[T]{db: db, st: st, from: from, to: to, codec: vc}, nil
}
