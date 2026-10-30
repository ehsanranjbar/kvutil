package concurrency

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/kvsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/wl"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/badgerkv"
	"github.com/ehsanranjbar/kvutil/store/record"
)

func encodeOrder(o *dataset.Order) []byte { return wl.Msgpack(o) }

// kvEnv is kvutil on a database of its own. The counters are raw keys (the counter in 8 bytes
// under a key of 8 bytes) and so are the blind writes; the records of the mixed cases are a
// record store with the default codec.
type kvEnv struct {
	db     *kvsys.DB
	orders *record.Store[int64, dataset.Order]
}

func openKV(ctx context.Context, sys *kvsys.System, s spec) (env, error) {
	var tuning *kvsys.Tuning
	if s.retries > 0 {
		tuning = &kvsys.Tuning{KV: []badgerkv.Option{badgerkv.WithMaxRetries(s.retries)}}
	}
	db, err := sys.OpenWith("concurrency-"+s.name, 0, tuning)
	if err != nil {
		return nil, err
	}
	e := &kvEnv{db: db, orders: record.New[int64, dataset.Order]("orders")}
	if _, err := e.orders.Migrate(ctx, db); err != nil {
		db.Destroy()
		return nil, err
	}
	return e, nil
}

// counterPrefix keeps the counters apart from the records of the store, and from the blind writes.
var (
	counterPrefix = []byte("c")
	blindPrefix   = []byte("b")
)

func counterKey(prefix []byte, k int64) []byte {
	return binary.BigEndian.AppendUint64(append([]byte(nil), prefix...), uint64(k))
}

func (e *kvEnv) clear(prefix []byte) error {
	return kv.DeleteRange(context.Background(), e.db, prefix, kv.PrefixEnd(prefix))
}

func (e *kvEnv) ResetCounters(_ context.Context, n int64) error {
	if err := e.clear(counterPrefix); err != nil {
		return err
	}
	zero := make([]byte, 8)
	for lo := int64(0); lo < n; lo += 2000 {
		err := e.db.Update(func(tx kv.Tx) error {
			for k := lo; k < min(lo+2000, n); k++ {
				if err := tx.Set(counterKey(counterPrefix, k), zero); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// Inc reads a counter, adds one and writes it, in db.Update: when another transaction has
// written the key since this one read it, the commit conflicts and Update runs the function again.
func (e *kvEnv) Inc(_ context.Context, key int64) error {
	k := counterKey(counterPrefix, key)
	return e.db.Update(func(tx kv.Tx) error {
		v, err := tx.Get(k)
		if err != nil {
			return err
		}
		out := make([]byte, 8)
		binary.BigEndian.PutUint64(out, binary.BigEndian.Uint64(v)+1)
		return tx.Set(k, out)
	})
}

func (e *kvEnv) SumCounters(context.Context) (int64, error) {
	var sum int64
	err := e.db.View(func(tx kv.Tx) error {
		_, sum = scanCounters(tx)
		return nil
	})
	return sum, err
}

// scanCounters adds up the counters that tx sees and returns how many there are.
func scanCounters(tx kv.Tx) (n, sum int64) {
	it, err := tx.Iterator(counterPrefix, kv.PrefixEnd(counterPrefix))
	if err != nil {
		return 0, 0
	}
	defer it.Close()
	for ; it.Valid(); it.Next() {
		v, err := it.Value()
		if err != nil {
			return n, sum
		}
		sum += int64(binary.BigEndian.Uint64(v))
		n++
	}
	return n, sum
}

// LongRead holds one read transaction open and scans the counters in it until it is stopped: the
// snapshot of the transaction is the same at every scan.
func (e *kvEnv) LongRead(_ context.Context, stop <-chan struct{}, st *readerStats) error {
	return e.db.View(func(tx kv.Tx) error {
		for {
			t0 := time.Now()
			n, sum := scanCounters(tx)
			st.note(sum, n, time.Since(t0))
			select {
			case <-stop:
				return nil
			default:
			}
		}
	})
}

func (e *kvEnv) ResetBlind(context.Context) error { return e.clear(blindPrefix) }

func (e *kvEnv) Blind(_ context.Context, id int64, value []byte) error {
	k := counterKey(blindPrefix, id)
	return e.db.Update(func(tx kv.Tx) error { return tx.Set(k, value) })
}

func (e *kvEnv) CountBlind(context.Context) (int64, error) {
	var n int64
	err := e.db.View(func(tx kv.Tx) error {
		it, err := tx.Iterator(blindPrefix, kv.PrefixEnd(blindPrefix))
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

func (e *kvEnv) LoadOrders(_ context.Context, spec dataset.Spec, n int64) error {
	_, err := wl.BulkLoad(e.db, 1, n+1, 1, func() func(kv.BulkWriter, int64) error {
		o := &dataset.Order{}
		return func(w kv.BulkWriter, id int64) error {
			spec.GenInto(o, id, 0)
			k, err := e.orders.KeyCodec().Encode(id)
			if err != nil {
				return err
			}
			v, err := e.orders.ValueCodec().Encode(*o)
			if err != nil {
				return err
			}
			return w.Set(append(append([]byte(nil), e.orders.DataPrefix()...), k...), v)
		}
	})
	return err
}

func (e *kvEnv) ReadOrder(_ context.Context, id int64) error {
	return e.db.View(func(tx kv.Tx) error {
		_, err := e.orders.Instantiate(tx).Get(id)
		return err
	})
}

func (e *kvEnv) BumpOrder(_ context.Context, id int64) error {
	return e.db.Update(func(tx kv.Tx) error {
		inst := e.orders.Instantiate(tx)
		o, err := inst.Get(id)
		if err != nil {
			return err
		}
		o.Qty++
		return inst.Set(o)
	})
}

func (e *kvEnv) SumQty(context.Context) (int64, error) {
	var sum int64
	err := e.db.View(func(tx kv.Tx) error {
		it, err := e.orders.Instantiate(tx).All()
		if err != nil {
			return err
		}
		defer it.Close()
		for ; it.Valid(); it.Next() {
			o, err := it.Value()
			if err != nil {
				return err
			}
			sum += int64(o.Qty)
		}
		return nil
	})
	return sum, err
}

func (e *kvEnv) Facts(context.Context) map[string]float64 { return nil }

func (e *kvEnv) Settle(context.Context) error { return e.db.Sync() }

func (e *kvEnv) Sizes(context.Context) (harness.Sizes, error) {
	n, err := e.db.DiskBytes()
	return harness.Sizes{Data: n}, err
}

func (e *kvEnv) Close(context.Context) error { return e.db.Destroy() }
