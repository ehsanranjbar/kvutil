// Package wl has what the workloads share: the mapping of the records to PostgreSQL rows, the
// loaders of the two systems, and the pools of ids that the workers take from.
package wl

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/jackc/pgx/v5"
	"github.com/vmihailenco/msgpack/v5"
)

// Msgpack returns the record as the default codec of kvutil encodes it.
func Msgpack(o *dataset.Order) []byte {
	bz, err := msgpack.Marshal(o)
	if err != nil {
		panic(err) // a record of the dataset always encodes
	}
	return bz
}

// DecodeMsgpack decodes a record encoded by Msgpack.
func DecodeMsgpack(bz []byte, id int64) (*dataset.Order, error) {
	var o dataset.Order
	if err := msgpack.Unmarshal(bz, &o); err != nil {
		return nil, err
	}
	o.ID = id
	return &o, nil
}

// Key8 is the key of a record in a raw store: its id in 8 bytes that sort as the ids do.
func Key8(id int64) []byte {
	return binary.BigEndian.AppendUint64(make([]byte, 0, 8), uint64(id))
}

// OrderColumns are the columns of the table of the records, in the order of OrderArgs.
const OrderColumns = "id, code, customer, name, status, total, qty, created_at, note, tags, payload"

// OrderTable returns the statement that creates a table of the records as columns.
func OrderTable(name string) string {
	return fmt.Sprintf(`CREATE TABLE %s (
	id         bigint PRIMARY KEY,
	code       text NOT NULL,
	customer   text NOT NULL,
	name       text NOT NULL,
	status     text NOT NULL,
	total      bigint NOT NULL,
	qty        integer NOT NULL,
	created_at timestamptz NOT NULL,
	note       text,
	tags       text[] NOT NULL,
	payload    bytea
)`, name)
}

// OrderPlaceholders returns "$1, $2, ..." for the columns, starting after the given number of
// parameters.
func OrderPlaceholders(after int) string {
	s := ""
	for i := 1; i <= 11; i++ {
		if i > 1 {
			s += ", "
		}
		s += fmt.Sprintf("$%d", after+i)
	}
	return s
}

// OrderArgs returns the values of the columns of a record.
func OrderArgs(o *dataset.Order) []any {
	tags := o.Tags
	if tags == nil {
		tags = []string{}
	}
	return []any{o.ID, o.Code, o.Customer, o.Name, o.Status, o.Total, o.Qty, o.At, o.Note, tags, o.Payload}
}

// ScanOrder reads a row of the columns into a record.
func ScanOrder(row pgx.Row, o *dataset.Order) error {
	err := row.Scan(&o.ID, &o.Code, &o.Customer, &o.Name, &o.Status, &o.Total, &o.Qty, &o.At, &o.Note, &o.Tags, &o.Payload)
	if err == nil {
		o.At = o.At.UTC()
	}
	return err
}

// Cursor hands out the ids of a range one by one to the workers, each once.
type Cursor struct {
	next atomic.Int64
	end  int64
}

// NewCursor returns a cursor over [from, to).
func NewCursor(from, to int64) *Cursor {
	c := &Cursor{end: to}
	c.next.Store(from)
	return c
}

// Take returns the next id, or false when the range is used up.
func (c *Cursor) Take() (int64, bool) {
	id := c.next.Add(1) - 1
	return id, id < c.end
}

// Split divides [from, to) into n contiguous ranges of about the same size.
func Split(from, to int64, n int) [][2]int64 {
	if n < 1 {
		n = 1
	}
	total := to - from
	ranges := make([][2]int64, 0, n)
	for i := 0; i < n; i++ {
		lo := from + total*int64(i)/int64(n)
		hi := from + total*int64(i+1)/int64(n)
		if hi > lo {
			ranges = append(ranges, [2]int64{lo, hi})
		}
	}
	return ranges
}

func parallel(ranges [][2]int64, fn func(lo, hi int64) error) error {
	var wg sync.WaitGroup
	errs := make([]error, len(ranges))
	for i, r := range ranges {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = fn(r[0], r[1])
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// BulkLoad writes the records of [from, to) with a bulk writer (Badger's WriteBatch) for each of
// the workers, which load contiguous parts. newPut makes the function of a worker, which can
// keep state of its own.
func BulkLoad(db kv.DB, from, to int64, workers int, newPut func() func(kv.BulkWriter, int64) error) (int64, error) {
	err := parallel(Split(from, to, workers), func(lo, hi int64) (err error) {
		bw, err := kv.NewBulkWriter(db)
		if err != nil {
			return err
		}
		defer bw.Cancel()
		put := newPut()
		for id := lo; id < hi; id++ {
			if err := put(bw, id); err != nil {
				return err
			}
		}
		return bw.Flush()
	})
	if err != nil {
		return 0, err
	}
	return to - from, nil
}

// TxLoad writes the records of [from, to) in transactions of batch records, on a number of
// workers that load contiguous parts. newPut makes the function of a worker.
func TxLoad(ctx context.Context, db kv.DB, from, to int64, batch, workers int, newPut func() func(kv.Tx, int64) error) (int64, error) {
	err := parallel(Split(from, to, workers), func(lo, hi int64) error {
		put := newPut()
		for start := lo; start < hi; start += int64(batch) {
			if err := ctx.Err(); err != nil {
				return err
			}
			end := min(start+int64(batch), hi)
			err := db.Update(func(tx kv.Tx) error {
				for id := start; id < end; id++ {
					if err := put(tx, id); err != nil {
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
	})
	if err != nil {
		return 0, err
	}
	return to - from, nil
}

// ParallelPG runs fn on contiguous parts of [from, to), one per worker.
func ParallelPG(from, to int64, workers int, fn func(lo, hi int64) error) error {
	return parallel(Split(from, to, workers), fn)
}
