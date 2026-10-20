package bulkload

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/pgsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/wl"
	"github.com/jackc/pgx/v5"
)

// pgTarget is a table: of the key and the bytes of the record, or of the columns of the record.
type pgTarget struct {
	sys     *pgsys.System
	sc      *pgsys.Schema
	rows    bool
	table   string
	columns []string

	mu  sync.Mutex
	sql map[int]string
}

func openPGBytea(ctx context.Context, sys harness.System, p harness.Params) (target, error) {
	return openPG(ctx, sys, false)
}

func openPGRows(ctx context.Context, sys harness.System, p harness.Params) (target, error) {
	return openPG(ctx, sys, true)
}

func openPG(ctx context.Context, sys harness.System, rows bool) (target, error) {
	ps := sys.(*pgsys.System)
	sc, err := ps.Schema(ctx, "bulk-load")
	if err != nil {
		return nil, err
	}
	t := &pgTarget{sys: ps, sc: sc, rows: rows, table: sc.T("t"), sql: map[int]string{}}
	ddl := "CREATE TABLE " + t.table + " (k bigint PRIMARY KEY, v bytea NOT NULL)"
	t.columns = []string{"k", "v"}
	if rows {
		ddl = wl.OrderTable(t.table)
		t.columns = strings.Split(strings.ReplaceAll(wl.OrderColumns, " ", ""), ",")
	}
	if err := sc.Exec(ctx, ddl); err != nil {
		sc.Drop(ctx)
		return nil, err
	}
	return t, nil
}

// args returns the values of a row for the record.
func (t *pgTarget) args(o *dataset.Order) []any {
	if t.rows {
		return wl.OrderArgs(o)
	}
	return []any{o.ID, wl.Msgpack(o)}
}

// insertSQL returns the statement that inserts a number of rows.
func (t *pgTarget) insertSQL(rows int) string {
	t.mu.Lock()
	defer t.mu.Unlock()
	if s, ok := t.sql[rows]; ok {
		return s
	}
	s := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", t.table, strings.Join(t.columns, ", "), placeholders(rows, len(t.columns)))
	t.sql[rows] = s
	return s
}

func (t *pgTarget) Check(ctx context.Context, digest bool) (int64, string, error) {
	pool := t.sys.Pool()
	d := newDigester()
	if !digest {
		var n int64
		err := pool.QueryRow(ctx, "SELECT count(*) FROM "+t.table).Scan(&n)
		return n, "", err
	}
	if t.rows {
		rows, err := pool.Query(ctx, "SELECT "+wl.OrderColumns+" FROM "+t.table+" ORDER BY id")
		if err != nil {
			return 0, "", err
		}
		defer rows.Close()
		for rows.Next() {
			var o dataset.Order
			if err := wl.ScanOrder(rows, &o); err != nil {
				return 0, "", err
			}
			d.add(&o)
		}
		return d.n, d.sum(), rows.Err()
	}
	rows, err := pool.Query(ctx, "SELECT k, v FROM "+t.table+" ORDER BY k")
	if err != nil {
		return 0, "", err
	}
	defer rows.Close()
	for rows.Next() {
		var k int64
		var v []byte
		if err := rows.Scan(&k, &v); err != nil {
			return 0, "", err
		}
		o, err := wl.DecodeMsgpack(v, k)
		if err != nil {
			return 0, "", err
		}
		d.add(o)
	}
	return d.n, d.sum(), rows.Err()
}

func (t *pgTarget) Sizes(ctx context.Context) (harness.Sizes, error) { return t.sc.Sizes(ctx) }

func (t *pgTarget) Close(ctx context.Context) error { return t.sc.Drop(ctx) }

func pgStrategies() []strategy {
	copyFrom := strategy{
		name: "copy",
		load: func(ctx context.Context, t target, spec dataset.Spec, n int64, workers int) error {
			pt := t.(*pgTarget)
			return wl.ParallelPG(0, n, workers, func(lo, hi int64) error {
				o := &dataset.Order{}
				id := lo
				_, err := pt.sys.Pool().CopyFrom(ctx, pgx.Identifier{pt.sc.Name, "t"}, pt.columns,
					pgx.CopyFromFunc(func() ([]any, error) {
						if id >= hi {
							return nil, nil
						}
						spec.GenInto(o, id, 0)
						id++
						return pt.args(o), nil
					}))
				return err
			})
		},
	}

	insert := func(batch int) strategy {
		return strategy{
			name:    fmt.Sprintf("insert-b%d", batch),
			params:  map[string]any{"batch": batch},
			singles: batch == 1,
			load: func(ctx context.Context, t target, spec dataset.Spec, n int64, workers int) error {
				pt := t.(*pgTarget)
				return wl.ParallelPG(0, n, workers, func(lo, hi int64) error {
					// A record of its own for each row of a statement: the arguments refer to it.
					orders := make([]dataset.Order, batch)
					args := make([]any, 0, batch*len(pt.columns))
					for start := lo; start < hi; start += int64(batch) {
						end := min(start+int64(batch), hi)
						args = args[:0]
						for id := start; id < end; id++ {
							o := &orders[id-start]
							spec.GenInto(o, id, 0)
							args = append(args, pt.args(o)...)
						}
						if _, err := pt.sys.Pool().Exec(ctx, pt.insertSQL(int(end-start)), args...); err != nil {
							return err
						}
					}
					return nil
				})
			},
		}
	}
	return []strategy{copyFrom, insert(1000), insert(100), insert(1)}
}
