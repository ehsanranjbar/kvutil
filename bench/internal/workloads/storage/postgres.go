package storage

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/dockerapi"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/pgsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/wl"
	"github.com/jackc/pgx/v5"
)

// indexDDL creates the index of a kind.
func indexDDL(table, kind string) string {
	create := fmt.Sprintf("CREATE INDEX ix_%s ON %s ", kind, table)
	switch kind {
	case "customer":
		return create + "(customer)"
	case "code":
		return strings.Replace(create, "CREATE INDEX", "CREATE UNIQUE INDEX", 1) + "(code)"
	case "composite":
		return create + "(customer, created_at)"
	case "tags":
		return create + "USING gin (tags)"
	}
	panic("unknown index " + kind)
}

// How many rows a batch of statements has: the same as the records of a transaction of kvutil.
const batchRows = 500

// pgDriver is PostgreSQL with a table of its own.
type pgDriver struct {
	sys   *pgsys.System
	sc    *pgsys.Schema
	meter *dockerapi.Meter
	table string
	cols  []string
	kinds []string

	insertPlain, insertTTL, update, delete string
	job                                    *jobStats
}

func openPG(ctx context.Context, sys *pgsys.System, v *variant, meter *dockerapi.Meter) (driver, error) {
	sc, err := sys.Schema(ctx, "storage")
	if err != nil {
		return nil, err
	}
	d := &pgDriver{sys: sys, sc: sc, meter: meter, table: sc.T("orders")}
	d.cols = strings.Split(strings.ReplaceAll(wl.OrderColumns, " ", ""), ",")
	if err := sc.Exec(ctx, wl.OrderTable(d.table)); err != nil {
		sc.Drop(ctx)
		return nil, err
	}
	if v.what == "footprint" {
		// The vacuum is run by the workload when it says so, so that the sizes between are the
		// ones that the writes left.
		err = sc.Exec(ctx, "ALTER TABLE "+d.table+" SET (autovacuum_enabled = false)")
	} else {
		err = sc.Exec(ctx, "ALTER TABLE "+d.table+" ADD COLUMN expires_at timestamptz",
			"CREATE INDEX orders_expires ON "+d.table+" (expires_at) WHERE expires_at IS NOT NULL")
	}
	if err != nil {
		sc.Drop(ctx)
		return nil, err
	}

	d.update = fmt.Sprintf("UPDATE %s SET code = $2, customer = $3, name = $4, status = $5, total = $6, qty = $7, created_at = $8, note = $9, tags = $10, payload = $11 WHERE id = $1", d.table)
	d.delete = fmt.Sprintf("DELETE FROM %s WHERE id = $1", d.table)
	d.insertPlain = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", d.table, wl.OrderColumns, wl.OrderPlaceholders(0))
	d.insertTTL = fmt.Sprintf("INSERT INTO %s (%s, expires_at) VALUES (%s, now() + $12::float8 * interval '1 second')", d.table, wl.OrderColumns, wl.OrderPlaceholders(0))
	return d, nil
}

func (d *pgDriver) Load(ctx context.Context, spec dataset.Spec, n int64) error {
	o := &order{}
	id := int64(1)
	_, err := d.sys.Pool().CopyFrom(ctx, pgx.Identifier{d.sc.Name, "orders"}, d.cols, pgx.CopyFromFunc(func() ([]any, error) {
		if id > n {
			return nil, nil
		}
		spec.GenInto(o, id, 0)
		id++
		return wl.OrderArgs(o), nil
	}))
	return err
}

func (d *pgDriver) AddIndex(ctx context.Context, kind string) error {
	if err := d.sc.Exec(ctx, indexDDL(d.table, kind)); err != nil {
		return err
	}
	d.kinds = append(d.kinds, kind)
	return nil
}

// Components are the sizes that PostgreSQL says: the table with its TOAST table, and each index.
func (d *pgDriver) Components(ctx context.Context) (map[string]float64, error) {
	out := map[string]float64{}
	var table, total int64
	err := d.sys.Pool().QueryRow(ctx, "SELECT pg_table_size($1::regclass), pg_total_relation_size($1::regclass)", d.table).Scan(&table, &total)
	if err != nil {
		return nil, err
	}
	out["table_bytes"], out["total_bytes"] = float64(table), float64(total)
	for _, kind := range d.kinds {
		var n int64
		if err := d.sys.Pool().QueryRow(ctx, "SELECT pg_relation_size($1::regclass)", d.sc.Name+".ix_"+kind).Scan(&n); err != nil {
			return nil, err
		}
		out["index_"+kind+"_bytes"] = float64(n)
	}
	return out, nil
}

// batch runs the statements of rows [1, n] in transactions of batchRows rows: one round trip
// and one commit for each, as a transaction of kvutil.
func (d *pgDriver) batch(ctx context.Context, n int64, add func(b *pgx.Batch, id int64)) error {
	for lo := int64(1); lo <= n; lo += batchRows {
		b := &pgx.Batch{}
		for id := lo; id < min(lo+batchRows, n+1); id++ {
			add(b, id)
		}
		if b.Len() == 0 {
			continue
		}
		if err := d.sys.Pool().SendBatch(ctx, b).Close(); err != nil {
			return err
		}
	}
	return nil
}

func (d *pgDriver) Rewrite(ctx context.Context, spec dataset.Spec, n int64, version uint32) error {
	o := &order{}
	return d.batch(ctx, n, func(b *pgx.Batch, id int64) {
		spec.GenInto(o, id, version)
		b.Queue(d.update, wl.OrderArgs(o)...)
	})
}

func (d *pgDriver) DeleteOdd(ctx context.Context, n int64) error {
	return d.batch(ctx, n, func(b *pgx.Batch, id int64) {
		if id%2 == 1 {
			b.Queue(d.delete, id)
		}
	})
}

func (d *pgDriver) Count(ctx context.Context) (int64, error) {
	var n int64
	err := d.sys.Pool().QueryRow(ctx, "SELECT count(*) FROM "+d.table).Scan(&n)
	return n, err
}

// Reclaims: VACUUM, which makes the space of dead rows reusable and gives back only the empty
// pages at the end of the table, and VACUUM FULL, which writes the table and its indexes again.
func (d *pgDriver) Reclaims() []reclaim {
	return []reclaim{
		{"vacuum", func(ctx context.Context) error { return d.sc.Exec(ctx, "VACUUM "+d.table) }},
		{"vacuum-full", func(ctx context.Context) error { return d.sc.Exec(ctx, "VACUUM FULL "+d.table) }},
	}
}

func (d *pgDriver) ExpiryPrepare(context.Context) error { return nil }

func (d *pgDriver) ExpiryInsert(ctx context.Context, o *dataset.Order, ttl time.Duration) error {
	args := wl.OrderArgs(o)
	if ttl > 0 {
		_, err := d.sys.Pool().Exec(ctx, d.insertTTL, append(args, ttl.Seconds())...)
		return err
	}
	_, err := d.sys.Pool().Exec(ctx, d.insertPlain, args...)
	return err
}

// ExpiryStartJob deletes the rows that have expired, every interval: the job that a table with an
// expiry time needs. It uses the index on the expiry time.
func (d *pgDriver) ExpiryStartJob(ctx context.Context, every time.Duration) func() map[string]float64 {
	stats := &jobStats{}
	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(every)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			t0 := time.Now()
			tag, err := d.sys.Pool().Exec(ctx, "DELETE FROM "+d.table+" WHERE expires_at IS NOT NULL AND expires_at < now()")
			if ctx.Err() != nil {
				return
			}
			stats.note(tag.RowsAffected(), time.Since(t0), err)
		}
	}()
	return func() map[string]float64 {
		cancel()
		<-done
		return stats.result()
	}
}

func (d *pgDriver) ExpiryLive(ctx context.Context) (plain, expiring int64, err error) {
	err = d.sys.Pool().QueryRow(ctx, `SELECT count(*) FILTER (WHERE expires_at IS NULL),
	count(*) FILTER (WHERE expires_at IS NOT NULL AND expires_at > now()) FROM `+d.table).Scan(&plain, &expiring)
	return
}

// Flush checkpoints, which writes the dirty pages, and waits for the statistics of the processes
// that write (the checkpointer, the background writer) to reach the views.
func (d *pgDriver) Flush(ctx context.Context) error {
	if err := d.sc.Exec(ctx, "CHECKPOINT"); err != nil {
		return err
	}
	select {
	case <-time.After(1200 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (d *pgDriver) Sizes(ctx context.Context) (int64, int64, error) {
	s, err := d.sc.Sizes(ctx)
	return s.Data, s.WAL, err
}

func (d *pgDriver) Meter() *dockerapi.Meter { return d.meter }

// Counters are the bytes of WAL that the server has made, and the bytes that it has written
// according to its I/O statistics (pg_stat_io): to the relations and to the WAL.
func (d *pgDriver) Counters(ctx context.Context) map[string]float64 {
	out := map[string]float64{}
	var wal float64
	if err := d.sys.Pool().QueryRow(ctx, "SELECT wal_bytes::float8 FROM pg_stat_wal").Scan(&wal); err == nil {
		out["wal_bytes"] = wal
	}
	var written float64
	err := d.sys.Pool().QueryRow(ctx,
		"SELECT (coalesce(sum(write_bytes), 0) + coalesce(sum(extend_bytes), 0))::float8 FROM pg_stat_io").Scan(&written)
	if err == nil {
		out["pg_stat_io_write_bytes"] = written
	}
	return out
}

func (d *pgDriver) Close(ctx context.Context) error { return d.sc.Drop(ctx) }
