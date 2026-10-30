package concurrency

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/pgsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/wl"
	"github.com/jackc/pgx/v5"
)

// The statements of the scenarios. The counters are incremented either way: a transaction that
// locks the row and writes it, or the one statement that PostgreSQL makes atomic.
const (
	sqlLockCounter = `SELECT n FROM %s WHERE k = $1 FOR UPDATE`
	sqlSetCounter  = `UPDATE %s SET n = $2 WHERE k = $1`
	sqlAtomicInc   = `UPDATE %s SET n = n + 1 WHERE k = $1`
	sqlSumCounters = `SELECT coalesce(sum(n), 0)::bigint, count(*) FROM %s`
	sqlFillCounter = `INSERT INTO %s SELECT g, 0 FROM generate_series(0, $1::bigint - 1) AS g`
	sqlBlind       = `INSERT INTO %s (k, v) VALUES ($1, $2)`
	sqlBump        = `UPDATE %s SET qty = qty + 1 WHERE id = $1`
	sqlSumQty      = `SELECT coalesce(sum(qty), 0)::bigint FROM %s`
)

type pgEnv struct {
	sys  *pgsys.System
	sc   *pgsys.Schema
	mode incMode
	// scenario says which table is vacuumed after a reset.
	scenario scenario

	counters, blind, orders string
}

func openPG(ctx context.Context, sys *pgsys.System, s spec) (env, error) {
	sc, err := sys.Schema(ctx, "concurrency")
	if err != nil {
		return nil, err
	}
	e := &pgEnv{sys: sys, sc: sc, mode: s.mode, scenario: s.scenario,
		counters: sc.T("counters"), blind: sc.T("blind"), orders: sc.T("orders")}
	err = sc.Exec(ctx,
		fmt.Sprintf("CREATE TABLE %s (k bigint PRIMARY KEY, n bigint NOT NULL)", e.counters),
		fmt.Sprintf("CREATE TABLE %s (k bigint PRIMARY KEY, v bytea NOT NULL)", e.blind),
		wl.OrderTable(e.orders))
	if err != nil {
		sc.Drop(ctx)
		return nil, err
	}
	return e, nil
}

func (e *pgEnv) ResetCounters(ctx context.Context, n int64) error {
	if err := e.sc.Exec(ctx, "TRUNCATE "+e.counters); err != nil {
		return err
	}
	_, err := e.sys.Pool().Exec(ctx, fmt.Sprintf(sqlFillCounter, e.counters), n)
	return err
}

// Inc adds one to a counter. With for-update it locks the row, reads it and writes it back in a
// transaction, so the writers of a row take turns; with atomic it is one statement.
func (e *pgEnv) Inc(ctx context.Context, key int64) error {
	pool := e.sys.Pool()
	if e.mode == modeAtomic {
		tag, err := pool.Exec(ctx, fmt.Sprintf(sqlAtomicInc, e.counters), key)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("counter %d: %d rows updated", key, tag.RowsAffected())
		}
		return nil
	}

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	var n int64
	if err := tx.QueryRow(ctx, fmt.Sprintf(sqlLockCounter, e.counters), key).Scan(&n); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(sqlSetCounter, e.counters), key, n+1); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (e *pgEnv) SumCounters(ctx context.Context) (int64, error) {
	var sum, n int64
	err := e.sys.Pool().QueryRow(ctx, fmt.Sprintf(sqlSumCounters, e.counters)).Scan(&sum, &n)
	return sum, err
}

// LongRead holds a transaction of the isolation level repeatable read open, which takes its
// snapshot at its first statement, and sums the counters in it until it is stopped.
func (e *pgEnv) LongRead(ctx context.Context, stop <-chan struct{}, st *readerStats) error {
	tx, err := e.sys.Pool().BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	query := fmt.Sprintf(sqlSumCounters, e.counters)
	for {
		t0 := time.Now()
		var sum, n int64
		if err := tx.QueryRow(ctx, query).Scan(&sum, &n); err != nil {
			return err
		}
		st.note(sum, n, time.Since(t0))
		select {
		case <-stop:
			return nil
		default:
		}
	}
}

func (e *pgEnv) ResetBlind(ctx context.Context) error {
	return e.sc.Exec(ctx, "TRUNCATE "+e.blind)
}

func (e *pgEnv) Blind(ctx context.Context, id int64, value []byte) error {
	_, err := e.sys.Pool().Exec(ctx, fmt.Sprintf(sqlBlind, e.blind), id, value)
	return err
}

func (e *pgEnv) CountBlind(ctx context.Context) (int64, error) {
	var n int64
	err := e.sys.Pool().QueryRow(ctx, "SELECT count(*) FROM "+e.blind).Scan(&n)
	return n, err
}

func (e *pgEnv) LoadOrders(ctx context.Context, spec dataset.Spec, n int64) error {
	o := &dataset.Order{}
	id := int64(1)
	cols := strings.Split(strings.ReplaceAll(wl.OrderColumns, " ", ""), ",")
	_, err := e.sys.Pool().CopyFrom(ctx, pgx.Identifier{e.sc.Name, "orders"}, cols,
		pgx.CopyFromFunc(func() ([]any, error) {
			if id > n {
				return nil, nil
			}
			spec.GenInto(o, id, 0)
			id++
			return wl.OrderArgs(o), nil
		}))
	return err
}

func (e *pgEnv) ReadOrder(ctx context.Context, id int64) error {
	var o dataset.Order
	return wl.ScanOrder(e.sys.Pool().QueryRow(ctx,
		fmt.Sprintf("SELECT %s FROM %s WHERE id = $1", wl.OrderColumns, e.orders), id), &o)
}

func (e *pgEnv) BumpOrder(ctx context.Context, id int64) error {
	tag, err := e.sys.Pool().Exec(ctx, fmt.Sprintf(sqlBump, e.orders), id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("order %d: %d rows updated", id, tag.RowsAffected())
	}
	return nil
}

func (e *pgEnv) SumQty(ctx context.Context) (int64, error) {
	var sum int64
	err := e.sys.Pool().QueryRow(ctx, fmt.Sprintf(sqlSumQty, e.orders)).Scan(&sum)
	return sum, err
}

// Facts are the dead tuples and the size of the table of the counters, which a transaction that
// stays open keeps the vacuum from cleaning.
func (e *pgEnv) Facts(ctx context.Context) map[string]float64 {
	var dead, size int64
	err := e.sys.Pool().QueryRow(ctx, `
SELECT coalesce((SELECT n_dead_tup FROM pg_stat_user_tables WHERE relid = $1::regclass), 0)::bigint,
       pg_total_relation_size($1::regclass)`, e.counters).Scan(&dead, &size)
	if err != nil {
		return nil
	}
	return map[string]float64{"dead_tuples": float64(dead), "table_bytes": float64(size)}
}

func (e *pgEnv) Settle(ctx context.Context) error {
	table := map[scenario]string{scenarioRMW: "counters", scenarioLong: "counters", scenarioBlind: "blind", scenarioMixed: "orders"}[e.scenario]
	return e.sc.Settle(ctx, table)
}

func (e *pgEnv) Sizes(ctx context.Context) (harness.Sizes, error) { return e.sc.Sizes(ctx) }

func (e *pgEnv) Close(ctx context.Context) error { return e.sc.Drop(ctx) }
