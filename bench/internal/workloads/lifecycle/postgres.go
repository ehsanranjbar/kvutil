package lifecycle

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/dockerapi"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/pgsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/wl"
	"github.com/jackc/pgx/v5"
)

// indexDDL is the statement that creates the index of a kind; concurrently makes it the one that
// does not block writes.
func indexDDL(table, kind string, concurrently bool) string {
	create := "CREATE INDEX "
	if kind == "code" {
		create = "CREATE UNIQUE INDEX "
	}
	if concurrently {
		create += "CONCURRENTLY "
	}
	create += fmt.Sprintf("ix_%s ON %s ", kind, table)
	switch kind {
	case "customer":
		return create + "(customer)"
	case "status":
		return create + "(status)"
	case "total":
		return create + "(total)"
	case "code":
		return create + "(code)"
	case "composite":
		return create + "(customer, created_at)"
	case "tags":
		return create + "USING gin (tags)"
	}
	panic("unknown index kind " + kind)
}

type pgDriver struct {
	sys   *pgsys.System
	sc    *pgsys.Schema
	table string
	empty string
}

func openPGDriver(ctx context.Context, sys *pgsys.System) (driver, error) {
	sc, err := sys.Schema(ctx, "lifecycle")
	if err != nil {
		return nil, err
	}
	d := &pgDriver{sys: sys, sc: sc, table: sc.T("orders"), empty: sc.T("empty")}
	if err := sc.Exec(ctx, wl.OrderTable(d.table), wl.OrderTable(d.empty)); err != nil {
		sc.Drop(ctx)
		return nil, err
	}
	return d, nil
}

func (d *pgDriver) Load(ctx context.Context, spec dataset.Spec, n int64) error {
	o := &dataset.Order{}
	id := int64(1)
	cols := strings.Split(strings.ReplaceAll(wl.OrderColumns, " ", ""), ",")
	_, err := d.sys.Pool().CopyFrom(ctx, pgx.Identifier{d.sc.Name, "orders"}, cols, pgx.CopyFromFunc(func() ([]any, error) {
		if id > n {
			return nil, nil
		}
		spec.GenInto(o, id, 0)
		id++
		return wl.OrderArgs(o), nil
	}))
	return err
}

func (d *pgDriver) AddIndexes(ctx context.Context, kinds []string) error {
	for _, k := range kinds {
		if err := d.sc.Exec(ctx, indexDDL(d.table, k, false)); err != nil {
			return err
		}
	}
	return nil
}

func (d *pgDriver) DropIndexes(ctx context.Context, kinds []string) error {
	for _, k := range kinds {
		if err := d.sc.Exec(ctx, fmt.Sprintf("DROP INDEX %s.ix_%s", d.sc.Name, k)); err != nil {
			return err
		}
	}
	return nil
}

// CreateConcurrently builds the index without blocking writes, which takes two scans of the
// table and waits for the transactions that were open to end.
func (d *pgDriver) CreateConcurrently(ctx context.Context, kind string) error {
	return d.sc.Exec(ctx, indexDDL(d.table, kind, true))
}

func (d *pgDriver) Probe(ctx context.Context, kind string, p probe) ([]int64, error) {
	var where string
	var args []any
	switch kind {
	case "customer":
		where, args = "customer = $1", []any{p.S}
	case "status":
		where, args = "status = $1", []any{p.S}
	case "total":
		where, args = "total BETWEEN $1 AND $2", []any{p.Lo, p.Hi}
	case "code":
		where, args = "code = $1", []any{p.S}
	case "composite":
		where, args = "customer = $1 AND created_at BETWEEN $2 AND $3", []any{p.S, wl.Seconds(p.Lo), wl.Seconds(p.Hi)}
	case "tags":
		where, args = "tags @> ARRAY[$1]::text[]", []any{p.S}
	default:
		return nil, fmt.Errorf("unknown probe %q", kind)
	}
	return d.ids(ctx, "SELECT id FROM "+d.table+" WHERE "+where, args...)
}

func (d *pgDriver) ids(ctx context.Context, query string, args ...any) ([]int64, error) {
	rows, err := d.sys.Pool().Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// Convert adds a column and computes it for every row: the two statements are cases of their own.
func (d *pgDriver) Convert(ctx context.Context, step stepFunc) error {
	err := step("add-column", func(ctx context.Context) error {
		return d.sc.Exec(ctx, "ALTER TABLE "+d.table+" ADD COLUMN priority integer")
	})
	if err != nil {
		return err
	}
	return step("update", func(ctx context.Context) error {
		return d.sc.Exec(ctx, "UPDATE "+d.table+" SET priority = total / 10000")
	})
}

func (d *pgDriver) Priority(ctx context.Context, k int32) ([]int64, error) {
	return d.ids(ctx, "SELECT id FROM "+d.table+" WHERE priority = $1", k)
}

// AddField adds a column with a default, which PostgreSQL records in the catalog and does not
// write to the rows.
func (d *pgDriver) AddField(ctx context.Context) error {
	return d.sc.Exec(ctx, "ALTER TABLE "+d.table+" ADD COLUMN extra text NOT NULL DEFAULT 'x'")
}

func (d *pgDriver) Exists(ctx context.Context, id int64) (bool, error) {
	ids, err := d.ids(ctx, "SELECT id FROM "+d.table+" WHERE id = $1", id)
	return len(ids) == 1, err
}

func (d *pgDriver) Settle(ctx context.Context) error { return d.sc.Settle(ctx, "orders") }

func (d *pgDriver) Sizes(ctx context.Context) (int64, int64, error) {
	s, err := d.sc.Sizes(ctx)
	return s.Data, s.WAL, err
}

func (d *pgDriver) Close(ctx context.Context) error { return d.sc.Drop(ctx) }

// connectAndAsk connects as a new client does and runs a first query, and returns the two times.
func (d *pgDriver) connectAndAsk(ctx context.Context, query string, wait time.Duration) (connectD, firstD time.Duration, err error) {
	t0 := time.Now()
	deadline := t0.Add(wait)
	var conn *pgx.Conn
	for {
		conn, err = d.sys.Connect(ctx)
		if err == nil {
			break
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return 0, 0, err
		}
		time.Sleep(20 * time.Millisecond)
	}
	connectD = time.Since(t0)
	defer conn.Close(context.WithoutCancel(ctx))

	t1 := time.Now()
	var n int64
	err = conn.QueryRow(ctx, query).Scan(&n)
	return connectD, time.Since(t1), err
}

func (d *pgDriver) FreshOpen(ctx context.Context) (time.Duration, error) {
	c, f, err := d.connectAndAsk(ctx, "SELECT count(*) FROM "+d.empty, 0)
	return c + f, err
}

// Reopen has nothing to close: a client connects to a server that is running, and asks.
func (d *pgDriver) Reopen(ctx context.Context) (closeD, openD, readyD time.Duration, err error) {
	openD, readyD, err = d.connectAndAsk(ctx, "SELECT id FROM "+d.table+" WHERE id = 1", 0)
	return
}

// Restart restarts the container of the server through the Docker API, and returns when a new
// connection has its first answer. Without the socket of Docker it is only a new connection to
// the server that kept running, which the result says.
func (d *pgDriver) Restart(ctx context.Context) (time.Duration, string, error) {
	query := "SELECT id FROM " + d.table + " WHERE id = 1"
	container := os.Getenv("KVB_PG_CONTAINER")
	client := dockerapi.New(os.Getenv("KVB_DOCKER_SOCKET"))
	if container == "" || !client.Available(ctx) {
		c, f, err := d.connectAndAsk(ctx, query, 0)
		return c + f, "not a restart: no Docker socket to restart the container with, a new connection and a first query to the running server", err
	}

	// What was written is on disk, as it would be before a planned restart.
	if err := d.sc.Exec(ctx, "CHECKPOINT"); err != nil {
		return 0, "", err
	}
	t0 := time.Now()
	if err := client.Restart(ctx, container, 60*time.Second); err != nil {
		return 0, "", err
	}
	c, f, err := d.connectAndAsk(ctx, query, 120*time.Second)
	total := time.Since(t0)
	if err != nil {
		return 0, "", err
	}
	_ = c + f
	// The pool of the harness has connections of the server that was stopped.
	if err := d.sys.Reset(ctx, 60*time.Second); err != nil {
		return 0, "", err
	}
	return total, "restart of the container (stop, start) through the Docker API, then a new connection and a first query", nil
}
