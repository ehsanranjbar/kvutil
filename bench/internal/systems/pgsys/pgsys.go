// Package pgsys is PostgreSQL as a system of the benchmarks: a pool of connections with prepared
// statements, the durability of the run, and a schema for each workload that is created and
// dropped by it.
package pgsys

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Config is how the system connects.
type Config struct {
	DSN        string
	Durability harness.Durability
	// MaxConns is the size of the pool: the most workers that run at once, and a few more.
	MaxConns int32
}

// System is a PostgreSQL server.
type System struct {
	cfg      Config
	pool     *pgxpool.Pool
	version  string
	settings map[string]string
}

// reported are the settings of the server that the results say.
var reported = []string{
	"server_version", "shared_buffers", "effective_cache_size", "work_mem", "maintenance_work_mem",
	"synchronous_commit", "fsync", "full_page_writes", "wal_level", "wal_buffers", "max_wal_size",
	"min_wal_size", "checkpoint_timeout", "checkpoint_completion_target", "wal_compression",
	"autovacuum", "max_connections", "random_page_cost", "max_parallel_workers_per_gather",
}

// Open connects, and reads the version and the settings of the server. Every connection has
// synchronous_commit as the durability of the run says, whatever the server is started with;
// fsync can only be set for the server, which the compose file does.
func Open(ctx context.Context, cfg Config) (*System, error) {
	pc, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("pg: bad DSN: %w", err)
	}
	if cfg.MaxConns <= 0 {
		cfg.MaxConns = 16
	}
	pc.MaxConns = cfg.MaxConns
	pc.MinConns = min(cfg.MaxConns, 4)
	// Statements are prepared on first use on a connection and kept.
	pc.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement
	pc.ConnConfig.StatementCacheCapacity = 256
	if pc.ConnConfig.RuntimeParams == nil {
		pc.ConnConfig.RuntimeParams = map[string]string{}
	}
	if cfg.Durability == harness.Relaxed {
		pc.ConnConfig.RuntimeParams["synchronous_commit"] = "off"
	} else {
		pc.ConnConfig.RuntimeParams["synchronous_commit"] = "on"
	}

	// The server may be starting still.
	var pool *pgxpool.Pool
	deadline := time.Now().Add(60 * time.Second)
	for {
		pool, err = pgxpool.NewWithConfig(ctx, pc)
		if err == nil {
			err = pool.Ping(ctx)
			if err == nil {
				break
			}
			pool.Close()
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return nil, fmt.Errorf("pg: cannot connect: %w", err)
		}
		time.Sleep(time.Second)
	}

	s := &System{cfg: cfg, pool: pool, settings: map[string]string{}}
	if err := pool.QueryRow(ctx, "SELECT version()").Scan(&s.version); err != nil {
		pool.Close()
		return nil, err
	}
	// current_setting has a value as it is written (1GB), where pg_settings has it in units.
	rows, err := pool.Query(ctx, "SELECT name, current_setting(name) FROM unnest($1::text[]) AS name", reported)
	if err != nil {
		pool.Close()
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var name, setting string
		if err := rows.Scan(&name, &setting); err != nil {
			pool.Close()
			return nil, err
		}
		s.settings[name] = setting
	}
	if err := rows.Err(); err != nil {
		pool.Close()
		return nil, err
	}
	// The session value is the one the workloads run with.
	var sc string
	if err := pool.QueryRow(ctx, "SHOW synchronous_commit").Scan(&sc); err == nil {
		s.settings["synchronous_commit"] = sc
	}
	return s, nil
}

// Name implements harness.System.
func (s *System) Name() string { return "postgres" }

// Version implements harness.System.
func (s *System) Version() string { return s.version }

// Settings implements harness.System.
func (s *System) Settings() map[string]string { return s.settings }

// Close implements harness.System.
func (s *System) Close() error {
	s.pool.Close()
	return nil
}

// Pool is the pool of connections.
func (s *System) Pool() *pgxpool.Pool { return s.pool }

// Durability is the durability that the system was opened with.
func (s *System) Durability() harness.Durability { return s.cfg.Durability }

// WALBytes returns the size of the write-ahead log of the server.
func (s *System) WALBytes(ctx context.Context) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, "SELECT coalesce(sum(size), 0)::bigint FROM pg_ls_waldir()").Scan(&n)
	return n, err
}

// Schema is a schema of a workload.
type Schema struct {
	Name string
	sys  *System
}

// Schema creates a schema for a workload, with another one of the name dropped first.
func (s *System) Schema(ctx context.Context, name string) (*Schema, error) {
	sc := &Schema{Name: "kvb_" + strings.ReplaceAll(name, "-", "_"), sys: s}
	if err := sc.Drop(ctx); err != nil {
		return nil, err
	}
	if _, err := s.pool.Exec(ctx, "CREATE SCHEMA "+sc.Name); err != nil {
		return nil, err
	}
	return sc, nil
}

// T returns the name of a table of the schema, as it is written in a statement.
func (sc *Schema) T(table string) string {
	return sc.Name + "." + table
}

// Exec runs statements that are not prepared and have no parameters, one after the other.
func (sc *Schema) Exec(ctx context.Context, stmts ...string) error {
	for _, stmt := range stmts {
		if _, err := sc.sys.pool.Exec(ctx, stmt, pgx.QueryExecModeSimpleProtocol); err != nil {
			return fmt.Errorf("%s: %w", firstLine(stmt), err)
		}
	}
	return nil
}

// Drop drops the schema and everything in it.
func (sc *Schema) Drop(ctx context.Context) error {
	_, err := sc.sys.pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+sc.Name+" CASCADE")
	return err
}

// DiskBytes returns the size of the tables of the schema with their indexes and TOAST tables.
func (sc *Schema) DiskBytes(ctx context.Context) (int64, error) {
	var n int64
	err := sc.sys.pool.QueryRow(ctx, `
SELECT coalesce(sum(pg_total_relation_size(c.oid)), 0)::bigint
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relkind IN ('r', 'm')`, sc.Name).Scan(&n)
	return n, err
}

// Sizes returns the size of the schema and of the log of the server, as the harness wants it.
func (sc *Schema) Sizes(ctx context.Context) (harness.Sizes, error) {
	data, err := sc.DiskBytes(ctx)
	if err != nil {
		return harness.Sizes{}, err
	}
	wal, werr := sc.sys.WALBytes(ctx)
	if werr != nil {
		wal = 0
	}
	return harness.Sizes{Data: data, WAL: wal}, nil
}

// Settle makes the server ready to be measured after a load: vacuum and analyze the tables, and
// a checkpoint, so that the work of the load does not run during the next measurement.
func (sc *Schema) Settle(ctx context.Context, tables ...string) error {
	for _, t := range tables {
		if err := sc.Exec(ctx, "VACUUM (ANALYZE) "+sc.T(t)); err != nil {
			return err
		}
	}
	return sc.Exec(ctx, "CHECKPOINT")
}

// IsNotFound reports whether the error is that a query had no row.
func IsNotFound(err error) bool { return errors.Is(err, pgx.ErrNoRows) }

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	return s
}
