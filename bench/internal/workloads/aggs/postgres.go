package aggs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/pgsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/tssys"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// querier is what a pool and a connection of it have in common.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// sqlEnv is the events and their aggregates in PostgreSQL (computed at read time, or in a summary
// table that a trigger keeps) or in TimescaleDB (a hypertable and continuous aggregates).
type sqlEnv struct {
	s       setup
	variant string
	pg      *pgsys.System
	ts      *tssys.System // nil on PostgreSQL
	sc      *pgsys.Schema
	n       names
	q       querier
	plain   bool

	// src is where Read reads from.
	src source
	// jobs are the refresh policies of the continuous aggregates, once they are added.
	jobs []int32
	// zone is a connection with another time zone, for the verification.
	zone *pgxpool.Conn
}

func openSQL(ctx context.Context, pg *pgsys.System, ts *tssys.System, name string, s setup, schema string, plain bool) (env, error) {
	sc, err := pg.Schema(ctx, schema+"-"+name)
	if err != nil {
		return nil, err
	}
	e := &sqlEnv{s: s, variant: name, pg: pg, ts: ts, sc: sc, n: namesIn(sc.Name), q: pg.Pool(), plain: plain}
	var stmts []string
	switch name {
	case pgComputed:
		e.src = srcRaw
		stmts = ddlEvents(e.n, false)
	case pgTrigger:
		e.src = srcSummary
		stmts = ddlEvents(e.n, false)
		if !plain {
			stmts = append(stmts, ddlSummary(e.n)...)
		}
	case tsRealtime, tsMaterialized, tsCaggs:
		e.src = srcCagg
		stmts = ddlEvents(e.n, true)
		if !plain {
			for _, u := range caggUnits {
				stmts = append(stmts, ddlCagg(e.n, u, false))
			}
			if name == tsRealtime {
				for _, u := range caggUnits {
					stmts = append(stmts, ddlRealtime(e.n, u, true))
				}
			}
		}
	default:
		sc.Drop(ctx)
		return nil, fmt.Errorf("aggs: unknown variant %q", name)
	}
	if err := sc.Exec(ctx, stmts...); err != nil {
		sc.Drop(ctx)
		return nil, err
	}
	return e, nil
}

func (e *sqlEnv) timescale() bool { return e.ts != nil }

var eventColumns = []string{"id", "customer", "amount", "at", "kind"}

func eventArgs(ev *event) []any { return []any{ev.ID, ev.Customer, ev.Amount, ev.At, ev.Kind} }

// copyEvents loads the events 1 to n with COPY.
func (e *sqlEnv) copyEvents(ctx context.Context, n int64) error {
	ev := &event{}
	id := int64(1)
	_, err := e.pg.Pool().CopyFrom(ctx, pgx.Identifier{e.sc.Name, "events"}, eventColumns,
		pgx.CopyFromFunc(func() ([]any, error) {
			if id > n {
				return nil, nil
			}
			e.s.spec.GenInto(ev, id, 0)
			id++
			return eventArgs(ev), nil
		}))
	return err
}

// LoadPlain loads the events and nothing else.
func (e *sqlEnv) LoadPlain(ctx context.Context, n int64) error {
	return e.copyEvents(ctx, n)
}

// Load copies the events in. The trigger of the summary is off while it does, and the summary is
// built from the events after it, so that the load is not a call of a function for each row; on
// TimescaleDB the continuous aggregates have the events invalidated for them and CatchUp refreshes.
func (e *sqlEnv) Load(ctx context.Context, n int64) error {
	if e.variant != pgTrigger {
		return e.copyEvents(ctx, n)
	}
	if err := e.sc.Exec(ctx, fmt.Sprintf("ALTER TABLE %s DISABLE TRIGGER events_summary", e.n.events)); err != nil {
		return err
	}
	if err := e.copyEvents(ctx, n); err != nil {
		return err
	}
	return e.sc.Exec(ctx,
		fmt.Sprintf("TRUNCATE %s", e.n.summary),
		buildSummary(e.n),
		fmt.Sprintf("ALTER TABLE %s ENABLE TRIGGER events_summary", e.n.events),
		fmt.Sprintf("ANALYZE %s", e.n.summary))
}

// Build defines the aggregates over the events that are there; each is a step.
func (e *sqlEnv) Build(ctx context.Context, step func(name string, fn func(context.Context) error) error) error {
	if e.variant == pgTrigger {
		if err := e.sc.Exec(ctx, ddlSummary(e.n)[0]); err != nil {
			return err
		}
		if err := step("build-summary", func(ctx context.Context) error {
			return e.sc.Exec(ctx, buildSummary(e.n))
		}); err != nil {
			return err
		}
		e.plain = false
		return e.sc.Exec(ctx, ddlSummary(e.n)[1:]...)
	}

	// The continuous aggregates are made with the data once: the initial materialization.
	err := step("create-with-data", func(ctx context.Context) error {
		for _, u := range caggUnits {
			if err := e.sc.Exec(ctx, ddlCagg(e.n, u, true)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	// And made again without it, and refreshed over the whole range, which is the other way to build them.
	for _, u := range slicesReverse(caggUnits) {
		if err := e.sc.Exec(ctx, "DROP MATERIALIZED VIEW "+e.n.cagg(u)); err != nil {
			return err
		}
	}
	for _, u := range caggUnits {
		if err := e.sc.Exec(ctx, ddlCagg(e.n, u, false)); err != nil {
			return err
		}
	}
	for _, u := range caggUnits {
		if err := step("refresh-"+u.String(), func(ctx context.Context) error {
			return e.refresh(ctx, u)
		}); err != nil {
			return err
		}
	}
	e.plain = false
	return nil
}

func slicesReverse(us []unit) []unit {
	out := make([]unit, len(us))
	for i, u := range us {
		out[len(us)-1-i] = u
	}
	return out
}

func (e *sqlEnv) CatchUp(ctx context.Context) error {
	if !e.timescale() {
		return nil
	}
	for _, u := range caggUnits {
		if err := e.refresh(ctx, u); err != nil {
			return err
		}
	}
	return nil
}

// refresh refreshes a continuous aggregate over its whole range: up to the end of the data for the
// materialized aggregates, and up to the end of the events that were loaded (the buckets that are
// complete before it) for the real-time ones, so that what is added after it stays with the
// real-time part. A refresh that the policy is running at that moment makes this one fail
// (55P03, lock not available); it is tried again.
func (e *sqlEnv) refresh(ctx context.Context, u unit) error {
	end := "NULL"
	if e.variant == tsRealtime {
		end = fmt.Sprintf("'%s'::timestamptz", e.s.spec.End.UTC().Format(time.RFC3339))
	}
	deadline := time.Now().Add(time.Minute)
	for {
		err := e.sc.Exec(ctx, refreshAll(e.n, u, end))
		var pe *pgconn.PgError
		if err == nil || !errors.As(err, &pe) || pe.Code != "55P03" || time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// endOffset is where the refresh window of the policies ends. Policies of materialized aggregates
// refresh up to the last data (NULL), which keeps the materialized state as current as the schedule
// allows; the policies of real-time aggregates leave the last hour to the real-time part, as the
// documentation of TimescaleDB advises, so that what is written to the current buckets is read
// exactly without a refresh.
func (e *sqlEnv) endOffset() string {
	if e.variant == tsRealtime {
		return "INTERVAL '1 hour'"
	}
	return "NULL"
}

func (e *sqlEnv) Background(ctx context.Context, on bool) error {
	if !e.timescale() {
		return nil
	}
	pool := e.pg.Pool()
	if e.jobs == nil {
		if !on {
			return nil
		}
		for _, u := range caggUnits {
			var id int32
			err := pool.QueryRow(ctx, policy(e.n, u, e.endOffset(), e.s.interval), pgx.QueryExecModeSimpleProtocol).Scan(&id)
			if err != nil {
				return fmt.Errorf("policy of %s: %w", u, err)
			}
			e.jobs = append(e.jobs, id)
		}
		return nil
	}
	for _, id := range e.jobs {
		var err error
		if on {
			_, err = pool.Exec(ctx, "SELECT alter_job($1, scheduled => true, next_start => now())", id)
		} else {
			_, err = pool.Exec(ctx, "SELECT alter_job($1, scheduled => false)", id)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *sqlEnv) Insert(ctx context.Context, ev *event) error {
	_, err := e.q.Exec(ctx, insertSQL(e.n), eventArgs(ev)...)
	return err
}

func (e *sqlEnv) InsertBatch(ctx context.Context, es []*event) error {
	var b pgx.Batch
	for _, ev := range es {
		b.Queue(insertSQL(e.n), eventArgs(ev)...)
	}
	res := e.pg.Pool().SendBatch(ctx, &b)
	defer res.Close()
	for range es {
		if _, err := res.Exec(); err != nil {
			return err
		}
	}
	return nil
}

func (e *sqlEnv) Update(ctx context.Context, old, ev *event) error {
	args := eventArgs(ev)
	if e.timescale() {
		if !old.At.Equal(ev.At) {
			// A row cannot be updated into another chunk of a hypertable: it is deleted and
			// inserted again, in one transaction.
			var b pgx.Batch
			b.Queue(deleteSQL(e.n, true), old.ID, old.At)
			b.Queue(insertSQL(e.n), args...)
			res := e.pg.Pool().SendBatch(ctx, &b)
			defer res.Close()
			for range 2 {
				if _, err := res.Exec(); err != nil {
					return err
				}
			}
			return nil
		}
		args = append(args, old.At)
	}
	_, err := e.q.Exec(ctx, updateSQL(e.n, e.timescale()), args...)
	return err
}

func (e *sqlEnv) Delete(ctx context.Context, old *event) error {
	args := []any{old.ID}
	if e.timescale() {
		args = append(args, old.At)
	}
	_, err := e.q.Exec(ctx, deleteSQL(e.n, e.timescale()), args...)
	return err
}

func (e *sqlEnv) Read(ctx context.Context, q query) ([]row, error) { return e.read(ctx, e.src, q) }

func (e *sqlEnv) Truth(ctx context.Context, q query) ([]row, error) { return e.read(ctx, srcRaw, q) }

func (e *sqlEnv) read(ctx context.Context, src source, q query) ([]row, error) {
	rows, err := e.q.Query(ctx, readSQL(e.n, src, q), readArgs(src, q)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []row
	for rows.Next() {
		var r row
		switch q.Kind {
		case qSeries:
			err = rows.Scan(&r.Bucket, &r.Sum, &r.Count, &r.Max)
			r.Bucket = r.Bucket.UTC()
		case qTotal:
			err = rows.Scan(&r.Sum, &r.Count)
		default:
			err = rows.Scan(&r.Customer, &r.Sum)
		}
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// SetZone makes the connection that the reads use one with another time zone, until the function
// that it returns is called. The time zone must not change what any system answers.
func (e *sqlEnv) SetZone(ctx context.Context, zone string) (func(), error) {
	conn, err := e.pg.Pool().Acquire(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := conn.Exec(ctx, "SET TimeZone = '"+zone+"'"); err != nil {
		conn.Release()
		return nil, err
	}
	e.q, e.zone = conn, conn
	return func() {
		conn.Exec(context.WithoutCancel(ctx), "RESET TimeZone")
		e.q, e.zone = e.pg.Pool(), nil
		conn.Release()
	}, nil
}

func (e *sqlEnv) Sizes(ctx context.Context) (harness.Sizes, error) {
	if !e.timescale() {
		return e.sc.Sizes(ctx)
	}
	events, aggs, err := e.hypertableSizes(ctx)
	if err != nil {
		return harness.Sizes{}, err
	}
	wal, werr := e.pg.WALBytes(ctx)
	if werr != nil {
		wal = 0
	}
	return harness.Sizes{Data: events + aggs, WAL: wal}, nil
}

func (e *sqlEnv) hypertableSizes(ctx context.Context) (events, aggs int64, err error) {
	err = e.pg.Pool().QueryRow(ctx, hypertableSizes, e.sc.Name).Scan(&events, &aggs)
	return
}

func (e *sqlEnv) Storage(ctx context.Context) (storage, error) {
	if e.timescale() {
		events, aggs, err := e.hypertableSizes(ctx)
		return storage{Events: events, Aggs: aggs}, err
	}
	var st storage
	size := func(rel string) (int64, error) {
		var n int64
		err := e.pg.Pool().QueryRow(ctx, "SELECT pg_total_relation_size($1::regclass)", rel).Scan(&n)
		return n, err
	}
	var err error
	if st.Events, err = size(e.n.events); err != nil {
		return st, err
	}
	if e.variant == pgTrigger && !e.plain {
		st.Aggs, err = size(e.n.summary)
	}
	return st, err
}

func (e *sqlEnv) Stats() map[string]float64 { return nil }

func (e *sqlEnv) Close(ctx context.Context) error {
	if e.zone != nil {
		e.zone.Release()
		e.zone, e.q = nil, e.pg.Pool()
	}
	// The policies go with the aggregates.
	return e.sc.Drop(ctx)
}

// settle runs the housekeeping that a database gets before it is measured: statistics on the
// tables, so that the planner knows what is in them, and a checkpoint.
func (e *sqlEnv) settle(ctx context.Context) error {
	var stmts []string
	stmts = append(stmts, "ANALYZE "+e.n.events)
	if e.variant == pgTrigger && !e.plain {
		stmts = append(stmts, "ANALYZE "+e.n.summary)
	}
	stmts = append(stmts, "CHECKPOINT")
	return e.sc.Exec(ctx, stmts...)
}
