package queries

import (
	"context"
	"fmt"

	"github.com/ehsanranjbar/kvutil/bench/internal/bookdb"
	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/plans"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/pgsys"
	"github.com/jackc/pgx/v5"
)

// pgEnv is the schema of PostgreSQL with the books and their indexes.
type pgEnv struct {
	pg    *bookdb.PG
	pools map[string][]entry
	// statements are the prepared statements of the cases, with $1, $2, ...
	statements map[string]string
}

func (e *pgEnv) sizes(ctx context.Context) (harness.Sizes, error) { return e.pg.Sizes(ctx) }

func (e *pgEnv) close(ctx context.Context) { e.pg.Close(ctx) }

// openPG creates the schema, loads the books and builds the indexes, as phases of the run that
// asked for it, and runs ANALYZE and VACUUM (the statistics and the visibility map, which an
// index-only scan needs).
func openPG(ctx context.Context, rc *harness.RunContext, name string, d *data) (*pgEnv, error) {
	pg, err := bookdb.OpenPG(ctx, rc.Sys.(*pgsys.System), name)
	if err != nil {
		return nil, err
	}
	e := &pgEnv{pg: pg}
	rc.SizeFn = e.sizes

	n := int64(rc.Params.Scale)
	err = bookdb.Phase(ctx, rc, "load", func(ctx context.Context) (int64, error) {
		return n, pg.Load(ctx, d.spec, n)
	})
	if err == nil {
		err = bookdb.Phase(ctx, rc, "build", func(ctx context.Context) (int64, error) {
			return n, pg.Build(ctx, bookdb.IndexDDL(pg.Table))
		})
	}
	if err == nil {
		// The analyze and the vacuum are a part of getting ready, not of the build.
		err = pg.Settle(ctx)
	}
	if err != nil {
		pg.Close(context.WithoutCancel(ctx))
		return nil, err
	}

	e.pools = buildPools(d, rc.Params.Seed, pg.Table)
	e.statements = map[string]string{}
	for _, c := range cases() {
		e.statements[c.name] = c.statement(pg.Table)
	}
	return e, nil
}

// run runs a statement and reads everything it returns, as a client does: the rows are scanned into
// a book, or into the values that are selected. simple sends the text with the values in it, with
// the simple protocol, so that the server parses and plans it every time; otherwise the statement is
// prepared (by the pool, on first use of each connection) and run with the values as parameters.
func (e *pgEnv) run(ctx context.Context, c *qcase, ent *entry, simple bool, b *book) (outcome, error) {
	var o outcome
	var rows pgx.Rows
	var err error
	if simple {
		rows, err = e.pg.Sys.Pool().Query(ctx, ent.pgText, pgx.QueryExecModeSimpleProtocol)
	} else {
		rows, err = e.pg.Sys.Pool().Query(ctx, e.statements[c.name], ent.pgArgs...)
	}
	if err != nil {
		return o, err
	}
	defer rows.Close()

	switch c.kind {
	case countKind:
		for rows.Next() {
			var n int64
			if err := rows.Scan(&n); err != nil {
				return o, err
			}
			o.count = int(n)
		}
	case selectKind:
		var bytes int64
		for rows.Next() {
			var title string
			var price int64
			if err := rows.Scan(&title, &price); err != nil {
				return o, err
			}
			bytes += int64(len(title)) + price
			o.rows++
		}
		sink.Add(bytes)
	default:
		var bytes int64
		for rows.Next() {
			if err := bookdb.ScanBook(rows, b); err != nil {
				return o, err
			}
			bytes += int64(len(b.Title))
			o.rows++
		}
		sink.Add(bytes)
	}
	return o, rows.Err()
}

// answer runs the statement and returns what it gave in the form that kvutil's answers have.
func (e *pgEnv) answer(ctx context.Context, c *qcase, ent *entry, simple bool) (answer, error) {
	var ans answer
	var rows pgx.Rows
	var err error
	if simple {
		rows, err = e.pg.Sys.Pool().Query(ctx, ent.pgText, pgx.QueryExecModeSimpleProtocol)
	} else {
		rows, err = e.pg.Sys.Pool().Query(ctx, e.statements[c.name], ent.pgArgs...)
	}
	if err != nil {
		return ans, err
	}
	defer rows.Close()

	switch c.kind {
	case countKind:
		for rows.Next() {
			var n int64
			if err := rows.Scan(&n); err != nil {
				return ans, err
			}
			ans.count = int(n)
		}
	case selectKind:
		for rows.Next() {
			var title string
			var price int64
			if err := rows.Scan(&title, &price); err != nil {
				return ans, err
			}
			ans.lines = append(ans.lines, fmt.Sprintf("%s|%d", title, price))
		}
	default:
		for rows.Next() {
			var b dataset.Book
			if err := bookdb.ScanBook(rows, &b); err != nil {
				return ans, err
			}
			ans.ids = append(ans.ids, b.ID)
			ans.lines = append(ans.lines, dataset.BookCanon(&b))
		}
	}
	return ans, rows.Err()
}

// plans records the plans that the server has for the first arguments of the pool: those of the
// prepared statement after its first runs (as the server decides whether to use a generic plan),
// or those of the text with the values in it.
func (e *pgEnv) plans(ctx context.Context, c *qcase, simple bool) (map[string]string, error) {
	var s plans.Survey
	pool := e.pools[c.name]
	n := min(planSample, len(pool))
	if simple {
		for i := 0; i < n; i++ {
			p, err := plans.Explain(ctx, e.pg.Sys.Pool(), pool[i].pgText)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", c.label(pool[i].a), err)
			}
			s.Add(p)
		}
		return s.Info(), nil
	}
	sets := make([][]any, n)
	for i := range sets {
		sets[i] = pool[i].pgArgs
	}
	ps, err := plans.ExplainPrepared(ctx, e.pg.Sys.Pool(), e.statements[c.name], sets)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", c.name, err)
	}
	for _, p := range ps {
		s.Add(p)
	}
	return s.Info(), nil
}
