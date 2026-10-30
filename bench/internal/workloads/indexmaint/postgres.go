package indexmaint

import (
	"context"
	"fmt"
	"strings"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/pgsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/wl"
	"github.com/jackc/pgx/v5"
)

// pgIndexes returns the statements that create the indexes of a configuration on a table.
func pgIndexes(table, config string) []string {
	idx := func(i int, def string) string { return fmt.Sprintf("CREATE INDEX i%d ON %s %s", i, table, def) }
	switch config {
	case "none":
		return nil
	case "n1":
		return []string{idx(1, "(customer)")}
	case "n3":
		return []string{idx(1, "(customer)"), idx(2, "(status)"), idx(3, "(total)")}
	case "n5":
		return []string{idx(1, "(customer)"), idx(2, "(status)"), idx(3, "(total)"), idx(4, "(created_at)"), idx(5, "(name)")}
	case "unique":
		return []string{fmt.Sprintf("CREATE UNIQUE INDEX i1 ON %s (code)", table)}
	case "composite":
		return []string{idx(1, "(customer, created_at)")}
	case "multi":
		return []string{idx(1, "USING gin (tags)")}
	case "expr":
		return []string{idx(1, "(lower(name))")}
	case "conditional":
		return []string{idx(1, fmt.Sprintf("(customer) WHERE total >= %d", dataset.BigTotal))}
	case "covering":
		return []string{idx(1, "(customer) INCLUDE (total)")}
	}
	panic("unknown configuration " + config)
}

type pgTarget struct {
	config string
	sys    *pgsys.System
	sc     *pgsys.Schema
	table  string

	insert, update, delete string
}

func openPG(ctx context.Context, sys harness.System, config string) (target, error) {
	ps := sys.(*pgsys.System)
	sc, err := ps.Schema(ctx, "index-maintenance")
	if err != nil {
		return nil, err
	}
	t := &pgTarget{config: config, sys: ps, sc: sc, table: sc.T("orders")}
	if err := sc.Exec(ctx, wl.OrderTable(t.table)); err != nil {
		sc.Drop(ctx)
		return nil, err
	}
	t.insert = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", t.table, wl.OrderColumns, wl.OrderPlaceholders(0))
	cols := strings.Split(strings.ReplaceAll(wl.OrderColumns, " ", ""), ",")
	sets := make([]string, 0, len(cols)-1)
	for i, c := range cols[1:] {
		sets = append(sets, fmt.Sprintf("%s = $%d", c, i+2))
	}
	t.update = fmt.Sprintf("UPDATE %s SET %s WHERE id = $1", t.table, strings.Join(sets, ", "))
	t.delete = fmt.Sprintf("DELETE FROM %s WHERE id = $1", t.table)
	return t, nil
}

func (t *pgTarget) copyRows(ctx context.Context, spec dataset.Spec, from, to int64) error {
	o := &order{}
	id := from
	_, err := t.sys.Pool().CopyFrom(ctx, pgx.Identifier{t.sc.Name, "orders"}, strings.Split(strings.ReplaceAll(wl.OrderColumns, " ", ""), ","),
		pgx.CopyFromFunc(func() ([]any, error) {
			if id >= to {
				return nil, nil
			}
			spec.GenInto(o, id, 0)
			id++
			return wl.OrderArgs(o), nil
		}))
	return err
}

func (t *pgTarget) Load(ctx context.Context, spec dataset.Spec, n int64) error {
	return t.copyRows(ctx, spec, 1, n+1)
}

func (t *pgTarget) Build(ctx context.Context) ([]string, error) {
	var names []string
	for i, stmt := range pgIndexes(t.table, t.config) {
		if err := t.sc.Exec(ctx, stmt); err != nil {
			return nil, err
		}
		names = append(names, fmt.Sprintf("i%d", i+1))
	}
	return names, nil
}

func (t *pgTarget) Add(ctx context.Context, spec dataset.Spec, from, to int64) error {
	return t.copyRows(ctx, spec, from, to)
}

func (t *pgTarget) Insert(ctx context.Context, o *order) error {
	_, err := t.sys.Pool().Exec(ctx, t.insert, wl.OrderArgs(o)...)
	return err
}

func (t *pgTarget) Update(ctx context.Context, o *order) error {
	_, err := t.sys.Pool().Exec(ctx, t.update, wl.OrderArgs(o)...)
	return err
}

func (t *pgTarget) Delete(ctx context.Context, id int64) error {
	_, err := t.sys.Pool().Exec(ctx, t.delete, id)
	return err
}

func (t *pgTarget) Probe(ctx context.Context, p probe) ([]int64, error) {
	var where string
	var args []any
	switch p.Kind {
	case "all":
		where = "true"
	case "customer":
		where, args = "customer = $1", []any{p.S}
	case "status":
		where, args = "status = $1", []any{p.S}
	case "total":
		where, args = "total BETWEEN $1 AND $2", []any{p.Lo, p.Hi}
	case "created":
		where, args = "created_at BETWEEN $1 AND $2", []any{wl.Seconds(p.Lo), wl.Seconds(p.Hi)}
	case "name":
		where, args = "name = $1", []any{p.S}
	case "code":
		where, args = "code = $1", []any{p.S}
	case "composite":
		where, args = "customer = $1 AND created_at BETWEEN $2 AND $3", []any{p.S, wl.Seconds(p.Lo), wl.Seconds(p.Hi)}
	case "tag":
		where, args = "tags @> ARRAY[$1]::text[]", []any{p.S}
	case "lower":
		where, args = "lower(name) = $1", []any{p.S}
	case "conditional":
		where, args = fmt.Sprintf("customer = $1 AND total >= %d", dataset.BigTotal), []any{p.S}
	default:
		return nil, fmt.Errorf("unknown probe %q", p.Kind)
	}
	rows, err := t.sys.Pool().Query(ctx, "SELECT id FROM "+t.table+" WHERE "+where+" ORDER BY id", args...)
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

func (t *pgTarget) Settle(ctx context.Context) error { return t.sc.Settle(ctx, "orders") }

func (t *pgTarget) Sizes(ctx context.Context) (harness.Sizes, error) { return t.sc.Sizes(ctx) }

func (t *pgTarget) Close(ctx context.Context) error { return t.sc.Drop(ctx) }
