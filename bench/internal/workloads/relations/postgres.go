package relations

import (
	"context"
	"fmt"

	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/pgsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/wl"
	"github.com/jackc/pgx/v5"
)

// The tables: foreign keys with ON DELETE CASCADE, an index on each foreign key (the one of the
// children is on the key and the id, which is the order of kvutil's references; the one of the
// join table is the primary key for one side and an index for the other).
const pgSchema = `
CREATE TABLE %[1]s.parents (
	id         bigint PRIMARY KEY,
	name       text NOT NULL,
	region     text NOT NULL,
	created_at timestamptz NOT NULL
);
CREATE TABLE %[1]s.children (
	id        bigint PRIMARY KEY,
	parent_id bigint NOT NULL REFERENCES %[1]s.parents (id) ON DELETE CASCADE,
	title     text NOT NULL,
	qty       bigint NOT NULL,
	status    text NOT NULL,
	payload   bytea
);
CREATE INDEX children_parent ON %[1]s.children (parent_id, id);
CREATE TABLE %[1]s.lefts (id bigint PRIMARY KEY, name text NOT NULL, kind text NOT NULL);
CREATE TABLE %[1]s.rights (id bigint PRIMARY KEY, name text NOT NULL, kind text NOT NULL);
CREATE TABLE %[1]s.memberships (
	left_id  bigint NOT NULL REFERENCES %[1]s.lefts (id) ON DELETE CASCADE,
	right_id bigint NOT NULL REFERENCES %[1]s.rights (id) ON DELETE CASCADE,
	role     text NOT NULL,
	since    bigint NOT NULL,
	PRIMARY KEY (left_id, right_id)
);
CREATE INDEX memberships_right ON %[1]s.memberships (right_id, left_id);
`

// The statements of the cases; the schema is put in front of the names of the tables.
const (
	sqlInsertChild = `INSERT INTO %[1]s.children (id, parent_id, title, qty, status, payload) VALUES ($1, $2, $3, $4, $5, $6)`
	sqlChildren    = `SELECT id, title, qty, status, payload FROM %[1]s.children WHERE parent_id = $1 ORDER BY id`
	sqlChildKeys   = `SELECT id FROM %[1]s.children WHERE parent_id = $1 ORDER BY id`
	sqlParentOf    = `SELECT p.id, p.name, p.region, p.created_at FROM %[1]s.children c JOIN %[1]s.parents p ON p.id = c.parent_id WHERE c.id = $1`
	sqlDeleteOf    = `DELETE FROM %[1]s.%[2]s WHERE id = $1`

	sqlSetPair = `INSERT INTO %[1]s.memberships (left_id, right_id, role, since) VALUES ($1, $2, $3, $4)
ON CONFLICT (left_id, right_id) DO UPDATE SET role = EXCLUDED.role, since = EXCLUDED.since`
	sqlGetPair = `SELECT role, since FROM %[1]s.memberships WHERE left_id = $1 AND right_id = $2`

	sqlRightsKeys    = `SELECT right_id FROM %[1]s.memberships WHERE left_id = $1 ORDER BY right_id`
	sqlRightsData    = `SELECT right_id, role, since FROM %[1]s.memberships WHERE left_id = $1 ORDER BY right_id`
	sqlRightsRecords = `SELECT m.right_id, m.role, m.since, r.name, r.kind FROM %[1]s.memberships m JOIN %[1]s.rights r ON r.id = m.right_id WHERE m.left_id = $1 ORDER BY m.right_id`
	sqlLeftsKeys     = `SELECT left_id FROM %[1]s.memberships WHERE right_id = $1 ORDER BY left_id`
	sqlLeftsData     = `SELECT left_id, role, since FROM %[1]s.memberships WHERE right_id = $1 ORDER BY left_id`
	sqlLeftsRecords  = `SELECT m.left_id, m.role, m.since, l.name, l.kind FROM %[1]s.memberships m JOIN %[1]s.lefts l ON l.id = m.left_id WHERE m.right_id = $1 ORDER BY m.left_id`
)

type pgTarget struct {
	sys *pgsys.System
	sc  *pgsys.Schema
	sql map[string]string
}

func openPG(ctx context.Context, sys *pgsys.System) (target, error) {
	sc, err := sys.Schema(ctx, "relations")
	if err != nil {
		return nil, err
	}
	t := &pgTarget{sys: sys, sc: sc, sql: map[string]string{}}
	for name, stmt := range map[string]string{
		"insertChild": sqlInsertChild, "children": sqlChildren, "childKeys": sqlChildKeys, "parentOf": sqlParentOf,
		"setPair": sqlSetPair, "getPair": sqlGetPair,
		"rightsKeys": sqlRightsKeys, "rightsData": sqlRightsData, "rightsRecords": sqlRightsRecords,
		"leftsKeys": sqlLeftsKeys, "leftsData": sqlLeftsData, "leftsRecords": sqlLeftsRecords,
	} {
		t.sql[name] = fmt.Sprintf(stmt, sc.Name)
	}
	for _, tbl := range []string{"parents", "lefts", "rights"} {
		t.sql["delete-"+tbl] = fmt.Sprintf(sqlDeleteOf, sc.Name, tbl)
	}
	// The schema is several statements, which the simple protocol runs in one call.
	if err := sc.Exec(ctx, fmt.Sprintf(pgSchema, sc.Name)); err != nil {
		sc.Drop(ctx)
		return nil, err
	}
	return t, nil
}

func (t *pgTarget) copy(ctx context.Context, table string, cols []string, from, to int64, workers int, row func(id int64) []any) error {
	return wl.ParallelPG(from, to, workers, func(lo, hi int64) error {
		id := lo
		_, err := t.sys.Pool().CopyFrom(ctx, pgx.Identifier{t.sc.Name, table}, cols, pgx.CopyFromFunc(func() ([]any, error) {
			if id >= hi {
				return nil, nil
			}
			r := row(id)
			id++
			return r, nil
		}))
		return err
	})
}

func (t *pgTarget) LoadParents(ctx context.Context, m *model, workers int) error {
	return t.copy(ctx, "parents", []string{"id", "name", "region", "created_at"}, 1, m.parents+1, workers, func(id int64) []any {
		var p parent
		m.genParent(id, &p)
		return []any{p.ID, p.Name, p.Region, p.Created}
	})
}

// LoadChildren copies the children with their parent: the key is checked against the table of
// parents, and the index is kept, as a row is added.
func (t *pgTarget) LoadChildren(ctx context.Context, m *model, workers int) error {
	return t.copy(ctx, "children", []string{"id", "parent_id", "title", "qty", "status", "payload"}, 1, m.children+1, workers, func(id int64) []any {
		var c child
		m.genChild(id, &c)
		return []any{c.ID, m.parentOf(id), c.Title, c.Qty, c.Status, c.Payload}
	})
}

func (t *pgTarget) LoadEntities(ctx context.Context, m *model, workers int) error {
	for _, side := range []struct {
		name, table string
		n           int64
	}{{"left", "lefts", m.lefts}, {"right", "rights", m.rights}} {
		err := t.copy(ctx, side.table, []string{"id", "name", "kind"}, 1, side.n+1, 1, func(id int64) []any {
			var e entity
			m.genEntity(side.name, id, &e)
			return []any{e.ID, e.Name, e.Kind}
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *pgTarget) LoadPairs(ctx context.Context, m *model, workers int) error {
	return wl.ParallelPG(1, m.lefts+1, workers, func(lo, hi int64) error {
		l, j := lo, int64(0)
		_, err := t.sys.Pool().CopyFrom(ctx, pgx.Identifier{t.sc.Name, "memberships"},
			[]string{"left_id", "right_id", "role", "since"}, pgx.CopyFromFunc(func() ([]any, error) {
				for l < hi && j >= int64(m.leftCount[l]) {
					l, j = l+1, 0
				}
				if l >= hi {
					return nil, nil
				}
				r := m.rightOf(l, j)
				d := m.data(l, r)
				j++
				return []any{l, r, d.Role, d.Since}, nil
			}))
		return err
	})
}

func (t *pgTarget) InsertChild(ctx context.Context, c *child, parent int64) error {
	_, err := t.sys.Pool().Exec(ctx, t.sql["insertChild"], c.ID, parent, c.Title, c.Qty, c.Status, c.Payload)
	return err
}

func (t *pgTarget) Children(ctx context.Context, parent int64, fn func(*child)) error {
	rows, err := t.sys.Pool().Query(ctx, t.sql["children"], parent)
	if err != nil {
		return err
	}
	defer rows.Close()
	var c child
	for rows.Next() {
		if err := rows.Scan(&c.ID, &c.Title, &c.Qty, &c.Status, &c.Payload); err != nil {
			return err
		}
		fn(&c)
	}
	return rows.Err()
}

func (t *pgTarget) ChildKeys(ctx context.Context, parent int64, fn func(id int64)) error {
	rows, err := t.sys.Pool().Query(ctx, t.sql["childKeys"], parent)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return err
		}
		fn(id)
	}
	return rows.Err()
}

func (t *pgTarget) ParentOf(ctx context.Context, child int64) (*parent, error) {
	var p parent
	err := t.sys.Pool().QueryRow(ctx, t.sql["parentOf"], child).Scan(&p.ID, &p.Name, &p.Region, &p.Created)
	if pgsys.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	p.Created = p.Created.UTC()
	return &p, nil
}

func (t *pgTarget) DeleteParent(ctx context.Context, id int64) error {
	_, err := t.sys.Pool().Exec(ctx, t.sql["delete-parents"], id)
	return err
}

func (t *pgTarget) SetPair(ctx context.Context, left, right int64, d *pairData) error {
	_, err := t.sys.Pool().Exec(ctx, t.sql["setPair"], left, right, d.Role, d.Since)
	return err
}

func (t *pgTarget) GetPair(ctx context.Context, left, right int64) (*pairData, error) {
	var d pairData
	err := t.sys.Pool().QueryRow(ctx, t.sql["getPair"], left, right).Scan(&d.Role, &d.Since)
	if pgsys.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &d, nil
}

// pairs runs a statement of the pairs of a record and reads each row as the mode says.
func (t *pgTarget) pairs(ctx context.Context, name string, id int64, mode listMode, fn func(int64, *pairData, *entity)) error {
	rows, err := t.sys.Pool().Query(ctx, t.sql[name+string(titleCase(mode))], id)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var key int64
		var d pairData
		var rec entity
		switch mode {
		case modeKeys:
			err = rows.Scan(&key)
			fn(key, nil, nil)
		case modeData:
			err = rows.Scan(&key, &d.Role, &d.Since)
			fn(key, &d, nil)
		default:
			err = rows.Scan(&key, &d.Role, &d.Since, &rec.Name, &rec.Kind)
			rec.ID = key
			fn(key, &d, &rec)
		}
		if err != nil {
			return err
		}
	}
	return rows.Err()
}

func titleCase(m listMode) string {
	s := string(m)
	return string(s[0]-32) + s[1:]
}

func (t *pgTarget) Rights(ctx context.Context, left int64, mode listMode, fn func(int64, *pairData, *entity)) error {
	return t.pairs(ctx, "rights", left, mode, fn)
}

func (t *pgTarget) Lefts(ctx context.Context, right int64, mode listMode, fn func(int64, *pairData, *entity)) error {
	return t.pairs(ctx, "lefts", right, mode, fn)
}

func (t *pgTarget) DeleteLeft(ctx context.Context, id int64) error {
	_, err := t.sys.Pool().Exec(ctx, t.sql["delete-lefts"], id)
	return err
}

func (t *pgTarget) DeleteRight(ctx context.Context, id int64) error {
	_, err := t.sys.Pool().Exec(ctx, t.sql["delete-rights"], id)
	return err
}

func (t *pgTarget) Count(ctx context.Context, what string) (int64, error) {
	table := map[string]string{"parents": "parents", "children": "children", "lefts": "lefts", "rights": "rights", "pairs": "memberships"}[what]
	if table == "" {
		return 0, fmt.Errorf("unknown kind %q", what)
	}
	var n int64
	err := t.sys.Pool().QueryRow(ctx, "SELECT count(*) FROM "+t.sc.T(table)).Scan(&n)
	return n, err
}

func (t *pgTarget) Settle(ctx context.Context) error {
	return t.sc.Settle(ctx, "parents", "children", "lefts", "rights", "memberships")
}

func (t *pgTarget) Sizes(ctx context.Context) (harness.Sizes, error) { return t.sc.Sizes(ctx) }

func (t *pgTarget) Close(ctx context.Context) error { return t.sc.Drop(ctx) }
