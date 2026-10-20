package codecs

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/pgsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/wl"
	"github.com/jackc/pgx/v5"
)

// pgStore is a table of records in one of three shapes, which differ in the columns, in how a
// record becomes their values, and in how a row becomes a record.
type pgStore struct {
	sys   *pgsys.System
	sc    *pgsys.Schema
	table string
	// columns are the columns that are written; ddl creates the table.
	columns []string
	ddl     string
	// values returns the values of the columns for a record, and read does the opposite for
	// a row of the select list.
	values     func(o *dataset.Order) ([]any, error)
	read       func(row pgx.Row, id int64) (*dataset.Order, error)
	selectList string
	// upsert is the statement of Set.
	upsert string
	// size is an expression of the size of a row in bytes.
	size string
}

func openPGStore(ctx context.Context, sys harness.System, shape string) (store, error) {
	ps := sys.(*pgsys.System)
	sc, err := ps.Schema(ctx, "record-codecs")
	if err != nil {
		return nil, err
	}
	s := &pgStore{sys: ps, sc: sc, table: sc.T("t")}

	switch shape {
	case "columns":
		s.ddl = wl.OrderTable(s.table)
		s.columns = strings.Split(strings.ReplaceAll(wl.OrderColumns, " ", ""), ",")
		s.values = func(o *dataset.Order) ([]any, error) { return wl.OrderArgs(o), nil }
		s.selectList = wl.OrderColumns
		s.read = func(row pgx.Row, _ int64) (*dataset.Order, error) {
			var o dataset.Order
			return &o, wl.ScanOrder(row, &o)
		}
		var sets []string
		for _, c := range s.columns[1:] {
			sets = append(sets, c+" = EXCLUDED."+c)
		}
		s.upsert = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (id) DO UPDATE SET %s",
			s.table, wl.OrderColumns, wl.OrderPlaceholders(0), strings.Join(sets, ", "))
		s.size = "pg_column_size(t.*)"
	case "jsonb":
		s.ddl = "CREATE TABLE " + s.table + " (id bigint PRIMARY KEY, doc jsonb NOT NULL)"
		s.columns = []string{"id", "doc"}
		s.values = func(o *dataset.Order) ([]any, error) {
			bz, err := json.Marshal(o)
			return []any{o.ID, json.RawMessage(bz)}, err
		}
		s.selectList = "doc"
		s.read = func(row pgx.Row, id int64) (*dataset.Order, error) {
			var doc []byte
			if err := row.Scan(&doc); err != nil {
				return nil, err
			}
			var o dataset.Order
			if err := json.Unmarshal(doc, &o); err != nil {
				return nil, err
			}
			o.ID = id
			return &o, nil
		}
		s.upsert = "INSERT INTO " + s.table + " (id, doc) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET doc = EXCLUDED.doc"
		s.size = "pg_column_size(t.doc)"
	case "bytea":
		s.ddl = "CREATE TABLE " + s.table + " (id bigint PRIMARY KEY, v bytea NOT NULL)"
		s.columns = []string{"id", "v"}
		s.values = func(o *dataset.Order) ([]any, error) { return []any{o.ID, wl.Msgpack(o)}, nil }
		s.selectList = "v"
		s.read = func(row pgx.Row, id int64) (*dataset.Order, error) {
			var v []byte
			if err := row.Scan(&v); err != nil {
				return nil, err
			}
			return wl.DecodeMsgpack(v, id)
		}
		s.upsert = "INSERT INTO " + s.table + " (id, v) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v"
		s.size = "pg_column_size(t.v)"
	}

	if err := sc.Exec(ctx, s.ddl); err != nil {
		sc.Drop(ctx)
		return nil, err
	}
	return s, nil
}

func openColumns(ctx context.Context, sys harness.System, _ harness.Params) (store, error) {
	return openPGStore(ctx, sys, "columns")
}

func openJSONB(ctx context.Context, sys harness.System, _ harness.Params) (store, error) {
	return openPGStore(ctx, sys, "jsonb")
}

func openBytea(ctx context.Context, sys harness.System, _ harness.Params) (store, error) {
	return openPGStore(ctx, sys, "bytea")
}

func (s *pgStore) Load(ctx context.Context, spec dataset.Spec, n int64) error {
	o := &dataset.Order{}
	id := int64(1)
	_, err := s.sys.Pool().CopyFrom(ctx, pgx.Identifier{s.sc.Name, "t"}, s.columns,
		pgx.CopyFromFunc(func() ([]any, error) {
			if id > n {
				return nil, nil
			}
			spec.GenInto(o, id, 0)
			id++
			return s.values(o)
		}))
	return err
}

func (s *pgStore) Get(ctx context.Context, id int64) (*dataset.Order, error) {
	row := s.sys.Pool().QueryRow(ctx, "SELECT "+s.selectList+" FROM "+s.table+" AS t WHERE id = $1", id)
	o, err := s.read(row, id)
	if pgsys.IsNotFound(err) {
		return nil, nil
	}
	return o, err
}

func (s *pgStore) Set(ctx context.Context, o *dataset.Order) error {
	args, err := s.values(o)
	if err != nil {
		return err
	}
	_, err = s.sys.Pool().Exec(ctx, s.upsert, args...)
	return err
}

func (s *pgStore) AvgBytes(ctx context.Context, _ dataset.Spec, _ int64) (float64, error) {
	var avg float64
	err := s.sys.Pool().QueryRow(ctx, "SELECT coalesce(avg("+s.size+"), 0)::float8 FROM "+s.table+" AS t").Scan(&avg)
	return avg, err
}

func (s *pgStore) Settle(ctx context.Context) error { return s.sc.Settle(ctx, "t") }

func (s *pgStore) Sizes(ctx context.Context) (harness.Sizes, error) { return s.sc.Sizes(ctx) }

func (s *pgStore) Close(ctx context.Context) error { return s.sc.Drop(ctx) }
