package bookdb

import (
	"context"
	"fmt"
	"strings"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/pgsys"
	"github.com/jackc/pgx/v5"
)

// Columns are the columns of the table, in the order that ScanBook reads them and BookArgs gives
// them.
const Columns = "id, code, author, year, price, in_print, tags, title, subtitle, score, payload"

// CreateTable is the statement that creates the table of the books.
func CreateTable(table string) string {
	return fmt.Sprintf(`CREATE TABLE %s (
	id       bigint PRIMARY KEY,
	code     text NOT NULL,
	author   text NOT NULL,
	year     bigint NOT NULL,
	price    bigint NOT NULL,
	in_print boolean NOT NULL,
	tags     text[] NOT NULL,
	title    text NOT NULL,
	subtitle text,
	score    bigint NOT NULL,
	payload  bytea
)`, table)
}

// IndexDDL returns the statements that create the indexes of the queries workload on a table, in
// the order of AddQueryIndexes. The primary key is the key of kvutil's records, and every index
// of kvutil has the primary key at the end of its entries, which is why the two indexes that
// serve an order (year, and author with year) have id as their last column here: the order of
// the records that an ORDER BY ... , id asks for, with no sort.
func IndexDDL(table string) []string {
	return []string{
		fmt.Sprintf("CREATE UNIQUE INDEX %s_code ON %s (code)", IdxPrefix, table),
		fmt.Sprintf("CREATE INDEX %s_author ON %s (author)", IdxPrefix, table),
		fmt.Sprintf("CREATE INDEX %s_author_year ON %s (author, year DESC, id)", IdxPrefix, table),
		fmt.Sprintf("CREATE INDEX %s_year ON %s (year, id)", IdxPrefix, table),
		fmt.Sprintf("CREATE INDEX %s_tags ON %s USING gin (tags)", IdxPrefix, table),
		fmt.Sprintf("CREATE INDEX %s_title_lower ON %s (lower(title))", IdxPrefix, table),
		fmt.Sprintf("CREATE INDEX %s_in_print_price ON %s (price) WHERE in_print", IdxPrefix, table),
		fmt.Sprintf("CREATE INDEX %s_author_cover ON %s (author) INCLUDE (title, price)", IdxPrefix, table),
	}
}

// ScoreIndexDDL returns the statement of the index of the selectivity sweep.
func ScoreIndexDDL(table string) []string {
	return []string{fmt.Sprintf("CREATE INDEX %s_score ON %s (score)", IdxPrefix, table)}
}

// IdxPrefix starts the names of the indexes of PostgreSQL.
const IdxPrefix = "books"

// BookArgs returns the values of the columns of a book.
func BookArgs(b *Book) []any {
	tags := b.Tags
	if tags == nil {
		tags = []string{}
	}
	return []any{b.ID, b.Code, b.Author, b.Year, b.Price, b.InPrint, tags, b.Title, b.Subtitle, b.Score, b.Payload}
}

// ScanBook reads a row of the columns into a book.
func ScanBook(row pgx.Row, b *Book) error {
	return row.Scan(&b.ID, &b.Code, &b.Author, &b.Year, &b.Price, &b.InPrint, &b.Tags, &b.Title, &b.Subtitle, &b.Score, &b.Payload)
}

// PG is a schema of PostgreSQL with the table of the books in it.
type PG struct {
	Sys   *pgsys.System
	Sc    *pgsys.Schema
	Table string
}

// OpenPG creates the schema of a workload with an empty table of books in it.
func OpenPG(ctx context.Context, sys *pgsys.System, workload string) (*PG, error) {
	sc, err := sys.Schema(ctx, workload)
	if err != nil {
		return nil, err
	}
	p := &PG{Sys: sys, Sc: sc, Table: sc.T("books")}
	if err := sc.Exec(ctx, CreateTable(p.Table)); err != nil {
		sc.Drop(ctx)
		return nil, err
	}
	return p, nil
}

// Load writes the books [1, n] with COPY, before there are any indexes besides the primary key.
func (p *PG) Load(ctx context.Context, spec dataset.Spec, n int64) error {
	b := &Book{}
	id := int64(1)
	_, err := p.Sys.Pool().CopyFrom(ctx, pgx.Identifier{p.Sc.Name, "books"}, strings.Split(strings.ReplaceAll(Columns, " ", ""), ","),
		pgx.CopyFromFunc(func() ([]any, error) {
			if id > n {
				return nil, nil
			}
			spec.GenBookInto(b, id)
			id++
			return BookArgs(b), nil
		}))
	return err
}

// Build creates the indexes.
func (p *PG) Build(ctx context.Context, ddl []string) error {
	return p.Sc.Exec(ctx, ddl...)
}

// Settle runs VACUUM and ANALYZE, which gives the planner its statistics and the table its
// visibility map, so that index-only scans are possible, and a checkpoint.
func (p *PG) Settle(ctx context.Context) error { return p.Sc.Settle(ctx, "books") }

// Sizes is the size of the schema, as the harness wants it.
func (p *PG) Sizes(ctx context.Context) (harness.Sizes, error) { return p.Sc.Sizes(ctx) }

// Close drops the schema.
func (p *PG) Close(ctx context.Context) error { return p.Sc.Drop(ctx) }
