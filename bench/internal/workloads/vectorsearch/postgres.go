package vectorsearch

import (
	"context"
	"fmt"
	"strings"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/plans"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/pgsys"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgvector/pgvector-go"
	pgxvec "github.com/pgvector/pgvector-go/pgx"
)

// The parameters of the indexes of pgvector that are not left to its defaults. These are its
// defaults for HNSW, written out so that the results say them.
const (
	hnswM              = 16
	hnswEfConstruction = 64
	// hnswEfSearch is the default size of the list of candidates of an HNSW search.
	hnswEfSearch = 40
)

// ivfLists is the number of lists of an IVFFlat index, by the guidance of pgvector: a thousandth of
// the rows up to a million of them (10 at least), and their square root above.
func ivfLists(rows int) int {
	if rows > 1_000_000 {
		return ceilSqrt(rows)
	}
	return max(10, rows/1000)
}

// The statements, for the table t.

func createTableSQL(t string) string {
	return fmt.Sprintf(`CREATE TABLE %s (
	id        bigint PRIMARY KEY,
	lang      text NOT NULL,
	year      integer NOT NULL,
	embedding vector(%d) NOT NULL
)`, t, dataset.VectorDims)
}

// indexSQL returns the statements that build the indexes of a variant: the two B-trees, and the
// index of the vectors, if the variant has one.
func indexSQL(t, kind string, lists int) []string {
	stmts := []string{
		fmt.Sprintf("CREATE INDEX docs_lang ON %s (lang)", t),
		fmt.Sprintf("CREATE INDEX docs_year ON %s (year)", t),
	}
	switch kind {
	case "hnsw":
		stmts = append(stmts, fmt.Sprintf("CREATE INDEX docs_embedding ON %s USING hnsw (embedding vector_cosine_ops) WITH (m = %d, ef_construction = %d)",
			t, hnswM, hnswEfConstruction))
	case "ivfflat":
		stmts = append(stmts, fmt.Sprintf("CREATE INDEX docs_embedding ON %s USING ivfflat (embedding vector_cosine_ops) WITH (lists = %d)", t, lists))
	}
	return stmts
}

// indexText says how the vectors are indexed, for the results.
func indexText(kind string, lists int) string {
	switch kind {
	case "hnsw":
		return fmt.Sprintf("hnsw (vector_cosine_ops, m = %d, ef_construction = %d)", hnswM, hnswEfConstruction)
	case "ivfflat":
		return fmt.Sprintf("ivfflat (vector_cosine_ops, lists = %d)", lists)
	}
	return "none: B-trees on lang and year only"
}

// searchSQL is the prepared statement of the nearest documents of the vector $1 by the cosine
// distance (<=>), with the condition of the filter.
func searchSQL(t string, f filter) string {
	where := ""
	switch f {
	case rareFilter:
		where = " WHERE lang = 'is'"
	case broadFilter:
		where = " WHERE year >= 2000"
	}
	return fmt.Sprintf("SELECT id FROM %s%s ORDER BY embedding <=> $1::vector LIMIT %d", t, where, k)
}

func insertSQL(t string) string {
	return fmt.Sprintf("INSERT INTO %s (id, lang, year, embedding) VALUES ($1, $2, $3, $4)", t)
}

// searchSettings are the settings of the session that a search of a variant runs with. The
// iterative scan (pgvector 0.8) keeps an index from returning fewer documents than asked for when
// a filter drops most of what it finds, so it is on for the cases that filter.
func searchSettings(kind string, f filter, kb *knob, lists int) []string {
	var s []string
	switch kind {
	case "hnsw":
		ef := hnswEfSearch
		if kb != nil && kb.kind == "ef_search" {
			ef = kb.value
		}
		s = append(s, fmt.Sprintf("hnsw.ef_search = %d", ef))
		if f != noFilter {
			s = append(s, "hnsw.iterative_scan = relaxed_order")
		}
	case "ivfflat":
		probes := ceilSqrt(lists)
		if kb != nil && kb.kind == "probes" {
			probes = kb.value
		}
		s = append(s, fmt.Sprintf("ivfflat.probes = %d", probes))
		if f != noFilter {
			s = append(s, "ivfflat.iterative_scan = relaxed_order")
		}
	}
	return s
}

// pgTarget is a schema of PostgreSQL with the documents in it.
type pgTarget struct {
	kind  string
	d     *data
	sys   *pgsys.System
	sc    *pgsys.Schema
	table string
	lists int
	// pool is the pool of the loads and the inserts, whose connections know the type vector.
	pool    *pgxpool.Pool
	version string
}

// newPool opens a pool of connections of the server that know the type vector, and have the
// settings: the pool of the system has neither.
func newPool(ctx context.Context, sys *pgsys.System, settings []string) (*pgxpool.Pool, error) {
	cfg := sys.Pool().Config()
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		if err := pgxvec.RegisterTypes(ctx, conn); err != nil {
			return err
		}
		for _, s := range settings {
			if _, err := conn.Exec(ctx, "SET "+s, pgx.QueryExecModeSimpleProtocol); err != nil {
				return fmt.Errorf("SET %s: %w", s, err)
			}
		}
		return nil
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

func openPG(ctx context.Context, sys *pgsys.System, kind string, d *data) (target, error) {
	sc, err := sys.Schema(ctx, "vector-search-"+kind)
	if err != nil {
		return nil, err
	}
	t := &pgTarget{kind: kind, d: d, sys: sys, sc: sc, table: sc.T("docs"), lists: ivfLists(d.n)}
	fail := func(err error) (target, error) {
		if t.pool != nil {
			t.pool.Close()
		}
		sc.Drop(context.WithoutCancel(ctx))
		return nil, err
	}

	// The extension is in the image of the server, and made in the database here: the connections
	// that know its types are opened after it is.
	if err := sc.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS vector"); err != nil {
		return fail(err)
	}
	if err := sys.Pool().QueryRow(ctx, "SELECT extversion FROM pg_extension WHERE extname = 'vector'").Scan(&t.version); err != nil {
		return fail(err)
	}
	if t.pool, err = newPool(ctx, sys, nil); err != nil {
		return fail(err)
	}
	if err := sc.Exec(ctx, createTableSQL(t.table)); err != nil {
		return fail(err)
	}
	return t, nil
}

// load copies the documents in, in the binary format of COPY.
func (t *pgTarget) load(ctx context.Context) error {
	var (
		id int64
		d  doc
	)
	_, err := t.pool.CopyFrom(ctx, pgx.Identifier{t.sc.Name, "docs"}, []string{"id", "lang", "year", "embedding"},
		pgx.CopyFromFunc(func() ([]any, error) {
			if id >= int64(t.d.n) {
				return nil, nil
			}
			t.d.vectors.GenInto(&d, id)
			id++
			return []any{d.ID, d.Lang, int32(d.Year), pgvector.NewVector(d.Embedding)}, nil
		}))
	return err
}

func (t *pgTarget) build(ctx context.Context) error {
	return t.sc.Exec(ctx, indexSQL(t.table, t.kind, t.lists)...)
}

func (t *pgTarget) settle(ctx context.Context) error { return t.sc.Settle(ctx, "docs") }

func (t *pgTarget) sizes(ctx context.Context) (harness.Sizes, error) { return t.sc.Sizes(ctx) }

func (t *pgTarget) close(ctx context.Context) {
	t.pool.Close()
	t.sc.Drop(ctx)
}

// knobs are the size of the list of an HNSW search, and the probes of the lists of an IVFFlat one at
// once, twice and four times the default.
func (t *pgTarget) knobs(context.Context) ([]knob, error) {
	switch t.kind {
	case "hnsw":
		var ks []knob
		for _, ef := range []int{40, 100, 200} {
			ks = append(ks, knob{name: fmt.Sprintf("ef-search-%d", ef), kind: "ef_search", value: ef})
		}
		return ks, nil
	case "ivfflat":
		return probeKnobs(t.lists), nil
	}
	return nil, nil
}

func (t *pgTarget) insert(ctx context.Context, d *doc) error {
	_, err := t.pool.Exec(ctx, insertSQL(t.table), d.ID, d.Lang, int32(d.Year), pgvector.NewVector(d.Embedding))
	return err
}

// maintain does nothing: the server keeps its indexes up to date in each statement.
func (t *pgTarget) maintain(context.Context) (func() error, error) {
	return func() error { return nil }, nil
}

// pgSearcher is the search of a case: a pool whose connections have its settings, and the
// prepared statement.
type pgSearcher struct {
	t        *pgTarget
	pool     *pgxpool.Pool
	sql      string
	settings []string
}

func (t *pgTarget) searcher(ctx context.Context, f filter, kb *knob) (searcher, error) {
	settings := searchSettings(t.kind, f, kb, t.lists)
	pool, err := newPool(ctx, t.sys, settings)
	if err != nil {
		return nil, err
	}
	return &pgSearcher{t: t, pool: pool, sql: searchSQL(t.table, f), settings: settings}, nil
}

func (s *pgSearcher) close() { s.pool.Close() }

func (s *pgSearcher) search(ctx context.Context, q []float32, dst []int64) ([]int64, error) {
	rows, err := s.pool.Query(ctx, s.sql, pgvector.NewVector(q))
	if err != nil {
		return dst, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return dst, err
		}
		dst = append(dst, id)
	}
	return dst, rows.Err()
}

// info records the plan of the first queries, as EXPLAIN gives it on a connection that has the
// settings of the case, and the settings.
func (s *pgSearcher) info(ctx context.Context) (map[string]string, error) {
	sets := make([][]any, planSample)
	for i := range sets {
		// The vector goes in as the text of a literal, which the cast in the statement reads.
		sets[i] = []any{pgvector.NewVector(s.t.d.queries[i]).String()}
	}
	ps, err := plans.ExplainPrepared(ctx, s.pool, s.sql, sets)
	if err != nil {
		return nil, err
	}
	var survey plans.Survey
	for _, p := range ps {
		survey.Add(p)
	}
	info := survey.Info()
	info["index"] = indexText(s.t.kind, s.t.lists)
	info["pgvector"] = s.t.version
	if len(s.settings) > 0 {
		info["settings"] = strings.Join(s.settings, "; ")
	}
	return info, nil
}
