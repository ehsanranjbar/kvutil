package queries

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ehsanranjbar/kvutil/bench/internal/bookdb"
	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/plans"
	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/query"
)

type (
	book  = dataset.Book
	kvQry = query.Query[int64, book]
)

// kind is what a case returns.
type kind int

const (
	// rowsKind returns the books.
	rowsKind kind = iota
	// selectKind returns some fields of the books: the rows of a projection, which an index may
	// answer without reading the books.
	selectKind
	// countKind returns how many books there are.
	countKind
)

// args are the arguments of one run of a case: texts and numbers, as the case says.
type args struct {
	S []string
	I []int64
}

// qcase is a query that is run on both systems, with how each system is asked.
type qcase struct {
	name string
	doc  string
	kind kind
	// ordered says that the order of the results is a part of the answer: the books are compared
	// in the order that they came in, and not as a set.
	ordered bool

	// gen makes the arguments of one run.
	gen func(d *data, r *harness.Rand) args

	// kvutil: the conditions as typed values, as text, and what is put on the query (order, offset
	// and limit) for the arguments. conds may be nil for a query with no condition.
	conds func(a args) []cond.Cond[book]
	text  func(a args) string
	shape func(q *kvQry, a args) *kvQry
	// items are the fields that a selectKind case selects.
	items []field.Item[book]

	// PostgreSQL: the WHERE clause and what follows it, with $1, $2, ..., the select list of a
	// selectKind case, and the values of the parameters.
	where, tail, selectList string
	pgArgs                  func(a args) []any
}

// statement returns the statement of PostgreSQL for the table.
func (c *qcase) statement(table string) string {
	list := bookdb.Columns
	switch c.kind {
	case selectKind:
		list = c.selectList
	case countKind:
		list = "count(*)"
	}
	s := "SELECT " + list + " FROM " + table
	if c.where != "" {
		s += " WHERE " + c.where
	}
	return s + c.tail
}

// ids of the years, as the cases pick them.
const years = dataset.LastYear - dataset.FirstYear + 1

func quote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// year returns a year with room for a range of n years after it.
func year(r *harness.Rand, n int) int64 {
	return int64(dataset.FirstYear + r.IntN(years-n))
}

// A book that is picked from the dataset, for the cases that ask for what there is.
func pick(d *data, r *harness.Rand) *book {
	return d.spec.GenBook(1 + r.Int64N(d.n))
}

// cases returns the cases of the workload, in the order that they run.
func cases() []*qcase {
	return []*qcase{
		{
			name: "point-lookup",
			doc:  "one book by its unique code",
			gen:  func(d *data, r *harness.Rand) args { return args{S: []string{pick(d, r).Code}} },
			conds: func(a args) []cond.Cond[book] {
				return []cond.Cond[book]{bookdb.FCode.Eq(a.S[0])}
			},
			text:   func(a args) string { return "Code = " + quote(a.S[0]) },
			where:  "code = $1",
			pgArgs: func(a args) []any { return []any{a.S[0]} },
		},
		{
			name: "author-few",
			doc:  "the books of an author who has a few",
			gen:  func(d *data, r *harness.Rand) args { return args{S: []string{d.few[r.IntN(len(d.few))]}} },
			conds: func(a args) []cond.Cond[book] {
				return []cond.Cond[book]{bookdb.FAuthor.Eq(a.S[0])}
			},
			text:   func(a args) string { return "Author = " + quote(a.S[0]) },
			where:  "author = $1",
			pgArgs: func(a args) []any { return []any{a.S[0]} },
		},
		{
			name: "author-many",
			doc:  "the books of a popular author (ranks 20 to 39 of the skew)",
			gen:  func(d *data, r *harness.Rand) args { return args{S: []string{d.many[r.IntN(len(d.many))]}} },
			conds: func(a args) []cond.Cond[book] {
				return []cond.Cond[book]{bookdb.FAuthor.Eq(a.S[0])}
			},
			text:   func(a args) string { return "Author = " + quote(a.S[0]) },
			where:  "author = $1",
			pgArgs: func(a args) []any { return []any{a.S[0]} },
		},
		{
			name: "year-narrow",
			doc:  "the books of two years: 0.35% of the table",
			gen: func(d *data, r *harness.Rand) args {
				y := year(r, 1)
				return args{I: []int64{y, y + 1}}
			},
			conds: func(a args) []cond.Cond[book] {
				return []cond.Cond[book]{bookdb.FYear.Between(a.I[0], a.I[1])}
			},
			text:   func(a args) string { return fmt.Sprintf("Year BETWEEN %d AND %d", a.I[0], a.I[1]) },
			where:  "year BETWEEN $1 AND $2",
			pgArgs: func(a args) []any { return []any{a.I[0], a.I[1]} },
		},
		{
			name: "year-wide",
			doc:  "the books of 25 years: 4.3% of the table",
			gen: func(d *data, r *harness.Rand) args {
				y := year(r, 24)
				return args{I: []int64{y, y + 24}}
			},
			conds: func(a args) []cond.Cond[book] {
				return []cond.Cond[book]{bookdb.FYear.Between(a.I[0], a.I[1])}
			},
			text:   func(a args) string { return fmt.Sprintf("Year BETWEEN %d AND %d", a.I[0], a.I[1]) },
			where:  "year BETWEEN $1 AND $2",
			pgArgs: func(a args) []any { return []any{a.I[0], a.I[1]} },
		},
		{
			name:    "author-year-order",
			doc:     "the 10 latest books of a popular author in 100 years: composite index in the order asked for",
			ordered: true,
			gen: func(d *data, r *harness.Rand) args {
				lo := year(r, 100)
				return args{S: []string{d.many[r.IntN(len(d.many))]}, I: []int64{lo, lo + 100}}
			},
			conds: func(a args) []cond.Cond[book] {
				return []cond.Cond[book]{bookdb.FAuthor.Eq(a.S[0]), bookdb.FYear.Between(a.I[0], a.I[1])}
			},
			text: func(a args) string {
				return fmt.Sprintf("Author = %s AND Year BETWEEN %d AND %d", quote(a.S[0]), a.I[0], a.I[1])
			},
			shape:  func(q *kvQry, _ args) *kvQry { return q.OrderBy(bookdb.FYear.Desc()).Limit(10) },
			where:  "author = $1 AND year BETWEEN $2 AND $3",
			tail:   " ORDER BY year DESC, id LIMIT 10",
			pgArgs: func(a args) []any { return []any{a.S[0], a.I[0], a.I[1]} },
		},
		{
			name:    "top-n-sort",
			doc:     "the 10 books of the highest score among those of 5 years: a sort of the books, no index has the order",
			ordered: true,
			gen: func(d *data, r *harness.Rand) args {
				y := year(r, 4)
				return args{I: []int64{y, y + 4}}
			},
			conds: func(a args) []cond.Cond[book] {
				return []cond.Cond[book]{bookdb.FYear.Between(a.I[0], a.I[1])}
			},
			text:   func(a args) string { return fmt.Sprintf("Year BETWEEN %d AND %d", a.I[0], a.I[1]) },
			shape:  func(q *kvQry, _ args) *kvQry { return q.OrderBy(bookdb.FScore.Desc()).Limit(10) },
			where:  "year BETWEEN $1 AND $2",
			tail:   " ORDER BY score DESC, id LIMIT 10",
			pgArgs: func(a args) []any { return []any{a.I[0], a.I[1]} },
		},
		{
			name:    "deep-paging",
			doc:     "a page of 20 books in the order of years, after an offset of 10,000 (less in a small dataset)",
			ordered: true,
			gen: func(d *data, r *harness.Rand) args {
				return args{I: []int64{d.offset + 20*int64(r.IntN(10))}}
			},
			text:   func(a args) string { return "TRUE" },
			shape:  func(q *kvQry, a args) *kvQry { return q.OrderBy(bookdb.FYear).Offset(int(a.I[0])).Limit(20) },
			tail:   " ORDER BY year, id OFFSET $1 LIMIT 20",
			pgArgs: func(a args) []any { return []any{a.I[0]} },
		},
		{
			name: "in-list-20",
			doc:  "20 books by their codes",
			gen: func(d *data, r *harness.Rand) args {
				codes := make([]string, 20)
				for i := range codes {
					codes[i] = pick(d, r).Code
				}
				return args{S: codes}
			},
			conds: func(a args) []cond.Cond[book] {
				return []cond.Cond[book]{bookdb.FCode.In(a.S...)}
			},
			text: func(a args) string {
				quoted := make([]string, len(a.S))
				for i, s := range a.S {
					quoted[i] = quote(s)
				}
				return "Code IN (" + strings.Join(quoted, ", ") + ")"
			},
			where:  "code = ANY($1::text[])",
			pgArgs: func(a args) []any { return []any{a.S} },
		},
		{
			name: "or-two-indexes",
			doc:  "the books of an author who has a few, or of a year: a union of two indexes",
			gen: func(d *data, r *harness.Rand) args {
				return args{S: []string{d.few[r.IntN(len(d.few))]}, I: []int64{year(r, 0)}}
			},
			conds: func(a args) []cond.Cond[book] {
				return []cond.Cond[book]{cond.Or[book](bookdb.FAuthor.Eq(a.S[0]), bookdb.FYear.Eq(a.I[0]))}
			},
			text:   func(a args) string { return fmt.Sprintf("Author = %s OR Year = %d", quote(a.S[0]), a.I[0]) },
			where:  "author = $1 OR year = $2",
			pgArgs: func(a args) []any { return []any{a.S[0], a.I[0]} },
		},
		{
			name: "and-two-indexes",
			doc:  "the books of a year that have a tag: an intersection of two indexes",
			gen: func(d *data, r *harness.Rand) args {
				for {
					if b := pick(d, r); len(b.Tags) > 0 {
						return args{S: []string{b.Tags[r.IntN(len(b.Tags))]}, I: []int64{b.Year}}
					}
				}
			},
			conds: func(a args) []cond.Cond[book] {
				return []cond.Cond[book]{bookdb.FYear.Eq(a.I[0]), bookdb.FTags.Eq(a.S[0])}
			},
			text:   func(a args) string { return fmt.Sprintf("Year = %d AND Tags = %s", a.I[0], quote(a.S[0])) },
			where:  "year = $1 AND tags @> ARRAY[$2]::text[]",
			pgArgs: func(a args) []any { return []any{a.I[0], a.S[0]} },
		},
		{
			name: "tag-membership",
			doc:  "the books that have a tag: 4% of the table",
			gen: func(d *data, r *harness.Rand) args {
				return args{S: []string{"tag-" + fmt.Sprintf("%02d", r.IntN(dataset.TagPool))}}
			},
			conds: func(a args) []cond.Cond[book] {
				return []cond.Cond[book]{bookdb.FTags.Eq(a.S[0])}
			},
			text:   func(a args) string { return "Tags = " + quote(a.S[0]) },
			where:  "tags @> ARRAY[$1]::text[]",
			pgArgs: func(a args) []any { return []any{a.S[0]} },
		},
		{
			name: "like-prefix",
			doc:  "the books whose title starts with an adjective and a noun: 0.4% of the table, no index serves a prefix",
			gen: func(d *data, r *harness.Rand) args {
				return args{S: []string{dataset.TitleWords(pick(d, r).Title)}}
			},
			conds: func(a args) []cond.Cond[book] {
				prefix := a.S[0]
				return []cond.Cond[book]{cond.Func("Title LIKE "+quote(prefix+"%"), func(b *book) bool {
					return strings.HasPrefix(b.Title, prefix)
				})}
			},
			text:   func(a args) string { return "Title LIKE " + quote(a.S[0]+"%") },
			where:  "title LIKE $1",
			pgArgs: func(a args) []any { return []any{a.S[0] + "%"} },
		},
		{
			name: "lower-title",
			doc:  "the books of a title, compared as lower case: an index on the expression",
			gen: func(d *data, r *harness.Rand) args {
				return args{S: []string{strings.ToLower(pick(d, r).Title)}}
			},
			conds: func(a args) []cond.Cond[book] {
				return []cond.Cond[book]{bookdb.FTitleLower.Eq(a.S[0])}
			},
			text:   func(a args) string { return "lower(Title) = " + quote(a.S[0]) },
			where:  "lower(title) = $1",
			pgArgs: func(a args) []any { return []any{a.S[0]} },
		},
		{
			name: "conditional-index",
			doc:  "the books in print with a price in a range of 3% of the prices: an index of the books in print",
			gen: func(d *data, r *harness.Rand) args {
				lo := int64(dataset.MinPrice + r.IntN(dataset.MaxPrice-dataset.MinPrice-300))
				return args{I: []int64{lo, lo + 300}}
			},
			conds: func(a args) []cond.Cond[book] {
				return []cond.Cond[book]{bookdb.FInPrint.Eq(true), bookdb.FPrice.Between(a.I[0], a.I[1])}
			},
			text:   func(a args) string { return fmt.Sprintf("InPrint AND Price BETWEEN %d AND %d", a.I[0], a.I[1]) },
			where:  "in_print AND price BETWEEN $1 AND $2",
			pgArgs: func(a args) []any { return []any{a.I[0], a.I[1]} },
		},
		{
			name: "index-only-select",
			kind: selectKind,
			doc:  "the title and price of the books of a popular author: held in an index",
			gen:  func(d *data, r *harness.Rand) args { return args{S: []string{d.many[r.IntN(len(d.many))]}} },
			conds: func(a args) []cond.Cond[book] {
				return []cond.Cond[book]{bookdb.FAuthor.Eq(a.S[0])}
			},
			text:       func(a args) string { return "Author = " + quote(a.S[0]) },
			items:      []field.Item[book]{bookdb.FTitle, bookdb.FPrice},
			selectList: "title, price",
			where:      "author = $1",
			pgArgs:     func(a args) []any { return []any{a.S[0]} },
		},
		{
			name: "count",
			kind: countKind,
			doc:  "how many books there are in 25 years",
			gen: func(d *data, r *harness.Rand) args {
				y := year(r, 24)
				return args{I: []int64{y, y + 24}}
			},
			conds: func(a args) []cond.Cond[book] {
				return []cond.Cond[book]{bookdb.FYear.Between(a.I[0], a.I[1])}
			},
			text:   func(a args) string { return fmt.Sprintf("Year BETWEEN %d AND %d", a.I[0], a.I[1]) },
			where:  "year BETWEEN $1 AND $2",
			pgArgs: func(a args) []any { return []any{a.I[0], a.I[1]} },
		},
	}
}

// data is what the cases pick their arguments from: the dataset, and the authors that have a few
// books and many.
type data struct {
	spec dataset.Spec
	n    int64
	// few are the authors with 1 to 4 books and many those of the ranks 20 to 39, which have many.
	few, many []string
	// offset is the offset of the deep paging.
	offset int64
}

// newData reads the number of books of every author.
func newData(spec dataset.Spec, n int64) *data {
	counts := spec.AuthorCounts(n)
	d := &data{spec: spec, n: n}

	for rank, c := range counts {
		if c >= 1 && c <= 4 {
			d.few = append(d.few, dataset.AuthorName(rank))
		}
	}
	if len(d.few) == 0 {
		// A dataset so small that every author has many: those with the least.
		for rank := len(counts) - 1; rank >= 0 && len(d.few) < 20; rank-- {
			if counts[rank] > 0 {
				d.few = append(d.few, dataset.AuthorName(rank))
			}
		}
	}
	// A pool of a few hundred is enough, taken evenly from the tail.
	if len(d.few) > 500 {
		step := len(d.few) / 500
		var evenly []string
		for i := 0; i < len(d.few); i += step {
			evenly = append(evenly, d.few[i])
		}
		d.few = evenly
	}

	lo, hi := 20, 40
	if len(counts) <= lo {
		lo, hi = 0, len(counts)
	}
	for rank := lo; rank < hi && rank < len(counts); rank++ {
		if counts[rank] > 0 {
			d.many = append(d.many, dataset.AuthorName(rank))
		}
	}
	// 10,000 books are skipped at most, and a dataset must have 20 pages more after the offset
	// of the last of the ten pages that the case picks from.
	d.offset = min(10_000, max(0, (n-300)/2))
	return d
}

// itoa is strconv.Itoa for the labels.
func itoa(n int64) string { return strconv.FormatInt(n, 10) }

// label describes the arguments of a run, for the lines of the verification.
func (c *qcase) label(a args) string {
	parts := []string{c.name}
	if len(a.S) > 0 {
		if len(a.S) > 3 {
			parts = append(parts, fmt.Sprintf("%s..(%d)", a.S[0], len(a.S)))
		} else {
			parts = append(parts, strings.Join(a.S, ","))
		}
	}
	for _, i := range a.I {
		parts = append(parts, itoa(i))
	}
	return strings.Join(parts, " ")
}

// pgStatement returns the statement with the literals of the arguments in it, which is what a
// client that does not prepare statements sends.
func (c *qcase) pgStatement(table string, a args) string {
	return plans.Inline(c.statement(table), c.pgArgs(a))
}
