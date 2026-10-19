package sql_test

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/query/sql"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

const bookCount = 300

type library struct {
	books *extstore.Store[int64, Book]
	all   map[int64]*Book
}

// newLibrary defines a store of books with indexes, and fills a database with random books.
func newLibrary(t *testing.T, db kv.DB, define func(books *extstore.Store[int64, Book])) *library {
	t.Helper()

	books := extstore.New[int64, Book]("books")
	define(books)
	require.NoError(t, books.Check(db))

	rng := rand.New(rand.NewPCG(5, 6))
	lib := &library{books: books, all: map[int64]*Book{}}
	require.NoError(t, db.Update(func(tx kv.Tx) error {
		store := books.Instantiate(tx)
		for id := int64(1); id <= bookCount; id++ {
			b := randomBook(rng)
			b.Code = fmt.Sprintf("c%03d", id)
			lib.all[id] = b
			if err := store.Set(id, b); err != nil {
				return err
			}
		}
		return nil
	}))
	return lib
}

// expect returns the keys of the books that satisfy a condition, by checking each of them.
func (l *library) expect(t *testing.T, c cond.Cond[Book]) []int64 {
	t.Helper()

	var keys []int64
	for id := int64(1); id <= bookCount; id++ {
		if match(t, c, l.all[id]) {
			keys = append(keys, id)
		}
	}
	return keys
}

func TestQueryAndExplain(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		lib := newLibrary(t, db, func(books *extstore.Store[int64, Book]) {
			index.Composite(books, "by_author_year", field.List(author, year.Desc()))
			index.On(books, "by_tag", tags)
			// An index of some of the books is for a query about them.
			index.On(books, "in_print_by_decade", decade, index.Where(inPrint.Eq(true)))
			index.Unique(books, "by_code", field.Path[Book, string]("Code"))
		})
		p := sql.For(lib.books)

		for _, test := range []struct {
			where   string
			explain string
		}{
			// Without an order the records come in the order of the index, and nothing is sorted.
			{`Author = 'Herbert'`, `IndexLookup by_author_year: 1 range`},
			{`Author = 'Herbert' AND Year >= 1965`, `IndexLookup by_author_year: 1 range`},
			{`Year BETWEEN 1960 AND 1970 AND Author IN ('Herbert', 'Austen')`,
				`IndexLookup by_author_year: 2 ranges`},
			{`Author = 'Herbert' AND Year > 1900 AND Title LIKE '%Dune' AND NOT InPrint`,
				`IndexLookup by_author_year: 1 range, then check ["Title" LIKE '%Dune'; not (InPrint = true)]`},
			{`Author = 'Herbert' OR Year = 1815`, `StoreScan, then check [(Author = "Herbert") or (Year = 1815)]`},
			// A condition on the second field alone does not narrow anything.
			{`Year = 1965`, `StoreScan, then check [Year = 1965]`},
			{`'scifi' IN Tags AND Price > 5`, `IndexLookup by_tag: 1 range, then check [Price > 5]`},
			{`Tags IN ('scifi', 'new')`, `IndexLookup by_tag: 2 ranges`},
			{`Tags = 'scifi' AND Year = 1965`, `IndexLookup by_tag: 1 range, then check [Year = 1965]`},
			// The index of the books that are in print is only used by a query that is about them.
			{`Decade = 1960`, `StoreScan, then check [Decade = 1960]`},
			{`NOT InPrint AND Decade = 1960`, `StoreScan, then check [not (InPrint = true); Decade = 1960]`},
			{`InPrint AND Decade >= 1970 AND lower(Author) = 'herbert'`,
				`IndexLookup in_print_by_decade: 1 range, then check [InPrint = true; lower("Author") = 'herbert']`},
			// A unique index finds one book at most, and costs the least.
			{`Code = 'c007'`, `IndexLookup by_code: 1 range`},
			{`Code IN ('c007', 'c008', 'c009')`, `IndexLookup by_code: 3 ranges`},
			{`Code = 'c007' AND Author = 'Herbert'`, `IndexLookup by_code: 1 range, then check [Author = "Herbert"]`},
			{`Code >= 'c200' AND Author = 'Herbert' AND Year = 1965`,
				// Both fields have the one value, so the entries are in the order of the keys.
				`IndexLookup by_author_year: 1 range, then check [Code >= "c200"]`},
			{`Author = 'Herbert' AND Tags = 'scifi' AND Code = 'c007'`,
				`IndexLookup by_code: 1 range, then check [Author = "Herbert"; Tags = "scifi"]`},
			{`Tags = 'scifi' AND Code = 'c007'`, `IndexLookup by_code: 1 range, then check [Tags = "scifi"]`},
			// A contradiction reads nothing.
			{`Code = 'c007' AND Code = 'c008'`, `Empty`},
			{`Year BETWEEN 1970 AND 1960 AND Author = 'Herbert'`, `Empty`},
			{`TRUE`, `StoreScan`},
			{`FALSE`, `Empty`},
			{`Title LIKE 'D%'`, `StoreScan, then check ["Title" LIKE 'D%']`},
		} {
			got, err := p.Explain(test.where)
			require.NoError(t, err, test.where)
			require.Equal(t, test.explain, got, test.where)
			// The same text always gives the same plan.
			again, err := p.Explain(test.where)
			require.NoError(t, err, test.where)
			require.Equal(t, got, again, test.where)

			c, err := p.Parse(test.where)
			require.NoError(t, err, test.where)
			requireQuery(t, db, lib, p, test.where, c)
		}

		t.Run("Preferences", func(t *testing.T) {
			// The indexes that win are defined after the ones that lose, so that the order in
			// which they are defined cannot be what decides.
			code := field.Path[Book, string]("Code")
			books := extstore.New[int64, Book]("preferences")
			index.On(books, "by_author", author)
			index.Composite(books, "by_author_year", field.List(author, year))
			index.On(books, "by_code_any", code)
			index.Unique(books, "by_code", code)
			p := sql.For(books)

			for where, want := range map[string]string{
				// Nothing is left to check with the second index, and the year with the first.
				`Author = 'Herbert' AND Year = 1965`: `IndexLookup by_author_year: 1 range`,
				// Both find the book with nothing left to check, and one of them is unique.
				`Code = 'c007'`: `IndexLookup by_code: 1 range`,
			} {
				got, err := p.Explain(where)
				require.NoError(t, err, where)
				require.Equal(t, want, got, where)
			}
		})

		t.Run("Errors", func(t *testing.T) {
			_, err := p.Explain(`Athor = 'Herbert'`)
			require.ErrorIs(t, err, sql.ErrUnknownField)
			require.NoError(t, db.View(func(tx kv.Tx) error {
				it, err := p.Query(tx, `Author = "Herbert"`)
				require.ErrorIs(t, err, sql.ErrUnknownField)
				require.Nil(t, it.Iterator)
				_, err = p.Query(tx, `Year = 1 AND`)
				require.ErrorIs(t, err, sql.ErrSyntax)
				return nil
			}))
		})

		t.Run("EvaluationFailsWhileIterating", func(t *testing.T) {
			// Data is true, text or nothing in the books: a book with text is not a condition.
			for _, where := range []string{
				`Data`,                        // a scan
				`Author = 'Herbert' AND Data`, // the residual of an index
				`Code >= 'c000' AND NOT Data`, // the residual of a unique index
				`Year = 1965 OR coalesce(Data, FALSE)`,
			} {
				require.NoError(t, db.View(func(tx kv.Tx) error {
					it, err := p.Query(tx, where)
					require.NoError(t, err, where)
					defer it.Close()

					n := 0
					for ; it.Valid(); it.Next() {
						n++
					}
					require.ErrorIs(t, it.Err(), sql.ErrEvaluation, where)
					require.False(t, it.Valid(), where)
					_ = n
					return nil
				}))
			}
		})
	})
}

// requireQuery checks that a query finds the books that satisfy a condition, and no others.
func requireQuery(t *testing.T, db kv.DB, lib *library, p *sql.Parser[int64, Book], where string, c cond.Cond[Book]) {
	t.Helper()

	// A condition that some books fail to evaluate is checked in its own test.
	want := lib.expect(t, c)
	require.NoError(t, db.View(func(tx kv.Tx) error {
		it, err := p.Query(tx, where)
		require.NoError(t, err, where)
		defer it.Close()

		keys, err := iterutil.Of[int64, *Book](it).Keys()
		require.NoError(t, err, where)
		// There is no order to the books of a query that has none.
		slices.Sort(keys)
		require.Equal(t, want, keys, where)

		// The records that come are the ones of the keys.
		it2, err := p.Query(tx, where)
		require.NoError(t, err, where)
		defer it2.Close()
		for ; it2.Valid(); it2.Next() {
			key, err := it2.Key()
			require.NoError(t, err)
			record, err := it2.Value()
			require.NoError(t, err)
			require.Equal(t, lib.all[key].Code, record.Code, where)
			require.True(t, match(t, c, record), where)
		}
		require.NoError(t, it2.Err())
		return nil
	}))
	// A condition that no book satisfies checks little, but for the ones that are meant to.
	if where != "FALSE" && !strings.Contains(where, "Code") && !strings.Contains(where, "1970 AND 1960") {
		require.NotEmpty(t, want, where)
	}
}

// TestQueryChoosesTheFewestRanges checks the last of the preferences, between indexes that have
// as many conditions left to check.
func TestQueryChoosesTheFewestRanges(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		lib := newLibrary(t, db, func(books *extstore.Store[int64, Book]) {
			index.On(books, "by_author", author)
			index.On(books, "by_year", year)
		})
		p := sql.For(lib.books)

		explain, err := p.Explain(`Author IN ('Herbert', 'Austen', 'herbert') AND Year = 1965`)
		require.NoError(t, err)
		require.Equal(t, `IndexLookup by_year: 1 range, then check [Author in ("Herbert", "Austen", "herbert")]`, explain)

		// With as many ranges, the first index of the store.
		explain, err = p.Explain(`Author = 'Herbert' AND Year = 1965`)
		require.NoError(t, err)
		require.Equal(t, `IndexLookup by_author: 1 range, then check [Year = 1965]`, explain)

		for _, where := range []string{
			`Author IN ('Herbert', 'Austen', 'herbert') AND Year = 1965`,
			`Author = 'Herbert' AND Year = 1965`,
			`Year IN (1960, 1970) AND Author != 'Herbert'`,
		} {
			c, err := p.Parse(where)
			require.NoError(t, err)
			requireQuery(t, db, lib, p, where, c)
		}
	})
}

// TestQueryWithoutIndexes checks a store that has no indexes, and the results of an index come
// in its order.
func TestQueryWithoutIndexes(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		lib := newLibrary(t, db, func(*extstore.Store[int64, Book]) {})
		p := sql.For(lib.books)
		for _, where := range []string{`Author = 'Herbert'`, `Year > 1960 AND lower(Title) LIKE '%dune%'`, `Subtitle IS NULL`} {
			explain, err := p.Explain(where)
			require.NoError(t, err)
			require.Contains(t, explain, "StoreScan")
			c, err := p.Parse(where)
			require.NoError(t, err)
			requireQuery(t, db, lib, p, where, c)
		}
	})

	testkv.Each(t, func(t *testing.T, db kv.DB) {
		lib := newLibrary(t, db, func(books *extstore.Store[int64, Book]) {
			index.Composite(books, "by_author_year", field.List(author, year.Desc()))
		})
		p := sql.For(lib.books)
		q, err := p.Where(`Author = 'Herbert'`)
		require.NoError(t, err)
		// Year is descending in the index, and the index gives the order that is asked for.
		q = q.OrderBy(year.Desc())
		explain, err := q.Explain()
		require.NoError(t, err)
		require.Equal(t, "IndexLookup by_author_year (in order of Year desc): 1 range", explain)

		require.NoError(t, db.View(func(tx kv.Tx) error {
			it := q.Run(tx)
			defer it.Close()

			var years []int
			for ; it.Valid(); it.Next() {
				b, err := it.Value()
				require.NoError(t, err)
				years = append(years, b.Year)
			}
			require.NoError(t, it.Err())
			require.NotEmpty(t, years)
			require.True(t, slices.IsSortedFunc(years, func(a, b int) int { return b - a }), "%v", years)
			return nil
		}))
	})
}
