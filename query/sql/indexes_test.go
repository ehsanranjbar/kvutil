package sql_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/migrate"
	"github.com/ehsanranjbar/kvutil/query"
	"github.com/ehsanranjbar/kvutil/query/sql"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

func TestExprCanonicalText(t *testing.T) {
	// The path of the field is the expression as the parser prints it, with the names of the
	// functions in lower case.
	for _, text := range []string{
		`lower(Author)`, `LOWER(Author)`, `lower("Author")`, `lower( Author )`, `Lower( "Author" )`,
		"lower(`Author`)", `lower([Author])`, `(lower(Author))`, "lower(\n\tAuthor\n)",
	} {
		f := sql.Expr[Book, string](text)
		require.Equal(t, `lower("Author")`, f.Path(), text)
	}
	for text, want := range map[string]string{
		`Year+1`:                                   `"Year" + 1`,
		`Year  /10*  10`:                           `"Year" / 10 * 10`,
		`(Year + 1) * 2`:                           `("Year" + 1) * 2`,
		`substr(Title,1,2) || 'x'`:                 `substr("Title", 1, 2) || 'x'`,
		`UPPER(Tags)`:                              `upper("Tags")`,
		`lower(Publisher.City)`:                    `lower("Publisher"."City")`,
		`coalesce(Subtitle, 'none')`:               `coalesce("Subtitle", 'none')`,
		`CASE WHEN Year > 1 THEN 'a' ELSE 'b' END`: `CASE WHEN "Year" > 1 THEN 'a' ELSE 'b' END`,
	} {
		var path string
		if strings.Contains(want, "Year") && !strings.Contains(want, "CASE") && !strings.Contains(want, "||") {
			path = sql.Expr[Book, int](text).Path()
		} else {
			path = sql.Expr[Book, string](text).Path()
		}
		require.Equal(t, want, path, text)
	}

	// The same goes for the text of a condition, which the identical-text rule of the planner reads.
	books := extstore.New[int64, Book]("books")
	var wheres []string
	for _, text := range []string{
		`length(Title) > 4 AND InPrint`,
		`LENGTH(Title) > 4 AND InPrint`,
		`length( "Title" )>4   and  InPrint`,
		"Length(`Title`) > 4 AND (InPrint)",
	} {
		ix := index.On(books, "by_year_"+fmt.Sprint(len(wheres)), year, sql.Where[Book](text))
		wheres = append(wheres, ix.Descriptor().Where)
	}
	for _, where := range wheres[1:] {
		require.Equal(t, wheres[0], where)
	}
	require.Equal(t, `(length("Title") > 4) and (InPrint = true)`, wheres[0])
}

func TestExprAndWhereAreDefinitionsThatPanic(t *testing.T) {
	fns := sql.Functions{}.Add(sql.Func1("shout", func(s string) string { return s }))

	// A text that does not parse.
	require.Panics(t, func() { sql.Expr[Book, string](`lower(Author`) })
	require.Panics(t, func() { sql.Expr[Book, string](``) })
	require.Panics(t, func() { sql.Expr[Book, string](`lower(Author) lower(Author)`) })
	require.Panics(t, func() { sql.Where[Book](`Year >`) })
	require.Panics(t, func() { sql.Where[Book](``) })
	// A name that there is not.
	require.Panics(t, func() { sql.Expr[Book, string](`lower(Nope)`) })
	require.Panics(t, func() { sql.Expr[Book, string](`nope(Author)`) })
	require.Panics(t, func() { sql.Expr[Book, string](`shout(Author)`) }, "functions are given")
	require.NotPanics(t, func() { sql.Expr[Book, string](`shout(Author)`, sql.WithFunctions[Book](fns)) })
	require.Panics(t, func() { sql.Where[Book](`Nope = 1`) })
	require.Panics(t, func() { sql.Where[Book](`nope(Year) = 1`) })
	require.Panics(t, func() { sql.Where[Book](`Year = 'x'`) })
	// A type that the expression is not of.
	require.Panics(t, func() { sql.Expr[Book, int](`lower(Author)`) })
	require.Panics(t, func() { sql.Expr[Book, string](`Year + 1`) })
	require.Panics(t, func() { sql.Expr[Book, bool](`Year + 1`) })
	require.Panics(t, func() { sql.Expr[Book, float64](`Author || 'x'`) })
	require.Panics(t, func() { sql.Expr[Book, string](`Year > 1`) })
	require.Panics(t, func() { sql.Expr[Book, string](`NULL`) })
	require.Panics(t, func() { sql.Expr[Book, string](`'x' + 1`) })
	// A type that has no key codec.
	require.Panics(t, func() { sql.Expr[Book, map[string]int](`Year`) })
	require.Panics(t, func() { sql.Expr[Book, struct{ A int }](`Year`) })
	// A condition is true or false.
	require.Panics(t, func() { sql.Where[Book](`Year + 1`) })

	// Types that agree.
	require.NotPanics(t, func() {
		sql.Expr[Book, int](`Year + 1`)
		sql.Expr[Book, int64](`Year`)
		sql.Expr[Book, uint8](`Rating`)
		sql.Expr[Book, float64](`Price * 2`)
		sql.Expr[Book, float32](`Price`)
		sql.Expr[Book, Genre](`lower(Genre)`)
		sql.Expr[Book, bool](`Year > 1 AND InPrint`)
		sql.Expr[Book, bool](`InPrint`)
		sql.Expr[Book, string](`Title || 'x'`)
		sql.Expr[Book, []byte](`x'01'`)
		sql.Expr[Book, time.Time](`Released`)
		sql.Expr[Book, int](`CASE WHEN InPrint THEN 1 ELSE 2 END`)
		// A field of an interface type is anything, so it is found out when there is a book.
		sql.Expr[Book, string](`Data`)
		sql.Expr[Book, int](`Data`)
	})
}

func TestExprValues(t *testing.T) {
	lower := sql.Expr[Book, string](`lower(Author)`)
	require.False(t, lower.Always())
	require.False(t, lower.Multi())
	require.Equal(t, "string", lower.Type().String())
	require.Equal(t, []string{"herbert"}, lower.Values(dune))
	require.Equal(t, []any{"herbert"}, lower.AnyValues(dune))
	keys, err := lower.Keys(dune)
	require.NoError(t, err)
	want, err := field.Of("x", func(*Book) string { return "herbert" }).Keys(dune)
	require.NoError(t, err)
	require.Equal(t, want, keys)

	// A record that has no value for the expression has none.
	lowerSubtitle := sql.Expr[Book, string](`lower(Subtitle)`)
	require.Empty(t, lowerSubtitle.Values(&Book{}))
	keys, err = lowerSubtitle.Keys(&Book{})
	require.NoError(t, err)
	require.Empty(t, keys)
	require.Equal(t, []string{"a novel"}, lowerSubtitle.Values(dune))
	require.False(t, lowerSubtitle.Always())

	// A field of several values makes an expression of several values.
	upper := sql.Expr[Book, string](`upper(Tags)`)
	require.True(t, upper.Multi())
	require.Equal(t, []string{"SCIFI", "CLASSIC"}, upper.Values(dune))
	require.Empty(t, upper.Values(&Book{}))
	require.True(t, sql.Expr[Book, int](`length(Tags) + Year`).Multi(), "a field of several values is named")
	require.False(t, sql.Expr[Book, int](`Year + Rating`).Multi())
	require.False(t, sql.Expr[Book, string](`Publisher.City`).Multi())

	// The values are converted to the type of the field.
	require.Equal(t, []int{1966}, sql.Expr[Book, int](`Year + 1`).Values(dune))
	require.Equal(t, []uint8{9}, sql.Expr[Book, uint8](`Rating`).Values(dune))
	require.Equal(t, []Genre{"scifi"}, sql.Expr[Book, Genre](`lower(Genre)`).Values(dune))
	require.Equal(t, []float64{19}, sql.Expr[Book, float64](`Price * 2`).Values(dune))
	require.Equal(t, []bool{true}, sql.Expr[Book, bool](`Year > 1900 AND InPrint`).Values(dune))
	require.Equal(t, []int{1965}, sql.Expr[Book, int](`Year * 1.0`).Values(dune), "a whole number is an int")

	// A comparison of the field is one of an index.
	require.Equal(t, lower.Eq("herbert").Values, mustParse(t, `lower(Author) = 'herbert'`, sql.WithFields[Book](lower)).(*cond.Comparison[Book]).Values)
}

func mustParse(t *testing.T, text string, opts ...sql.Option[Book]) cond.Cond[Book] {
	t.Helper()

	c, err := sql.ParseWhere(text, opts...)
	require.NoError(t, err, text)
	return c
}

func TestExprErrorsAreNotDropped(t *testing.T) {
	// A value that cannot be one of the field.
	cents := sql.Expr[Book, int](`Price * 100`)
	require.Equal(t, []int{950}, cents.Values(dune))
	odd := &Book{Price: 0.001}
	require.Empty(t, cents.Values(odd))
	_, err := cents.Keys(odd)
	require.ErrorIs(t, err, sql.ErrEvaluation)
	c, err := cents.EqAny(1)
	require.NoError(t, err)
	_, err = c.Match(odd)
	require.ErrorIs(t, err, sql.ErrEvaluation)
	ok, err := c.Match(&Book{Price: 0.01})
	require.NoError(t, err)
	require.True(t, ok)

	// A field of an interface type that has what the field cannot.
	data := sql.Expr[Book, int](`Data`)
	_, err = data.Keys(&Book{Data: "text"})
	require.ErrorIs(t, err, sql.ErrEvaluation)
	keys, err := data.Keys(&Book{Data: int64(5)})
	require.NoError(t, err)
	require.Len(t, keys, 1)
	keys, err = data.Keys(&Book{})
	require.NoError(t, err)
	require.Empty(t, keys)

	// A function that fails.
	boom := sql.Functions{}.Add(sql.FuncN("boom", []sql.Type{sql.Integer}, false, sql.Integer, func(args []any) (any, error) {
		if args[0].(int64) < 0 {
			return nil, errBoom
		}
		return args[0], nil
	}))
	f := sql.Expr[Book, int](`boom(Year)`, sql.WithFunctions[Book](boom))
	_, err = f.Keys(&Book{Year: -1})
	require.ErrorIs(t, err, sql.ErrEvaluation)
	require.ErrorIs(t, err, errBoom)
	require.Empty(t, f.Values(&Book{Year: -1}))

	// Setting a record that the index cannot have fails, and the record is not set.
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		books := extstore.New[int64, Book]("books")
		index.On(books, "by_boom", f)
		index.On(books, "by_cents", cents)
		require.NoError(t, books.Check(db))

		require.NoError(t, db.Update(func(tx kv.Tx) error {
			return books.Instantiate(tx).Set(1, &Book{Year: 5, Price: 1})
		}))
		for _, b := range []*Book{{Year: -1, Price: 1}, {Year: 5, Price: 0.001}} {
			err := db.Update(func(tx kv.Tx) error { return books.Instantiate(tx).Set(2, b) })
			require.ErrorIs(t, err, sql.ErrEvaluation)
		}
		require.NoError(t, db.View(func(tx kv.Tx) error {
			ok, err := books.Contains(tx, 2)
			require.NoError(t, err)
			require.False(t, ok)
			return nil
		}))

		// A change of a record that makes it fail fails too, and the old record is kept.
		err := db.Update(func(tx kv.Tx) error { return books.Instantiate(tx).Set(1, &Book{Year: -3, Price: 1}) })
		require.ErrorIs(t, err, errBoom)
	})

	// A condition of an index that fails.
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		books := extstore.New[int64, Book]("books")
		index.On(books, "by_year", year, sql.Where[Book](`boom(Year) > 0`, sql.WithFunctions[Book](boom)))
		require.NoError(t, books.Check(db))

		require.NoError(t, db.Update(func(tx kv.Tx) error { return books.Instantiate(tx).Set(1, &Book{Year: 5}) }))
		err := db.Update(func(tx kv.Tx) error { return books.Instantiate(tx).Set(2, &Book{Year: -5}) })
		require.ErrorIs(t, err, sql.ErrEvaluation)
		require.ErrorIs(t, err, errBoom)
	})
}

// lowerAuthor and the others are the expressions that the books are indexed by.
var (
	lowerAuthor = sql.Expr[Book, string](`lower(Author)`)
	decadeOf    = sql.Expr[Book, int](`Year / 10 * 10`)
	upperTags   = sql.Expr[Book, string](`upper(Tags)`)
	lowerNote   = sql.Expr[Book, string](`lower(Subtitle)`)
)

func TestExpressionIndexes(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		lib := newLibrary(t, db, func(books *extstore.Store[int64, Book]) {
			index.On(books, "by_lower_author", lowerAuthor)
			index.On(books, "by_decade_of", decadeOf)
			index.On(books, "by_upper_tags", upperTags)
			index.On(books, "by_lower_subtitle", lowerNote)
		})
		p := sql.For(lib.books)

		for _, test := range []struct {
			where   string
			explain string
		}{
			// The expression is found whatever the spelling of the text is.
			{`lower(Author) = 'herbert'`, `IndexLookup by_lower_author: 1 range`},
			{`LOWER(Author) = 'herbert'`, `IndexLookup by_lower_author: 1 range`},
			{`lower("Author") = 'herbert'`, `IndexLookup by_lower_author: 1 range`},
			{`lower( Author )='herbert'`, `IndexLookup by_lower_author: 1 range`},
			{`(lower(Author)) = 'herbert'`, `IndexLookup by_lower_author: 1 range`},
			{`'herbert' = lower(Author)`, `IndexLookup by_lower_author: 1 range`},
			{`lower(Author) IS 'herbert'`, `IndexLookup by_lower_author: 1 range`},
			{`lower(Author) IN ('herbert', 'austen')`, `IndexLookup by_lower_author: 2 ranges`},
			{`lower(Author) > 'austen'`, `IndexLookup by_lower_author: 1 range`},
			{`lower(Author) >= 'austen' AND lower(Author) < 'herbert'`, `IndexLookup by_lower_author: 1 range`},
			{`'herbert' <= lower(Author)`, `IndexLookup by_lower_author: 1 range`},
			{`lower(Author) BETWEEN 'a' AND 'h'`, `IndexLookup by_lower_author: 1 range`},
			// What the index needs a value of is a comparison with values, not the others.
			{`lower(Author) != 'herbert'`, `StoreScan, then check [not (lower("Author") = "herbert")]`},
			{`lower(Author) NOT IN ('herbert')`, `StoreScan, then check [not (lower("Author") in ("herbert"))]`},
			{`lower(Author) = 'herbert' OR Year = 1`, `StoreScan, then check [(lower("Author") = "herbert") or (Year = 1)]`},
			// Another expression, or none, is evaluated.
			{`upper(Author) = 'HERBERT'`, `StoreScan, then check [upper("Author") = 'HERBERT']`},
			{`lower(Title) = 'dune'`, `StoreScan, then check [lower("Title") = 'dune']`},
			{`lower(Author) = lower(Title)`, `StoreScan, then check [lower("Author") = lower("Title")]`},
			{`lower(Author) = Title`, `StoreScan, then check [lower("Author") = "Title"]`},
			{`lower(Author) LIKE 'her%'`, `StoreScan, then check [lower("Author") LIKE 'her%']`},
			{`length(Author) = 7`, `StoreScan, then check [length("Author") = 7]`},
			// Numbers and several values, and no value.
			{`Year / 10 * 10 = 1960`, `IndexLookup by_decade_of: 1 range`},
			{`Year/10*10 IN (1960, 1970)`, `IndexLookup by_decade_of: 2 ranges`},
			{`1960 <= Year / 10 * 10 AND Year / 10 * 10 < 1980`, `IndexLookup by_decade_of: 1 range`},
			{`Year / 10 * 10 = 1960.0`, `IndexLookup by_decade_of: 1 range`},
			{`Year / 10 = 196`, `StoreScan, then check ["Year" / 10 = 196]`},
			{`upper(Tags) = 'SCIFI'`, `IndexLookup by_upper_tags: 1 range`},
			{`upper(Tags) IN ('SCIFI', 'NEW')`, `IndexLookup by_upper_tags: 2 ranges`},
			{`lower(Subtitle) = 'a novel'`, `IndexLookup by_lower_subtitle: 1 range`},
			{`lower(Subtitle) = 'a novel' AND lower(Author) = 'herbert'`, `IndexLookup by_lower_author: 1 range, then check [lower("Subtitle") = "a novel"]`},
			{`lower(Subtitle) = ''`, `IndexLookup by_lower_subtitle: 1 range`},
			// Used with others.
			{`lower(Author) = 'herbert' AND Year > 1900 AND Title LIKE '%dune%'`,
				`IndexLookup by_lower_author: 1 range, then check [Year > 1900; "Title" LIKE '%dune%']`},
		} {
			got, err := p.Explain(test.where)
			require.NoError(t, err, test.where)
			require.Equal(t, test.explain, got, test.where)

			c, err := p.Parse(test.where)
			require.NoError(t, err, test.where)
			requireQuery(t, db, lib, p, test.where, c)
		}

		// A value that the field cannot have is an error of the text, as it is for any field.
		_, err := p.Parse(`Year / 10 * 10 = 'soon'`)
		require.ErrorIs(t, err, sql.ErrType)
		_, err = p.Parse(`Year / 10 * 10 = 1960.5`)
		require.ErrorIs(t, err, sql.ErrType)
		_, err = p.Parse(`lower(Author) = 5`)
		require.ErrorIs(t, err, sql.ErrType)
		_, err = p.Parse(`lower(Author) = NULL`)
		require.ErrorIs(t, err, sql.ErrType)
		_, err = p.Parse(`lower(Author) IN ('a', 5)`)
		require.ErrorIs(t, err, sql.ErrType)

		// Ordered by the expression, as by any field, and paged.
		q, err := p.Where(`lower(Author) >= 'a'`)
		require.NoError(t, err)
		ordered := q.OrderBy(lowerAuthor).Limit(5)
		explain, err := ordered.Explain()
		require.NoError(t, err)
		require.Equal(t, "Limit 5\n└─ IndexLookup by_lower_author (in order of lower(\"Author\")): 1 range", explain)
		require.NoError(t, db.View(func(tx kv.Tx) error {
			it := ordered.Run(tx)
			defer it.Close()
			var authors []string
			for ; it.Valid(); it.Next() {
				b, err := it.Value()
				require.NoError(t, err)
				authors = append(authors, strings.ToLower(b.Author))
			}
			require.NoError(t, it.Err())
			require.Len(t, authors, 5)
			require.IsNonDecreasing(t, authors)
			return nil
		}))
	})

	// Without an index on the expression it is evaluated, with the same results.
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		lib := newLibrary(t, db, func(books *extstore.Store[int64, Book]) {
			index.On(books, "by_author", author)
		})
		p := sql.For(lib.books)
		for _, where := range []string{`lower(Author) = 'herbert'`, `upper(Tags) = 'SCIFI'`, `Year / 10 * 10 IN (1960, 1970)`} {
			explain, err := p.Explain(where)
			require.NoError(t, err, where)
			require.Contains(t, explain, "StoreScan, then check", where)
			c, err := p.Parse(where)
			require.NoError(t, err)
			requireQuery(t, db, lib, p, where, c)
		}
		// A field that is given to the parser is the same for a text.
		with := sql.For(lib.books, sql.WithFields[Book](lowerAuthor))
		c, err := with.Parse(`lower(Author) = 'herbert'`)
		require.NoError(t, err)
		require.IsType(t, &cond.Comparison[Book]{}, c)
	})
}

// TestExpressionsAreTheSameWhenIndexed checks that what a text means does not depend on a field
// being there for the expression: a comparison of the field and the evaluator agree.
func TestExpressionsAreTheSameWhenIndexed(t *testing.T) {
	books := extstore.New[int64, Book]("books")
	index.On(books, "by_lower_author", lowerAuthor)
	index.On(books, "by_decade_of", decadeOf)
	index.On(books, "by_upper_tags", upperTags)
	index.On(books, "by_lower_subtitle", lowerNote)
	indexed := sql.For(books)

	// The texts are comparisons of the fields, and not what is evaluated.
	for _, text := range []string{`lower(Author) = 'herbert'`, `'herbert' = lower(Author)`, `lower(Author) IN ('a')`,
		`Year / 10 * 10 > 1960`, `upper(Tags) = 'SCIFI'`, `lower(Subtitle) BETWEEN 'a' AND 'b'`} {
		c, err := indexed.Parse(text)
		require.NoError(t, err, text)
		require.IsType(t, &cond.Comparison[Book]{}, c, text)
	}

	for _, text := range []string{
		`lower(Author) = 'herbert'`,
		`lower(Author) != 'herbert'`,
		`'herbert' = lower(Author)`,
		`lower(Author) IN ('herbert', 'austen', '')`,
		`lower(Author) NOT IN ('herbert')`,
		`lower(Author) > 'austen'`,
		`lower(Author) <= 'austen'`,
		`lower(Author) BETWEEN 'a' AND 'h'`,
		`lower(Author) NOT BETWEEN 'a' AND 'h'`,
		`lower(Author) = ''`,
		`Year / 10 * 10 = 1960`,
		`Year / 10 * 10 != 1960`,
		`Year / 10 * 10 IN (1960, 1970)`,
		`Year / 10 * 10 >= 1960 AND Year / 10 * 10 < 1970`,
		`Year / 10 * 10 BETWEEN -50 AND 1900`,
		`upper(Tags) = 'SCIFI'`,
		`upper(Tags) != 'SCIFI'`,
		`upper(Tags) IN ('NEW', 'CLASSIC')`,
		`upper(Tags) NOT IN ('NEW', 'CLASSIC')`,
		`upper(Tags) > 'C' AND upper(Tags) < 'N'`,
		`lower(Subtitle) = 'a novel'`,
		`lower(Subtitle) != 'a novel'`,
		`lower(Subtitle) = ''`,
		`lower(Subtitle) IN ('', 'a novel')`,
		`NOT lower(Subtitle) = 'a novel'`,
		`lower(Subtitle) = 'a novel' OR lower(Author) = 'austen'`,
	} {
		translated, err := indexed.Parse(text)
		require.NoError(t, err, text)
		evaluated, err := sql.ParseWhere[Book](text)
		require.NoError(t, err, text)

		rng := rand.New(rand.NewPCG(3, 4))
		for range 500 {
			b := randomBook(rng)
			require.Equal(t, match(t, evaluated, b), match(t, translated, b), "%s on %+v", text, b)
		}
	}
}

func TestExpressionIndexesAreBuiltAndKeptUpToDate(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		rng := rand.New(rand.NewPCG(5, 6))
		write := func(books *extstore.Store[int64, Book], from, to int64) map[int64]*Book {
			all := map[int64]*Book{}
			require.NoError(t, db.Update(func(tx kv.Tx) error {
				store := books.Instantiate(tx)
				for id := from; id <= to; id++ {
					b := randomBook(rng)
					all[id] = b
					if err := store.Set(id, b); err != nil {
						return err
					}
				}
				return nil
			}))
			return all
		}
		migrated := func(books *extstore.Store[int64, Book]) string {
			report, err := books.Migrate(context.Background(), db)
			require.NoError(t, err)
			return report.String()
		}

		// A program without the index writes the books.
		v1 := extstore.New[int64, Book]("books")
		migrated(v1)
		all := write(v1, 1, 300)

		// The next one defines it on an expression, which is built for the books that are there.
		v2 := extstore.New[int64, Book]("books")
		index.On(v2, "by_lower_author", lowerAuthor)
		require.ErrorIs(t, v2.Check(db), migrate.ErrNeedsMigration)
		require.Equal(t, `built index "books/by_lower_author"`, migrated(v2))
		require.NoError(t, v2.Check(db))
		require.Empty(t, migrated(v2))

		verify := func(books *extstore.Store[int64, Book]) {
			t.Helper()

			p := sql.For(books)
			for _, name := range []string{"herbert", "austen", "", "nobody"} {
				where := fmt.Sprintf(`lower(Author) = '%s'`, name)
				explain, err := p.Explain(where)
				require.NoError(t, err)
				require.Equal(t, `IndexLookup by_lower_author: 1 range`, explain)

				var want []int64
				for id := int64(1); id <= 400; id++ {
					if b, ok := all[id]; ok && strings.ToLower(b.Author) == name {
						want = append(want, id)
					}
				}
				require.NoError(t, db.View(func(tx kv.Tx) error {
					it, err := p.Query(tx, where)
					require.NoError(t, err)
					defer it.Close()

					var got []int64
					for ; it.Valid(); it.Next() {
						key, err := it.Key()
						require.NoError(t, err)
						got = append(got, key)
					}
					require.NoError(t, it.Err())
					require.ElementsMatch(t, want, got, where)
					return nil
				}))
			}
		}
		verify(v2)

		// It is kept up to date as any index is.
		for id, b := range write(v2, 150, 400) {
			all[id] = b
		}
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			store := v2.Instantiate(tx)
			for id := int64(1); id <= 400; id += 7 {
				if err := store.Delete(id); err != nil {
					return err
				}
				delete(all, id)
			}
			return nil
		}))
		verify(v2)
	})
}

// program is a version of a program that has a store of books, with indexes on two functions
// that have versions: on an expression of shout, on a condition of shout, and on an expression
// of loud.
type program struct {
	shoutVersion, loudVersion string
	// expr and where are the texts, which are those of shout if they are empty.
	expr, where string
}

func (p program) store(indexes bool) *extstore.Store[int64, Book] {
	shout := sql.Func1("shout", func(s string) string { return strings.ToUpper(s) + "!" })
	loud := sql.Func1("loud", func(s string) string { return strings.ToUpper(s) })
	other := sql.Func1("other", func(s string) string { return s })
	if p.shoutVersion != "" {
		shout = shout.WithVersion(p.shoutVersion)
	}
	if p.loudVersion != "" {
		loud = loud.WithVersion(p.loudVersion)
	}
	opts := []sql.Option[Book]{sql.WithFunctions[Book](sql.Functions{}.Add(shout).Add(loud).Add(other))}

	expr, where := p.expr, p.where
	if expr == "" {
		expr = `shout(Author)`
	}
	if where == "" {
		where = `InPrint AND shout(Author) LIKE 'H%'`
	}

	books := extstore.New[int64, Book]("books")
	if indexes {
		index.On(books, "by_shout", sql.Expr[Book, string](expr, opts...))
		index.On(books, "by_year", year, sql.Where[Book](where, opts...))
		index.On(books, "by_loud", sql.Expr[Book, string](`loud(Author)`, opts...))
	}
	return books
}

func TestVersionsAndTextsAreWhatIndexesAreIdentifiedBy(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		migrated := func(books *extstore.Store[int64, Book]) string {
			report, err := books.Migrate(context.Background(), db)
			require.NoError(t, err)
			return report.String()
		}
		rng := rand.New(rand.NewPCG(8, 9))
		seed := program{}.store(false)
		migrated(seed)
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			for id := int64(1); id <= 50; id++ {
				if err := seed.Instantiate(tx).Set(id, randomBook(rng)); err != nil {
					return err
				}
			}
			return nil
		}))

		const builtAll = "built index \"books/by_shout\"\nbuilt index \"books/by_year\"\nbuilt index \"books/by_loud\""
		v1 := program{shoutVersion: "1", loudVersion: "1"}.store(true)
		require.ErrorIs(t, v1.Check(db), migrate.ErrNeedsMigration)
		require.Equal(t, builtAll, migrated(v1))
		require.NoError(t, v1.Check(db))

		// The same again, spelled in another way.
		again := program{shoutVersion: "1", loudVersion: "1", expr: `SHOUT( "Author" )`,
			where: `InPrint AND  SHOUT( Author ) LIKE 'H%'`}.store(true)
		require.NoError(t, again.Check(db))

		// Another text is another index.
		require.ErrorIs(t, program{shoutVersion: "1", loudVersion: "1", expr: `other(Author)`}.store(true).Check(db),
			migrate.ErrNeedsMigration, "another expression")
		require.ErrorIs(t, program{shoutVersion: "1", loudVersion: "1", where: `InPrint AND shout(Author) LIKE 'A%'`}.store(true).Check(db),
			migrate.ErrNeedsMigration, "another condition")

		// A new version of a function that an expression and a condition call: both are built
		// again, and the index of the other function is not.
		v2 := program{shoutVersion: "2", loudVersion: "1"}.store(true)
		require.ErrorIs(t, v2.Check(db), migrate.ErrNeedsMigration)
		require.Equal(t, "built index \"books/by_shout\"\nbuilt index \"books/by_year\"", migrated(v2))
		require.NoError(t, v2.Check(db))
		require.Empty(t, migrated(v2))
		require.ErrorIs(t, v1.Check(db), migrate.ErrNeedsMigration, "the old version")

		v3 := program{shoutVersion: "2", loudVersion: "3"}.store(true)
		require.Equal(t, `built index "books/by_loud"`, migrated(v3))

		// A function without a version is one whose version is nothing, and a version that is
		// taken away is a change.
		v4 := program{shoutVersion: "", loudVersion: "3"}.store(true)
		require.ErrorIs(t, v4.Check(db), migrate.ErrNeedsMigration)
		require.Equal(t, "built index \"books/by_shout\"\nbuilt index \"books/by_year\"", migrated(v4))
		require.NoError(t, v4.Check(db))

		// A function that an expression does not call has no part in it, even if it is another
		// function that has the version: the text is what changes then.
		v5 := program{shoutVersion: "5", loudVersion: "3", expr: `other(Author)`, where: `InPrint AND other(Author) LIKE 'H%'`}.store(true)
		require.Equal(t, "built index \"books/by_shout\"\nbuilt index \"books/by_year\"", migrated(v5))
		v6 := program{shoutVersion: "6", loudVersion: "3", expr: `other(Author)`, where: `InPrint AND other(Author) LIKE 'H%'`}.store(true)
		require.NoError(t, v6.Check(db), "functions that are not called")
	})
}

func TestExpressionsInConcurrentUse(t *testing.T) {
	f := sql.Expr[Book, string](`upper(Tags)`)
	books := extstore.New[int64, Book]("books")
	index.On(books, "by_upper_tags", f)
	index.On(books, "recent", year, sql.Where[Book](`InPrint AND length(Title) > 3`))
	c, err := sql.ParseWhere[Book](`InPrint AND length(Title) > 3`)
	require.NoError(t, err)
	p := sql.For(books)

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			rng := rand.New(rand.NewPCG(uint64(g), 3))
			for range 200 {
				b := randomBook(rng)
				if _, err := f.Keys(b); err != nil {
					t.Error(err)
					return
				}
				if _, err := c.Match(b); err != nil {
					t.Error(err)
					return
				}
				if _, err := p.Explain(`upper(Tags) = 'NEW' AND InPrint AND length(Title) > 3`); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestWhereIndexes(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		lib := newLibrary(t, db, func(books *extstore.Store[int64, Book]) {
			index.On(books, "by_year", year)
			index.On(books, "recent_by_year", year, sql.Where[Book](`InPrint AND Year >= 1900`))
			index.On(books, "herbert_by_year", year, sql.Where[Book](`Author IN ('Herbert', 'herbert') OR lower(Author) = 'herbert'`))
			index.On(books, "no_subtitle_by_year", year, sql.Where[Book](`Subtitle IS NULL`))
			index.On(books, "short_by_year", year, sql.Where[Book](`length(Title) < 5`))
			index.On(books, "many_by_year", year, sql.Where[Book](`TRUE`))
		})
		p := sql.For(lib.books)

		// The conditions of the indexes are in the canonical text, with the others of Go.
		for name, want := range map[string]string{
			"recent_by_year":      `(InPrint = true) and (Year >= 1900)`,
			"herbert_by_year":     `(Author in ("Herbert", "herbert")) or (lower("Author") = 'herbert')`,
			"no_subtitle_by_year": `"Subtitle" IS NULL`,
			"short_by_year":       `length("Title") < 5`,
		} {
			require.Equal(t, want, whereOf(t, lib.books, name), name)
		}

		for _, test := range []struct {
			where   string
			explain string
		}{
			// The query is about the books of the index.
			{`InPrint AND Year >= 1970`, `IndexLookup recent_by_year: 1 range, then check [InPrint = true]`},
			{`InPrint AND Year >= 1970 AND Year <= 1980`, `IndexLookup recent_by_year: 1 range, then check [InPrint = true]`},
			{`InPrint AND Year = 1970`, `IndexLookup recent_by_year: 1 range, then check [InPrint = true]`},
			{`InPrint AND Year IN (1970, 1971)`, `IndexLookup recent_by_year: 2 ranges, then check [InPrint = true]`},
			{`InPrint AND Year > 1900 AND Author = 'Austen'`,
				`IndexLookup recent_by_year: 1 range, then check [InPrint = true; Author = "Austen"]`},
			{`Year >= 1900 AND InPrint AND Year >= 1930`, `IndexLookup recent_by_year: 1 range, then check [InPrint = true]`},
			{`Author = 'Herbert' AND Year = 1965`, `IndexLookup herbert_by_year: 1 range, then check [Author = "Herbert"]`},
			{`Author IN ('herbert', 'Herbert') AND Year > 1900`, `IndexLookup herbert_by_year: 1 range, then check [Author in ("herbert", "Herbert")]`},
			{`Subtitle IS NULL AND Year = 1965`, `IndexLookup no_subtitle_by_year: 1 range, then check ["Subtitle" IS NULL]`},
			{`length(Title) < 5 AND Year = 1965`, `IndexLookup short_by_year: 1 range, then check [length("Title") < 5]`},
			{`LENGTH( Title )<5 AND Year = 1965`, `IndexLookup short_by_year: 1 range, then check [length("Title") < 5]`},
			// The condition that is true is implied by any query, so its index is a candidate;
			// the index of all the books comes first, and costs the same.
			{`Year = 1965`, `IndexLookup by_year: 1 range`},
			// The conditions of the others are not implied by the query, so they are not read.
			{`Year >= 1970`, `IndexLookup by_year: 1 range`},
			{`InPrint AND Year >= 1800`, `IndexLookup by_year: 1 range, then check [InPrint = true]`},
			{`InPrint OR Year >= 1970`, `StoreScan, then check [(InPrint = true) or (Year >= 1970)]`},
			{`NOT InPrint AND Year = 1965`, `IndexLookup by_year: 1 range, then check [not (InPrint = true)]`},
			{`Author = 'Austen' AND Year = 1965`, `IndexLookup by_year: 1 range, then check [Author = "Austen"]`},
			{`Subtitle IS NOT NULL AND Year = 1965`, `IndexLookup by_year: 1 range, then check ["Subtitle" IS NOT NULL]`},
			{`length(Title) < 6 AND Year = 1965`, `IndexLookup by_year: 1 range, then check [length("Title") < 6]`},
		} {
			got, err := p.Explain(test.where)
			require.NoError(t, err, test.where)
			require.Equal(t, test.explain, got, test.where)

			c, err := p.Parse(test.where)
			require.NoError(t, err, test.where)
			requireQuery(t, db, lib, p, test.where, c)
		}

		// The index is looked up on, and nothing else, by a query that hints it.
		q := func(where, hint string) (string, error) {
			c, err := p.Where(where)
			require.NoError(t, err)
			return c.Using(hint).Explain()
		}
		explain, err := q(`InPrint AND Year >= 1970`, "recent_by_year")
		require.NoError(t, err)
		require.Equal(t, `IndexLookup recent_by_year: 1 range, then check [InPrint = true]`, explain)
		for _, where := range []string{`Year >= 1970`, `InPrint`, `Year >= 1900`, `InPrint OR Year >= 1900`, `NOT InPrint AND Year >= 1900`, `InPrint AND Year >= 1899`} {
			_, err := q(where, "recent_by_year")
			require.ErrorIs(t, err, query.ErrHint, where)
		}
		_, err = q(`Year >= 1900`, "herbert_by_year")
		require.ErrorIs(t, err, query.ErrHint)
		explain, err = q(`Year >= 1900`, "many_by_year")
		require.NoError(t, err)
		require.Equal(t, `IndexLookup many_by_year: 1 range`, explain)
	})
}

func whereOf(t *testing.T, books *extstore.Store[int64, Book], name string) string {
	t.Helper()

	x, ok := books.Extension(name).(interface {
		Index() *index.Index[int64, Book]
	})
	require.True(t, ok, name)
	return x.Index().Descriptor().Where
}
