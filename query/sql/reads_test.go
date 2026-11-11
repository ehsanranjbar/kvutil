package sql_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/query/sql"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

// TestEvaluatedReads checks that what a condition of a text reads is the fields that the text
// names, and that it is not known if the reads of one of them are not.
func TestEvaluatedReads(t *testing.T) {
	// A field of a function, that reads what it says, or what is not known.
	hidden := field.Of("Hidden", func(b *Book) int { return b.Year * 2 })
	declared := field.Of("Declared", func(b *Book) int { return b.Year * 2 }).Reading("Year", "Price")
	none := field.Of("None", func(b *Book) int { return 7 }).Reading()
	type parse func(text string) (cond.Cond[Book], error)
	withOptions := func(opts ...sql.Option[Book]) parse {
		return func(text string) (cond.Cond[Book], error) { return sql.ParseWhere[Book](text, opts...) }
	}
	p := withOptions(sql.WithFields(hidden, declared, none))

	reads := func(p parse, text string) ([]string, bool) {
		t.Helper()
		c, err := p(text)
		require.NoError(t, err, text)
		return cond.Reads(c)
	}
	known := func(p parse, text string, want ...string) {
		t.Helper()
		paths, ok := reads(p, text)
		require.True(t, ok, text)
		if len(want) == 0 {
			require.Empty(t, paths, text)
			return
		}
		require.Equal(t, want, paths, text)
	}
	unknown := func(p parse, text string) {
		t.Helper()
		paths, ok := reads(p, text)
		require.False(t, ok, text)
		require.Nil(t, paths, text)
	}

	plain := withOptions()

	// Evaluated expressions read the fields that they name.
	known(plain, `lower(Author) LIKE 'a%'`, "Author")
	known(plain, `Year + Price > 10`, "Price", "Year")
	known(plain, `Year * 2 > Year - 3`, "Year")
	known(plain, `Publisher.City = 'Oslo' OR lower(Title) = 'dune'`, "Publisher.City", "Title")
	known(plain, `CASE WHEN Year > 1900 THEN Title ELSE Author END = 'a'`, "Author", "Title", "Year")
	known(plain, `Year IN (Rating, 4)`, "Rating", "Year")
	known(plain, `Rating * 2 = 4 AND NOT (Year * 2 = 4)`, "Rating", "Year")
	// A text with no field reads nothing.
	known(plain, `1 + 1 = 2`)
	known(plain, `lower('A') = 'a'`)
	// A function that is given to the parser gets values, not the record.
	custom := withOptions(withFunctions(testFunctions()))
	known(custom, `double(Year) > 10`, "Year")
	known(custom, `double(3) > 5`)

	// The comparisons and the checks of a field read it.
	known(plain, `Year >= 1960 AND Year < 1970`, "Year")
	known(plain, `Year BETWEEN 1 AND 2`, "Year")
	known(plain, `Year NOT IN (1, 2)`, "Year")
	known(plain, `Year <> 3`, "Year")
	known(plain, `Author LIKE 'a%'`, "Author")
	known(plain, `Subtitle IS NULL`, "Subtitle")
	known(plain, `Publisher.City IS NOT NULL`, "Publisher.City")
	known(plain, `InPrint`, "InPrint")
	known(plain, `NOT InPrint AND Year = 3`, "InPrint", "Year")
	known(plain, `'scifi' IN Tags`, "Tags")
	known(plain, `TRUE`)
	known(plain, `FALSE OR Year = 1`, "Year")

	// A field that is given reads what it is told to, and what is not told is not known.
	known(p, `Declared > 3`, "Price", "Year")
	known(p, `Declared + 1 > 3`, "Price", "Year")
	known(p, `Declared + Rating = 5`, "Price", "Rating", "Year")
	known(p, `None > 3`)
	known(p, `None + Year > 3`, "Year")
	known(p, `Declared IS NULL`, "Price", "Year")
	known(p, `Declared BETWEEN 1 AND 2 OR Rating = 1`, "Price", "Rating", "Year")
	unknown(p, `Hidden > 3`)
	unknown(p, `Hidden + 1 > 3`)
	unknown(p, `Hidden IS NULL`)
	unknown(p, `Hidden IN (1, 2)`)
	unknown(p, `Year = 1 AND Hidden > 3`)
	unknown(p, `Year = 1 OR NOT (Hidden > 3)`)
	unknown(p, `lower(Author) = 'a' AND Hidden + Year > 3`)

	// A field of the struct that is not found by reflection reads its path.
	known(plain, `Extra IS NULL`, "Extra")
	known(plain, `Data IS NULL`, "Data")

	// An expression that is a field reads what its columns do. It is indexed by the text.
	lowerAuthor := sql.Expr[Book, string](`lower(Author)`)
	paths, ok := lowerAuthor.Reads()
	require.True(t, ok)
	require.Equal(t, []string{"Author"}, paths)
	paths, ok = lowerAuthor.Eq("herbert").Reads()
	require.True(t, ok)
	require.Equal(t, []string{"Author"}, paths)

	known(plain, `1 = 1`)
	sum := sql.Expr[Book, float64](`Year + Price * Rating`)
	paths, ok = sum.Reads()
	require.True(t, ok)
	require.Equal(t, []string{"Price", "Rating", "Year"}, paths)
	nothing := sql.Expr[Book, int](`1 + 1`)
	paths, ok = nothing.Reads()
	require.True(t, ok)
	require.Empty(t, paths)
	multi := sql.Expr[Book, string](`upper(Tags)`)
	paths, ok = multi.Reads()
	require.True(t, ok)
	require.Equal(t, []string{"Tags"}, paths)

	// An expression over a field that is given, with and without reads.
	over := sql.Expr[Book, int](`Declared + 1`, sql.WithFields[Book](declared))
	paths, ok = over.Reads()
	require.True(t, ok)
	require.Equal(t, []string{"Price", "Year"}, paths)
	_, ok = sql.Expr[Book, int](`Hidden + 1`, sql.WithFields[Book](hidden)).Reads()
	require.False(t, ok)

	// The conditions over the fields of the indexes of a store read what those do, and the
	// expression that is the field of an index is compared, not evaluated.
	books := extstore.New[int64, Book]("books")
	index.On(books, "by_author", lowerAuthor)
	index.On(books, "by_decade", decade)
	index.On(books, "by_decade_declared", declared)
	index.On(books, "by_year", year)
	indexed := sql.For(books).Parse
	known(indexed, `lower(Author) = 'herbert'`, "Author")
	known(indexed, `LOWER("Author") IN ('herbert', 'asimov')`, "Author")
	known(indexed, `Year > 1950`, "Year")
	known(indexed, `Year > 1950 AND lower(Author) LIKE 'h%'`, "Author", "Year")
	// The field of an index that does not say what it reads is not known; the one that does, is.
	unknown(indexed, `Decade = 1960`)
	unknown(indexed, `Decade + 1 > 3`)
	known(indexed, `Declared = 1960`, "Price", "Year")
	c, err := indexed(`lower(Author) = 'herbert'`)
	require.NoError(t, err)
	require.IsType(t, &cond.Comparison[Book]{}, c)
}
