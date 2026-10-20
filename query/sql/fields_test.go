package sql_test

import (
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/query/sql"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/ehsanranjbar/kvutil/store/record"
)

// A parser is made for the stores of both kinds that indexes can be defined on.
var (
	_ sql.Store[int64, Book] = (*extstore.Store[int64, Book])(nil)
	_ sql.Store[int64, Book] = (*record.Store[int64, Book])(nil)
)

// TestFieldsOfIndexes checks that what is parsed for a store is written with the fields of its
// indexes, which may be computed, or have another type than the struct has.
func TestFieldsOfIndexes(t *testing.T) {
	books := extstore.New[int64, Book]("books")
	index.On(books, "in_print_by_decade", decade, index.Where(inPrint.Eq(true)))
	// A field whose codec is not the one of its type: the values of a text are encoded with it.
	cents := field.Of("Price", func(b *Book) int64 { return int64(b.Price * 100) })
	index.On(books, "by_cents", cents)
	// The first index that has a path gives the field.
	index.On(books, "by_cents_again", field.Of("Price", func(b *Book) float64 { return b.Price }))
	p := sql.For(books)

	// Price is the field of the index that is defined on it, with the type that it has there:
	// whole cents. It is not the field of the struct.
	c, err := p.Parse(`Price = 950`)
	require.NoError(t, err)
	require.Equal(t, cents.Eq(950).Values, c.(*cond.Comparison[Book]).Values)
	require.Equal(t, "int64", c.(*cond.Comparison[Book]).Codec)
	require.True(t, match(t, c, dune))
	_, err = p.Parse(`Price = 9.5`)
	require.ErrorIs(t, err, sql.ErrType)

	// The evaluator reads the value that the index has, too.
	c, err = p.Parse(`Price / 100 = 9 AND Decade = 1960 AND Decade + 5 = 1965`)
	require.NoError(t, err)
	require.True(t, match(t, c, dune))
	require.False(t, match(t, c, &Book{Year: 1970, Price: 9.5}))

	// A parser that is not of the store has the field of the struct, and does not know of the
	// field that is computed, unless it is given.
	c, err = sql.ParseWhere[Book](`Price = 9.5`)
	require.NoError(t, err)
	require.Equal(t, "float64", c.(*cond.Comparison[Book]).Codec)
	_, err = sql.ParseWhere[Book](`Decade = 1960`)
	require.ErrorIs(t, err, sql.ErrUnknownField)
	c, err = sql.ParseWhere[Book](`Decade = 1960 AND Decade * 2 = 3920`, sql.WithFields(decade))
	require.NoError(t, err)
	require.True(t, match(t, c, dune))
	require.False(t, match(t, c, &Book{Year: 1970}))

	// A field that is given comes before the one of an index.
	c, err = sql.For(books, sql.WithFields(price)).Parse(`Price = 9.5`)
	require.NoError(t, err)
	require.Equal(t, "float64", c.(*cond.Comparison[Book]).Codec)
}

type Leaf struct {
	N    int
	Name *string
}

type Level2 struct{ Leaf *Leaf }
type Level3 struct{ Level2 *Level2 }
type Level4 struct{ Level3 *Level3 }

// TestNestedPaths checks the paths of fields in structs in structs, which rqlite reads as one,
// two or three names, and as one name that has dots.
func TestNestedPaths(t *testing.T) {
	deep := &Level4{Level3: &Level3{Level2: &Level2{Leaf: &Leaf{N: 7, Name: ptr("seven")}}}}
	none := &Level4{Level3: &Level3{}}
	nothing := &Level4{}

	check := func(t *testing.T, text string, want bool, b *Level4) {
		t.Helper()

		c, err := sql.ParseWhere[Level4](text)
		require.NoError(t, err, text)
		ok, err := c.Match(b)
		require.NoError(t, err, text)
		require.Equal(t, want, ok, text)
	}

	// Four names are a name in quotes with dots in it, or three of them and one more.
	for _, text := range []string{
		`"Level3.Level2.Leaf.N" = 7`,
		`"Level3.Level2.Leaf.N" >= 7 AND "Level3.Level2.Leaf.N" < 8`,
		`"Level3.Level2"."Leaf.N" = 7`,
		`7 = "Level3.Level2.Leaf.N"`,
		`"Level3.Level2.Leaf.N" + 1 = 8`,
		`"Level3.Level2.Leaf.Name" LIKE 'sev%'`,
		`"Level3.Level2.Leaf.Name" = 'seven'`,
		`Level3."Level2.Leaf.N" = 7`,
		`"Level3"."Level2.Leaf.N" = 7`,
		"`Level3.Level2.Leaf.N` = 7",
		`[Level3.Level2.Leaf.N] = 7`,
		`"Level3.Level2.Leaf.N" IN (6, 7)`,
		`"Level3.Level2.Leaf.N" BETWEEN 7 AND 9`,
	} {
		check(t, text, true, deep)
		check(t, text, false, none)
		check(t, text, false, nothing)
	}
	check(t, `"Level3.Level2.Leaf.N" IS NULL`, false, deep)
	check(t, `"Level3.Level2.Leaf.N" IS NULL`, true, none)
	check(t, `"Level3.Level2.Leaf.N" IS NULL`, true, nothing)
	check(t, `"Level3.Level2.Leaf.Name" IS NOT NULL`, true, deep)
	check(t, `NOT "Level3.Level2.Leaf.N" = 7`, false, deep)
	check(t, `NOT "Level3.Level2.Leaf.N" = 7`, true, none)

	// Up to three names without quotes: the parser reads no more.
	check(t, `Level3.Level2.Leaf IS NOT NULL`, true, deep)
	check(t, `Level3.Level2.Leaf IS NOT NULL`, false, none)
	check(t, `"Level3"."Level2"."Leaf" IS NULL`, true, none)
	check(t, `Level3.Level2 IS NULL`, true, none)
	check(t, `Level3 IS NOT NULL`, true, none)
	check(t, `Level3 IS NOT NULL`, false, nothing)
	for _, text := range []string{`Level3.Level2.Leaf.N = 7`, `"Level3".Level2.Leaf.N = 7`, `Level3.Level2.Leaf.N IS NULL`} {
		_, err := sql.ParseWhere[Level4](text)
		require.ErrorIs(t, err, sql.ErrSyntax, text)
	}

	// Three names that lead to a field with a value.
	three := &Level3{Level2: &Level2{Leaf: &Leaf{N: 7}}}
	c, err := sql.ParseWhere[Level3](`Level2.Leaf.N = 7 AND Level2.Leaf.N + 1 = 8 AND "Level2"."Leaf"."N" = 7`)
	require.NoError(t, err)
	ok, err := c.Match(three)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = c.Match(&Level3{})
	require.NoError(t, err)
	require.False(t, ok)
	_, err = sql.ParseWhere[Level3](`Level2.Leaf.M = 7`)
	require.ErrorIs(t, err, sql.ErrUnknownField)
	_, err = sql.ParseWhere[Level3](`Level2.Leaf.N.X = 7`)
	require.ErrorIs(t, err, sql.ErrSyntax)
	_, err = sql.ParseWhere[Level3](`"Level2.Leaf.N.X" = 7`)
	require.ErrorIs(t, err, sql.ErrUnknownField)
}

func TestConcurrentUse(t *testing.T) {
	books := extstore.New[int64, Book]("books")
	index.On(books, "by_decade", decade)
	p := sql.For(books)

	shared, err := p.Parse(`Author = 'Herbert' AND (lower(Title) LIKE '%dune' OR length(Tags) > 1) AND Decade > 1900`)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			rng := rand.New(rand.NewPCG(uint64(g), 7))
			for i := range 200 {
				own, err := p.Parse(fmt.Sprintf(`Year = %d AND Publisher.City = 'London' AND Title LIKE '%%e%%' AND Decade >= 1900`, 1960+i%20))
				if err != nil {
					t.Error(err)
					return
				}
				b := randomBook(rng)
				for _, c := range []cond.Cond[Book]{shared, own} {
					if _, err := c.Match(b); err != nil {
						t.Error(err)
						return
					}
				}
				if _, err := p.Explain(`Decade = 1960`); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestLargeNumbers(t *testing.T) {
	type Counter struct {
		N uint64
		I int64
	}
	n := field.Path[Counter, uint64]("N")
	i := field.Path[Counter, int64]("I")

	// The numbers that a float does not have exactly are not read as floats.
	for text, want := range map[string]*cond.Comparison[Counter]{
		`N = 18446744073709551615`:  n.Eq(math.MaxUint64),
		`N >= 9223372036854775808`:  n.Gte(1 << 63),
		`N >= 0x8000000000000000`:   n.Gte(1 << 63),
		`I = 9223372036854775807`:   i.Eq(math.MaxInt64),
		`I = -9223372036854775807`:  i.Eq(-math.MaxInt64),
		`I = -9223372036854775808`:  i.Eq(math.MinInt64),
		`I IN (9007199254740993)`:   i.In(1<<53 + 1),
		`N IN (9007199254740993.0)`: n.In(1 << 53),
		`18446744073709551615 = N`:  n.Eq(math.MaxUint64),
	} {
		got, err := sql.ParseWhere[Counter](text)
		require.NoError(t, err, text)
		require.Equal(t, want.Values, got.(*cond.Comparison[Counter]).Values, text)
		require.Equal(t, want.Low, got.(*cond.Comparison[Counter]).Low, text)
	}

	for _, text := range []string{`N = 18446744073709551616`, `I = 9223372036854775808`, `N = -1`, `I = -9223372036854775809`} {
		_, err := sql.ParseWhere[Counter](text)
		require.ErrorIs(t, err, sql.ErrType, text)
	}

	// The evaluator has integers where they do not overflow, and floats where they would.
	c, err := sql.ParseWhere[Counter](`I + 1 > 0 AND I - 1 < 9223372036854775807 AND I * 2 > 0`)
	require.NoError(t, err)
	ok, err := c.Match(&Counter{I: math.MaxInt64})
	require.NoError(t, err)
	require.True(t, ok)
	c, err = sql.ParseWhere[Counter](`N > 9e18 AND N + 0 > 9e18`)
	require.NoError(t, err)
	ok, err = c.Match(&Counter{N: math.MaxUint64})
	require.NoError(t, err)
	require.True(t, ok)
}
