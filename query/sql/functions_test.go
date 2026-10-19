package sql_test

import (
	"errors"
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
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/query/sql"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

var errBoom = errors.New("boom")

type shortText string

// testFunctions are functions of every kind of argument and of result.
func testFunctions() sql.Functions {
	return sql.Functions{}.
		// Integers and floats, and the Go types that are defined on them.
		Add(sql.Func1("double", func(n int) int { return n * 2 })).
		Add(sql.Func1("half", func(f float64) float64 { return f / 2 })).
		Add(sql.Func2("plus", func(a int, b uint8) int64 { return int64(a) + int64(b) })).
		Add(sql.Func3("clamp", func(x, lo, hi float32) float64 { return float64(min(max(x, lo), hi)) })).
		// Text, bytes, times and booleans.
		Add(sql.Func1("Shout", func(s string) string { return strings.ToUpper(s) + "!" })).
		Add(sql.Func1("brief", func(s shortText) shortText { return s[:min(len(s), 3)] })).
		Add(sql.Func1("size", func(b []byte) int { return len(b) })).
		Add(sql.Func1("year_of", func(t time.Time) int { return t.Year() })).
		Add(sql.Func1("later", func(t time.Time) time.Time { return t.AddDate(1, 0, 0) })).
		Add(sql.Func1("is_old", func(year int) bool { return year < 1900 })).
		Add(sql.Func1("negate", func(b bool) bool { return !b })).
		Add(sql.Func1("name_of_type", func(v any) string { return fmt.Sprintf("%T", v) })).
		Add(sql.Func1("genre_is", func(g Genre) bool { return g == "scifi" })).
		// The ones that are made from types.
		Add(sql.FuncN("total", []sql.Type{sql.Number}, true, sql.Number, func(args []any) (any, error) {
			sum := 0.0
			for _, arg := range args {
				switch n := arg.(type) {
				case int64:
					sum += float64(n)
				case float64:
					sum += n
				}
			}
			return sum, nil
		})).
		Add(sql.FuncN("describe", []sql.Type{sql.Text, sql.Integer, sql.Bool, sql.Bytes, sql.Time, sql.Real}, false, sql.Text,
			func(args []any) (any, error) {
				return fmt.Sprintf("%v/%v/%v/%x/%v/%v", args...), nil
			})).
		Add(sql.FuncN("describe_kinds", []sql.Type{sql.Any}, true, sql.Text, func(args []any) (any, error) {
			kinds := make([]string, len(args))
			for i, arg := range args {
				kinds[i] = fmt.Sprintf("%T", arg)
			}
			return strings.Join(kinds, ","), nil
		})).
		Add(sql.FuncN("pi", nil, false, sql.Real, func([]any) (any, error) { return 3.14, nil })).
		Add(sql.FuncN("boom", []sql.Type{sql.Integer}, false, sql.Integer, func([]any) (any, error) { return nil, errBoom })).
		Add(sql.FuncN("lie", []sql.Type{sql.Integer}, false, sql.Text, func([]any) (any, error) { return 5, nil })).
		Add(sql.FuncN("many", []sql.Type{sql.Integer}, false, sql.Text, func([]any) (any, error) { return []string{"a"}, nil })).
		Add(sql.FuncN("none_at_all", []sql.Type{sql.Integer}, false, sql.Text, func([]any) (any, error) { return nil, nil })).
		Add(sql.FuncN("even_if_none", []sql.Type{sql.Any}, false, sql.Bool, func(args []any) (any, error) {
			return args[0] == nil, nil
		}))
}

func withFunctions(fs sql.Functions) sql.Option[Book] { return sql.WithFunctions[Book](fs) }

func TestFunctionsOfEveryType(t *testing.T) {
	fns := testFunctions()
	parse := func(text string) cond.Cond[Book] {
		t.Helper()

		c, err := sql.ParseWhere(text, withFunctions(fns))
		require.NoError(t, err, text)
		return c
	}

	// Each of these is true for the book Dune.
	for _, text := range []string{
		// Integers, floats and the Go types that are defined on them.
		`double(Year) = 3930`,
		`double(Year + 1) = 3932`,
		`double(5.0) = 10`,
		`half(Price) = 4.75`,
		`half(Year) = 982.5`,
		`plus(Year, Rating) = 1974`,
		`clamp(Price, 1, 5) = 5`,
		`clamp(Price, 1, 50) = 9.5`,
		// Text, bytes, times and booleans.
		`shout(Author) = 'HERBERT!'`,
		`SHOUT(Author) = 'HERBERT!'`,
		`brief(Title) = 'Dun'`,
		`brief(Genre) = 'sci'`,
		`size(x'44756e65') = 4`,
		`year_of(Released) = 1965`,
		`later(Released) < Reprinted AND year_of(later(Released)) = 1966`,
		`later(Released) > '1966-01-01'`,
		`is_old(Year) = FALSE AND NOT is_old(Year)`,
		`is_old(1815)`,
		`negate(InPrint) = FALSE`,
		`negate(is_old(Year))`,
		`name_of_type(Year) = 'int64'`,
		`genre_is(Genre) AND NOT genre_is(Title)`,
		`name_of_type(Author) = 'string'`,
		// The ones that are made from types.
		`total(1, 2, 3) = 6`,
		`total(Year, Price) = 1974.5`,
		`total(5) = 5`,
		`describe('a', 3, TRUE, x'0a0b', Released, 2.5) = 'a/3/true/0a0b/1965-08-01 00:00:00 +0000 UTC/2.5'`,
		`describe_kinds(1, 1.5, 'a', TRUE, x'01', Released, Subtitle) = 'int64,float64,string,bool,[]uint8,time.Time,string'`,
		`pi() = 3.14`,
		`pi() * 2 > 6`,
		// Functions are applied to each value of a field of several values.
		`shout(Tags) = 'SCIFI!'`,
		`shout(Tags) = 'CLASSIC!' AND shout(Tags) = 'SCIFI!'`,
		`double(Year) IN (3930, 1)`,
		`length(shout(Author)) = 8`,
		// A function is not called for no value.
		`shout(Data) IS NULL`,
		`shout(Publisher.City) = 'PHILADELPHIA!'`,
		`even_if_none(Data)`,
		`even_if_none(NULL)`,
		`none_at_all(1) IS NULL`,
		`coalesce(shout(Data), 'none') = 'none'`,
	} {
		require.True(t, match(t, parse(text), dune), text)
	}

	// Not the others.
	for _, text := range []string{
		`double(Year) = 1965`,
		`shout(Author) = 'HERBERT'`,
		`later(Released) > Reprinted`,
		`is_old(Year)`,
		`genre_is(Title)`,
		`shout(Tags) = 'NEW!'`,
		`even_if_none(Year)`,
	} {
		require.False(t, match(t, parse(text), dune), text)
	}

	// A book that has no value for what a function is given has no value from it.
	noSubtitle := &Book{Year: 1}
	require.True(t, match(t, parse(`shout(Subtitle) IS NULL`), noSubtitle))
	require.False(t, match(t, parse(`shout(Subtitle) = 'A NOVEL!'`), noSubtitle))
	require.True(t, match(t, parse(`NOT shout(Subtitle) = 'A NOVEL!'`), noSubtitle))
	require.True(t, match(t, parse(`shout(Subtitle) = 'A NOVEL!'`), dune))
	require.True(t, match(t, parse(`even_if_none(Subtitle)`), noSubtitle))
}

func TestFunctionsAreCheckedWhenParsed(t *testing.T) {
	fns := testFunctions()

	for text, want := range map[string]error{
		// A function that there is not.
		`nope(Year) = 1`:              sql.ErrUnknownFunction,
		`nope(Year)`:                  sql.ErrUnknownFunction,
		`double(nope(Year))`:          sql.ErrUnknownFunction,
		`length(Year) > 1 AND nope()`: sql.ErrUnknownFunction,
		// The wrong number of arguments.
		`double() = 1`:           sql.ErrType,
		`double(Year, 1) = 1`:    sql.ErrType,
		`plus(Year) = 1`:         sql.ErrType,
		`clamp(1, 2) = 1`:        sql.ErrType,
		`total() = 1`:            sql.ErrType,
		`describe_kinds() = ''`:  sql.ErrType,
		`describe('a', 1) = 'x'`: sql.ErrType,
		`pi(1) = 1`:              sql.ErrType,
		// The wrong kinds of arguments.
		`double(Author) = 1`:                              sql.ErrType,
		`double('5') = 1`:                                 sql.ErrType,
		`shout(Year) = 'x'`:                               sql.ErrType,
		`size(Author) = 1`:                                sql.ErrType,
		`year_of(Author) = 1`:                             sql.ErrType,
		`year_of('1965-01-01') = 1`:                       sql.ErrType,
		`negate(Year)`:                                    sql.ErrType,
		`total(Year, Author) = 1`:                         sql.ErrType,
		`plus(Year, TRUE) = 1`:                            sql.ErrType,
		`describe('a', 1, 1, x'0a', Released, 2.5) = 'x'`: sql.ErrType,
		// The result of the wrong kind.
		`double(Year)`:                                   sql.ErrType,
		`double(Year) AND InPrint`:                       sql.ErrType,
		`NOT shout(Author)`:                              sql.ErrType,
		`shout(Author) + 1 = 2`:                          sql.ErrType,
		`double(Year) = 'x'`:                             sql.ErrType,
		`shout(Author) > 5`:                              sql.ErrType,
		`year_of(Released) LIKE 5 AND shout(Year) = 'x'`: sql.ErrType,
		`CASE WHEN InPrint THEN double(Year) END`:        sql.ErrType,
		`later(Released) > Author`:                       sql.ErrType,
	} {
		_, err := sql.ParseWhere(text, withFunctions(fns))
		require.ErrorIs(t, err, want, text)
	}

	// Without the functions they are not there.
	_, err := sql.ParseWhere[Book](`double(Year) = 3930`)
	require.ErrorIs(t, err, sql.ErrUnknownFunction)

	// Texts that are fine.
	for _, text := range []string{
		`is_old(Year)`, `NOT is_old(Year)`, `is_old(Year) AND InPrint`, `negate(InPrint)`, `genre_is(Genre) = TRUE`,
		`CASE WHEN is_old(Year) THEN FALSE ELSE TRUE END`, `name_of_type(Data) = 'string'`, `double(Rating) = 18`,
		// A field of an interface type may be anything, so it is found out when there is a book.
		`double(Data) = 1`, `is_old(Data)`, `shout(Flag) = 'TRUE!'`,
	} {
		_, err := sql.ParseWhere(text, withFunctions(fns))
		require.NoError(t, err, text)
	}
}

func TestFunctionsFailWhileEvaluating(t *testing.T) {
	fns := testFunctions()
	parse := func(text string) cond.Cond[Book] {
		t.Helper()

		c, err := sql.ParseWhere(text, withFunctions(fns))
		require.NoError(t, err, text)
		return c
	}

	// An error of the function is an error of the evaluation, and the function's own.
	for _, text := range []string{
		`boom(Year) = 1`,
		`NOT boom(Year) = 1`,
		`InPrint AND boom(Year) = 1`,
		`Year > 0 AND (Year = 0 OR boom(Year) > 0)`,
		`CASE WHEN InPrint THEN boom(Year) END = 1`,
		`boom(Year) IS NULL`,
		`length(Title) + boom(Year) > 0`,
		`coalesce(boom(Year), 1) = 1`,
	} {
		ok, err := parse(text).Match(dune)
		require.ErrorIs(t, err, sql.ErrEvaluation, text)
		require.ErrorIs(t, err, errBoom, text)
		require.False(t, ok, text)
	}
	// The error does not come where the rest of the text decides.
	ok, err := parse(`Year = 0 AND boom(Year) = 1`).Match(dune)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = parse(`Year = 1965 OR boom(Year) = 1`).Match(dune)
	require.NoError(t, err)
	require.True(t, ok)
	// A function of FuncN is called for no value, to tell what to do with it, so it can fail.
	_, err = parse(`boom(NULL) = 1`).Match(&Book{})
	require.ErrorIs(t, err, errBoom)
	// The ones of Func1, Func2 and Func3 are not.
	never := sql.Functions{}.
		Add(sql.Func1("never", func(string) string { panic("called") })).
		Add(sql.Func2("never2", func(int, string) string { panic("called") })).
		Add(sql.Func3("never3", func(int, int, string) string { panic("called") }))
	for _, text := range []string{`never(Subtitle) IS NULL`, `never2(Year, Subtitle) IS NULL`, `never3(Year, Year, Subtitle) IS NULL`, `never(NULL) IS NULL`} {
		c, err := sql.ParseWhere(text, withFunctions(never))
		require.NoError(t, err, text)
		require.True(t, match(t, c, &Book{Year: 1}), text)
	}
	_, err = sql.ParseWhere(`main.double(Year) = 1`, withFunctions(testFunctions()))
	require.ErrorIs(t, err, sql.ErrSyntax)

	// A function that returns what it is not declared to.
	for _, text := range []string{`lie(Year) = 'x'`, `many(Year) = 'a'`} {
		_, err := parse(text).Match(dune)
		require.ErrorIs(t, err, sql.ErrEvaluation, text)
	}
	ok, err = parse(`none_at_all(Year) IS NULL`).Match(dune)
	require.NoError(t, err)
	require.True(t, ok)

	// A value that the function cannot be given.
	for text, book := range map[string]*Book{
		`double(Price) = 1`:    dune,                  // 9.5 is not a whole number
		`plus(Year, Year) = 1`: dune,                  // 1965 does not fit a uint8
		`double(Data) = 1`:     {Data: "five"},        // text is not a number
		`shout(Data) = 'x'`:    {Data: int64(5)},      // a number is not text
		`size(Data) = 1`:       {Data: true},          // a boolean is not bytes
		`year_of(Data) = 1`:    {Data: true},          // a boolean is not a time
		`negate(Data)`:         {Data: "x"},           // text is not a boolean
		`double(Data) = 10`:    {Data: []string{"a"}}, // a list of text
	} {
		_, err := parse(text).Match(book)
		require.ErrorIs(t, err, sql.ErrEvaluation, text)
	}
	ok, err = parse(`double(Data) = 10`).Match(&Book{Data: int64(5)})
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = parse(`double(Data) = 10`).Match(&Book{Data: 5.0})
	require.NoError(t, err)
	require.True(t, ok)
}

func TestFunctionsFailWhileQuerying(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		lib := newLibrary(t, db, func(books *extstore.Store[int64, Book]) {
			index.On(books, "by_author", author)
		})
		p := sql.For(lib.books, withFunctions(testFunctions()))

		for _, where := range []string{
			`boom(Year) = 1`,                        // a scan
			`Author = 'Herbert' AND boom(Year) = 1`, // the residual of an index
		} {
			require.NoError(t, db.View(func(tx kv.Tx) error {
				it, err := p.Query(tx, where)
				require.NoError(t, err, where)
				defer it.Close()

				for ; it.Valid(); it.Next() {
				}
				require.ErrorIs(t, it.Err(), sql.ErrEvaluation, where)
				require.ErrorIs(t, it.Err(), errBoom, where)
				return nil
			}))
		}

		// A function that works is run on the books of the index.
		where := `Author = 'Herbert' AND double(Year) > 3900`
		c, err := p.Parse(where)
		require.NoError(t, err)
		requireQuery(t, db, lib, p, where, c)
		explain, err := p.Explain(where)
		require.NoError(t, err)
		require.Equal(t, `IndexLookup by_author: 1 range, then check [double("Year") > 3900]`, explain)
	})
}

func TestFunctionNamesAndSets(t *testing.T) {
	double := sql.Func1("double", func(n int) int { return n * 2 })
	require.Equal(t, "double", double.Name())
	require.Empty(t, double.Version())
	require.Equal(t, "2", double.WithVersion("2").Version())
	require.Empty(t, double.Version(), "a function is not changed by its version")

	// The names are not case sensitive, like the names of the functions that there always are.
	upper := sql.Functions{}.Add(sql.Func1("DoUbLe", func(n int) int { return n * 2 }))
	for _, text := range []string{`double(Year) = 3930`, `DOUBLE(Year) = 3930`, `DoUbLe(Year) = 3930`} {
		c, err := sql.ParseWhere(text, withFunctions(upper))
		require.NoError(t, err, text)
		require.True(t, match(t, c, dune), text)
	}
	require.PanicsWithValue(t, "sql: the set has a function double already", func() {
		upper.Add(double)
	})

	// A name that is taken, or is empty, or is of a function that there always is, or no function.
	taken := sql.Functions{}.Add(double)
	require.Panics(t, func() { taken.Add(double) })
	require.Panics(t, func() { taken.Add(sql.Func1("DOUBLE", func(n int) int { return n })) })
	require.Panics(t, func() { sql.Functions{}.Add(sql.Func1("", func(n int) int { return n })) })
	require.Panics(t, func() { sql.Functions{}.Add(sql.Function{}) })
	for _, name := range []string{"lower", "UPPER", "length", "abs", "substr", "instr", "trim", "ltrim", "rtrim", "round", "coalesce", "ifnull"} {
		require.Panics(t, func() { sql.Functions{}.Add(sql.Func1(name, func(s string) string { return s })) }, name)
	}

	// A set is a value: adding to it makes another one.
	base := sql.Functions{}.Add(double)
	more := base.Add(sql.Func1("triple", func(n int) int { return n * 3 }))
	_, err := sql.ParseWhere(`triple(Year) = 1`, withFunctions(base))
	require.ErrorIs(t, err, sql.ErrUnknownFunction)
	_, err = sql.ParseWhere(`triple(Year) = 5895 AND double(Year) = 3930`, withFunctions(more))
	require.NoError(t, err)
	_, err = sql.ParseWhere(`double(Year) = 1`, withFunctions(sql.Functions{}))
	require.ErrorIs(t, err, sql.ErrUnknownFunction)
	// The same name can be added to the sets that were not added to.
	_ = base.Add(sql.Func1("quad", func(n int) int { return n * 4 }))
	_ = base.Add(sql.Func1("quad", func(n int) int { return n * 5 }))

	// The parsers do not see the functions of each other.
	a := sql.ParseWhere[Book]
	c, err := a(`f(Year) = 1966`, withFunctions(sql.Functions{}.Add(sql.Func1("f", func(n int) int { return n + 1 }))))
	require.NoError(t, err)
	require.True(t, match(t, c, dune))
	c, err = a(`f(Year) = 1964`, withFunctions(sql.Functions{}.Add(sql.Func1("f", func(n int) int { return n - 1 }))))
	require.NoError(t, err)
	require.True(t, match(t, c, dune))
	_, err = a(`f(Year) = 1`)
	require.ErrorIs(t, err, sql.ErrUnknownFunction)

	books := extstore.New[int64, Book]("books")
	p1 := sql.For(books, withFunctions(sql.Functions{}.Add(sql.Func1("f", func(n int) int { return n + 1 }))))
	p2 := sql.For(books)
	_, err = p1.Parse(`f(Year) = 1`)
	require.NoError(t, err)
	_, err = p2.Parse(`f(Year) = 1`)
	require.ErrorIs(t, err, sql.ErrUnknownFunction)
}

func TestFunctionsPanicOnWhatHasNoType(t *testing.T) {
	// A Go type that the evaluator has no Type for.
	require.Panics(t, func() { sql.Func1("f", func(m map[string]int) int { return len(m) }) })
	require.Panics(t, func() { sql.Func1("f", func(n int) []string { return nil }) })
	require.Panics(t, func() { sql.Func1("f", func(n *int) int { return 0 }) })
	require.Panics(t, func() { sql.Func1("f", func(n int) struct{} { return struct{}{} }) })
	require.Panics(t, func() { sql.Func1("f", func(n int) error { return nil }) })
	require.Panics(t, func() { sql.Func2("f", func(n int, s []int) int { return 0 }) })
	require.Panics(t, func() { sql.Func3("f", func(a, b int, c complex128) int { return 0 }) })
	// Types that there are.
	require.NotPanics(t, func() {
		sql.Func3("f", func(a Genre, b []byte, c any) shortText { return "" })
		sql.Func1("f", func(t time.Time) uint64 { return 0 })
		sql.Func1("f", func(t float32) bool { return false })
	})

	ok := func([]any) (any, error) { return nil, nil }
	require.Panics(t, func() { sql.FuncN("f", []sql.Type{0}, false, sql.Bool, ok) })
	require.Panics(t, func() { sql.FuncN("f", []sql.Type{sql.Bool}, false, 99, ok) })
	require.Panics(t, func() { sql.FuncN("f", nil, true, sql.Bool, ok) })
	require.Panics(t, func() { sql.FuncN("f", []sql.Type{sql.Bool}, false, sql.Bool, nil) })
	for _, typ := range []sql.Type{sql.Bool, sql.Integer, sql.Real, sql.Number, sql.Text, sql.Bytes, sql.Time, sql.Any} {
		require.NotPanics(t, func() { sql.FuncN("f", []sql.Type{typ}, false, typ, ok) }, typ.String())
	}
	require.Equal(t, "Number", sql.Number.String())
	require.Contains(t, sql.Type(77).String(), "77")
}

func TestFunctionsInConcurrentUse(t *testing.T) {
	calls := 0
	var mu sync.Mutex
	fns := testFunctions().Add(sql.Func1("count", func(n int) int {
		mu.Lock()
		defer mu.Unlock()
		calls++
		return n
	}))
	books := extstore.New[int64, Book]("books")
	index.On(books, "by_author", author)
	p := sql.For(books, withFunctions(fns))

	shared, err := p.Parse(`double(Year) > 3000 AND shout(Author) LIKE 'H%' AND count(Year) > 0`)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			rng := rand.New(rand.NewPCG(uint64(g), 9))
			for i := range 200 {
				own, err := p.Parse(fmt.Sprintf(`plus(Year, %d) > 0 AND Author = 'Herbert'`, i%200))
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
			}
		}()
	}
	wg.Wait()
	require.NotZero(t, calls)
}

func TestFunctionsOfAStoreAreCheckedByType(t *testing.T) {
	// A function with a result that is a number is compared as a number.
	fns := sql.Functions{}.Add(sql.Func1("cents", func(f float64) int64 { return int64(f * 100) }))
	c, err := sql.ParseWhere(`cents(Price) = 950 AND cents(Price) BETWEEN 900 AND 1000 AND cents(Price) IN (1, 950)`, withFunctions(fns))
	require.NoError(t, err)
	require.True(t, match(t, c, dune))
	_, err = sql.ParseWhere(`cents(Price) = 'x'`, withFunctions(fns))
	require.ErrorIs(t, err, sql.ErrType)

	// The records of a query come from the iterator of the functions' results too.
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		lib := newLibrary(t, db, func(*extstore.Store[int64, Book]) {})
		p := sql.For(lib.books, withFunctions(fns))
		where := `cents(Price) >= 950 AND NOT is_cheap(Price)`
		_, err := p.Parse(where)
		require.ErrorIs(t, err, sql.ErrUnknownFunction)

		where = `cents(Price) >= 950`
		cnd, err := p.Parse(where)
		require.NoError(t, err)
		requireQuery(t, db, lib, p, where, cnd)
		require.NoError(t, db.View(func(tx kv.Tx) error {
			it, err := p.Query(tx, where)
			require.NoError(t, err)
			defer it.Close()
			keys, err := iterutil.Of[int64, *Book](it).Keys()
			require.NoError(t, err)
			require.NotEmpty(t, keys)
			return nil
		}))
	})
}

// A field that no struct has can be given to a function through WithFields.
func TestFunctionsWithFields(t *testing.T) {
	fns := sql.Functions{}.Add(sql.Func1("double", func(n int) int { return n * 2 }))
	decade := field.Of("Decade", func(b *Book) int { return b.Year / 10 * 10 })
	c, err := sql.ParseWhere(`double(Decade) = 3920`, withFunctions(fns), sql.WithFields[Book](decade))
	require.NoError(t, err)
	require.True(t, match(t, c, dune))
}
