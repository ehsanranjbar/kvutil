package ql_test

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/ehsanranjbar/kvutil/query/ql"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/ehsanranjbar/kvutil/store/record"
	"github.com/stretchr/testify/require"
)

type Genre string

type Publisher struct {
	Name string
	City string
}

type Book struct {
	Title     string
	Author    string
	Year      int
	Price     float64
	Rating    uint8
	Genre     Genre
	Tags      []string
	InPrint   bool
	Released  time.Time
	Subtitle  *string
	Publisher *Publisher
	Extra     map[string]string
	hidden    int
}

var (
	author  = field.Path[Book, string]("Author")
	year    = field.Path[Book, int]("Year")
	price   = field.Path[Book, float64]("Price")
	rating  = field.Path[Book, uint8]("Rating")
	genre   = field.Path[Book, Genre]("Genre")
	tags    = field.Path[Book, string]("Tags")
	inPrint = field.Path[Book, bool]("InPrint")
	city    = field.Path[Book, string]("Publisher.City")
	decade  = field.Of("Decade", func(b *Book) int { return b.Year / 10 * 10 })
)

func ptr[T any](v T) *T { return &v }

var dune = &Book{
	Title:     "Dune",
	Author:    "Herbert",
	Year:      1965,
	Price:     9.5,
	Rating:    9,
	Genre:     "scifi",
	Tags:      []string{"scifi", "classic"},
	InPrint:   true,
	Released:  time.Date(1965, 8, 1, 0, 0, 0, 0, time.UTC),
	Subtitle:  ptr("A novel"),
	Publisher: &Publisher{Name: "Chilton", City: "Philadelphia"},
	Extra:     map[string]string{"cover": "hard"},
}

func match(t *testing.T, c cond.Cond[Book], b *Book) bool {
	t.Helper()

	ok, err := c.Match(b)
	require.NoError(t, err, c.String())
	return ok
}

// randomBook returns a book whose fields have a few values each, so that conditions on them
// are satisfied by some books and not by others.
func randomBook(rng *rand.Rand) *Book {
	pick := func(values ...string) string { return values[rng.IntN(len(values))] }

	b := &Book{
		Title:    pick("Dune", "Emma", "Persuasion", "Children of Dune", ""),
		Author:   pick("Herbert", "Austen", "herbert", ""),
		Year:     []int{-50, 0, 1815, 1960, 1965, 1970, 1977}[rng.IntN(7)],
		Price:    []float64{0, 4.25, 9.5, 10, 25}[rng.IntN(5)],
		Rating:   uint8(rng.IntN(11)),
		Genre:    Genre(pick("scifi", "classic", "")),
		InPrint:  rng.IntN(2) == 0,
		Released: time.Date(1960+rng.IntN(20), time.Month(1+rng.IntN(12)), 1+rng.IntN(28), rng.IntN(24), 0, 0, 0, time.UTC),
	}
	for _, tag := range []string{"scifi", "classic", "new"} {
		if rng.IntN(3) == 0 {
			b.Tags = append(b.Tags, tag)
		}
	}
	if rng.IntN(2) == 0 {
		b.Subtitle = ptr(pick("A novel", ""))
	}
	if rng.IntN(3) > 0 {
		b.Publisher = &Publisher{Name: pick("Chilton", "Murray"), City: pick("Philadelphia", "London")}
	}
	return b
}

func has(tags []string, tag string) bool {
	for _, t := range tags {
		if t == tag {
			return true
		}
	}
	return false
}

// TestTranslation checks what the parts of a text become: the comparisons of a field with
// values are the conditions that the fields make, and the rest is evaluated.
func TestTranslation(t *testing.T) {
	p := ql.New[Book]()

	for text, want := range map[string]cond.Cond[Book]{
		`Author = "Herbert"`:  author.Eq("Herbert"),
		`Author == 'Herbert'`: author.Eq("Herbert"),
		`"Herbert" = Author`:  author.Eq("Herbert"),
		`Author != "Herbert"`: cond.Not[Book](author.Eq("Herbert")),
		`Author <> "Herbert"`: cond.Not[Book](author.Eq("Herbert")),
		`Year > 1965`:         year.Gt(1965),
		`Year >= 1965`:        year.Gte(1965),
		`Year < 1965`:         year.Lt(1965),
		`Year <= 1965`:        year.Lte(1965),
		// With the value on the left, the comparison is turned around.
		`1965 < Year`:  year.Gt(1965),
		`1965 <= Year`: year.Gte(1965),
		`1965 > Year`:  year.Lt(1965),
		`1965 >= Year`: year.Lte(1965),
		`Year > -50`:   year.Gt(-50),
		`Year = -50`:   year.Eq(-50),

		// The values are converted to the types of the fields.
		`Year = 1965.0`: year.Eq(1965),
		`Year = "1965"`: year.Eq(1965),
		`Price > 9`:     price.Gt(9),
		`Price <= 9.5`:  price.Lte(9.5),
		`Price = -0.5`:  price.Eq(-0.5),
		// A number that only an unsigned integer has, and its negative, which only a float has.
		`Price < 18446744073709551615`:  price.Lt(18446744073709551615),
		`Price > -18446744073709551615`: price.Gt(-18446744073709551615),
		`Rating = 9`:                    rating.Eq(9),
		`Genre = "scifi"`:               genre.Eq("scifi"),
		`InPrint = true`:                inPrint.Eq(true),
		`InPrint == FALSE`:              inPrint.Eq(false),
		`InPrint != true`:               cond.Not[Book](inPrint.Eq(true)),
		`InPrint = "false"`:             inPrint.Eq(false),

		// A field that is true or false is a condition by itself.
		`InPrint`:     inPrint.Eq(true),
		`NOT InPrint`: cond.Not[Book](inPrint.Eq(true)),
		`true`:        cond.And[Book](),
		`false`:       cond.Or[Book](),

		`Year IN (1965, 1970)`:            year.In(1965, 1970),
		`Year IN (1965)`:                  year.In(1965),
		`Year NOT IN (1965, "1970")`:      cond.Not[Book](year.In(1965, 1970)),
		`Author IN ("Herbert", 'Austen')`: author.In("Herbert", "Austen"),
		`Year BETWEEN 1960 AND 1970`:      year.Between(1960, 1970),
		`Year NOT BETWEEN 1960 AND 1970`:  cond.Not[Book](year.Between(1960, 1970)),
		`Price BETWEEN -1 AND 9.5`:        price.Between(-1, 9.5),

		// A field of several values is compared by each of them.
		`Tags = "scifi"`:           tags.Eq("scifi"),
		`"scifi" IN Tags`:          tags.Eq("scifi"),
		`Tags IN ("scifi", "new")`: tags.In("scifi", "new"),
		`Tags != "scifi"`:          cond.Not[Book](tags.Eq("scifi")),

		// The fields of fields, with and without quotes.
		`Publisher.City = "London"`:   city.Eq("London"),
		"`Publisher.City` = 'London'": city.Eq("London"),

		`Author = "Herbert" AND Year >= 1970`:     cond.And[Book](author.Eq("Herbert"), year.Gte(1970)),
		`Author = "Herbert" and Year >= 1970`:     cond.And[Book](author.Eq("Herbert"), year.Gte(1970)),
		`Author = "Herbert" && Year >= 1970`:      cond.And[Book](author.Eq("Herbert"), year.Gte(1970)),
		`Author = "Herbert" OR Year >= 1970`:      cond.Or[Book](author.Eq("Herbert"), year.Gte(1970)),
		`Author = "Herbert" || Year >= 1970`:      cond.Or[Book](author.Eq("Herbert"), year.Gte(1970)),
		`NOT (Author = "Herbert")`:                cond.Not[Book](author.Eq("Herbert")),
		`!(Author = "Herbert")`:                   cond.Not[Book](author.Eq("Herbert")),
		`NOT Author = "Herbert"`:                  cond.Not[Book](author.Eq("Herbert")),
		`(Author = "Herbert")`:                    author.Eq("Herbert"),
		`((Author = "Herbert") AND (Year > 0))`:   cond.And[Book](author.Eq("Herbert"), year.Gt(0)),
		`Author = "a" AND Year = 1 AND Price = 2`: cond.And[Book](author.Eq("a"), year.Eq(1), price.Eq(2)),
		`Author = "a" OR Year = 1 OR Price = 2`:   cond.Or[Book](author.Eq("a"), year.Eq(1), price.Eq(2)),
		`Author = "a" AND (Year = 1 AND Price = 2)`: cond.And[Book](
			author.Eq("a"), year.Eq(1), price.Eq(2)),
		// AND binds more than OR does.
		`Author = "a" OR Year = 1 AND Price = 2`: cond.Or[Book](
			author.Eq("a"), cond.And[Book](year.Eq(1), price.Eq(2))),
		`(Author = "a" OR Year = 1) AND Price = 2`: cond.And[Book](
			cond.Or[Book](author.Eq("a"), year.Eq(1)), price.Eq(2)),
		`NOT (Author = "a" OR NOT InPrint) AND true`: cond.And[Book](
			cond.Not(cond.Or[Book](author.Eq("a"), cond.Not[Book](inPrint.Eq(true)))), cond.And[Book]()),
	} {
		got, err := p.Parse(text)
		require.NoError(t, err, text)
		require.Equal(t, want.String(), got.String(), text)
		require.IsType(t, want, got, text)

		if want, ok := want.(*cond.Comparison[Book]); ok {
			got := got.(*cond.Comparison[Book])
			require.Equal(t, want.Path, got.Path, text)
			require.Equal(t, want.Codec, got.Codec, text)
			require.Equal(t, want.Op, got.Op, text)
			require.Equal(t, want.Values, got.Values, text)
			require.Equal(t, want.Low, got.Low, text)
			require.Equal(t, want.High, got.High, text)
		}

		rng := rand.New(rand.NewPCG(1, 2))
		for range 200 {
			b := randomBook(rng)
			require.Equal(t, match(t, want, b), match(t, got, b), "%s on %+v", text, b)
		}
	}
}

// TestAgreesWithGo checks every kind of condition against the same condition written in Go,
// on random books.
func TestAgreesWithGo(t *testing.T) {
	p := ql.New[Book]()
	y1970 := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

	matched := map[string]int{}
	for text, want := range map[string]func(b *Book) bool{
		// Comparisons of a field with values.
		`Author = "Herbert"`:              func(b *Book) bool { return b.Author == "Herbert" },
		`Author != ""`:                    func(b *Book) bool { return b.Author != "" },
		`Author > "A" AND Author < "I"`:   func(b *Book) bool { return b.Author > "A" && b.Author < "I" },
		`Year BETWEEN 1815 AND 1965`:      func(b *Book) bool { return b.Year >= 1815 && b.Year <= 1965 },
		`Year NOT BETWEEN 1815 AND 1965`:  func(b *Book) bool { return b.Year < 1815 || b.Year > 1965 },
		`Year < 0 OR Year IN (1970, 0)`:   func(b *Book) bool { return b.Year < 0 || b.Year == 1970 || b.Year == 0 },
		`Price >= 9.5 AND NOT InPrint`:    func(b *Book) bool { return b.Price >= 9.5 && !b.InPrint },
		`Rating > 5 AND Rating <= "9"`:    func(b *Book) bool { return b.Rating > 5 && b.Rating <= 9 },
		`Genre IN ("scifi", "")`:          func(b *Book) bool { return b.Genre == "scifi" || b.Genre == "" },
		`Tags = "scifi" AND Tags = "new"`: func(b *Book) bool { return has(b.Tags, "scifi") && has(b.Tags, "new") },
		`Tags != "scifi"`:                 func(b *Book) bool { return !has(b.Tags, "scifi") },
		`"new" IN Tags OR InPrint`:        func(b *Book) bool { return has(b.Tags, "new") || b.InPrint },
		`Released >= "1970-01-01"`:        func(b *Book) bool { return !b.Released.Before(y1970) },
		`Released < "1970-01-01T00:00:00Z" AND Released > "1965-06-15 12:00:00"`: func(b *Book) bool {
			return b.Released.Before(y1970) && b.Released.After(time.Date(1965, 6, 15, 12, 0, 0, 0, time.UTC))
		},

		// A field that a record has no value for is equal to nothing.
		`Subtitle = "A novel"`:  func(b *Book) bool { return b.Subtitle != nil && *b.Subtitle == "A novel" },
		`Subtitle != "A novel"`: func(b *Book) bool { return b.Subtitle == nil || *b.Subtitle != "A novel" },
		`Subtitle = ""`:         func(b *Book) bool { return b.Subtitle != nil && *b.Subtitle == "" },
		`Publisher.City = "London" OR Publisher.Name = "Chilton"`: func(b *Book) bool {
			return b.Publisher != nil && (b.Publisher.City == "London" || b.Publisher.Name == "Chilton")
		},
		`Publisher.City < "M"`: func(b *Book) bool { return b.Publisher != nil && b.Publisher.City < "M" },

		// What is evaluated on every record.
		`tolower(Author) = "herbert"`:        func(b *Book) bool { return strings.ToLower(b.Author) == "herbert" },
		`contains(Title, "Dune")`:            func(b *Book) bool { return strings.Contains(b.Title, "Dune") },
		`Title LIKE "%Dune"`:                 func(b *Book) bool { return strings.HasSuffix(b.Title, "Dune") },
		`Title NOT LIKE "Dune%"`:             func(b *Book) bool { return !strings.HasPrefix(b.Title, "Dune") },
		`hasprefix(Title, "Chi")`:            func(b *Book) bool { return strings.HasPrefix(b.Title, "Chi") },
		`len(Tags) >= 2`:                     func(b *Book) bool { return len(b.Tags) >= 2 },
		`Year + 10 > 1975`:                   func(b *Book) bool { return b.Year+10 > 1975 },
		`Price * 2 < Rating`:                 func(b *Book) bool { return b.Price*2 < float64(b.Rating) },
		`Year > Rating`:                      func(b *Book) bool { return b.Year > int(b.Rating) },
		`Year BETWEEN Rating AND 1965`:       func(b *Book) bool { return b.Year >= int(b.Rating) && b.Year <= 1965 },
		`Rating BETWEEN 3 AND Price`:         func(b *Book) bool { return b.Rating >= 3 && float64(b.Rating) <= b.Price },
		`Rating NOT BETWEEN Price AND 9`:     func(b *Book) bool { return !(float64(b.Rating) >= b.Price && b.Rating <= 9) },
		`EXISTS Subtitle`:                    func(b *Book) bool { return b.Subtitle != nil },
		`NOT EXISTS Publisher.City`:          func(b *Book) bool { return b.Publisher == nil },
		`Tags intersects ("new", "classic")`: func(b *Book) bool { return has(b.Tags, "new") || has(b.Tags, "classic") },
		`yy(Released) >= 70`:                 func(b *Book) bool { return b.Released.Year() >= 1970 },
		`tolower(Genre) = "scifi"`:           func(b *Book) bool { return b.Genre == "scifi" },

		// Both kinds in one text.
		`Author = "Herbert" AND (contains(Title, "Dune") OR Year < 1900) AND NOT Tags = "new"`: func(b *Book) bool {
			return b.Author == "Herbert" && (strings.Contains(b.Title, "Dune") || b.Year < 1900) && !has(b.Tags, "new")
		},
		`NOT (len(Tags) = 0 OR Price > 9.5) OR Genre = "classic" AND toint(Rating) / 2 >= 4`: func(b *Book) bool {
			return !(len(b.Tags) == 0 || b.Price > 9.5) || b.Genre == "classic" && float64(b.Rating)/2 >= 4
		},
	} {
		c, err := p.Parse(text)
		require.NoError(t, err, text)

		rng := rand.New(rand.NewPCG(3, 4))
		for range 400 {
			b := randomBook(rng)
			got, err := c.Match(b)
			require.NoError(t, err, "%s on %+v", text, b)
			require.Equal(t, want(b), got, "%s (%s) on %+v", text, c, b)
			if got {
				matched[text]++
			}
		}
		// A condition that all the books satisfy, or none, checks little.
		require.NotZero(t, matched[text], text)
		require.NotEqual(t, 400, matched[text], text)
	}
}

func TestErrors(t *testing.T) {
	p := ql.New[Book]()

	for text, want := range map[string]error{
		// Not written as a condition.
		``:                          ql.ErrSyntax,
		`Year =`:                    ql.ErrSyntax,
		`Year = 1965 AND`:           ql.ErrSyntax,
		`(Year = 1965`:              ql.ErrSyntax,
		`Year = 1965) AND Year = 3`: ql.ErrSyntax,
		`Year IN (1965`:             ql.ErrSyntax,
		`Year BETWEEN 1 AND`:        ql.ErrSyntax,
		`Author = "Herbert`:         ql.ErrSyntax,
		`Year = 1965 Year = 1`:      ql.ErrSyntax,
		`(1965, 1970)`:              ql.ErrSyntax,
		`Year = 1965 # no`:          ql.ErrSyntax,
		// One thing too many, and several.
		`Year = 1965 Author`: ql.ErrSyntax,
		`Year = 1965 5`:      ql.ErrSyntax,
		`InPrint InPrint`:    ql.ErrSyntax,
		`Year = 1965)`:       ql.ErrSyntax,

		// A field that the books do not have. A text without quotes is the name of a field.
		`Athor = "Herbert"`:                         ql.ErrUnknownField,
		`Author = Herbert`:                          ql.ErrUnknownField,
		`Publisher.Country = "US"`:                  ql.ErrUnknownField,
		`Title.Length > 3`:                          ql.ErrUnknownField,
		`hidden = 1`:                                ql.ErrUnknownField,
		`year = 1965`:                               ql.ErrUnknownField,
		`Missing`:                                   ql.ErrUnknownField,
		`EXISTS Missing`:                            ql.ErrUnknownField,
		`tolower(Missing) = "x"`:                    ql.ErrUnknownField,
		`Year > Missing`:                            ql.ErrUnknownField,
		`Year + Missing > 3`:                        ql.ErrUnknownField,
		`Missing IN (1, 2)`:                         ql.ErrUnknownField,
		`"a" IN Missing`:                            ql.ErrUnknownField,
		`Missing BETWEEN 1 AND 2`:                   ql.ErrUnknownField,
		`Year BETWEEN Missing AND 2`:                ql.ErrUnknownField,
		`Year = 1 OR (InPrint AND NOT Missing = 2)`: ql.ErrUnknownField,
		`contains(Title, tolower(Missing))`:         ql.ErrUnknownField,

		// A function that there is not.
		`nosuchfunc(Author) = "x"`:                ql.ErrUnknownFunction,
		`nosuchfunc(Author)`:                      ql.ErrUnknownFunction,
		`Year = 1 AND tolower(nope(Title)) = "x"`: ql.ErrUnknownFunction,

		// A value that the field cannot have.
		`Year = "abc"`:                  ql.ErrType,
		`Year > "abc"`:                  ql.ErrType,
		`Year = 1965.5`:                 ql.ErrType,
		`Year = true`:                   ql.ErrType,
		`Year IN (1965, "abc")`:         ql.ErrType,
		`Year BETWEEN 1 AND "abc"`:      ql.ErrType,
		`Rating = 300`:                  ql.ErrType,
		`Rating = -1`:                   ql.ErrType,
		`Author = 5`:                    ql.ErrType,
		`Author = true`:                 ql.ErrType,
		`InPrint = 1`:                   ql.ErrType,
		`InPrint = "yes"`:               ql.ErrType,
		`Released > "yesterday"`:        ql.ErrType,
		`Released > 1965`:               ql.ErrType,
		`Tags = 5`:                      ql.ErrType,
		`5 IN Tags`:                     ql.ErrType,
		`Year = 1965 AND Price < "low"`: ql.ErrType,

		// A field that has no values to compare, and NULL.
		`Extra = "x"`:     ql.ErrType,
		`Publisher = "x"`: ql.ErrType,
		`Extra IN ("a")`:  ql.ErrType,
		`Author = NULL`:   ql.ErrType,
		`NULL != Author`:  ql.ErrType,

		// What is not true or false where a condition is expected.
		`Author`:                   ql.ErrType,
		`Year`:                     ql.ErrType,
		`"text"`:                   ql.ErrType,
		`5`:                        ql.ErrType,
		`Year + 5`:                 ql.ErrType,
		`Year = 1965 AND Author`:   ql.ErrType,
		`NOT Year`:                 ql.ErrType,
		`Year = 1965 OR Price * 2`: ql.ErrType,
		`[1965, 1970]`:             ql.ErrType,
		// A negative number is only read as one on the right of a comparison.
		`Year = 1 AND -5 < Year`: ql.ErrType,
	} {
		// Every text is checked, whatever the others give.
		c, err := p.Parse(text)
		if !errors.Is(err, want) || c != nil {
			t.Errorf("%q gave %v and the error %v, not %v", text, c, err, want)
		}
	}

	// What is wrong is named.
	_, err := p.Parse(`Athor = "Herbert"`)
	require.ErrorContains(t, err, `"Athor"`)
	_, err = p.Parse(`nosuchfunc(Author) = "x"`)
	require.ErrorContains(t, err, `"nosuchfunc"`)
	_, err = p.Parse(`Year = "abc"`)
	require.ErrorContains(t, err, "Year")
	require.ErrorContains(t, err, "abc")
}

func TestEvaluation(t *testing.T) {
	p := ql.New[Book]()
	parse := func(text string) cond.Cond[Book] {
		t.Helper()

		c, err := p.Parse(text)
		require.NoError(t, err, text)
		return c
	}

	// What the evaluator has nothing to say of is not true, and its opposite is.
	for _, text := range []string{
		`Year / 0 > 1`,
		`Year > Author`,
		`tolower(Subtitle) = "a novel"`,
		`Publisher.City LIKE "Phil%"`,
		`contains(Publisher.Name, "Chil")`,
	} {
		require.False(t, match(t, parse(text), &Book{Year: 1965, Author: "Herbert"}), text)
		require.True(t, match(t, parse("NOT ("+text+")"), &Book{Year: 1965, Author: "Herbert"}), text)
	}
	require.True(t, match(t, parse(`tolower(Subtitle) = "a novel"`), dune))
	require.True(t, match(t, parse(`Publisher.City LIKE "Phil%"`), dune))

	// A function that does not return true or false is not a condition, which only shows on
	// a record.
	_, err := parse(`tolower(Author)`).Match(dune)
	require.ErrorIs(t, err, ql.ErrEvaluation)
	_, err = parse(`Year = 1965 AND len(Tags)`).Match(dune)
	require.ErrorIs(t, err, ql.ErrEvaluation)

	// The fields of every kind as the evaluator reads them.
	for _, text := range []string{
		`tolower(Genre) = "scifi"`,
		`Rating * 2 = 18`,
		`len(Extra) = 1`,
		`Extra.cover = "hard"`,
		`"classic" IN Tags AND len(Tags) = 2`,
		`yy(Released) = 65`,
		`Released > "1965-01-01" AND Released < todate("1966-01-01")`,
		`EXISTS Extra AND EXISTS Publisher AND EXISTS Tags AND EXISTS Title`,
		`Price + Rating = 18.5`,
	} {
		if strings.HasPrefix(text, "Extra.") {
			// The keys of a map are not fields.
			_, err := p.Parse(text)
			require.ErrorIs(t, err, ql.ErrUnknownField)
			continue
		}
		require.True(t, match(t, parse(text), dune), text)
	}

	// A record has a value for a field or it has not, whatever the value is.
	empty := &Book{Subtitle: ptr("")}
	require.True(t, match(t, parse(`EXISTS Subtitle AND EXISTS Title AND EXISTS Year`), empty))
	require.False(t, match(t, parse(`EXISTS Tags OR EXISTS Publisher OR EXISTS Publisher.City OR EXISTS Extra`), empty))
	require.Equal(t, "(EXISTS Subtitle) and (not (EXISTS Publisher.City))",
		parse(`EXISTS Subtitle AND NOT EXISTS Publisher.City`).String())

	// The text of what is evaluated is the text as it was written.
	require.Equal(t, `(Author = "Herbert") and (contains(Title, "Dune"))`,
		parse(`Author = 'Herbert' AND contains(Title, "Dune")`).String())
}

// TestStore checks that what is parsed for a store is written with the fields of its indexes,
// so that they serve it.
func TestStore(t *testing.T) {
	books := extstore.New[int64, Book]("books")
	byAuthorYear := index.Composite(books, "by_author_year", field.List(author, year.Desc()))
	byTag := index.On(books, "by_tag", tags)
	// A field that is computed can only be found through an index, or be given.
	inPrintByDecade := index.On(books, "in_print_by_decade", decade, index.Where(inPrint.Eq(true)))
	// A field whose codec is not the one of its type: the values of a text are encoded with it.
	cents := field.Of("Price", func(b *Book) int64 { return int64(b.Price * 100) })
	byCents := index.On(books, "by_cents", cents)

	db := memkv.New()
	require.NoError(t, books.Check(db))

	rng := rand.New(rand.NewPCG(5, 6))
	all := map[int64]*Book{}
	require.NoError(t, db.Update(func(tx kv.Tx) error {
		store := books.Instantiate(tx)
		for id := int64(1); id <= 300; id++ {
			all[id] = randomBook(rng)
			if err := store.Set(id, all[id]); err != nil {
				return err
			}
		}
		return nil
	}))

	p := ql.For(books)

	// expect returns the keys of the books that satisfy a condition, by checking each of them.
	expect := func(c cond.Cond[Book], also func(*Book) bool) []int64 {
		var keys []int64
		for id := int64(1); id <= 300; id++ {
			if match(t, c, all[id]) && (also == nil || also(all[id])) {
				keys = append(keys, id)
			}
		}
		return keys
	}

	type lookup interface {
		Lookup(conds ...cond.Cond[Book]) (*index.Iterator[int64, Book], error)
		Plan(conds ...cond.Cond[Book]) ([]index.Bounds, []cond.Cond[Book], error)
	}
	require.NoError(t, db.View(func(tx kv.Tx) error {
		for _, test := range []struct {
			text     string
			x        lookup
			ranges   int
			residual string
			// What the index itself is limited to.
			also func(*Book) bool
		}{
			{`Author = "Herbert"`, byAuthorYear.Instantiate(tx), 1, "", nil},
			{`Author = "Herbert" AND Year >= 1965`, byAuthorYear.Instantiate(tx), 1, "", nil},
			{`Year BETWEEN 1960 AND 1970 AND Author IN ("Herbert", "Austen")`, byAuthorYear.Instantiate(tx), 2, "", nil},
			{`Author = "Herbert" AND Year > 1900 AND contains(Title, "Dune") AND NOT InPrint`,
				byAuthorYear.Instantiate(tx), 1, `contains(Title, "Dune"); not (InPrint = true)`, nil},
			// A condition on the second field alone is checked on every book of the index.
			{`Year = 1965`, byAuthorYear.Instantiate(tx), 1, "Year = 1965", nil},
			{`Author = "Herbert" OR Year = 1815`, byAuthorYear.Instantiate(tx), 1,
				`(Author = "Herbert") or (Year = 1815)`, nil},
			{`"scifi" IN Tags AND Price > 5`, byTag.Instantiate(tx), 1, "Price > 5", nil},
			{`Tags IN ("scifi", "new")`, byTag.Instantiate(tx), 2, "", nil},
			{`Decade = 1960`, inPrintByDecade.Instantiate(tx), 1, "", func(b *Book) bool { return b.InPrint }},
			{`Decade >= "1970" AND tolower(Author) = "herbert"`, inPrintByDecade.Instantiate(tx), 1,
				`tolower(Author) = "herbert"`, func(b *Book) bool { return b.InPrint }},
		} {
			c, err := p.Parse(test.text)
			require.NoError(t, err, test.text)

			ranges, residual, err := test.x.Plan(c)
			require.NoError(t, err, test.text)
			require.Len(t, ranges, test.ranges, test.text)
			var texts []string
			for _, r := range residual {
				texts = append(texts, r.String())
			}
			require.Equal(t, test.residual, strings.Join(texts, "; "), test.text)

			it, err := test.x.Lookup(c)
			require.NoError(t, err, test.text)
			keys, err := iterutil.Of[int64, *Book](it).Keys()
			require.NoError(t, err, test.text)
			slices.Sort(keys)
			require.Equal(t, expect(c, test.also), keys, test.text)
			require.NotEmpty(t, keys, test.text)
		}
		return nil
	}))

	t.Run("FieldsOfTheIndexes", func(t *testing.T) {
		// Price is the field of the index that is defined on it, with the type that it has
		// there: whole cents. It is not the field of the struct.
		c, err := p.Parse(`Price = 950`)
		require.NoError(t, err)
		require.Equal(t, cents.Eq(950).Values, c.(*cond.Comparison[Book]).Values)
		require.Equal(t, "int64", c.(*cond.Comparison[Book]).Codec)
		require.True(t, match(t, c, dune))
		require.NoError(t, db.View(func(tx kv.Tx) error {
			_, residual, err := byCents.Instantiate(tx).Plan(c)
			require.NoError(t, err)
			require.Empty(t, residual)
			return nil
		}))
		_, err = p.Parse(`Price = 9.5`)
		require.ErrorIs(t, err, ql.ErrType)

		// A parser that is not of the store has the field of the struct.
		c, err = ql.New[Book]().Parse(`Price = 9.5`)
		require.NoError(t, err)
		require.Equal(t, "float64", c.(*cond.Comparison[Book]).Codec)
		// And it does not know of the field that is computed, unless it is given.
		_, err = ql.New[Book]().Parse(`Decade = 1960`)
		require.ErrorIs(t, err, ql.ErrUnknownField)
		c, err = ql.New(ql.WithFields[Book](decade)).Parse(`Decade = 1960 AND tostring(Decade) = "1960"`)
		require.NoError(t, err)
		require.True(t, match(t, c, dune))
		require.False(t, match(t, c, &Book{Year: 1970}))

		// A field that is given comes before the one of an index.
		c, err = ql.For(books, ql.WithFields[Book](price)).Parse(`Price = 9.5`)
		require.NoError(t, err)
		require.Equal(t, "float64", c.(*cond.Comparison[Book]).Codec)
	})
}

func TestConcurrentUse(t *testing.T) {
	p := ql.New[Book]()
	shared, err := p.Parse(`Author = "Herbert" AND (tolower(Title) LIKE "%dune" OR len(Tags) > 1)`)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			rng := rand.New(rand.NewPCG(uint64(g), 7))
			for i := range 200 {
				own, err := p.Parse(fmt.Sprintf(`Year = %d AND Publisher.City = "London" AND contains(Title, "e")`, 1960+i%20))
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
}

func TestLargeNumbers(t *testing.T) {
	type Counter struct {
		N uint64
		I int64
	}
	n := field.Path[Counter, uint64]("N")
	i := field.Path[Counter, int64]("I")
	p := ql.New[Counter]()

	// The numbers that a float does not have exactly are not read as floats.
	for text, want := range map[string]*cond.Comparison[Counter]{
		`N = 18446744073709551615`:  n.Eq(math.MaxUint64),
		`N >= 9223372036854775808`:  n.Gte(1 << 63),
		`I = 9223372036854775807`:   i.Eq(math.MaxInt64),
		`I = -9223372036854775807`:  i.Eq(-math.MaxInt64),
		`I IN (9007199254740993)`:   i.In(1<<53 + 1),
		`N IN (9007199254740993.0)`: n.In(1 << 53),
	} {
		got, err := p.Parse(text)
		require.NoError(t, err, text)
		require.Equal(t, want.Values, got.(*cond.Comparison[Counter]).Values, text)
		require.Equal(t, want.Low, got.(*cond.Comparison[Counter]).Low, text)
	}

	for _, text := range []string{`N = 18446744073709551616`, `I = 9223372036854775808`, `N = -1`} {
		_, err := p.Parse(text)
		require.ErrorIs(t, err, ql.ErrType, text)
	}
}

// A parser is made for the stores of both kinds that indexes can be defined on.
var (
	_ ql.Store[int64, Book] = (*extstore.Store[int64, Book])(nil)
	_ ql.Store[int64, Book] = (*record.Store[int64, Book])(nil)
)
