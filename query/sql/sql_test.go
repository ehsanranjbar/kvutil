package sql_test

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/query/sql"
)

type Genre string

type Publisher struct {
	Name string
	City string
}

type Book struct {
	Code      string
	Title     string
	Author    string
	Year      int
	Price     float64
	Rating    uint8
	Genre     Genre
	Tags      []string
	InPrint   bool
	Released  time.Time
	Reprinted time.Time
	Subtitle  *string
	Publisher *Publisher
	Extra     map[string]string
	Data      any
	Flag      any
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
	Reprinted: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
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
		Title:    pick("Dune", "Emma", "Persuasion", "Children of Dune", "", "  padded ", "DUNE", "50% off"),
		Author:   pick("Herbert", "Austen", "herbert", ""),
		Year:     []int{-50, 0, 1815, 1960, 1965, 1970, 1977}[rng.IntN(7)],
		Price:    []float64{0, 4.25, 9.5, 10, 25}[rng.IntN(5)],
		Rating:   uint8(rng.IntN(11)),
		Genre:    Genre(pick("scifi", "classic", "")),
		InPrint:  rng.IntN(2) == 0,
		Released: time.Date(1960+rng.IntN(20), time.Month(1+rng.IntN(12)), 1+rng.IntN(28), rng.IntN(24), 0, 0, 0, time.UTC),
	}
	b.Reprinted = b.Released.Add(time.Duration(rng.IntN(2000)-1000) * time.Hour)
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
	// Fields of an interface type have no key codec: they have text, true and false.
	switch rng.IntN(4) {
	case 0:
		b.Data = "x"
	case 1:
		b.Data = true
	}
	switch rng.IntN(3) {
	case 0:
		b.Flag = true
	case 1:
		b.Flag = false
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
	for text, want := range map[string]cond.Cond[Book]{
		`Author = 'Herbert'`:  author.Eq("Herbert"),
		`Author == 'Herbert'`: author.Eq("Herbert"),
		`'Herbert' = Author`:  author.Eq("Herbert"),
		`Author != 'Herbert'`: cond.Not[Book](author.Eq("Herbert")),
		`Author <> 'Herbert'`: cond.Not[Book](author.Eq("Herbert")),
		`Author = ''`:         author.Eq(""),
		`Author = 'O''Hara'`:  author.Eq("O'Hara"),
		`Year > 1965`:         year.Gt(1965),
		`Year >= 1965`:        year.Gte(1965),
		`Year < 1965`:         year.Lt(1965),
		`Year <= 1965`:        year.Lte(1965),
		// With the value on the left, the comparison is turned around.
		`1965 < Year`:  year.Gt(1965),
		`1965 <= Year`: year.Gte(1965),
		`1965 > Year`:  year.Lt(1965),
		`1965 >= Year`: year.Lte(1965),
		`-50 < Year`:   year.Gt(-50),
		`Year > -50`:   year.Gt(-50),
		`Year = -50`:   year.Eq(-50),
		`Year = +5`:    year.Eq(5),
		`Year = - -5`:  year.Eq(5),
		`Year = (5)`:   year.Eq(5),
		`Year = 0x7B`:  year.Eq(123),

		// The values are converted to the types of the fields.
		`Year = 1965.0`: year.Eq(1965),
		`Year = '1965'`: year.Eq(1965),
		`Price > 9`:     price.Gt(9),
		`Price <= 9.5`:  price.Lte(9.5),
		`Price = -0.5`:  price.Eq(-0.5),
		`Price = .5`:    price.Eq(0.5),
		`Price = 1e1`:   price.Eq(10),
		// A number that only an unsigned integer has, and its negative, which only a float has.
		`Price < 18446744073709551615`:  price.Lt(18446744073709551615),
		`Price > -18446744073709551615`: price.Gt(-18446744073709551615),
		`Rating = 9`:                    rating.Eq(9),
		`Genre = 'scifi'`:               genre.Eq("scifi"),
		`InPrint = TRUE`:                inPrint.Eq(true),
		`InPrint = true`:                inPrint.Eq(true),
		`InPrint == FALSE`:              inPrint.Eq(false),
		`InPrint != TRUE`:               cond.Not[Book](inPrint.Eq(true)),
		`InPrint = 'false'`:             inPrint.Eq(false),

		// A field that is true or false is a condition by itself.
		`InPrint`:     inPrint.Eq(true),
		`(InPrint)`:   inPrint.Eq(true),
		`NOT InPrint`: cond.Not[Book](inPrint.Eq(true)),
		`TRUE`:        cond.And[Book](),
		`FALSE`:       cond.Or[Book](),

		// IS with a value is = with it.
		`Year IS 1965`:     year.Eq(1965),
		`Year IS NOT 1965`: cond.Not[Book](year.Eq(1965)),

		`Year IN (1965, 1970)`:            year.In(1965, 1970),
		`Year IN (1965)`:                  year.In(1965),
		`Year NOT IN (1965, '1970')`:      cond.Not[Book](year.In(1965, 1970)),
		`Author IN ('Herbert', 'Austen')`: author.In("Herbert", "Austen"),
		`Year IN ()`:                      cond.Or[Book](),
		`Year NOT IN ()`:                  cond.And[Book](),
		`Year BETWEEN 1960 AND 1970`:      year.Between(1960, 1970),
		`Year NOT BETWEEN 1960 AND 1970`:  cond.Not[Book](year.Between(1960, 1970)),
		`Price BETWEEN -1 AND 9.5`:        price.Between(-1, 9.5),

		// A field of several values is compared by each of them.
		`Tags = 'scifi'`:           tags.Eq("scifi"),
		`'scifi' IN Tags`:          tags.Eq("scifi"),
		`'scifi' NOT IN Tags`:      cond.Not[Book](tags.Eq("scifi")),
		`Tags IN ('scifi', 'new')`: tags.In("scifi", "new"),
		`Tags != 'scifi'`:          cond.Not[Book](tags.Eq("scifi")),
		`Tags BETWEEN 'a' AND 'd'`: tags.Between("a", "d"),

		// The fields of fields, written in every way.
		`Publisher.City = 'London'`:     city.Eq("London"),
		`"Publisher"."City" = 'London'`: city.Eq("London"),
		`"Publisher.City" = 'London'`:   city.Eq("London"),
		"`Publisher`.`City` = 'London'": city.Eq("London"),
		`[Publisher].[City] = 'London'`: city.Eq("London"),
		`"Author" = 'Herbert'`:          author.Eq("Herbert"),
		"`Author` = 'Herbert'":          author.Eq("Herbert"),
		`[Author] = 'Herbert'`:          author.Eq("Herbert"),
		`Publisher.City < 'M'`:          city.Lt("M"),
		`'London' = "Publisher"."City"`: city.Eq("London"),

		`Author = 'Herbert' AND Year >= 1970`:     cond.And[Book](author.Eq("Herbert"), year.Gte(1970)),
		`Author = 'Herbert' and Year >= 1970`:     cond.And[Book](author.Eq("Herbert"), year.Gte(1970)),
		`Author = 'Herbert' OR Year >= 1970`:      cond.Or[Book](author.Eq("Herbert"), year.Gte(1970)),
		`NOT (Author = 'Herbert')`:                cond.Not[Book](author.Eq("Herbert")),
		`NOT Author = 'Herbert'`:                  cond.Not[Book](author.Eq("Herbert")),
		`(Author = 'Herbert')`:                    author.Eq("Herbert"),
		`((Author = 'Herbert') AND (Year > 0))`:   cond.And[Book](author.Eq("Herbert"), year.Gt(0)),
		`Author = 'a' AND Year = 1 AND Price = 2`: cond.And[Book](author.Eq("a"), year.Eq(1), price.Eq(2)),
		`Author = 'a' OR Year = 1 OR Price = 2`:   cond.Or[Book](author.Eq("a"), year.Eq(1), price.Eq(2)),
		`Author = 'a' AND (Year = 1 AND Price = 2)`: cond.And[Book](
			author.Eq("a"), year.Eq(1), price.Eq(2)),
		// AND binds more than OR does.
		`Author = 'a' OR Year = 1 AND Price = 2`: cond.Or[Book](
			author.Eq("a"), cond.And[Book](year.Eq(1), price.Eq(2))),
		`(Author = 'a' OR Year = 1) AND Price = 2`: cond.And[Book](
			cond.Or[Book](author.Eq("a"), year.Eq(1)), price.Eq(2)),
		`NOT (Author = 'a' OR NOT InPrint) AND TRUE`: cond.And[Book](
			cond.Not(cond.Or[Book](author.Eq("a"), cond.Not[Book](inPrint.Eq(true)))), cond.And[Book]()),
	} {
		got, err := sql.ParseWhere[Book](text)
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

// TestEvaluatedText checks what the evaluated parts of a text are printed as.
func TestEvaluatedText(t *testing.T) {
	parse := func(text string) cond.Cond[Book] {
		t.Helper()

		c, err := sql.ParseWhere[Book](text)
		require.NoError(t, err, text)
		return c
	}

	require.Equal(t, `"Title" LIKE '%Dune'`, parse(`Title  like  '%Dune'`).String())
	require.Equal(t, `lower("Author") = 'herbert'`, parse(`lower(Author) == 'herbert'`).String())
	require.Equal(t, `(Author = "Herbert") and ("Title" GLOB 'D*')`,
		parse(`Author = 'Herbert' AND Title GLOB 'D*'`).String())
	require.Equal(t, `"Publisher"."City" IS NULL`, parse(`Publisher.City IS NULL`).String())
	require.Equal(t, `"Subtitle" IS NOT NULL`, parse(`Subtitle NOTNULL`).String())
	require.Equal(t, `("Subtitle" IS NULL) and (not ("Publisher"."City" IS NULL))`,
		parse(`Subtitle IS NULL AND NOT Publisher.City IS NULL`).String())
}
