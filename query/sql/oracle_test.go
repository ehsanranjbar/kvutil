package sql_test

import (
	"fmt"
	"math"
	"math/rand/v2"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/query/sql"
)

// TestAgreesWithGo checks every kind of condition against the same condition written in Go,
// on random books.
func TestAgreesWithGo(t *testing.T) {
	y1970 := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	lower := strings.ToLower

	matched := map[string]int{}
	for text, want := range map[string]func(b *Book) bool{
		// Comparisons of a field with values.
		`Author = 'Herbert'`:              func(b *Book) bool { return b.Author == "Herbert" },
		`Author != ''`:                    func(b *Book) bool { return b.Author != "" },
		`Author > 'A' AND Author < 'I'`:   func(b *Book) bool { return b.Author > "A" && b.Author < "I" },
		`Year BETWEEN 1815 AND 1965`:      func(b *Book) bool { return b.Year >= 1815 && b.Year <= 1965 },
		`Year NOT BETWEEN 1815 AND 1965`:  func(b *Book) bool { return b.Year < 1815 || b.Year > 1965 },
		`Year < 0 OR Year IN (1970, 0)`:   func(b *Book) bool { return b.Year < 0 || b.Year == 1970 || b.Year == 0 },
		`Price >= 9.5 AND NOT InPrint`:    func(b *Book) bool { return b.Price >= 9.5 && !b.InPrint },
		`Rating > 5 AND Rating <= '9'`:    func(b *Book) bool { return b.Rating > 5 && b.Rating <= 9 },
		`Genre IN ('scifi', '')`:          func(b *Book) bool { return b.Genre == "scifi" || b.Genre == "" },
		`Tags = 'scifi' AND Tags = 'new'`: func(b *Book) bool { return has(b.Tags, "scifi") && has(b.Tags, "new") },
		`Tags != 'scifi'`:                 func(b *Book) bool { return !has(b.Tags, "scifi") },
		`'new' IN Tags OR InPrint`:        func(b *Book) bool { return has(b.Tags, "new") || b.InPrint },
		`'new' NOT IN Tags`:               func(b *Book) bool { return !has(b.Tags, "new") },
		`Released >= '1970-01-01'`:        func(b *Book) bool { return !b.Released.Before(y1970) },
		`Released < '1970-01-01T00:00:00Z' AND Released > '1965-06-15 12:00:00'`: func(b *Book) bool {
			return b.Released.Before(y1970) && b.Released.After(time.Date(1965, 6, 15, 12, 0, 0, 0, time.UTC))
		},
		`Data IS NULL`:     func(b *Book) bool { return b.Data == nil },
		`Data IS NOT NULL`: func(b *Book) bool { return b.Data != nil },

		// A field that a record has no value for is equal to nothing.
		`Subtitle = 'A novel'`:  func(b *Book) bool { return b.Subtitle != nil && *b.Subtitle == "A novel" },
		`Subtitle != 'A novel'`: func(b *Book) bool { return b.Subtitle == nil || *b.Subtitle != "A novel" },
		`Subtitle = ''`:         func(b *Book) bool { return b.Subtitle != nil && *b.Subtitle == "" },
		`Publisher.City = 'London' OR Publisher.Name = 'Chilton'`: func(b *Book) bool {
			return b.Publisher != nil && (b.Publisher.City == "London" || b.Publisher.Name == "Chilton")
		},
		`Publisher.City < 'M'`: func(b *Book) bool { return b.Publisher != nil && b.Publisher.City < "M" },

		// Whether there is a value.
		`Subtitle IS NULL`:           func(b *Book) bool { return b.Subtitle == nil },
		`Subtitle IS NOT NULL`:       func(b *Book) bool { return b.Subtitle != nil },
		`Subtitle NOTNULL`:           func(b *Book) bool { return b.Subtitle != nil },
		`Publisher.City IS NULL`:     func(b *Book) bool { return b.Publisher == nil },
		`NOT Publisher.City IS NULL`: func(b *Book) bool { return b.Publisher != nil },
		`Tags IS NULL`:               func(b *Book) bool { return len(b.Tags) == 0 },
		`Tags IS NOT NULL`:           func(b *Book) bool { return len(b.Tags) > 0 },
		`Title IS NOT NULL`:          func(b *Book) bool { return true },
		`Title IS NULL OR Year = 0`:  func(b *Book) bool { return b.Year == 0 },
		`Publisher IS NULL`:          func(b *Book) bool { return b.Publisher == nil },
		`Extra IS NULL`:              func(b *Book) bool { return b.Extra == nil },
		`lower(Subtitle) IS NULL`:    func(b *Book) bool { return b.Subtitle == nil },
		`Year + NULL IS NULL`:        func(b *Book) bool { return true },

		// LIKE and GLOB.
		`Title LIKE '%dune'`:            func(b *Book) bool { return strings.HasSuffix(lower(b.Title), "dune") },
		`Title LIKE 'D_ne'`:             func(b *Book) bool { return lower(b.Title) == "dune" },
		`Title NOT LIKE 'dune%'`:        func(b *Book) bool { return !strings.HasPrefix(lower(b.Title), "dune") },
		`Title LIKE '%'`:                func(b *Book) bool { return true },
		`Title LIKE ''`:                 func(b *Book) bool { return b.Title == "" },
		`Title LIKE '50!%%' ESCAPE '!'`: func(b *Book) bool { return strings.HasPrefix(b.Title, "50%") },
		`Title LIKE '50%%'`:             func(b *Book) bool { return strings.HasPrefix(b.Title, "50") },
		`Title LIKE '%e%e%'`:            func(b *Book) bool { return strings.Count(lower(b.Title), "e") >= 2 },
		`Title LIKE Author`:             func(b *Book) bool { return lower(b.Title) == lower(b.Author) },
		`Title LIKE '%' || Author`:      func(b *Book) bool { return strings.HasSuffix(lower(b.Title), lower(b.Author)) },
		`Title GLOB 'D*'`:               func(b *Book) bool { return strings.HasPrefix(b.Title, "D") },
		`Title GLOB '?une'`:             func(b *Book) bool { return utf8.RuneCountInString(b.Title) == 4 && strings.HasSuffix(b.Title, "une") },
		`Title NOT GLOB '*e'`:           func(b *Book) bool { return !strings.HasSuffix(b.Title, "e") },
		`Title GLOB 'dune'`:             func(b *Book) bool { return b.Title == "dune" },
		`Tags LIKE 'SCI%'`:              func(b *Book) bool { return has(b.Tags, "scifi") },
		`Tags NOT LIKE 'sci%'`:          func(b *Book) bool { return !has(b.Tags, "scifi") },
		`Tags GLOB 'c*'`:                func(b *Book) bool { return has(b.Tags, "classic") },
		`Publisher.City LIKE 'phil%'`:   func(b *Book) bool { return b.Publisher != nil && b.Publisher.City == "Philadelphia" },
		`Year LIKE '19%'`:               func(b *Book) bool { return strings.HasPrefix(fmt.Sprint(b.Year), "19") },

		// Functions.
		`lower(Author) = 'herbert'`:           func(b *Book) bool { return lower(b.Author) == "herbert" },
		`upper(Title) = 'DUNE'`:               func(b *Book) bool { return strings.ToUpper(b.Title) == "DUNE" },
		`length(Title) > 4`:                   func(b *Book) bool { return utf8.RuneCountInString(b.Title) > 4 },
		`length(Tags) >= 2`:                   func(b *Book) bool { return len(b.Tags) >= 2 },
		`abs(Year) = 50`:                      func(b *Book) bool { return b.Year == -50 },
		`abs(Year - 1950) < 20`:               func(b *Book) bool { return math.Abs(float64(b.Year-1950)) < 20 },
		`abs(Price - 10) < 1`:                 func(b *Book) bool { return math.Abs(b.Price-10) < 1 },
		`substr(Title, 1, 4) = 'Dune'`:        func(b *Book) bool { return strings.HasPrefix(b.Title, "Dune") },
		`substr(Title, -4) = 'Dune'`:          func(b *Book) bool { return strings.HasSuffix(b.Title, "Dune") },
		`substr(Title, 2) = 'une'`:            func(b *Book) bool { return b.Title == "Dune" },
		`substr(Title, 0, 2) = 'D'`:           func(b *Book) bool { return strings.HasPrefix(b.Title, "D") },
		`substr(Title, 3, -2) = 'DU'`:         func(b *Book) bool { return b.Title == "DUNE" },
		`instr(Title, 'Dune') = 13`:           func(b *Book) bool { return strings.Index(b.Title, "Dune") == 12 },
		`instr(Title, 'une') = 2`:             func(b *Book) bool { return b.Title == "Dune" },
		`instr(Title, 'zzz') = 0`:             func(b *Book) bool { return true },
		`trim(Title) = 'padded'`:              func(b *Book) bool { return strings.TrimSpace(b.Title) == "padded" },
		`ltrim(Title) = 'padded '`:            func(b *Book) bool { return strings.TrimLeft(b.Title, " ") == "padded " },
		`rtrim(Title) = '  padded'`:           func(b *Book) bool { return strings.TrimRight(b.Title, " ") == "  padded" },
		`trim(Title, 'De') = 'un'`:            func(b *Book) bool { return strings.Trim(b.Title, "De") == "un" },
		`round(Price) = 10`:                   func(b *Book) bool { return math.Round(b.Price) == 10 },
		`round(Price, 1) = 4.3`:               func(b *Book) bool { return b.Price == 4.25 },
		`round(Price / 3, 2) = 3.17`:          func(b *Book) bool { return b.Price == 9.5 },
		`coalesce(Subtitle, 'none') = 'none'`: func(b *Book) bool { return b.Subtitle == nil },
		`ifnull(Publisher.City, '?') = '?'`:   func(b *Book) bool { return b.Publisher == nil },
		`coalesce(Subtitle, Title) = 'Dune'`: func(b *Book) bool {
			s := b.Title
			if b.Subtitle != nil {
				s = *b.Subtitle
			}
			return s == "Dune"
		},
		`coalesce(Data, 'y') = 'y'`: func(b *Book) bool { return b.Data == nil },
		`coalesce(Data, 'y') = 'x'`: func(b *Book) bool { return b.Data == "x" },
		`coalesce(Subtitle, Publisher.City, 'z') = 'London'`: func(b *Book) bool {
			return b.Subtitle == nil && b.Publisher != nil && b.Publisher.City == "London"
		},
		`lower(Tags) = 'scifi'`:      func(b *Book) bool { return has(b.Tags, "scifi") },
		`upper(Tags) = 'NEW'`:        func(b *Book) bool { return has(b.Tags, "new") },
		`length(Publisher.Name) > 6`: func(b *Book) bool { return b.Publisher != nil && len(b.Publisher.Name) > 6 },

		// Arithmetic.
		`Year + 10 > 1975`:                       func(b *Book) bool { return b.Year+10 > 1975 },
		`Year - 10 < 0`:                          func(b *Book) bool { return b.Year-10 < 0 },
		`Price * 2 < Rating`:                     func(b *Book) bool { return b.Price*2 < float64(b.Rating) },
		`Year / 10 = 196`:                        func(b *Book) bool { return b.Year/10 == 196 },
		`Year % 100 = 65`:                        func(b *Book) bool { return b.Year%100 == 65 },
		`Year % 7 = -1`:                          func(b *Book) bool { return b.Year%7 == -1 },
		`Price / 4 = 2.375`:                      func(b *Book) bool { return b.Price == 9.5 },
		`Price % 4 = 1.5`:                        func(b *Book) bool { return math.Mod(b.Price, 4) == 1.5 },
		`Rating + 250 > 255`:                     func(b *Book) bool { return int(b.Rating)+250 > 255 },
		`-Year < -1900`:                          func(b *Book) bool { return -b.Year < -1900 },
		`- (Year + 1) < -1900`:                   func(b *Book) bool { return -(b.Year + 1) < -1900 },
		`+Year = 1965`:                           func(b *Book) bool { return b.Year == 1965 },
		`Year * 1000000 * 1000000 * 1000000 > 0`: func(b *Book) bool { return b.Year > 0 },
		`Title || '!' = 'Dune!'`:                 func(b *Book) bool { return b.Title == "Dune" },
		`Year || '' = '1965'`:                    func(b *Book) bool { return b.Year == 1965 },
		`Title || Author = 'DuneHerbert'`:        func(b *Book) bool { return b.Title == "Dune" && b.Author == "Herbert" },
		`Tags || '!' = 'new!'`:                   func(b *Book) bool { return has(b.Tags, "new") },

		// Two fields.
		`Year > Rating`:            func(b *Book) bool { return b.Year > int(b.Rating) },
		`Price <= Rating`:          func(b *Book) bool { return b.Price <= float64(b.Rating) },
		`Author = Genre`:           func(b *Book) bool { return b.Author == string(b.Genre) },
		`Author != Genre`:          func(b *Book) bool { return b.Author != string(b.Genre) },
		`Released > Reprinted`:     func(b *Book) bool { return b.Released.After(b.Reprinted) },
		`Released <= Reprinted`:    func(b *Book) bool { return !b.Released.After(b.Reprinted) },
		`Tags = Genre`:             func(b *Book) bool { return has(b.Tags, string(b.Genre)) },
		`Subtitle = Title`:         func(b *Book) bool { return b.Subtitle != nil && *b.Subtitle == b.Title },
		`Subtitle != Title`:        func(b *Book) bool { return b.Subtitle == nil || *b.Subtitle != b.Title },
		`Subtitle IS Title`:        func(b *Book) bool { return b.Subtitle != nil && *b.Subtitle == b.Title },
		`Subtitle IS NOT Title`:    func(b *Book) bool { return b.Subtitle == nil || *b.Subtitle != b.Title },
		`Year = 1965 + 5`:          func(b *Book) bool { return b.Year == 1970 },
		`5 = 5`:                    func(b *Book) bool { return true },
		`'a' < 'b'`:                func(b *Book) bool { return true },
		`1 > 2`:                    func(b *Book) bool { return false },
		`(Year > 1960) = InPrint`:  func(b *Book) bool { return (b.Year > 1960) == b.InPrint },
		`(Year > 1960) != InPrint`: func(b *Book) bool { return (b.Year > 1960) != b.InPrint },
		`Released BETWEEN '1965-01-01' AND Reprinted`: func(b *Book) bool {
			return !b.Released.Before(time.Date(1965, 1, 1, 0, 0, 0, 0, time.UTC)) && !b.Released.After(b.Reprinted)
		},
		`Released > '1969-06-01' AND Released < Reprinted`: func(b *Book) bool {
			return b.Released.After(time.Date(1969, 6, 1, 0, 0, 0, 0, time.UTC)) && b.Released.Before(b.Reprinted)
		},

		// BETWEEN and IN of more than values.
		`Year BETWEEN Rating AND 1965`:   func(b *Book) bool { return b.Year >= int(b.Rating) && b.Year <= 1965 },
		`Rating BETWEEN 3 AND Price`:     func(b *Book) bool { return b.Rating >= 3 && float64(b.Rating) <= b.Price },
		`Rating NOT BETWEEN Price AND 9`: func(b *Book) bool { return !(float64(b.Rating) >= b.Price && b.Rating <= 9) },
		`Tags BETWEEN 'a' || '' AND Genre`: func(b *Book) bool {
			for _, tag := range b.Tags {
				if tag >= "a" && tag <= string(b.Genre) {
					return true
				}
			}
			return false
		},
		`Year IN (Rating, 1965)`:    func(b *Book) bool { return b.Year == int(b.Rating) || b.Year == 1965 },
		`Rating NOT IN (Price, 10)`: func(b *Book) bool { return float64(b.Rating) != b.Price && b.Rating != 10 },
		`Genre IN (lower(Author), 'classic')`: func(b *Book) bool {
			return string(b.Genre) == lower(b.Author) || b.Genre == "classic"
		},
		`Genre IN (Title, NULL)`: func(b *Book) bool { return string(b.Genre) == b.Title },
		`Genre IN Tags`:          func(b *Book) bool { return has(b.Tags, string(b.Genre)) },
		`Genre NOT IN Tags`:      func(b *Book) bool { return !has(b.Tags, string(b.Genre)) },
		`lower(Genre) IN Tags`:   func(b *Book) bool { return has(b.Tags, string(b.Genre)) },

		// CASE.
		`CASE WHEN InPrint THEN Year ELSE 0 END > 1960`: func(b *Book) bool { return b.InPrint && b.Year > 1960 },
		`CASE Genre WHEN 'scifi' THEN 1 WHEN 'classic' THEN 2 ELSE 3 END = 2`: func(b *Book) bool {
			return b.Genre == "classic"
		},
		`CASE WHEN Year < 0 THEN 'bc' WHEN Year < 1900 THEN 'old' END = 'old'`: func(b *Book) bool {
			return b.Year >= 0 && b.Year < 1900
		},
		`CASE WHEN Year < 0 THEN 'bc' END IS NULL`:                   func(b *Book) bool { return b.Year >= 0 },
		`CASE WHEN InPrint AND Year > 1900 THEN TRUE ELSE FALSE END`: func(b *Book) bool { return b.InPrint && b.Year > 1900 },
		`CASE WHEN InPrint THEN Flag ELSE FALSE END`:                 func(b *Book) bool { return b.InPrint && b.Flag == true },

		// Booleans of a field of an interface type.
		`Flag`:                  func(b *Book) bool { return b.Flag == true },
		`NOT Flag`:              func(b *Book) bool { return b.Flag != true },
		`Flag AND InPrint`:      func(b *Book) bool { return b.Flag == true && b.InPrint },
		`coalesce(Flag, FALSE)`: func(b *Book) bool { return b.Flag == true },
		`coalesce(Flag, FALSE) = TRUE OR Year = 0`: func(b *Book) bool { return b.Flag == true || b.Year == 0 },
		`coalesce(Flag, 5) = 5`:                    func(b *Book) bool { return b.Flag == nil },

		// Both kinds in one text.
		`Author = 'Herbert' AND (Title LIKE '%dune%' OR Year < 1900) AND NOT Tags = 'new'`: func(b *Book) bool {
			return b.Author == "Herbert" && (strings.Contains(lower(b.Title), "dune") || b.Year < 1900) && !has(b.Tags, "new")
		},
		`NOT (length(Tags) = 0 OR Price > 9.5) OR Genre = 'classic' AND Rating / 2 >= 4`: func(b *Book) bool {
			return !(len(b.Tags) == 0 || b.Price > 9.5) || b.Genre == "classic" && b.Rating/2 >= 4
		},
	} {
		c, err := sql.ParseWhere[Book](text)
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
	}

	// Some conditions have nothing to match or everything, which is what they are about; the
	// others must have both kinds of books to be checked by.
	for text, n := range matched {
		switch text {
		case `5 = 5`, `'a' < 'b'`, `Title LIKE '%'`, `Title IS NOT NULL`, `instr(Title, 'zzz') = 0`, `Year + NULL IS NULL`:
			require.Equal(t, 400, n, text)
		}
	}
	for _, text := range []string{
		`Author = 'Herbert'`, `Title LIKE '%dune'`, `Title LIKE '50!%%' ESCAPE '!'`, `Title GLOB 'D*'`,
		`lower(Author) = 'herbert'`, `length(Tags) >= 2`, `abs(Year) = 50`, `substr(Title, 1, 4) = 'Dune'`,
		`substr(Title, -4) = 'Dune'`, `instr(Title, 'Dune') = 13`, `trim(Title) = 'padded'`, `round(Price) = 10`,
		`round(Price, 1) = 4.3`, `coalesce(Subtitle, 'none') = 'none'`, `Year / 10 = 196`, `Year % 100 = 65`,
		`Price % 4 = 1.5`, `Title || '!' = 'Dune!'`, `Year > Rating`, `Released > Reprinted`, `Tags = Genre`,
		`Year BETWEEN Rating AND 1965`, `Year IN (Rating, 1965)`, `Genre IN Tags`, `Flag`, `NOT Flag`,
		`CASE WHEN InPrint THEN Year ELSE 0 END > 1960`, `Subtitle IS NULL`, `Tags IS NULL`,
		`Year LIKE '19%'`, `lower(Tags) = 'scifi'`, `Rating + 250 > 255`, `Year * 1000000 * 1000000 * 1000000 > 0`,
		`Released BETWEEN '1965-01-01' AND Reprinted`, `(Year > 1960) = InPrint`, `-Year < -1900`,
	} {
		require.NotZero(t, matched[text], text)
		require.NotEqual(t, 400, matched[text], text)
	}
}

// TestEvaluation checks the behaviors of the evaluator that random books do not show.
func TestEvaluation(t *testing.T) {
	parse := func(text string) cond.Cond[Book] {
		t.Helper()

		c, err := sql.ParseWhere[Book](text)
		require.NoError(t, err, text)
		return c
	}

	// What has no value is not true, and its opposite is.
	for _, text := range []string{
		`Year / 0 > 1`,
		`Year % 0 = 0`,
		`Price / 0 > 1`,
		`lower(Subtitle) = 'a novel'`,
		`Publisher.City LIKE 'Phil%'`,
		`length(Publisher.Name) > 3`,
		`Subtitle || 'x' = 'x'`,
		`Year = NULL + 1`,
		`Year BETWEEN NULL AND 3000`,
		`Year IN (NULL)`,
		`CASE WHEN Year > 5000 THEN 1 END = 1`,
		`Extra = Extra`,
		`Data = Data`,
		`coalesce(Data, Subtitle) = 'x'`,
		`Year + Data > 0`,
		`Tags = Subtitle`,
		`substr(Subtitle, 1) = 'x'`,
		`round(Data) = 1`,
		`Title GLOB Subtitle`,
		`Title LIKE Data`,
		`substr(Title, NULL) = 'x'`,
		`trim(Title, NULL) = 'x'`,
	} {
		b := &Book{Year: 1965, Author: "Herbert", Title: "Dune", Extra: map[string]string{"a": "b"}}
		require.False(t, match(t, parse(text), b), text)
		require.True(t, match(t, parse("NOT ("+text+")"), b), text)
	}
	require.True(t, match(t, parse(`lower(Subtitle) = 'a novel'`), dune))
	require.True(t, match(t, parse(`Publisher.City LIKE 'Phil%'`), dune))

	// NULL is no value.
	require.True(t, match(t, parse(`NULL IS NULL`), dune))
	require.True(t, match(t, parse(`Year IS NOT NULL AND NULL IS NULL`), dune))
	require.False(t, match(t, parse(`Year IS NULL`), dune))

	// A field of an interface type that is not true or false where it is expected to be.
	for _, text := range []string{`Data`, `NOT Data`, `coalesce(Data, FALSE)`, `Year = 0 OR Data`, `CASE WHEN InPrint THEN Data END`} {
		for _, data := range []any{"x", int64(5), 1.5, []byte("b"), time.Time{}, []string{"a"}, map[string]any{}} {
			b := &Book{Data: data, InPrint: true, Year: 1}
			_, err := parse(text).Match(b)
			require.ErrorIs(t, err, sql.ErrEvaluation, "%s with %v", text, data)
		}
		for data, want := range map[any]bool{nil: false, true: true, false: false} {
			b := &Book{Data: data, InPrint: true, Year: 1}
			if strings.HasPrefix(text, "NOT") {
				want = !want
			}
			require.Equal(t, want, match(t, parse(text), b), "%s with %v", text, data)
		}
	}
	// The error does not come when the rest of the condition decides.
	ok, err := parse(`Year = 1 AND Data`).Match(&Book{Data: "x", Year: 2})
	require.NoError(t, err)
	require.False(t, ok)

	// The fields of every kind as the evaluator reads them.
	for _, text := range []string{
		`lower(Genre) = 'scifi'`,
		`Rating * 2 = 18`,
		`length(Extra) = 1`,
		`'classic' IN Tags AND length(Tags) = 2`,
		`Released > '1965-01-01' AND Released < '1966-01-01'`,
		`Released > '1965-01-01T00:00:00Z' AND Released < Reprinted`,
		`Extra IS NOT NULL AND Publisher IS NOT NULL AND Tags IS NOT NULL AND Title IS NOT NULL`,
		`Price + Rating = 18.5`,
		`Price - Rating = 0.5`,
		`Rating + Price = 18.5`,
		`Genre || Title = 'scifiDune'`,
		`Year BETWEEN 1900 AND Year + 1`,
		`x'44756e65' = x'44756e65'`,
		`length(x'44756e65') = 4`,
		`Title IS 'Dune' AND Title IS NOT 'dune'`,
		`Title IS NOT DISTINCT FROM 'Dune' AND Title IS DISTINCT FROM 'dune'`,
		`round(Price) = 10`,
		`round(-2.5) = -3`,
		`round(2.567, 2) = 2.57`,
		`round(2, 5) = 2`,
		`abs(-5) = 5 AND abs(-5.5) = 5.5`,
		`abs(-9223372036854775807 - 1) > 9e18`,
		`9223372036854775807 + 1 > 9e18`,
		`-9223372036854775807 - 2 < -9e18`,
		`4611686018427387904 * 4 > 9e18`,
		`-9223372036854775807 / -1 > 0`,
		`(-9223372036854775807 - 1) / -1 > 9e18`,
		`10 / 4 = 2 AND 10 / 4.0 = 2.5 AND -7 / 2 = -3 AND -7 % 3 = -1`,
		`5 % -1 = 0`,
		`instr('dune', 'ne') = 3 AND instr('añb', 'b') = 3`,
		`length('añb') = 3`,
		`substr('hello', 2, 3) = 'ell' AND substr('hello', -3, 2) = 'll' AND substr('hello', 10) = ''`,
		`substr('hello', 0) = 'hello' AND substr('hello', -10, 7) = 'he' AND substr('hello', 3, -2) = 'he'`,
		`trim('  x ') = 'x' AND ltrim('xxy', 'x') = 'y' AND rtrim('yxx', 'x') = 'y'`,
		`'a%b' LIKE 'a\%b' ESCAPE '\' AND NOT 'axb' LIKE 'a\%b' ESCAPE '\'`,
		`'A' LIKE 'a' AND 'É' NOT LIKE 'é' AND 'ab' LIKE 'A_' AND 'x' NOT LIKE '_x'`,
		`'a*c' GLOB 'a*c' AND 'abc' GLOB 'a?c' AND 'ABC' NOT GLOB 'abc' AND 'aXbc' GLOB 'a*c'`,
		`'aaa' LIKE '%a%a%a%' AND 'aa' NOT LIKE '%a%a%a%' AND '' LIKE '%%' AND 'ab' NOT LIKE ''`,
		`coalesce(NULL, NULL, 3) = 3 AND ifnull(NULL, 4) = 4`,
		`(1 < 2) AND (2 <= 2) AND (3 > 2) AND (3 >= 3) AND (1 != 2) AND (1 <> 2) AND (1 == 1)`,
		`1 = 1.0 AND 'a' < 'b' AND x'01' < x'02' AND FALSE < TRUE AND 'ab' = x'6162'`,
		`1 IN (3, 2, 1) AND 4 NOT IN (3, 2, 1) AND 1 NOT IN ()`,
		`2 BETWEEN 1 AND 3 AND 4 NOT BETWEEN 1 AND 3 AND 'b' BETWEEN 'a' AND 'c'`,
		`CASE 2 WHEN 1 THEN FALSE WHEN 2 THEN TRUE END AND CASE WHEN FALSE THEN FALSE ELSE TRUE END`,
		`-(-3) = 3 AND +3 = 3 AND - - 3 = 3`,
		`1e0 = 1 AND .5 + .5 = 1 AND 0x10 = 16 AND 0xFFFFFFFFFFFFFFFF > 1e19`,
		`'it''s' = 'it' || '''s'`,
		`NOT NOT TRUE`,
	} {
		require.True(t, match(t, parse(text), dune), text)
	}

	// Times are compared as times, and a text with one as the time that it is.
	_, err = sql.ParseWhere[Book](`Released > Title`)
	require.ErrorIs(t, err, sql.ErrType)
	_, err = sql.ParseWhere[Book](`Released BETWEEN 'yesterday' AND Reprinted`)
	require.ErrorIs(t, err, sql.ErrType)
}
