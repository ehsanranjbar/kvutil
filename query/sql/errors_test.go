package sql_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ehsanranjbar/kvutil/query/sql"
)

func TestErrors(t *testing.T) {
	for text, want := range map[string]error{
		// Not written as a condition.
		``:                           sql.ErrSyntax,
		`   `:                        sql.ErrSyntax,
		`Year =`:                     sql.ErrSyntax,
		`Year = 1965 AND`:            sql.ErrSyntax,
		`(Year = 1965`:               sql.ErrSyntax,
		`Year = 1965) AND Year = 3`:  sql.ErrSyntax,
		`Year IN (1965`:              sql.ErrSyntax,
		`Year BETWEEN 1 AND`:         sql.ErrSyntax,
		`Author = 'Herbert`:          sql.ErrSyntax,
		`Year = 1965 # no`:           sql.ErrSyntax,
		`Year = 0xFFFFFFFFFFFFFFFFF`: sql.ErrSyntax,
		// One thing too many, and several.
		`Year = 1965 Year = 1`:      sql.ErrSyntax,
		`Year = 1965 Author`:        sql.ErrSyntax,
		`Year = 1965 5`:             sql.ErrSyntax,
		`InPrint InPrint`:           sql.ErrSyntax,
		`Year = 1965)`:              sql.ErrSyntax,
		`Year = 1965;`:              sql.ErrSyntax,
		`Year = 1965; DROP TABLE x`: sql.ErrSyntax,
		`Year = 1 SELECT 2`:         sql.ErrSyntax,
		`Year = 1 ORDER BY Year`:    sql.ErrSyntax,
		`Year = 1 LIMIT 3`:          sql.ErrSyntax,
		`Publisher.City.X.Y = 1`:    sql.ErrSyntax,
		// What the package does not have.
		`Year IN (SELECT 1)`:         sql.ErrSyntax,
		`EXISTS (SELECT 1)`:          sql.ErrSyntax,
		`NOT EXISTS (SELECT 1)`:      sql.ErrSyntax,
		`Year = (SELECT 1)`:          sql.ErrSyntax,
		`Year = ?`:                   sql.ErrSyntax,
		`Year = :year`:               sql.ErrSyntax,
		`Year = 1 COLLATE NOCASE`:    sql.ErrSyntax,
		`Year & 1 = 1`:               sql.ErrSyntax,
		`~Year = 1`:                  sql.ErrSyntax,
		`Year << 1 = 1`:              sql.ErrSyntax,
		`Title REGEXP 'a'`:           sql.ErrSyntax,
		`Title MATCH 'a'`:            sql.ErrSyntax,
		`CAST(Year AS TEXT) = '1'`:   sql.ErrSyntax,
		`(Year, Rating) = (1, 2)`:    sql.ErrSyntax,
		`Year = CURRENT_DATE`:        sql.ErrSyntax,
		`Title -> 'a' = 1`:           sql.ErrSyntax,
		`lower(DISTINCT Title) = ''`: sql.ErrSyntax,
		`lower(Title) OVER () = ''`:  sql.ErrSyntax,
		`Year = Publisher.*`:         sql.ErrSyntax,

		// A field that the books do not have. Double quotes make a name, not a string.
		`Athor = 'Herbert'`:                         sql.ErrUnknownField,
		`Author = "Herbert"`:                        sql.ErrUnknownField,
		`"Author" = "Herbert"`:                      sql.ErrUnknownField,
		`Author = Herbert`:                          sql.ErrUnknownField,
		`"Missing" = 1`:                             sql.ErrUnknownField,
		"`Missing` = 1":                             sql.ErrUnknownField,
		`Publisher.Country = 'US'`:                  sql.ErrUnknownField,
		`"Publisher"."Country" = 'US'`:              sql.ErrUnknownField,
		`"Publisher.Country" = 'US'`:                sql.ErrUnknownField,
		`Title.Length > 3`:                          sql.ErrUnknownField,
		`hidden = 1`:                                sql.ErrUnknownField,
		`year = 1965`:                               sql.ErrUnknownField,
		`Missing`:                                   sql.ErrUnknownField,
		`Missing IS NULL`:                           sql.ErrUnknownField,
		`lower(Missing) = 'x'`:                      sql.ErrUnknownField,
		`Year > Missing`:                            sql.ErrUnknownField,
		`Year + Missing > 3`:                        sql.ErrUnknownField,
		`Missing IN (1, 2)`:                         sql.ErrUnknownField,
		`'a' IN Missing`:                            sql.ErrUnknownField,
		`Year IN Missing`:                           sql.ErrUnknownField,
		`Missing BETWEEN 1 AND 2`:                   sql.ErrUnknownField,
		`Year BETWEEN Missing AND 2`:                sql.ErrUnknownField,
		`Year = 1 OR (InPrint AND NOT Missing = 2)`: sql.ErrUnknownField,
		`Title LIKE lower(Missing)`:                 sql.ErrUnknownField,
		`CASE WHEN Missing THEN 1 END = 1`:          sql.ErrUnknownField,

		// A function that there is not.
		`nosuchfunc(Author) = 'x'`:              sql.ErrUnknownFunction,
		`nosuchfunc(Author)`:                    sql.ErrUnknownFunction,
		`Year = 1 AND lower(nope(Title)) = 'x'`: sql.ErrUnknownFunction,
		`count(*) > 1`:                          sql.ErrUnknownFunction,
		`contains(Title, 'Dune')`:               sql.ErrUnknownFunction,
		`Missing(1) = 1`:                        sql.ErrUnknownFunction,

		// A value that the field cannot have.
		`Year = 'abc'`:                  sql.ErrType,
		`Year > 'abc'`:                  sql.ErrType,
		`Year = 1965.5`:                 sql.ErrType,
		`Year = TRUE`:                   sql.ErrType,
		`Year IN (1965, 'abc')`:         sql.ErrType,
		`Year BETWEEN 1 AND 'abc'`:      sql.ErrType,
		`Year NOT BETWEEN 'abc' AND 1`:  sql.ErrType,
		`Year IS 'abc'`:                 sql.ErrType,
		`Rating = 300`:                  sql.ErrType,
		`Rating = -1`:                   sql.ErrType,
		`Author = 5`:                    sql.ErrType,
		`Author = TRUE`:                 sql.ErrType,
		`InPrint = 1`:                   sql.ErrType,
		`InPrint = 'yes'`:               sql.ErrType,
		`Released > 'yesterday'`:        sql.ErrType,
		`Released > 1965`:               sql.ErrType,
		`Tags = 5`:                      sql.ErrType,
		`5 IN Tags`:                     sql.ErrType,
		`Year = 1965 AND Price < 'low'`: sql.ErrType,

		// A field that has no values to compare, and NULL.
		`Extra = 'x'`:     sql.ErrType,
		`Publisher = 'x'`: sql.ErrType,
		`Extra IN ('a')`:  sql.ErrType,
		`Data = 5`:        sql.ErrType,
		`Author = NULL`:   sql.ErrType,
		`Author == NULL`:  sql.ErrType,
		`NULL != Author`:  sql.ErrType,
		`Year <> NULL`:    sql.ErrType,
		`Year < NULL`:     sql.ErrType,
		`NULL = NULL`:     sql.ErrType,

		// What is not true or false where a condition is expected.
		`Author`:                        sql.ErrType,
		`Year`:                          sql.ErrType,
		`'text'`:                        sql.ErrType,
		`5`:                             sql.ErrType,
		`NULL`:                          sql.ErrType,
		`-5`:                            sql.ErrType,
		`x'ab'`:                         sql.ErrType,
		`Year + 5`:                      sql.ErrType,
		`Year = 1965 AND Author`:        sql.ErrType,
		`NOT Year`:                      sql.ErrType,
		`Year = 1965 OR Price * 2`:      sql.ErrType,
		`lower(Author)`:                 sql.ErrType,
		`length(Title)`:                 sql.ErrType,
		`Tags`:                          sql.ErrType,
		`Released`:                      sql.ErrType,
		`CASE WHEN Year > 1 THEN 1 END`: sql.ErrType,
		`CASE WHEN Year THEN TRUE END`:  sql.ErrType,
		`NOT lower(Author)`:             sql.ErrType,
		`(Year + 1) AND InPrint`:        sql.ErrType,

		// Types that can be known before there is a record.
		`Year > Author`:    sql.ErrType,
		`Year = Title`:     sql.ErrType,
		`Released > Title`: sql.ErrType,
		`Released BETWEEN 'yesterday' AND Reprinted`: sql.ErrType,
		`InPrint = Year`:                    sql.ErrType,
		`Year + Author > 3`:                 sql.ErrType,
		`Author - 1 > 3`:                    sql.ErrType,
		`-Author = 'x'`:                     sql.ErrType,
		`InPrint + 1 > 1`:                   sql.ErrType,
		`Author || InPrint = 'x'`:           sql.ErrType,
		`Title LIKE InPrint`:                sql.ErrType,
		`InPrint LIKE 'a'`:                  sql.ErrType,
		`Title LIKE 'a' ESCAPE 'ab'`:        sql.ErrType,
		`Title LIKE 'a' ESCAPE ''`:          sql.ErrType,
		`Title LIKE 'a' ESCAPE Author`:      sql.ErrType,
		`Year IN (Author)`:                  sql.ErrType,
		`Year IN (1, Author)`:               sql.ErrType,
		`Year IN Rating`:                    sql.ErrType,
		`Year BETWEEN Author AND 3`:         sql.ErrType,
		`CASE Year WHEN 'a' THEN 1 END = 1`: sql.ErrType,
		`CASE WHEN Year THEN 1 END = 1`:     sql.ErrType,
		`lower(Year) = 'x'`:                 sql.ErrType,
		`lower() = 'x'`:                     sql.ErrType,
		`lower(Author, Title) = 'x'`:        sql.ErrType,
		`substr(Title) = 'x'`:               sql.ErrType,
		`substr(Title, 1, 2, 3) = 'x'`:      sql.ErrType,
		`substr(Title, 'a') = 'x'`:          sql.ErrType,
		`abs(Title) > 1`:                    sql.ErrType,
		`round(Year, 1, 2) > 1`:             sql.ErrType,
		`round(Title) > 1`:                  sql.ErrType,
		`coalesce(Title) = 'x'`:             sql.ErrType,
		`ifnull(Title, 1, 2) = 'x'`:         sql.ErrType,
		`length(Released) > 1`:              sql.ErrType,
		`length() > 1`:                      sql.ErrType,
		`instr(Title) = 1`:                  sql.ErrType,
		`instr(Title, Year) = 1`:            sql.ErrType,
		`trim(Title, 1, 2) = ''`:            sql.ErrType,
	} {
		// Every text is checked, whatever the others give.
		c, err := sql.ParseWhere[Book](text)
		if !errors.Is(err, want) || c != nil {
			t.Errorf("%q gave %v and the error %v, not %v", text, c, err, want)
		}
	}

	// What is wrong is named.
	_, err := sql.ParseWhere[Book](`Athor = 'Herbert'`)
	require.ErrorContains(t, err, `"Athor"`)
	_, err = sql.ParseWhere[Book](`Author = "Herbert"`)
	require.ErrorContains(t, err, `"Herbert"`)
	_, err = sql.ParseWhere[Book](`nosuchfunc(Author) = 'x'`)
	require.ErrorContains(t, err, `"nosuchfunc"`)
	_, err = sql.ParseWhere[Book](`Year = 'abc'`)
	require.ErrorContains(t, err, "Year")
	require.ErrorContains(t, err, "abc")
	_, err = sql.ParseWhere[Book](`Author = NULL`)
	require.ErrorContains(t, err, "IS NULL")
	_, err = sql.ParseWhere[Book](`Year = 1965 )`)
	require.ErrorContains(t, err, "unexpected")
	_, err = sql.ParseWhere[Book](`lower(Year) = 'x'`)
	require.ErrorContains(t, err, "lower")
}
