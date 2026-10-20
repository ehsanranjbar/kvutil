package sql_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/query/sql"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

// TestSelectAndCount checks that the query of a WHERE clause selects values and counts like any
// other, from an index that holds them and from the records.
func TestSelectAndCount(t *testing.T) {
	title := field.Path[Book, string]("Title")
	shout := index.Cover("Rating10", func(b *Book) int { return int(b.Rating) * 10 })

	testkv.Each(t, func(t *testing.T, db kv.DB) {
		lib := newLibrary(t, db, func(books *extstore.Store[int64, Book]) {
			index.Composite(books, "by_author_year", field.List(author, year.Desc()),
				index.Including(price), index.Covering(shout))
		})
		p := sql.For(lib.books)

		for _, test := range []struct {
			where, plan string
		}{
			// The index holds the price and the cover, and the year in its key.
			{`Author = 'Herbert' AND Year >= 1965`, `IndexOnlyScan by_author_year: 1 range`},
			{`Author = 'Herbert' AND Price > 5`, `IndexOnlyScan by_author_year: 1 range, then check [Price > 5]`},
			// The title is not held, and a LIKE is not a comparison.
			{`Author = 'Herbert' AND Title LIKE '%Dune'`, `IndexLookup by_author_year: 1 range, then check ["Title" LIKE '%Dune']`},
		} {
			q, err := p.Where(test.where)
			require.NoError(t, err, test.where)

			sel := q.Select(price, shout, year)
			plan, err := sel.Explain()
			require.NoError(t, err, test.where)
			require.Equal(t, test.plan, plan, test.where)

			want := lib.expect(t, must(p.Parse(test.where)))
			require.NotEmpty(t, want, test.where)

			require.NoError(t, db.View(func(tx kv.Tx) error {
				rows := sel.Run(tx)
				defer rows.Close()

				var keys []int64
				for ; rows.Valid(); rows.Next() {
					row, err := rows.Value()
					require.NoError(t, err)
					keys = append(keys, row.Key())

					book := lib.all[row.Key()]
					gotPrice, ok := row.Get(price)
					require.True(t, ok)
					require.Equal(t, book.Price, gotPrice)
					gotRating, ok := row.Get(shout)
					require.True(t, ok)
					require.Equal(t, int(book.Rating)*10, gotRating)
					gotYear, ok := row.Get(year)
					require.True(t, ok)
					require.Equal(t, book.Year, gotYear)
					_, ok = row.Get(title)
					require.False(t, ok, "the title was not selected")

					record, err := row.Record()
					require.NoError(t, err)
					require.Equal(t, book.Code, record.Code)
				}
				require.NoError(t, rows.Err())
				require.ElementsMatch(t, want, keys, test.where)

				count, err := q.Count(tx)
				require.NoError(t, err)
				require.Equal(t, len(want), count, test.where)
				return nil
			}), test.where)
		}
	})
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
