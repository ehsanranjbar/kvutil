package query_test

import (
	"fmt"
	"strings"

	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/ehsanranjbar/kvutil/query"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

// ExampleQuery_Select selects the values of the records instead of the records. An index that
// holds the values answers the query alone, and the records are only read when they are asked for.
func ExampleQuery_Select() {
	books := extstore.New[int64, Book]("books")

	author := field.Path[Book, string]("Author")
	year := field.Path[Book, int]("Year")
	title := field.Path[Book, string]("Title")
	// A value that is computed from a book, which an index can keep with its entries.
	shout := index.Cover("Shout", func(b *Book) string { return strings.ToUpper(b.Title) })

	// The title and the shouted title are kept with the entries, besides the author and the year
	// that are in their keys.
	index.Composite(books, "by_author_year", field.List(author, year.Desc()),
		index.Including(title), index.Covering(shout))

	db := memkv.New()
	if err := books.Check(db); err != nil {
		panic(err)
	}
	err := db.Update(func(tx kv.Tx) error {
		store := books.Instantiate(tx)
		for id, book := range []Book{
			{Title: "Dune", Author: "Herbert", Year: 1965},
			{Title: "Emma", Author: "Austen", Year: 1815},
			{Title: "Children of Dune", Author: "Herbert", Year: 1976},
			{Title: "Persuasion", Author: "Austen", Year: 1817},
			{Title: "The Dosadi Experiment", Author: "Herbert", Year: 1977},
		} {
			if err := store.Set(int64(id+1), &book); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	herbert := query.From(books).Where(author.Eq("Herbert")).OrderBy(year.Desc())

	err = db.View(func(tx kv.Tx) error {
		// Select the items that the rows have: fields, and covers. The index holds all of them.
		p := herbert.Select(year, title, shout)
		plan, err := p.Explain()
		if err != nil {
			return err
		}
		fmt.Println(plan)

		rows := p.Run(tx)
		defer rows.Close()
		for ; rows.Valid(); rows.Next() {
			row, err := rows.Value()
			if err != nil {
				return err
			}
			// Get is typed by the item: the year is an int and the title a string.
			y, _ := row.Get(year)
			t, _ := row.Get(title)
			s, _ := row.Get(shout)
			_, selected := row.Get(author)
			fmt.Println(row.Key(), y, t, s, "author selected:", selected)
		}
		if err := rows.Err(); err != nil {
			return err
		}

		// A row can give the record, which is read when it is asked for.
		rows2 := herbert.Limit(1).Select(year).Run(tx)
		defer rows2.Close()
		row, err := rows2.Value()
		if err != nil {
			return err
		}
		book, err := row.Record()
		if err != nil {
			return err
		}
		fmt.Println("the latest:", book.Title, book.Year)

		// The plan is the same for the years alone, and for a condition that the index checks on
		// its entries, as it holds the year.
		fmt.Println(mustExplain(herbert.Select(year)))
		fmt.Println(mustExplain(query.From(books).Where(year.Lt(1900)).Select(author)))

		// Count does not read the records where an index holds the conditions.
		n, err := herbert.Count(tx)
		if err != nil {
			return err
		}
		fmt.Println("books of Herbert:", n)
		n, err = query.From(books).Where(author.Eq("Austen"), year.Gt(1816)).Count(tx)
		if err != nil {
			return err
		}
		fmt.Println("books of Austen after 1816:", n)
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Output:
	// IndexOnlyScan by_author_year (in order of Year desc): 1 range
	// 5 1977 The Dosadi Experiment THE DOSADI EXPERIMENT author selected: false
	// 3 1976 Children of Dune CHILDREN OF DUNE author selected: false
	// 1 1965 Dune DUNE author selected: false
	// the latest: The Dosadi Experiment 1977
	// IndexOnlyScan by_author_year (in order of Year desc): 1 range
	// IndexOnlyScan by_author_year: 1 range, then check [Year < 1900]
	// books of Herbert: 3
	// books of Austen after 1816: 1
}

func mustExplain[K, V any](p *query.Projection[K, V]) string {
	plan, err := p.Explain()
	if err != nil {
		panic(err)
	}
	return plan
}
