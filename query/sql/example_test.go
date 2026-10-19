package sql_test

import (
	"errors"
	"fmt"

	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/ehsanranjbar/kvutil/query/sql"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

type Novel struct {
	Title   string
	Author  string
	Year    int
	Tags    []string
	InPrint bool
}

// Example parses the WHERE clauses of queries on a store, and runs them with an index and
// without one.
func Example() {
	novels := extstore.New[int64, Novel]("novels")
	index.Composite(novels, "by_author_year", field.List(
		field.Path[Novel, string]("Author"),
		field.Path[Novel, int]("Year"),
	))
	// A field that is computed is known to the parser by the index that is defined on it.
	index.On(novels, "by_decade", field.Of("Decade", func(n *Novel) int { return n.Year / 10 * 10 }))

	db := memkv.New()
	if err := novels.Check(db); err != nil {
		panic(err)
	}
	err := db.Update(func(tx kv.Tx) error {
		store := novels.Instantiate(tx)
		for id, novel := range []Novel{
			{Title: "Dune", Author: "Herbert", Year: 1965, Tags: []string{"scifi", "classic"}, InPrint: true},
			{Title: "Emma", Author: "Austen", Year: 1815, Tags: []string{"classic"}, InPrint: true},
			{Title: "Children of Dune", Author: "Herbert", Year: 1976, Tags: []string{"scifi"}},
			{Title: "The Dosadi Experiment", Author: "Herbert", Year: 1977, InPrint: true},
		} {
			if err := store.Set(int64(id+1), &novel); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	// The fields are the ones of the indexes of the store and the ones of the struct.
	parser := sql.For(novels)

	err = db.View(func(tx kv.Tx) error {
		query := func(where string) {
			explain, err := parser.Explain(where)
			if err != nil {
				panic(err)
			}
			it, err := parser.Query(tx, where)
			if err != nil {
				panic(err)
			}
			titles, err := iterutil.Of(it).
				Map(func(_ int64, n *Novel) (string, error) { return n.Title, nil }).
				Values()
			if err != nil {
				panic(err)
			}
			fmt.Printf("%s\n  %s\n  %v\n", where, explain, titles)
		}

		// The comparisons of the fields of an index are served by it, and the rest of the
		// text is checked on the novels that it finds.
		query(`Author = 'Herbert' AND Year >= 1970 AND Title LIKE '%dune'`)
		// Nothing serves a condition that is about a part of a field alone, so the store is scanned.
		query(`Decade = 1970 OR ('classic' IN Tags AND NOT Year >= 1900)`)
		query(`length(Title) > 10 AND InPrint`)

		// The condition itself is a filter of anything that has records.
		c, err := parser.Parse(`Author = 'Herbert' AND Year >= 1970 AND Title LIKE '%dune'`)
		if err != nil {
			return err
		}
		fmt.Println("condition:", c)
		return nil
	})
	if err != nil {
		panic(err)
	}

	// What cannot be right for any novel is an error of the text. Double quotes make a name.
	for _, where := range []string{
		`Athor = 'Herbert'`,
		`Author = "Herbert"`,
		`Year = 'soon'`,
		`contains(Title, 'Dune')`,
		`Year = 1965 AND`,
		`Year + 1`,
	} {
		_, err := parser.Parse(where)
		fmt.Printf("%s: %v %v\n", where,
			errors.Is(err, sql.ErrUnknownField) || errors.Is(err, sql.ErrType) ||
				errors.Is(err, sql.ErrUnknownFunction) || errors.Is(err, sql.ErrSyntax), err == nil)
	}
	_, err = parser.Parse(`Year = 'soon'`)
	fmt.Println(err)

	// Output:
	// Author = 'Herbert' AND Year >= 1970 AND Title LIKE '%dune'
	//   index by_author_year: 1 range, then check ["Title" LIKE '%dune']
	//   [Children of Dune]
	// Decade = 1970 OR ('classic' IN Tags AND NOT Year >= 1900)
	//   scan, then check [(Decade = 1970) or ((Tags = "classic") and (not (Year >= 1900)))]
	//   [Emma Children of Dune The Dosadi Experiment]
	// length(Title) > 10 AND InPrint
	//   scan, then check [length("Title") > 10; InPrint = true]
	//   [The Dosadi Experiment]
	// condition: (Author = "Herbert") and (Year >= 1970) and ("Title" LIKE '%dune')
	// Athor = 'Herbert': true false
	// Author = "Herbert": true false
	// Year = 'soon': true false
	// contains(Title, 'Dune'): true false
	// Year = 1965 AND: true false
	// Year + 1: true false
	// sql: type mismatch: field Year: cannot use "soon" as a value of the type int: it is not a whole number that the type has
}
