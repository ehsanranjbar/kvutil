package ql_test

import (
	"errors"
	"fmt"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/ehsanranjbar/kvutil/query/ql"
	"github.com/ehsanranjbar/kvutil/schema"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

type Novel struct {
	Title   string
	Author  string
	Year    int
	Tags    []string
	InPrint bool
}

// Example parses the text of conditions for a store, and looks the records that satisfy them
// up with an index and without one.
func Example() {
	sch := schema.New()
	novels := extstore.New[int64, Novel](sch.Namespace("novels"))
	byAuthorYear := index.Composite(novels, "by_author_year", field.List(
		field.Path[Novel, string]("Author"),
		field.Path[Novel, int]("Year"),
	))
	// A field that is computed is known to the parser by the index that is defined on it.
	index.On(novels, "by_decade", field.Of("Decade", func(n *Novel) int { return n.Year / 10 * 10 }))

	db := memkv.New()
	if err := sch.Check(db); err != nil {
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
	parser := ql.For(novels)

	err = db.View(func(tx kv.Tx) error {
		titles := func(it iterutil.Iterator[int64, *Novel], err error) []string {
			if err != nil {
				panic(err)
			}
			titles, err := iterutil.Of(it).
				Map(func(_ int64, n *Novel) (string, error) { return n.Title, nil }).
				Values()
			if err != nil {
				panic(err)
			}
			return titles
		}

		// The comparisons of the fields of an index are served by it, and the rest of the
		// text is checked on the novels that it finds.
		c, err := parser.Parse(`Author = "Herbert" AND Year >= 1970 AND contains(Title, "Dune")`)
		if err != nil {
			return err
		}
		fmt.Println("condition:", c)
		ranges, residual, err := byAuthorYear.Instantiate(tx).Plan(c)
		if err != nil {
			return err
		}
		fmt.Println("plan:", len(ranges), "range, then check", residual)
		it, err := byAuthorYear.Instantiate(tx).Lookup(c)
		fmt.Println("with the index:", titles(it, err))

		// A condition is a filter of anything that iterates over the records, too.
		c, err = parser.Parse(`Decade = 1970 OR ("classic" IN Tags AND NOT Year >= 1900)`)
		if err != nil {
			return err
		}
		all, err := novels.Instantiate(tx).Iterator(kvutil.All[int64]())
		if err != nil {
			return err
		}
		var failed error
		filtered := iterutil.Of[int64, *Novel](all).Filter(func(_ int64, n *Novel) bool {
			ok, err := c.Match(n)
			failed = errors.Join(failed, err)
			return ok
		})
		fmt.Println("without an index:", titles(filtered, nil), failed)
		return nil
	})
	if err != nil {
		panic(err)
	}

	// What cannot be right for any novel is an error of the text.
	for _, text := range []string{
		`Athor = "Herbert"`,
		`Author = Herbert`,
		`Year = "soon"`,
		`upper(Author) = "HERBERT"`,
		`Year = 1965 AND`,
	} {
		_, err := parser.Parse(text)
		fmt.Printf("%s: %v\n", text,
			errors.Is(err, ql.ErrUnknownField) || errors.Is(err, ql.ErrType) ||
				errors.Is(err, ql.ErrUnknownFunction) || errors.Is(err, ql.ErrSyntax))
	}
	_, err = parser.Parse(`Year = "soon"`)
	fmt.Println(err)

	// Output:
	// condition: (Author = "Herbert") and (Year >= 1970) and (contains(Title, "Dune"))
	// plan: 1 range, then check [contains(Title, "Dune")]
	// with the index: [Children of Dune]
	// without an index: [Emma Children of Dune The Dosadi Experiment] <nil>
	// Athor = "Herbert": true
	// Author = Herbert: true
	// Year = "soon": true
	// upper(Author) = "HERBERT": true
	// Year = 1965 AND: true
	// ql: type mismatch: field Year: cannot use "soon" as a value of the type int: it is not a whole number that the type has
}
