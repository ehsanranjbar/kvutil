package vector_test

import (
	"fmt"

	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/ehsanranjbar/kvutil/vector"
)

// Doc is an article with an embedding of its text. The embeddings here are made by hand with
// three dimensions, about animals, food and space; a model makes ones with hundreds.
type Doc struct {
	Title     string
	Lang      string
	Embedding []float32
}

// Lang is the language of a doc, which conditions and an index are made with.
var Lang = field.Path[Doc, string]("Lang")

// Example defines a store with a vector index and finds the docs that are about something like
// a text is, by the embedding of the text.
func Example() {
	docs := extstore.New[int64, Doc]("docs")

	// The index is told how to get the vector of a doc, and how many dimensions it has and how
	// the distance between two of them is measured. It is kept up to date with the docs.
	embeddings := vector.On(docs, "by_embedding", 3, vector.Cosine,
		func(d *Doc) []float32 { return d.Embedding })
	// An index on the language, to limit a search to the docs of one.
	byLang := index.On(docs, "by_lang", Lang)

	db := memkv.New()
	if err := docs.Check(db); err != nil {
		panic(err)
	}

	err := db.Update(func(tx kv.Tx) error {
		store := docs.Instantiate(tx)
		for id, d := range map[int64]Doc{
			1: {Title: "Cats and kittens", Lang: "en", Embedding: []float32{0.9, 0.1, 0}},
			2: {Title: "Dogs and puppies", Lang: "en", Embedding: []float32{0.8, 0.3, 0.1}},
			3: {Title: "Pasta recipes", Lang: "en", Embedding: []float32{0.1, 0.9, 0.1}},
			4: {Title: "Chats et chatons", Lang: "fr", Embedding: []float32{0.9, 0.1, 0.05}},
			5: {Title: "Rockets and orbits", Lang: "en", Embedding: []float32{0, 0.1, 1}},
			6: {Title: "Pain et brioches", Lang: "fr", Embedding: []float32{0.1, 0.8, 0}},
			// A doc without an embedding is not in the index, and is never found by it.
			7: {Title: "Untitled", Lang: "en"},
		} {
			// A vector of the wrong size or with a component that is not a number fails the write.
			if err := store.Set(id, &d); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	// show prints the results of a search under a label: the key, the title and the distance.
	show := func(label string, res *vector.Results[int64, Doc]) error {
		defer res.Close()

		fmt.Println(label)
		for ; res.Valid(); res.Next() {
			key, err := res.Key()
			if err != nil {
				return err
			}
			// The doc is only read when it is asked for.
			doc, err := res.Value()
			if err != nil {
				return err
			}
			fmt.Printf("  %d %s %.2f\n", key, doc.Title, res.Distance())
		}
		return nil
	}

	err = db.View(func(tx kv.Tx) error {
		ins := embeddings.Instantiate(tx)

		// The docs that are nearest to a vector, the nearest first. The distance is 0 for the same
		// direction and 2 for the opposite one. The query is the embedding of "pets".
		pets := []float32{1, 0.1, 0}
		res, err := ins.Nearest(pets, 3)
		if err != nil {
			return err
		}
		if err := show("nearest to pets:", res); err != nil {
			return err
		}

		// A search is described and then run. The conditions are checked on the docs, from the
		// nearest on, so only as many as are needed are read.
		res, err = ins.Search(pets).Where(Lang.Eq("en")).Within(0.1).Limit(5).Run()
		if err != nil {
			return err
		}
		if err := show("english, close enough:", res); err != nil {
			return err
		}

		// The candidates can come from somewhere else, like a lookup in an index: the index
		// finds the docs, and the vectors order them.
		french, err := byLang.Instantiate(tx).Match("fr")
		if err != nil {
			return err
		}
		res, err = ins.Search(pets).Among(french).Limit(2).Run()
		if err != nil {
			return err
		}
		if err := show("french:", res); err != nil {
			return err
		}

		// More like this: the vector that the index has for a doc is a query. The doc itself is
		// the nearest one, at a distance of 0.
		cats, err := ins.Vector(1)
		if err != nil {
			return err
		}
		res, err = ins.Search(cats).Limit(3).Run()
		if err != nil {
			return err
		}
		if err := show("more like the first:", res); err != nil {
			return err
		}

		// A doc that is not in the index has no vector.
		_, err = ins.Vector(7)
		fmt.Println("untitled:", err)
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Output:
	// nearest to pets:
	//   1 Cats and kittens 0.00
	//   4 Chats et chatons 0.00
	//   2 Dogs and puppies 0.04
	// english, close enough:
	//   1 Cats and kittens 0.00
	//   2 Dogs and puppies 0.04
	// french:
	//   4 Chats et chatons 0.00
	//   6 Pain et brioches 0.78
	// more like the first:
	//   1 Cats and kittens 0.00
	//   4 Chats et chatons 0.00
	//   2 Dogs and puppies 0.04
	// untitled: kv: key not found
}
