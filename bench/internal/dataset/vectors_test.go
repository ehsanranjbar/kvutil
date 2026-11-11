package dataset

import (
	"math"
	"reflect"
	"testing"
)

func TestVectorsAreDeterministicAndPerDocument(t *testing.T) {
	a := NewVectors(7, 10_000)
	b := NewVectors(7, 10_000)

	// Any document can be generated alone, in any order, by a generator made apart.
	want := a.Gen(42)
	b.Gen(3)
	b.Gen(9_999)
	if got := b.Gen(42); !reflect.DeepEqual(got, want) {
		t.Fatalf("document 42 differs:\n%+v\n%+v", got, want)
	}

	// GenInto over a document that was used for another gives the same one.
	d := a.Gen(5)
	a.GenInto(d, 42)
	if !reflect.DeepEqual(d, want) {
		t.Fatalf("GenInto over a used document differs")
	}
	if want.ID != 42 || len(want.Embedding) != VectorDims {
		t.Fatalf("id %d, %d components", want.ID, len(want.Embedding))
	}

	// The seed, the id and the stream matter.
	if reflect.DeepEqual(NewVectors(8, 10_000).Gen(42).Embedding, want.Embedding) {
		t.Error("another seed gives the same embedding")
	}
	if reflect.DeepEqual(a.Gen(43).Embedding, want.Embedding) {
		t.Error("another id gives the same embedding")
	}
	if reflect.DeepEqual(a.Query(42), want.Embedding) {
		t.Error("a query is the embedding of the document of its number")
	}
	if !reflect.DeepEqual(a.Query(3), b.Query(3)) || !reflect.DeepEqual(a.Queries(5)[3], a.Query(3)) {
		t.Error("a query is not a function of its number")
	}
}

func TestVectorsShape(t *testing.T) {
	const n = 10_000
	v := NewVectors(1, n)
	if v.Centres() != 50 {
		t.Errorf("%d centres, want ceil(sqrt(10000)/2) = 50", v.Centres())
	}

	langs := map[string]int{}
	years := map[int]int{}
	for id := int64(0); id < n; id++ {
		d := v.Gen(id)
		langs[d.Lang]++
		years[d.Year]++
		if d.Year < DocFirstYear || d.Year > DocLastYear {
			t.Fatalf("year %d", d.Year)
		}
		if id < 50 {
			var sum float64
			for _, c := range d.Embedding {
				if math.IsNaN(float64(c)) || math.IsInf(float64(c), 0) {
					t.Fatalf("document %d has %v", id, c)
				}
				sum += float64(c) * float64(c)
			}
			if math.Abs(sum-1) > 1e-5 {
				t.Errorf("document %d has the squared length %v", id, sum)
			}
		}
	}
	for lang, share := range map[string]float64{"en": 0.70, "fr": 0.15, "de": 0.13, "is": 0.02} {
		got := float64(langs[lang]) / n
		if math.Abs(got-share) > 0.02 {
			t.Errorf("%s: %.3f of the documents, want about %.2f", lang, got, share)
		}
	}
	if len(years) != DocLastYear-DocFirstYear+1 {
		t.Errorf("%d different years", len(years))
	}
	broad := 0
	for y, c := range years {
		if y >= 2000 {
			broad += c
		}
	}
	if got := float64(broad) / n; math.Abs(got-26.0/36) > 0.03 {
		t.Errorf("%.3f of the documents are from 2000 on, want about 0.72", got)
	}
}

// A few dimensions carry most of the variance: the first components of the vectors vary much more
// than the last, and the vectors of one centre are nearer to each other than to the others.
func TestVectorsAreUneven(t *testing.T) {
	v := NewVectors(1, 10_000)
	var head, tail float64
	for id := int64(0); id < 500; id++ {
		e := v.Gen(id).Embedding
		for j := 0; j < 20; j++ {
			head += float64(e[j]) * float64(e[j])
		}
		for j := VectorDims - 20; j < VectorDims; j++ {
			tail += float64(e[j]) * float64(e[j])
		}
	}
	if head < 20*tail {
		t.Errorf("the first 20 components have %.3g of the squared length, the last 20 %.3g", head, tail)
	}

	// The nearest of 500 documents to a query is nearer than a typical one. The clusters overlap
	// (see noise), so not by as much as clusters that do not.
	q := v.Query(0)
	var best, mean float64
	best = 2
	for id := int64(0); id < 500; id++ {
		var dot float64
		for j, c := range v.Gen(id).Embedding {
			dot += float64(c) * float64(q[j])
		}
		d := 1 - dot
		mean += d / 500
		best = math.Min(best, d)
	}
	if best > 0.75*mean {
		t.Errorf("nearest distance %.3f, mean %.3f: the vectors are not clustered", best, mean)
	}
}

func TestVectorDocsCap(t *testing.T) {
	for scale, want := range map[int]int{100: 100, 100_000: 100_000, 250_000: 250_000, 1_000_000: MaxVectorDocs} {
		if got := VectorDocs(scale); got != want {
			t.Errorf("VectorDocs(%d) = %d, want %d", scale, got, want)
		}
	}
}
