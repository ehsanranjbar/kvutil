package dataset

import (
	"math"
	"math/rand/v2"
)

const (
	// VectorDims is the number of components of an embedding: what a small model of text makes.
	VectorDims = 384
	// MaxVectorDocs is the most documents of the vector workload, whatever the scale is. It keeps
	// the build of an HNSW index in PostgreSQL within the memory of the default containers (4 GB).
	MaxVectorDocs = 250_000
	// QueryPool is the number of query vectors of the vector workload.
	QueryPool = 256
)

// Doc is the record of the vector workload: a document with a language, a year and an embedding of
// its text. The id is the key of the record in an extensible store (the first id is 0) and is not
// a part of the value.
type Doc struct {
	ID        int64 `msgpack:"-" json:"-"`
	Lang      string
	Year      int
	Embedding []float32
}

// The languages of the documents, and the share of each in a hundred. "is" is the selective one.
var docLangs = []struct {
	name  string
	share int
}{{"en", 70}, {"fr", 15}, {"de", 13}, {"is", 2}}

const (
	// DocFirstYear and DocLastYear bound the year of a document, both included.
	DocFirstYear = 1990
	DocLastYear  = 2025
)

// VectorDocs returns the number of documents of the vector workload for a scale.
func VectorDocs(scale int) int {
	return min(scale, MaxVectorDocs)
}

// The streams of a vector dataset, so that its centres, its documents and its queries draw from
// generators that do not overlap.
const (
	centreStream = 0x63656e74
	docStream    = 0x646f6373
	attrStream   = 0x61747472
	queryStream  = 0x71756572
)

// Vectors generates the documents and the query vectors of the vector workload. A document is a
// pure function of the seed and its id (and of the number of documents the dataset is for, which
// sets the number of centres), so any document can be generated alone, in any order.
//
// The vectors are around sqrt(n)/2 centres, like the embeddings of texts about some subjects are.
// Component j of a centre and of every noise draw is scaled by 3·exp(-j/60) + 0.05, so that a few
// dimensions carry most of the variance, as they do in the embeddings of a model. A vector is a
// centre plus noise of 1.5 times the spread of the centres, and is normalized to length 1.
type Vectors struct {
	seed    uint64
	centres [][]float32
	scale   []float64
}

// noise is the standard deviation of the noise of a vector, against 1 for the spread of the centres.
// The clusters overlap at 1.5: a search of an index of partitions that reads the square root of
// their number finds about 0.9 of the 10 nearest, and more of them with more partitions read,
// which is the range that such indexes are tuned in on real embeddings. At 0.3 every cluster was
// in a partition of its own, and every setting found all of them, which showed no trade-off.
const noise = 1.5

// NewVectors returns the generator of a dataset of n documents.
func NewVectors(seed uint64, n int) *Vectors {
	v := &Vectors{seed: seed, scale: make([]float64, VectorDims)}
	for j := range v.scale {
		v.scale[j] = 3*math.Exp(-float64(j)/60) + 0.05
	}
	k := max(2, int(math.Ceil(math.Sqrt(float64(n))/2)))
	v.centres = make([][]float32, k)
	for i := range v.centres {
		r := v.stream(centreStream, uint64(i))
		c := make([]float32, VectorDims)
		for j := range c {
			c[j] = float32(r.NormFloat64() * v.scale[j])
		}
		v.centres[i] = c
	}
	return v
}

// Centres is the number of centres that the vectors are around.
func (v *Vectors) Centres() int { return len(v.centres) }

// stream returns the generator of numbers of a stream and an index in it, seeded by the dataset's
// seed.
func (v *Vectors) stream(stream, index uint64) *rand.Rand {
	r := newRNG(v.seed^stream, index, 0)
	return rand.New(rand.NewPCG(r.next(), r.next()))
}

// Gen returns the document of an id.
func (v *Vectors) Gen(id int64) *Doc {
	d := &Doc{}
	v.GenInto(d, id)
	return d
}

// GenInto is Gen into a document that is given, which reuses its embedding.
func (v *Vectors) GenInto(d *Doc, id int64) {
	d.ID = id
	d.Embedding = v.draw(d.Embedding, v.stream(docStream, uint64(id)))

	a := newRNG(v.seed^attrStream, uint64(id), 0)
	pick := a.intn(100)
	for _, l := range docLangs {
		if pick < l.share {
			d.Lang = l.name
			break
		}
		pick -= l.share
	}
	d.Year = DocFirstYear + a.intn(DocLastYear-DocFirstYear+1)
}

// Query returns the i-th query vector, drawn the same way as the vectors of the documents from a
// stream of its own.
func (v *Vectors) Query(i int) []float32 {
	return v.draw(nil, v.stream(queryStream, uint64(i)))
}

// Queries returns the first n query vectors.
func (v *Vectors) Queries(n int) [][]float32 {
	qs := make([][]float32, n)
	for i := range qs {
		qs[i] = v.Query(i)
	}
	return qs
}

// draw writes a vector around a centre into dst, which is reused if it is large enough.
func (v *Vectors) draw(dst []float32, r *rand.Rand) []float32 {
	if cap(dst) < VectorDims {
		dst = make([]float32, VectorDims)
	}
	dst = dst[:VectorDims]
	centre := v.centres[r.IntN(len(v.centres))]

	var sum float64
	for j := range dst {
		x := float64(centre[j]) + r.NormFloat64()*noise*v.scale[j]
		dst[j] = float32(x)
		sum += float64(dst[j]) * float64(dst[j])
	}
	norm := math.Sqrt(sum)
	for j := range dst {
		dst[j] = float32(float64(dst[j]) / norm)
	}
	return dst
}
