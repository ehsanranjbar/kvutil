package relations

import (
	"testing"
	"time"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/flat/flattest"
)

// The codecs of the records of the relations in FlatBuffers: round trip, partial decoding, bytes
// that are not valid, and concurrent use, on what the model generates and on the edges.
func TestFlatCodecs(t *testing.T) {
	for _, size := range []dataset.ValueSize{dataset.Small, dataset.Large} {
		m := newModel(3, 1000, size)
		var parents []parent
		var children []child
		var entities []entity
		var pairs []pairData
		for id := int64(1); id <= 20; id++ {
			var p parent
			var c child
			var e entity
			m.genParent(id, &p)
			m.genChild(id, &c)
			m.genEntity("left", id, &e)
			p.ID, c.ID, e.ID = 0, 0, 0
			parents, children, entities = append(parents, p), append(children, c), append(entities, e)
			pairs = append(pairs, m.data(id, id+3))
		}
		// (The zero time.Time is not in the range of Unix nanoseconds: the model never makes it.)
		parents = append(parents, parent{Created: time.Unix(0, 0).UTC()}, parent{Created: time.Unix(-1, 0).UTC()})
		children = append(children, child{}, child{Title: "t", Qty: -1, Payload: []byte{1}})
		entities = append(entities, entity{})
		pairs = append(pairs, pairData{}, pairData{Role: "r", Since: -1})

		t.Run("parent", func(t *testing.T) { flattest.Check(t, parentCodec(), parents) })
		t.Run("child", func(t *testing.T) { flattest.Check(t, childCodec(), children) })
		t.Run("entity", func(t *testing.T) { flattest.Check(t, entityCodec(), entities) })
		t.Run("pair", func(t *testing.T) { flattest.Check(t, pairCodec(), pairs) })
	}
}
