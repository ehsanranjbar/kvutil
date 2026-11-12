package relations

import (
	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/ehsanranjbar/kvutil/bench/internal/flat"
	"github.com/ehsanranjbar/kvutil/bench/internal/flat/fbgen"
	"github.com/ehsanranjbar/kvutil/codec"
)

// The codecs of the records of the relations in FlatBuffers (the tables of
// bench/internal/flat/schema/records.fbs). The ids are not in the values.

var notStored = []string{"ID"}

var parentFlat = flat.New(flat.Def[parent]{
	Build: func(b *flatbuffers.Builder, p parent) {
		name := b.CreateString(p.Name)
		region := b.CreateString(p.Region)
		fbgen.ParentStart(b)
		fbgen.ParentAddName(b, name)
		fbgen.ParentAddRegion(b, region)
		fbgen.ParentAddCreated(b, p.Created.UnixNano())
		fbgen.FinishParentBuffer(b, fbgen.ParentEnd(b))
	},
	Read: func(bz []byte) (p parent) {
		t := fbgen.GetRootAsParent(bz, 0)
		p.Name = string(t.Name())
		p.Region = string(t.Region())
		p.Created = flat.Time(t.Created())
		return p
	},
	NotStored: notStored,
	Fields: map[string]func(bz []byte, p *parent){
		"Name":    func(bz []byte, p *parent) { p.Name = string(fbgen.GetRootAsParent(bz, 0).Name()) },
		"Region":  func(bz []byte, p *parent) { p.Region = string(fbgen.GetRootAsParent(bz, 0).Region()) },
		"Created": func(bz []byte, p *parent) { p.Created = flat.Time(fbgen.GetRootAsParent(bz, 0).Created()) },
	},
})

var childFlat = flat.New(flat.Def[child]{
	Build: func(b *flatbuffers.Builder, c child) {
		title := b.CreateString(c.Title)
		status := b.CreateString(c.Status)
		payload := flat.CreateBytes(b, c.Payload)
		fbgen.ChildStart(b)
		fbgen.ChildAddTitle(b, title)
		fbgen.ChildAddQty(b, c.Qty)
		fbgen.ChildAddStatus(b, status)
		fbgen.ChildAddPayload(b, payload)
		fbgen.FinishChildBuffer(b, fbgen.ChildEnd(b))
	},
	Read: func(bz []byte) (c child) {
		t := fbgen.GetRootAsChild(bz, 0)
		c.Title = string(t.Title())
		c.Qty = t.Qty()
		c.Status = string(t.Status())
		c.Payload = flat.CopyBytes(t.PayloadBytes())
		return c
	},
	NotStored: notStored,
	Fields: map[string]func(bz []byte, c *child){
		"Title":   func(bz []byte, c *child) { c.Title = string(fbgen.GetRootAsChild(bz, 0).Title()) },
		"Qty":     func(bz []byte, c *child) { c.Qty = fbgen.GetRootAsChild(bz, 0).Qty() },
		"Status":  func(bz []byte, c *child) { c.Status = string(fbgen.GetRootAsChild(bz, 0).Status()) },
		"Payload": func(bz []byte, c *child) { c.Payload = flat.CopyBytes(fbgen.GetRootAsChild(bz, 0).PayloadBytes()) },
	},
})

var entityFlat = flat.New(flat.Def[entity]{
	Build: func(b *flatbuffers.Builder, e entity) {
		name := b.CreateString(e.Name)
		kind := b.CreateString(e.Kind)
		fbgen.EntityStart(b)
		fbgen.EntityAddName(b, name)
		fbgen.EntityAddKind(b, kind)
		fbgen.FinishEntityBuffer(b, fbgen.EntityEnd(b))
	},
	Read: func(bz []byte) (e entity) {
		t := fbgen.GetRootAsEntity(bz, 0)
		e.Name = string(t.Name())
		e.Kind = string(t.Kind())
		return e
	},
	NotStored: notStored,
	Fields: map[string]func(bz []byte, e *entity){
		"Name": func(bz []byte, e *entity) { e.Name = string(fbgen.GetRootAsEntity(bz, 0).Name()) },
		"Kind": func(bz []byte, e *entity) { e.Kind = string(fbgen.GetRootAsEntity(bz, 0).Kind()) },
	},
})

var pairFlat = flat.New(flat.Def[pairData]{
	Build: func(b *flatbuffers.Builder, d pairData) {
		role := b.CreateString(d.Role)
		fbgen.PairDataStart(b)
		fbgen.PairDataAddRole(b, role)
		fbgen.PairDataAddSince(b, d.Since)
		fbgen.FinishPairDataBuffer(b, fbgen.PairDataEnd(b))
	},
	Read: func(bz []byte) (d pairData) {
		t := fbgen.GetRootAsPairData(bz, 0)
		d.Role = string(t.Role())
		d.Since = t.Since()
		return d
	},
	Fields: map[string]func(bz []byte, d *pairData){
		"Role":  func(bz []byte, d *pairData) { d.Role = string(fbgen.GetRootAsPairData(bz, 0).Role()) },
		"Since": func(bz []byte, d *pairData) { d.Since = fbgen.GetRootAsPairData(bz, 0).Since() },
	},
})

// parentCodec, childCodec, entityCodec and pairCodec are the codecs of the records of the
// relations in FlatBuffers, with partial decoding.
func parentCodec() codec.ValueCodec[parent] { return parentFlat }
func childCodec() codec.ValueCodec[child]   { return childFlat }
func entityCodec() codec.ValueCodec[entity] { return entityFlat }
func pairCodec() codec.ValueCodec[pairData] { return pairFlat }
