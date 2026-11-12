package lifecycle

import (
	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/ehsanranjbar/kvutil/bench/internal/flat"
	"github.com/ehsanranjbar/kvutil/bench/internal/flat/fbgen"
	"github.com/ehsanranjbar/kvutil/codec"
)

// The codecs of the versions of the order in FlatBuffers (the tables OrderV2 and OrderV3 of
// bench/internal/flat/schema/records.fbs), the version 1 being flat.Order.
//
// The two tables have the same fields in the same places, and OrderV3 has one more at the end.
// That is the whole point of the change that needs no migration: a record that was written as an
// OrderV2 reads as an OrderV3 whose extra is empty (a FlatBuffers reader finds no field where the
// writer's table was shorter), and one written as an OrderV3 reads as an OrderV2 that does not
// know of extra, as msgpack decodes a map into a struct without the key. They are two tables, and
// not one with extra, so that each version of the program has the schema of its own: the V2 codec
// never sees extra, and the V3 codec writes it only when it is not empty.

var notStored = []string{"ID"}

var orderV2Table = flat.New(flat.Def[orderV2]{
	Build: func(b *flatbuffers.Builder, o orderV2) {
		code := b.CreateString(o.Code)
		customer := b.CreateString(o.Customer)
		name := b.CreateString(o.Name)
		status := b.CreateString(o.Status)
		note := flat.CreateOptString(b, o.Note)
		tags := flat.CreateStrings(b, o.Tags, fbgen.OrderV2StartTagsVector)
		payload := flat.CreateBytes(b, o.Payload)

		fbgen.OrderV2Start(b)
		fbgen.OrderV2AddCode(b, code)
		fbgen.OrderV2AddCustomer(b, customer)
		fbgen.OrderV2AddName(b, name)
		fbgen.OrderV2AddStatus(b, status)
		fbgen.OrderV2AddTotal(b, o.Total)
		fbgen.OrderV2AddQty(b, o.Qty)
		fbgen.OrderV2AddAt(b, o.At.UnixNano())
		fbgen.OrderV2AddNote(b, note)
		fbgen.OrderV2AddTags(b, tags)
		fbgen.OrderV2AddPayload(b, payload)
		fbgen.OrderV2AddPriority(b, o.Priority)
		fbgen.FinishOrderV2Buffer(b, fbgen.OrderV2End(b))
	},
	Read: func(bz []byte) (o orderV2) {
		t := fbgen.GetRootAsOrderV2(bz, 0)
		o.Code = string(t.Code())
		o.Customer = string(t.Customer())
		o.Name = string(t.Name())
		o.Status = string(t.Status())
		o.Total = t.Total()
		o.Qty = t.Qty()
		o.At = flat.Time(t.At())
		o.Note = flat.OptString(t.Note())
		o.Tags = flat.Strings(bz, t.TagsLength(), t.Tags)
		o.Payload = flat.CopyBytes(t.PayloadBytes())
		o.Priority = t.Priority()
		return o
	},
	NotStored: notStored,
	Fields: map[string]func(bz []byte, o *orderV2){
		"Code":     func(bz []byte, o *orderV2) { o.Code = string(fbgen.GetRootAsOrderV2(bz, 0).Code()) },
		"Customer": func(bz []byte, o *orderV2) { o.Customer = string(fbgen.GetRootAsOrderV2(bz, 0).Customer()) },
		"Name":     func(bz []byte, o *orderV2) { o.Name = string(fbgen.GetRootAsOrderV2(bz, 0).Name()) },
		"Status":   func(bz []byte, o *orderV2) { o.Status = string(fbgen.GetRootAsOrderV2(bz, 0).Status()) },
		"Total":    func(bz []byte, o *orderV2) { o.Total = fbgen.GetRootAsOrderV2(bz, 0).Total() },
		"Qty":      func(bz []byte, o *orderV2) { o.Qty = fbgen.GetRootAsOrderV2(bz, 0).Qty() },
		"At":       func(bz []byte, o *orderV2) { o.At = flat.Time(fbgen.GetRootAsOrderV2(bz, 0).At()) },
		"Note":     func(bz []byte, o *orderV2) { o.Note = flat.OptString(fbgen.GetRootAsOrderV2(bz, 0).Note()) },
		"Tags": func(bz []byte, o *orderV2) {
			t := fbgen.GetRootAsOrderV2(bz, 0)
			o.Tags = flat.Strings(bz, t.TagsLength(), t.Tags)
		},
		"Payload":  func(bz []byte, o *orderV2) { o.Payload = flat.CopyBytes(fbgen.GetRootAsOrderV2(bz, 0).PayloadBytes()) },
		"Priority": func(bz []byte, o *orderV2) { o.Priority = fbgen.GetRootAsOrderV2(bz, 0).Priority() },
	},
})

var orderV3Table = flat.New(flat.Def[orderV3Flat]{
	Build: func(b *flatbuffers.Builder, o orderV3Flat) {
		code := b.CreateString(o.Code)
		customer := b.CreateString(o.Customer)
		name := b.CreateString(o.Name)
		status := b.CreateString(o.Status)
		note := flat.CreateOptString(b, o.Note)
		tags := flat.CreateStrings(b, o.Tags, fbgen.OrderV3StartTagsVector)
		payload := flat.CreateBytes(b, o.Payload)
		var extra flatbuffers.UOffsetT
		if o.Extra != "" {
			extra = b.CreateString(o.Extra)
		}

		fbgen.OrderV3Start(b)
		fbgen.OrderV3AddCode(b, code)
		fbgen.OrderV3AddCustomer(b, customer)
		fbgen.OrderV3AddName(b, name)
		fbgen.OrderV3AddStatus(b, status)
		fbgen.OrderV3AddTotal(b, o.Total)
		fbgen.OrderV3AddQty(b, o.Qty)
		fbgen.OrderV3AddAt(b, o.At.UnixNano())
		fbgen.OrderV3AddNote(b, note)
		fbgen.OrderV3AddTags(b, tags)
		fbgen.OrderV3AddPayload(b, payload)
		fbgen.OrderV3AddPriority(b, o.Priority)
		fbgen.OrderV3AddExtra(b, extra)
		fbgen.FinishOrderV3Buffer(b, fbgen.OrderV3End(b))
	},
	Read: func(bz []byte) (o orderV3Flat) {
		t := fbgen.GetRootAsOrderV3(bz, 0)
		o.Code = string(t.Code())
		o.Customer = string(t.Customer())
		o.Name = string(t.Name())
		o.Status = string(t.Status())
		o.Total = t.Total()
		o.Qty = t.Qty()
		o.At = flat.Time(t.At())
		o.Note = flat.OptString(t.Note())
		o.Tags = flat.Strings(bz, t.TagsLength(), t.Tags)
		o.Payload = flat.CopyBytes(t.PayloadBytes())
		o.Priority = t.Priority()
		o.Extra = string(t.Extra())
		return o
	},
	NotStored: notStored,
	Fields: map[string]func(bz []byte, o *orderV3Flat){
		"Code":     func(bz []byte, o *orderV3Flat) { o.Code = string(fbgen.GetRootAsOrderV3(bz, 0).Code()) },
		"Customer": func(bz []byte, o *orderV3Flat) { o.Customer = string(fbgen.GetRootAsOrderV3(bz, 0).Customer()) },
		"Name":     func(bz []byte, o *orderV3Flat) { o.Name = string(fbgen.GetRootAsOrderV3(bz, 0).Name()) },
		"Status":   func(bz []byte, o *orderV3Flat) { o.Status = string(fbgen.GetRootAsOrderV3(bz, 0).Status()) },
		"Total":    func(bz []byte, o *orderV3Flat) { o.Total = fbgen.GetRootAsOrderV3(bz, 0).Total() },
		"Qty":      func(bz []byte, o *orderV3Flat) { o.Qty = fbgen.GetRootAsOrderV3(bz, 0).Qty() },
		"At":       func(bz []byte, o *orderV3Flat) { o.At = flat.Time(fbgen.GetRootAsOrderV3(bz, 0).At()) },
		"Note":     func(bz []byte, o *orderV3Flat) { o.Note = flat.OptString(fbgen.GetRootAsOrderV3(bz, 0).Note()) },
		"Tags": func(bz []byte, o *orderV3Flat) {
			t := fbgen.GetRootAsOrderV3(bz, 0)
			o.Tags = flat.Strings(bz, t.TagsLength(), t.Tags)
		},
		"Payload": func(bz []byte, o *orderV3Flat) {
			o.Payload = flat.CopyBytes(fbgen.GetRootAsOrderV3(bz, 0).PayloadBytes())
		},
		"Priority": func(bz []byte, o *orderV3Flat) { o.Priority = fbgen.GetRootAsOrderV3(bz, 0).Priority() },
		"Extra":    func(bz []byte, o *orderV3Flat) { o.Extra = string(fbgen.GetRootAsOrderV3(bz, 0).Extra()) },
	},
})

// orderV2Codec and orderV3Codec are the codecs of the orders of the versions 2 and 3 in
// FlatBuffers, with partial decoding.
func orderV2Codec() codec.ValueCodec[orderV2]     { return orderV2Table }
func orderV3Codec() codec.ValueCodec[orderV3Flat] { return orderV3Table }
