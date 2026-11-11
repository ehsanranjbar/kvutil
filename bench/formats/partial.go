package formats

// The record types that kvutil stores with FlatBuffers and with Cap'n Proto, for the codec.Binary
// codec: the type of the record with MarshalBinary, UnmarshalBinary and UnmarshalPartial. They are
// what a store would be given (codec.Binary[OrderFB]()) to use these formats, and so what the
// how=partial rows of the benchmarks measure.
//
//	OrderFB, BookFB, DocFB                  FlatBuffers
//	OrderCapnp, BookCapnp, DocCapnp         Cap'n Proto, a message per decode (capnp.Unmarshal), which
//	                                        is all that a store can do: it has no reader per iterator
//	OrderCapnpReuse, BookCapnpReuse, DocCapnpReuse
//	                                        Cap'n Proto, a CapnpReader from a sync.Pool per decode,
//	                                        to show what the reuse of the message could give
//
// UnmarshalPartial reads each named top-level field (the path of a Go struct field) straight from
// the bytes with the accessor of the format and copies it out: a string with string(b), a slice by
// copying. The result never refers to the input. A path that it does not know (the ID, which is
// not in the encoding, or a nested one) decodes the whole record, which the contract allows.
// *string fields have the same nil-versus-empty semantics as the full decoders.

import (
	"slices"
	"sync"
	"time"

	capnp "capnproto.org/go/capnp/v3"
	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/ehsanranjbar/kvutil/bench/formats/capgen"
	"github.com/ehsanranjbar/kvutil/bench/formats/fbgen"
	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
)

// ---------------------------------------------------------------------------------------------
// OrderBin

var orderBinFields = []string{"Code", "Customer", "Name", "Status", "Total", "Qty", "At", "Note", "Tags", "Payload"}

// UnmarshalPartial implements codec.PartialUnmarshaler. The encoding is sequential, so it walks
// the fields up to the last one that is named, and skips the ones before it without copying.
func (o *OrderBin) UnmarshalPartial(bz []byte, fields []string) error {
	var want [10]bool
	last := -1
	for _, f := range fields {
		i := slices.Index(orderBinFields, f)
		if i < 0 {
			return o.UnmarshalBinary(bz)
		}
		want[i] = true
		last = max(last, i)
	}
	strs := [4]*string{&o.Code, &o.Customer, &o.Name, &o.Status}
	r := &reader{bz: bz}
	for i := 0; i <= last && r.err == nil; i++ {
		switch {
		case i < 4:
			if b := r.bytes(); want[i] {
				*strs[i] = string(b)
			}
		case i == 4:
			if v := r.varint(); want[i] {
				o.Total = v
			}
		case i == 5:
			if v := r.varint(); want[i] {
				o.Qty = int32(v)
			}
		case i == 6:
			if v := r.varint(); want[i] {
				o.At = time.Unix(0, v).UTC()
			}
		case i == 7:
			if len(r.bz) == 0 {
				return errShort
			}
			flag := r.bz[0]
			r.bz = r.bz[1:]
			if flag == 1 {
				if note := r.string(); want[i] {
					o.Note = &note
				}
			}
		case i == 8:
			n := r.uvarint()
			if r.err == nil && n > uint64(len(r.bz)) {
				return errShort
			}
			if want[i] && n > 0 {
				o.Tags = make([]string, n)
			}
			for j := range n {
				if b := r.bytes(); want[i] && r.err == nil {
					o.Tags[j] = string(b)
				}
			}
		default:
			if b := r.bytes(); want[i] && len(b) > 0 {
				o.Payload = append([]byte(nil), b...)
			}
		}
	}
	return r.err
}

// ---------------------------------------------------------------------------------------------
// FlatBuffers

func fbStrings(n int, at func(int) []byte) []string {
	if n <= 0 {
		return nil
	}
	ss := make([]string, n)
	for i := range ss {
		ss[i] = string(at(i))
	}
	return ss
}

// OrderFB is an Order stored as a FlatBuffer.
type OrderFB dataset.Order

// MarshalBinary implements encoding.BinaryMarshaler.
func (o *OrderFB) MarshalBinary() ([]byte, error) {
	b := flatbuffers.NewBuilder(1024)
	BuildOrderFB(b, (*dataset.Order)(o))
	return b.FinishedBytes(), nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (o *OrderFB) UnmarshalBinary(bz []byte) error {
	d, err := DecodeOrderFB(bz)
	*o = OrderFB(d)
	return err
}

// UnmarshalPartial implements codec.PartialUnmarshaler.
func (o *OrderFB) UnmarshalPartial(data []byte, fields []string) (err error) {
	defer recoverFB(&err)
	t := fbgen.GetRootAsOrder(data, 0)
	for _, f := range fields {
		switch f {
		case "Code":
			o.Code = string(t.Code())
		case "Customer":
			o.Customer = string(t.Customer())
		case "Name":
			o.Name = string(t.Name())
		case "Status":
			o.Status = string(t.Status())
		case "Total":
			o.Total = t.Total()
		case "Qty":
			o.Qty = t.Qty()
		case "At":
			o.At = time.Unix(0, t.At()).UTC()
		case "Note":
			if n := t.Note(); n != nil {
				s := string(n)
				o.Note = &s
			}
		case "Tags":
			o.Tags = fbStrings(t.TagsLength(), t.Tags)
		case "Payload":
			o.Payload = copyBytes(t.PayloadBytes())
		default:
			return o.UnmarshalBinary(data)
		}
	}
	return nil
}

// BookFB is a Book stored as a FlatBuffer.
type BookFB dataset.Book

// MarshalBinary implements encoding.BinaryMarshaler.
func (bk *BookFB) MarshalBinary() ([]byte, error) {
	b := flatbuffers.NewBuilder(1024)
	BuildBookFB(b, (*dataset.Book)(bk))
	return b.FinishedBytes(), nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (bk *BookFB) UnmarshalBinary(bz []byte) error {
	d, err := DecodeBookFB(bz)
	*bk = BookFB(d)
	return err
}

// UnmarshalPartial implements codec.PartialUnmarshaler.
func (bk *BookFB) UnmarshalPartial(data []byte, fields []string) (err error) {
	defer recoverFB(&err)
	t := fbgen.GetRootAsBook(data, 0)
	for _, f := range fields {
		switch f {
		case "Code":
			bk.Code = string(t.Code())
		case "Author":
			bk.Author = string(t.Author())
		case "Year":
			bk.Year = t.Year()
		case "Price":
			bk.Price = t.Price()
		case "InPrint":
			bk.InPrint = t.InPrint()
		case "Tags":
			bk.Tags = fbStrings(t.TagsLength(), t.Tags)
		case "Title":
			bk.Title = string(t.Title())
		case "Subtitle":
			if n := t.Subtitle(); n != nil {
				s := string(n)
				bk.Subtitle = &s
			}
		case "Score":
			bk.Score = t.Score()
		case "Payload":
			bk.Payload = copyBytes(t.PayloadBytes())
		default:
			return bk.UnmarshalBinary(data)
		}
	}
	return nil
}

// DocFB is a Doc stored as a FlatBuffer.
type DocFB dataset.Doc

// MarshalBinary implements encoding.BinaryMarshaler.
func (d *DocFB) MarshalBinary() ([]byte, error) {
	b := flatbuffers.NewBuilder(1024)
	BuildDocFB(b, (*dataset.Doc)(d))
	return b.FinishedBytes(), nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (d *DocFB) UnmarshalBinary(bz []byte) error {
	v, err := DecodeDocFB(bz)
	*d = DocFB(v)
	return err
}

// UnmarshalPartial implements codec.PartialUnmarshaler.
func (d *DocFB) UnmarshalPartial(data []byte, fields []string) (err error) {
	defer recoverFB(&err)
	t := fbgen.GetRootAsDoc(data, 0)
	for _, f := range fields {
		switch f {
		case "Lang":
			d.Lang = string(t.Lang())
		case "Year":
			d.Year = int(t.Year())
		case "Embedding":
			if n := t.EmbeddingLength(); n > 0 {
				d.Embedding = make([]float32, n)
				for i := range d.Embedding {
					d.Embedding[i] = t.Embedding(i)
				}
			}
		default:
			return d.UnmarshalBinary(data)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------------------------
// Cap'n Proto

// readers is the pool of the readers of the Reuse types. A reader is not safe for concurrent
// use, so a decode takes one out of the pool and puts it back.
var readers = sync.Pool{New: func() any { return new(CapnpReader) }}

// openPooled opens the message of an encoding with a reader of the pool. The caller calls
// release when it is done with the message, which refers to the data until then.
func openPooled(data []byte) (*capnp.Message, *CapnpReader, error) {
	r := readers.Get().(*CapnpReader)
	msg, err := r.Open(data)
	if err != nil {
		release(r)
		return nil, nil, err
	}
	return msg, r, nil
}

func release(r *CapnpReader) {
	r.Close()
	readers.Put(r)
}

// Close drops the reference of the reader to the data of the last message.
func (r *CapnpReader) Close() {
	_, _ = r.msg.Reset(nil)
}

func unmarshalCapnp(bz []byte, pooled bool, decode func(*capnp.Message) error) error {
	if !pooled {
		msg, err := capnp.Unmarshal(bz)
		if err != nil {
			return err
		}
		return decode(msg)
	}
	msg, r, err := openPooled(bz)
	if err != nil {
		return err
	}
	defer release(r)
	return decode(msg)
}

// OrderCapnp is an Order stored as a Cap'n Proto message. A decode makes a message with
// capnp.Unmarshal, as a store has to.
type OrderCapnp dataset.Order

// MarshalBinary implements encoding.BinaryMarshaler.
func (o *OrderCapnp) MarshalBinary() ([]byte, error) {
	return (&CapnpWriter{}).EncodeOrder((*dataset.Order)(o))
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (o *OrderCapnp) UnmarshalBinary(bz []byte) error {
	return unmarshalCapnp(bz, false, func(m *capnp.Message) error {
		d, err := DecodeOrderCapnp(m)
		*o = OrderCapnp(d)
		return err
	})
}

// UnmarshalPartial implements codec.PartialUnmarshaler. It makes a message too: the allocations of
// capnp.Unmarshal are the cost of this API for Cap'n Proto.
func (o *OrderCapnp) UnmarshalPartial(data []byte, fields []string) error {
	return unmarshalCapnp(data, false, func(m *capnp.Message) error {
		return partialOrderCapnp((*dataset.Order)(o), m, fields)
	})
}

// OrderCapnpReuse is OrderCapnp that reads with a pooled CapnpReader, which reuses a message and
// its arena for all the decodes.
type OrderCapnpReuse dataset.Order

// MarshalBinary implements encoding.BinaryMarshaler.
func (o *OrderCapnpReuse) MarshalBinary() ([]byte, error) {
	return (&CapnpWriter{}).EncodeOrder((*dataset.Order)(o))
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (o *OrderCapnpReuse) UnmarshalBinary(bz []byte) error {
	return unmarshalCapnp(bz, true, func(m *capnp.Message) error {
		d, err := DecodeOrderCapnp(m)
		*o = OrderCapnpReuse(d)
		return err
	})
}

// UnmarshalPartial implements codec.PartialUnmarshaler.
func (o *OrderCapnpReuse) UnmarshalPartial(data []byte, fields []string) error {
	return unmarshalCapnp(data, true, func(m *capnp.Message) error {
		return partialOrderCapnp((*dataset.Order)(o), m, fields)
	})
}

func partialOrderCapnp(o *dataset.Order, msg *capnp.Message, fields []string) error {
	c, err := capgen.ReadRootOrder(msg)
	if err != nil {
		return err
	}
	var a capnpAccess
	for _, f := range fields {
		switch f {
		case "Code":
			o.Code = a.str(c.Code())
		case "Customer":
			o.Customer = a.str(c.Customer())
		case "Name":
			o.Name = a.str(c.Name())
		case "Status":
			o.Status = a.str(c.Status())
		case "Total":
			o.Total = c.Total()
		case "Qty":
			o.Qty = c.Qty()
		case "At":
			o.At = time.Unix(0, c.At()).UTC()
		case "Note":
			if c.NoteSet() {
				s := a.str(c.Note())
				o.Note = &s
			}
		case "Tags":
			o.Tags = a.tags(c.Tags())
		case "Payload":
			o.Payload = a.data(c.Payload())
		default:
			d, err := DecodeOrderCapnp(msg)
			*o = d
			return err
		}
	}
	return a.err
}

// BookCapnp is a Book stored as a Cap'n Proto message.
type BookCapnp dataset.Book

// MarshalBinary implements encoding.BinaryMarshaler.
func (bk *BookCapnp) MarshalBinary() ([]byte, error) {
	return (&CapnpWriter{}).EncodeBook((*dataset.Book)(bk))
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (bk *BookCapnp) UnmarshalBinary(bz []byte) error {
	return unmarshalCapnp(bz, false, func(m *capnp.Message) error {
		d, err := DecodeBookCapnp(m)
		*bk = BookCapnp(d)
		return err
	})
}

// UnmarshalPartial implements codec.PartialUnmarshaler.
func (bk *BookCapnp) UnmarshalPartial(data []byte, fields []string) error {
	return unmarshalCapnp(data, false, func(m *capnp.Message) error {
		return partialBookCapnp((*dataset.Book)(bk), m, fields)
	})
}

// BookCapnpReuse is BookCapnp that reads with a pooled CapnpReader.
type BookCapnpReuse dataset.Book

// MarshalBinary implements encoding.BinaryMarshaler.
func (bk *BookCapnpReuse) MarshalBinary() ([]byte, error) {
	return (&CapnpWriter{}).EncodeBook((*dataset.Book)(bk))
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (bk *BookCapnpReuse) UnmarshalBinary(bz []byte) error {
	return unmarshalCapnp(bz, true, func(m *capnp.Message) error {
		d, err := DecodeBookCapnp(m)
		*bk = BookCapnpReuse(d)
		return err
	})
}

// UnmarshalPartial implements codec.PartialUnmarshaler.
func (bk *BookCapnpReuse) UnmarshalPartial(data []byte, fields []string) error {
	return unmarshalCapnp(data, true, func(m *capnp.Message) error {
		return partialBookCapnp((*dataset.Book)(bk), m, fields)
	})
}

func partialBookCapnp(bk *dataset.Book, msg *capnp.Message, fields []string) error {
	c, err := capgen.ReadRootBook(msg)
	if err != nil {
		return err
	}
	var a capnpAccess
	for _, f := range fields {
		switch f {
		case "Code":
			bk.Code = a.str(c.Code())
		case "Author":
			bk.Author = a.str(c.Author())
		case "Year":
			bk.Year = c.Year()
		case "Price":
			bk.Price = c.Price()
		case "InPrint":
			bk.InPrint = c.InPrint()
		case "Tags":
			bk.Tags = a.tags(c.Tags())
		case "Title":
			bk.Title = a.str(c.Title())
		case "Subtitle":
			if c.SubtitleSet() {
				s := a.str(c.Subtitle())
				bk.Subtitle = &s
			}
		case "Score":
			bk.Score = c.Score()
		case "Payload":
			bk.Payload = a.data(c.Payload())
		default:
			d, err := DecodeBookCapnp(msg)
			*bk = d
			return err
		}
	}
	return a.err
}

// DocCapnp is a Doc stored as a Cap'n Proto message.
type DocCapnp dataset.Doc

// MarshalBinary implements encoding.BinaryMarshaler.
func (d *DocCapnp) MarshalBinary() ([]byte, error) {
	return (&CapnpWriter{}).EncodeDoc((*dataset.Doc)(d))
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (d *DocCapnp) UnmarshalBinary(bz []byte) error {
	return unmarshalCapnp(bz, false, func(m *capnp.Message) error {
		v, err := DecodeDocCapnp(m)
		*d = DocCapnp(v)
		return err
	})
}

// UnmarshalPartial implements codec.PartialUnmarshaler.
func (d *DocCapnp) UnmarshalPartial(data []byte, fields []string) error {
	return unmarshalCapnp(data, false, func(m *capnp.Message) error {
		return partialDocCapnp((*dataset.Doc)(d), m, fields)
	})
}

// DocCapnpReuse is DocCapnp that reads with a pooled CapnpReader.
type DocCapnpReuse dataset.Doc

// MarshalBinary implements encoding.BinaryMarshaler.
func (d *DocCapnpReuse) MarshalBinary() ([]byte, error) {
	return (&CapnpWriter{}).EncodeDoc((*dataset.Doc)(d))
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (d *DocCapnpReuse) UnmarshalBinary(bz []byte) error {
	return unmarshalCapnp(bz, true, func(m *capnp.Message) error {
		v, err := DecodeDocCapnp(m)
		*d = DocCapnpReuse(v)
		return err
	})
}

// UnmarshalPartial implements codec.PartialUnmarshaler.
func (d *DocCapnpReuse) UnmarshalPartial(data []byte, fields []string) error {
	return unmarshalCapnp(data, true, func(m *capnp.Message) error {
		return partialDocCapnp((*dataset.Doc)(d), m, fields)
	})
}

func partialDocCapnp(d *dataset.Doc, msg *capnp.Message, fields []string) error {
	c, err := capgen.ReadRootDoc(msg)
	if err != nil {
		return err
	}
	var a capnpAccess
	for _, f := range fields {
		switch f {
		case "Lang":
			d.Lang = a.str(c.Lang())
		case "Year":
			d.Year = int(c.Year())
		case "Embedding":
			d.Embedding = a.floats(c.Embedding())
		default:
			v, err := DecodeDocCapnp(msg)
			*d = v
			return err
		}
	}
	return a.err
}
