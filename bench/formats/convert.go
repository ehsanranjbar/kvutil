// Package formats converts the records of the benchmarks (bench/internal/dataset) to the value
// formats that are compared (FlatBuffers and Cap'n Proto) and back. It is a throwaway module that
// answers one question: which of the two formats suits the stored records of kvutil. The id of a
// record is not in any of the formats: kvutil keeps it in the key.
//
// Mapping, the same in both formats:
//   - time.Time is Unix nanoseconds (int64). The dataset has second resolution in UTC, and a
//     decoded time is time.Unix(0, n).UTC(), which equals the generated one.
//   - an empty list (Tags, Payload, Embedding) is nil once decoded, as the dataset makes them.
//   - a string that is optional (Order.Note, Book.Subtitle, a *string in Go) is, in FlatBuffers,
//     an absent field for nil and a present one, even an empty string, for non-nil. Cap'n Proto
//     has no optional Text (an unset one reads as ""), so a Bool next to it (noteSet,
//     subtitleSet) says whether it is there.
//
// The decoders copy everything out of the input (strings, byte slices and float slices), because
// the ValueCodec contract of kvutil says that the result does not refer to the given slice.
package formats

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	capnp "capnproto.org/go/capnp/v3"
	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/ehsanranjbar/kvutil/bench/formats/capgen"
	"github.com/ehsanranjbar/kvutil/bench/formats/fbgen"
	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
)

// ---------------------------------------------------------------------------------------------
// FlatBuffers
//
// The encoders use the builder by hand (the generated Start/Add/End functions). The decoders read
// with the generated accessors and not with the object API (UnPack): that one maps an absent
// string to "" and so loses the nil of Order.Note and Book.Subtitle.

// BuildOrderFB builds the record into an empty builder and finishes it: the encoding is
// b.FinishedBytes(), a slice of the builder's buffer.
func BuildOrderFB(b *flatbuffers.Builder, o *dataset.Order) {
	code := b.CreateString(o.Code)
	customer := b.CreateString(o.Customer)
	name := b.CreateString(o.Name)
	status := b.CreateString(o.Status)
	var note flatbuffers.UOffsetT
	if o.Note != nil {
		note = b.CreateString(*o.Note)
	}
	tags := createStringVector(b, o.Tags, fbgen.OrderStartTagsVector)
	var payload flatbuffers.UOffsetT
	if len(o.Payload) > 0 {
		payload = b.CreateByteVector(o.Payload)
	}

	fbgen.OrderStart(b)
	fbgen.OrderAddCode(b, code)
	fbgen.OrderAddCustomer(b, customer)
	fbgen.OrderAddName(b, name)
	fbgen.OrderAddStatus(b, status)
	fbgen.OrderAddTotal(b, o.Total)
	fbgen.OrderAddQty(b, o.Qty)
	fbgen.OrderAddAt(b, o.At.UnixNano())
	if o.Note != nil {
		fbgen.OrderAddNote(b, note)
	}
	if tags != 0 {
		fbgen.OrderAddTags(b, tags)
	}
	if payload != 0 {
		fbgen.OrderAddPayload(b, payload)
	}
	fbgen.FinishOrderBuffer(b, fbgen.OrderEnd(b))
}

// BuildBookFB is BuildOrderFB for a book.
func BuildBookFB(b *flatbuffers.Builder, bk *dataset.Book) {
	code := b.CreateString(bk.Code)
	author := b.CreateString(bk.Author)
	title := b.CreateString(bk.Title)
	var subtitle flatbuffers.UOffsetT
	if bk.Subtitle != nil {
		subtitle = b.CreateString(*bk.Subtitle)
	}
	tags := createStringVector(b, bk.Tags, fbgen.BookStartTagsVector)
	var payload flatbuffers.UOffsetT
	if len(bk.Payload) > 0 {
		payload = b.CreateByteVector(bk.Payload)
	}

	fbgen.BookStart(b)
	fbgen.BookAddCode(b, code)
	fbgen.BookAddAuthor(b, author)
	fbgen.BookAddYear(b, bk.Year)
	fbgen.BookAddPrice(b, bk.Price)
	fbgen.BookAddInPrint(b, bk.InPrint)
	if tags != 0 {
		fbgen.BookAddTags(b, tags)
	}
	fbgen.BookAddTitle(b, title)
	if bk.Subtitle != nil {
		fbgen.BookAddSubtitle(b, subtitle)
	}
	fbgen.BookAddScore(b, bk.Score)
	if payload != 0 {
		fbgen.BookAddPayload(b, payload)
	}
	fbgen.FinishBookBuffer(b, fbgen.BookEnd(b))
}

// BuildDocFB is BuildOrderFB for a document.
func BuildDocFB(b *flatbuffers.Builder, d *dataset.Doc) {
	lang := b.CreateString(d.Lang)
	var emb flatbuffers.UOffsetT
	if len(d.Embedding) > 0 {
		fbgen.DocStartEmbeddingVector(b, len(d.Embedding))
		for i := len(d.Embedding) - 1; i >= 0; i-- {
			b.PrependFloat32(d.Embedding[i])
		}
		emb = b.EndVector(len(d.Embedding))
	}

	fbgen.DocStart(b)
	fbgen.DocAddLang(b, lang)
	fbgen.DocAddYear(b, int64(d.Year))
	if emb != 0 {
		fbgen.DocAddEmbedding(b, emb)
	}
	fbgen.FinishDocBuffer(b, fbgen.DocEnd(b))
}

func createStringVector(b *flatbuffers.Builder, ss []string, start func(*flatbuffers.Builder, int) flatbuffers.UOffsetT) flatbuffers.UOffsetT {
	if len(ss) == 0 {
		return 0
	}
	var buf [8]flatbuffers.UOffsetT
	offs := buf[:0]
	for _, s := range ss {
		offs = append(offs, b.CreateString(s))
	}
	start(b, len(offs))
	for i := len(offs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offs[i])
	}
	return b.EndVector(len(offs))
}

// EncodeOrderFB encodes a record with a builder that is reused: it resets the builder first, and
// returns a copy of the encoding that the caller owns (the builder's buffer is reused by the next
// call).
func EncodeOrderFB(b *flatbuffers.Builder, o *dataset.Order) []byte {
	b.Reset()
	BuildOrderFB(b, o)
	return bytes.Clone(b.FinishedBytes())
}

// EncodeBookFB is EncodeOrderFB for a book.
func EncodeBookFB(b *flatbuffers.Builder, bk *dataset.Book) []byte {
	b.Reset()
	BuildBookFB(b, bk)
	return bytes.Clone(b.FinishedBytes())
}

// EncodeDocFB is EncodeOrderFB for a document.
func EncodeDocFB(b *flatbuffers.Builder, d *dataset.Doc) []byte {
	b.Reset()
	BuildDocFB(b, d)
	return bytes.Clone(b.FinishedBytes())
}

// recoverFB turns the panic of the accessors on a buffer that is corrupt into an error: the
// FlatBuffers Go accessors do not check the bounds and panic with an index out of range.
func recoverFB(err *error) {
	if r := recover(); r != nil {
		*err = fmt.Errorf("formats: corrupt flatbuffer: %v", r)
	}
}

func copyBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return append([]byte(nil), b...)
}

// DecodeOrderFB decodes a record, copying out of bz.
func DecodeOrderFB(bz []byte) (o dataset.Order, err error) {
	defer recoverFB(&err)
	t := fbgen.GetRootAsOrder(bz, 0)
	o.Code = string(t.Code())
	o.Customer = string(t.Customer())
	o.Name = string(t.Name())
	o.Status = string(t.Status())
	o.Total = t.Total()
	o.Qty = t.Qty()
	o.At = time.Unix(0, t.At()).UTC()
	if n := t.Note(); n != nil { // absent is nil, and present (even empty) is not
		s := string(n)
		o.Note = &s
	}
	if n := t.TagsLength(); n > 0 {
		o.Tags = make([]string, n)
		for i := range o.Tags {
			o.Tags[i] = string(t.Tags(i))
		}
	}
	o.Payload = copyBytes(t.PayloadBytes())
	return o, nil
}

// DecodeBookFB decodes a book, copying out of bz.
func DecodeBookFB(bz []byte) (bk dataset.Book, err error) {
	defer recoverFB(&err)
	t := fbgen.GetRootAsBook(bz, 0)
	bk.Code = string(t.Code())
	bk.Author = string(t.Author())
	bk.Year = t.Year()
	bk.Price = t.Price()
	bk.InPrint = t.InPrint()
	if n := t.TagsLength(); n > 0 {
		bk.Tags = make([]string, n)
		for i := range bk.Tags {
			bk.Tags[i] = string(t.Tags(i))
		}
	}
	bk.Title = string(t.Title())
	if n := t.Subtitle(); n != nil {
		s := string(n)
		bk.Subtitle = &s
	}
	bk.Score = t.Score()
	bk.Payload = copyBytes(t.PayloadBytes())
	return bk, nil
}

// DecodeDocFB decodes a document, copying out of bz.
func DecodeDocFB(bz []byte) (d dataset.Doc, err error) {
	defer recoverFB(&err)
	t := fbgen.GetRootAsDoc(bz, 0)
	d.Lang = string(t.Lang())
	d.Year = int(t.Year())
	if n := t.EmbeddingLength(); n > 0 {
		d.Embedding = make([]float32, n)
		for i := range d.Embedding {
			d.Embedding[i] = t.Embedding(i)
		}
	}
	return d, nil
}

// ---------------------------------------------------------------------------------------------
// Cap'n Proto

// CapnpWriter encodes records into messages of one segment.
type CapnpWriter struct {
	// Reuse keeps one Message across the records, and so the pooled arena it releases and takes
	// again, instead of a message per record. The encoding is a copy that the caller owns either way.
	Reuse bool
	// Packed encodes with the packed form of the stream.
	Packed bool

	msg *capnp.Message
}

func (w *CapnpWriter) begin() (*capnp.Segment, error) {
	arena := capnp.SingleSegment(nil)
	if w.Reuse && w.msg != nil {
		return w.msg.Reset(arena)
	}
	msg, seg, err := capnp.NewMessage(arena)
	if w.Reuse {
		w.msg = msg
	}
	return seg, err
}

func (w *CapnpWriter) finish(msg *capnp.Message) ([]byte, error) {
	if w.Packed {
		return msg.MarshalPacked()
	}
	return msg.Marshal()
}

func setTags(n int32, newList func(int32) (capnp.TextList, error), tags []string) error {
	if len(tags) == 0 {
		return nil
	}
	l, err := newList(n)
	if err != nil {
		return err
	}
	for i, t := range tags {
		if err := l.Set(i, t); err != nil {
			return err
		}
	}
	return nil
}

// EncodeOrder encodes an order.
func (w *CapnpWriter) EncodeOrder(o *dataset.Order) ([]byte, error) {
	seg, err := w.begin()
	if err != nil {
		return nil, err
	}
	c, err := capgen.NewRootOrder(seg)
	if err != nil {
		return nil, err
	}
	err = errors.Join(
		c.SetCode(o.Code),
		c.SetCustomer(o.Customer),
		c.SetName(o.Name),
		c.SetStatus(o.Status),
	)
	c.SetTotal(o.Total)
	c.SetQty(o.Qty)
	c.SetAt(o.At.UnixNano())
	if o.Note != nil {
		c.SetNoteSet(true)
		err = errors.Join(err, c.SetNote(*o.Note))
	}
	err = errors.Join(err, setTags(int32(len(o.Tags)), c.NewTags, o.Tags))
	if len(o.Payload) > 0 {
		err = errors.Join(err, c.SetPayload(o.Payload))
	}
	if err != nil {
		return nil, err
	}
	return w.finish(c.Message())
}

// EncodeBook encodes a book.
func (w *CapnpWriter) EncodeBook(bk *dataset.Book) ([]byte, error) {
	seg, err := w.begin()
	if err != nil {
		return nil, err
	}
	c, err := capgen.NewRootBook(seg)
	if err != nil {
		return nil, err
	}
	err = errors.Join(
		c.SetCode(bk.Code),
		c.SetAuthor(bk.Author),
		c.SetTitle(bk.Title),
	)
	c.SetYear(bk.Year)
	c.SetPrice(bk.Price)
	c.SetInPrint(bk.InPrint)
	c.SetScore(bk.Score)
	if bk.Subtitle != nil {
		c.SetSubtitleSet(true)
		err = errors.Join(err, c.SetSubtitle(*bk.Subtitle))
	}
	err = errors.Join(err, setTags(int32(len(bk.Tags)), c.NewTags, bk.Tags))
	if len(bk.Payload) > 0 {
		err = errors.Join(err, c.SetPayload(bk.Payload))
	}
	if err != nil {
		return nil, err
	}
	return w.finish(c.Message())
}

// EncodeDoc encodes a document.
func (w *CapnpWriter) EncodeDoc(d *dataset.Doc) ([]byte, error) {
	seg, err := w.begin()
	if err != nil {
		return nil, err
	}
	c, err := capgen.NewRootDoc(seg)
	if err != nil {
		return nil, err
	}
	if err := c.SetLang(d.Lang); err != nil {
		return nil, err
	}
	c.SetYear(int64(d.Year))
	if len(d.Embedding) > 0 {
		l, err := c.NewEmbedding(int32(len(d.Embedding)))
		if err != nil {
			return nil, err
		}
		for i, f := range d.Embedding {
			l.Set(i, f)
		}
	}
	return w.finish(c.Message())
}

// CapnpReader opens the messages of the encodings with the segment of the bytes as they are,
// reusing one Message and one arena for all of them.
//
// capnp.Unmarshal allocates an arena (a MultiSegment) and a Message per call. A message of one
// segment is a header of 8 bytes and the segment, so the reader checks the header itself and
// resets one message over a read-only arena that points to the segment. It falls back on
// capnp.Unmarshal for a message that has several segments (the writers here never make one).
//
// The message that Open returns is valid until the next call and reads from the given slice.
type CapnpReader struct {
	msg   capnp.Message
	arena *capnp.ReadOnlySingleSegment
}

// Open returns the message of an encoding.
func (r *CapnpReader) Open(bz []byte) (*capnp.Message, error) {
	if len(bz) < 16 || binary.LittleEndian.Uint32(bz) != 0 ||
		uint64(binary.LittleEndian.Uint32(bz[4:]))*8 != uint64(len(bz)-8) {
		return capnp.Unmarshal(bz)
	}
	if r.arena == nil {
		r.arena = capnp.NewReadOnlySingleSegment(nil)
	}
	// Reset releases the arena that the message has, which drops its data; the arena can then be
	// given the next one. (ReplaceData panics on an arena that has data.)
	if _, err := r.msg.Reset(nil); err != nil {
		return nil, err
	}
	r.arena.ReplaceData(bz[8:])
	if _, err := r.msg.Reset(r.arena); err != nil {
		return nil, err
	}
	return &r.msg, nil
}

// capnpAccess collects the first error of the accessors, which return one next to each value.
// Every accessor of a pointer field (Text, Data, List) can fail on a message that is corrupt.
type capnpAccess struct{ err error }

func (a *capnpAccess) note(e error) {
	if e != nil && a.err == nil {
		a.err = e
	}
}

func (a *capnpAccess) str(s string, e error) string {
	a.note(e)
	return s
}

func (a *capnpAccess) data(b []byte, e error) []byte {
	a.note(e)
	return copyBytes(b)
}

func (a *capnpAccess) tags(l capnp.TextList, e error) []string {
	a.note(e)
	if l.Len() == 0 {
		return nil
	}
	ss := make([]string, l.Len())
	for i := range ss {
		s, e := l.At(i)
		a.note(e)
		ss[i] = s
	}
	return ss
}

func (a *capnpAccess) floats(l capnp.Float32List, e error) []float32 {
	a.note(e)
	n := l.Len()
	if n == 0 {
		return nil
	}
	fs := make([]float32, n)
	for i := range fs {
		fs[i] = l.At(i)
	}
	return fs
}

// DecodeOrderCapnp decodes the order in a message, copying out of it.
func DecodeOrderCapnp(msg *capnp.Message) (o dataset.Order, err error) {
	c, err := capgen.ReadRootOrder(msg)
	if err != nil {
		return o, err
	}
	var a capnpAccess
	o.Code = a.str(c.Code())
	o.Customer = a.str(c.Customer())
	o.Name = a.str(c.Name())
	o.Status = a.str(c.Status())
	o.Total = c.Total()
	o.Qty = c.Qty()
	o.At = time.Unix(0, c.At()).UTC()
	if c.NoteSet() {
		s := a.str(c.Note())
		o.Note = &s
	}
	o.Tags = a.tags(c.Tags())
	o.Payload = a.data(c.Payload())
	return o, a.err
}

// DecodeBookCapnp decodes the book in a message, copying out of it.
func DecodeBookCapnp(msg *capnp.Message) (bk dataset.Book, err error) {
	c, err := capgen.ReadRootBook(msg)
	if err != nil {
		return bk, err
	}
	var a capnpAccess
	bk.Code = a.str(c.Code())
	bk.Author = a.str(c.Author())
	bk.Year = c.Year()
	bk.Price = c.Price()
	bk.InPrint = c.InPrint()
	bk.Tags = a.tags(c.Tags())
	bk.Title = a.str(c.Title())
	if c.SubtitleSet() {
		s := a.str(c.Subtitle())
		bk.Subtitle = &s
	}
	bk.Score = c.Score()
	bk.Payload = a.data(c.Payload())
	return bk, a.err
}

// DecodeDocCapnp decodes the document in a message, copying out of it.
func DecodeDocCapnp(msg *capnp.Message) (d dataset.Doc, err error) {
	c, err := capgen.ReadRootDoc(msg)
	if err != nil {
		return d, err
	}
	var a capnpAccess
	d.Lang = a.str(c.Lang())
	d.Year = int(c.Year())
	d.Embedding = a.floats(c.Embedding())
	return d, a.err
}
