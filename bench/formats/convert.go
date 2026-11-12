// Package formats converts the records of the benchmarks (bench/internal/dataset) to the value
// formats that are compared (FlatBuffers and Cap'n Proto) and back. It is a throwaway module that
// answers one question: which of the two formats suits the stored records of kvutil. The id of a
// record is not in any of the formats: kvutil keeps it in the key.
//
// FlatBuffers is bench/internal/flat, which the benchmarks use for their stores (its codecs are
// what the rows of "flatbuffers" measure); this file is the Cap'n Proto side.
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
	"encoding/binary"
	"errors"
	"time"

	capnp "capnproto.org/go/capnp/v3"

	"github.com/ehsanranjbar/kvutil/bench/formats/capgen"
	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
)

// ---------------------------------------------------------------------------------------------
// Cap'n Proto

func copyBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return append([]byte(nil), b...)
}

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
