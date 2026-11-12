package flat

import (
	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/flat/fbgen"
	"github.com/ehsanranjbar/kvutil/codec"
)

// The ids are not in the values: a store sets the id of a record after it decodes it, so the
// ID of the records is NotStored, and a decode leaves it zero.
var notStored = []string{"ID"}

// ---------------------------------------------------------------------------------------------
// Order

var orders = New(Def[dataset.Order]{
	Build:     buildOrder,
	Read:      readOrder,
	NotStored: notStored,
	Fields: map[string]func(bz []byte, o *dataset.Order){
		"Code":     func(bz []byte, o *dataset.Order) { o.Code = string(fbgen.GetRootAsOrder(bz, 0).Code()) },
		"Customer": func(bz []byte, o *dataset.Order) { o.Customer = string(fbgen.GetRootAsOrder(bz, 0).Customer()) },
		"Name":     func(bz []byte, o *dataset.Order) { o.Name = string(fbgen.GetRootAsOrder(bz, 0).Name()) },
		"Status":   func(bz []byte, o *dataset.Order) { o.Status = string(fbgen.GetRootAsOrder(bz, 0).Status()) },
		"Total":    func(bz []byte, o *dataset.Order) { o.Total = fbgen.GetRootAsOrder(bz, 0).Total() },
		"Qty":      func(bz []byte, o *dataset.Order) { o.Qty = fbgen.GetRootAsOrder(bz, 0).Qty() },
		"At":       func(bz []byte, o *dataset.Order) { o.At = Time(fbgen.GetRootAsOrder(bz, 0).At()) },
		"Note":     func(bz []byte, o *dataset.Order) { o.Note = OptString(fbgen.GetRootAsOrder(bz, 0).Note()) },
		"Tags": func(bz []byte, o *dataset.Order) {
			t := fbgen.GetRootAsOrder(bz, 0)
			o.Tags = Strings(bz, t.TagsLength(), t.Tags)
		},
		"Payload": func(bz []byte, o *dataset.Order) { o.Payload = CopyBytes(fbgen.GetRootAsOrder(bz, 0).PayloadBytes()) },
	},
})

func buildOrder(b *flatbuffers.Builder, o dataset.Order) {
	code := b.CreateString(o.Code)
	customer := b.CreateString(o.Customer)
	name := b.CreateString(o.Name)
	status := b.CreateString(o.Status)
	note := CreateOptString(b, o.Note)
	tags := CreateStrings(b, o.Tags, fbgen.OrderStartTagsVector)
	payload := CreateBytes(b, o.Payload)

	fbgen.OrderStart(b)
	fbgen.OrderAddCode(b, code)
	fbgen.OrderAddCustomer(b, customer)
	fbgen.OrderAddName(b, name)
	fbgen.OrderAddStatus(b, status)
	fbgen.OrderAddTotal(b, o.Total)
	fbgen.OrderAddQty(b, o.Qty)
	fbgen.OrderAddAt(b, o.At.UnixNano())
	fbgen.OrderAddNote(b, note)
	fbgen.OrderAddTags(b, tags)
	fbgen.OrderAddPayload(b, payload)
	fbgen.FinishOrderBuffer(b, fbgen.OrderEnd(b))
}

func readOrder(bz []byte) (o dataset.Order) {
	t := fbgen.GetRootAsOrder(bz, 0)
	o.Code = string(t.Code())
	o.Customer = string(t.Customer())
	o.Name = string(t.Name())
	o.Status = string(t.Status())
	o.Total = t.Total()
	o.Qty = t.Qty()
	o.At = Time(t.At())
	o.Note = OptString(t.Note()) // absent is nil, and present (even empty) is not
	o.Tags = Strings(bz, t.TagsLength(), t.Tags)
	o.Payload = CopyBytes(t.PayloadBytes())
	return o
}

// Order returns the codec of the orders in FlatBuffers. Its name is "flatbuffers". The id is not
// in the value: a decode leaves it zero, and a store sets it. It has partial decoding.
func Order() codec.ValueCodec[dataset.Order] { return orders }

// BuildOrder builds the order into an empty builder and finishes it: the encoding is
// b.FinishedBytes(), a slice of the builder's buffer.
func BuildOrder(b *flatbuffers.Builder, o *dataset.Order) { buildOrder(b, *o) }

// EncodeOrder encodes the order, as a raw store keeps it (the codec of the store, for a record
// that has no store). The id is not in the encoding.
func EncodeOrder(o *dataset.Order) []byte { return orders.Marshal(o) }

// DecodeOrder decodes an order encoded by EncodeOrder, and sets its id.
func DecodeOrder(bz []byte, id int64) (*dataset.Order, error) {
	o, err := orders.Decode(bz)
	if err != nil {
		return nil, err
	}
	o.ID = id
	return &o, nil
}

// ---------------------------------------------------------------------------------------------
// Book

var books = New(Def[dataset.Book]{
	Build:     buildBook,
	Read:      readBook,
	NotStored: notStored,
	Fields: map[string]func(bz []byte, b *dataset.Book){
		"Code":    func(bz []byte, b *dataset.Book) { b.Code = string(fbgen.GetRootAsBook(bz, 0).Code()) },
		"Author":  func(bz []byte, b *dataset.Book) { b.Author = string(fbgen.GetRootAsBook(bz, 0).Author()) },
		"Year":    func(bz []byte, b *dataset.Book) { b.Year = fbgen.GetRootAsBook(bz, 0).Year() },
		"Price":   func(bz []byte, b *dataset.Book) { b.Price = fbgen.GetRootAsBook(bz, 0).Price() },
		"InPrint": func(bz []byte, b *dataset.Book) { b.InPrint = fbgen.GetRootAsBook(bz, 0).InPrint() },
		"Tags": func(bz []byte, b *dataset.Book) {
			t := fbgen.GetRootAsBook(bz, 0)
			b.Tags = Strings(bz, t.TagsLength(), t.Tags)
		},
		"Title":    func(bz []byte, b *dataset.Book) { b.Title = string(fbgen.GetRootAsBook(bz, 0).Title()) },
		"Subtitle": func(bz []byte, b *dataset.Book) { b.Subtitle = OptString(fbgen.GetRootAsBook(bz, 0).Subtitle()) },
		"Score":    func(bz []byte, b *dataset.Book) { b.Score = fbgen.GetRootAsBook(bz, 0).Score() },
		"Payload":  func(bz []byte, b *dataset.Book) { b.Payload = CopyBytes(fbgen.GetRootAsBook(bz, 0).PayloadBytes()) },
	},
})

func buildBook(b *flatbuffers.Builder, bk dataset.Book) {
	code := b.CreateString(bk.Code)
	author := b.CreateString(bk.Author)
	title := b.CreateString(bk.Title)
	subtitle := CreateOptString(b, bk.Subtitle)
	tags := CreateStrings(b, bk.Tags, fbgen.BookStartTagsVector)
	payload := CreateBytes(b, bk.Payload)

	fbgen.BookStart(b)
	fbgen.BookAddCode(b, code)
	fbgen.BookAddAuthor(b, author)
	fbgen.BookAddYear(b, bk.Year)
	fbgen.BookAddPrice(b, bk.Price)
	fbgen.BookAddInPrint(b, bk.InPrint)
	fbgen.BookAddTags(b, tags)
	fbgen.BookAddTitle(b, title)
	fbgen.BookAddSubtitle(b, subtitle)
	fbgen.BookAddScore(b, bk.Score)
	fbgen.BookAddPayload(b, payload)
	fbgen.FinishBookBuffer(b, fbgen.BookEnd(b))
}

func readBook(bz []byte) (bk dataset.Book) {
	t := fbgen.GetRootAsBook(bz, 0)
	bk.Code = string(t.Code())
	bk.Author = string(t.Author())
	bk.Year = t.Year()
	bk.Price = t.Price()
	bk.InPrint = t.InPrint()
	bk.Tags = Strings(bz, t.TagsLength(), t.Tags)
	bk.Title = string(t.Title())
	bk.Subtitle = OptString(t.Subtitle())
	bk.Score = t.Score()
	bk.Payload = CopyBytes(t.PayloadBytes())
	return bk
}

// Book returns the codec of the books in FlatBuffers; see Order.
func Book() codec.ValueCodec[dataset.Book] { return books }

// BuildBook is BuildOrder for a book.
func BuildBook(b *flatbuffers.Builder, bk *dataset.Book) { buildBook(b, *bk) }

// ---------------------------------------------------------------------------------------------
// Doc

var docs = New(Def[dataset.Doc]{
	Build:     buildDoc,
	Read:      readDoc,
	NotStored: notStored,
	Fields: map[string]func(bz []byte, d *dataset.Doc){
		"Lang": func(bz []byte, d *dataset.Doc) { d.Lang = string(fbgen.GetRootAsDoc(bz, 0).Lang()) },
		"Year": func(bz []byte, d *dataset.Doc) { d.Year = int(fbgen.GetRootAsDoc(bz, 0).Year()) },
		"Embedding": func(bz []byte, d *dataset.Doc) {
			t := fbgen.GetRootAsDoc(bz, 0)
			d.Embedding = Floats(bz, t.EmbeddingLength(), t.Embedding)
		},
	},
})

func buildDoc(b *flatbuffers.Builder, d dataset.Doc) {
	lang := b.CreateString(d.Lang)
	emb := CreateFloats(b, d.Embedding, fbgen.DocStartEmbeddingVector)

	fbgen.DocStart(b)
	fbgen.DocAddLang(b, lang)
	fbgen.DocAddYear(b, int64(d.Year))
	fbgen.DocAddEmbedding(b, emb)
	fbgen.FinishDocBuffer(b, fbgen.DocEnd(b))
}

func readDoc(bz []byte) (d dataset.Doc) {
	t := fbgen.GetRootAsDoc(bz, 0)
	d.Lang = string(t.Lang())
	d.Year = int(t.Year())
	d.Embedding = Floats(bz, t.EmbeddingLength(), t.Embedding)
	return d
}

// Doc returns the codec of the documents in FlatBuffers; see Order.
func Doc() codec.ValueCodec[dataset.Doc] { return docs }

// BuildDoc is BuildOrder for a document.
func BuildDoc(b *flatbuffers.Builder, d *dataset.Doc) { buildDoc(b, *d) }

// ---------------------------------------------------------------------------------------------
// Event

var events = New(Def[dataset.Event]{
	Build:     buildEvent,
	Read:      readEvent,
	NotStored: notStored,
	Fields: map[string]func(bz []byte, e *dataset.Event){
		"Customer": func(bz []byte, e *dataset.Event) { e.Customer = string(fbgen.GetRootAsEvent(bz, 0).Customer()) },
		"Amount":   func(bz []byte, e *dataset.Event) { e.Amount = fbgen.GetRootAsEvent(bz, 0).Amount() },
		"At":       func(bz []byte, e *dataset.Event) { e.At = Time(fbgen.GetRootAsEvent(bz, 0).At()) },
		"Kind":     func(bz []byte, e *dataset.Event) { e.Kind = string(fbgen.GetRootAsEvent(bz, 0).Kind()) },
	},
})

func buildEvent(b *flatbuffers.Builder, e dataset.Event) {
	customer := b.CreateString(e.Customer)
	kind := b.CreateString(e.Kind)

	fbgen.EventStart(b)
	fbgen.EventAddCustomer(b, customer)
	fbgen.EventAddAmount(b, e.Amount)
	fbgen.EventAddAt(b, e.At.UnixNano())
	fbgen.EventAddKind(b, kind)
	fbgen.FinishEventBuffer(b, fbgen.EventEnd(b))
}

func readEvent(bz []byte) (e dataset.Event) {
	t := fbgen.GetRootAsEvent(bz, 0)
	e.Customer = string(t.Customer())
	e.Amount = t.Amount()
	e.At = Time(t.At())
	e.Kind = string(t.Kind())
	return e
}

// Event returns the codec of the events in FlatBuffers; see Order.
func Event() codec.ValueCodec[dataset.Event] { return events }
