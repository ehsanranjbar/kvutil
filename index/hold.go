package index

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/codec/lex"
	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
)

// ErrNotHeld is returned for an item that an index does not hold: one that is neither a field of
// its key, nor a field that it includes, nor a value that it covers.
var ErrNotHeld = errors.New("index: the index does not hold the item")

// # What an entry holds
//
// An index holds in an entry what the fields of its key are, and what it is told to keep with
// the entry: fields that are not in the key (Including), and values that are computed from the
// record (Covering). Whatever it holds can be read without getting the record (Iterator.Get),
// and a query that only needs these is answered from the index alone.
//
// The key of an entry is the encoded fields of the index one after the other, each in the form
// that marks where it ends (the Append of its key codec), with the bytes inverted for a field that
// is descending; see FieldsIndexer. Reading a field back from a key undoes the inversion first.
//
// The value of an entry is the slots of the index one after the other, in the order that the
// options of the index define them (the slots of an option that is applied earlier come first),
// with nothing between them and nothing after the last one. A slot is
//
//   - for an included field: the number of the values that the record has for the field, as a
//     uvarint, and then the values, each encoded as it is in a key but never inverted, in the form
//     that marks where it ends. So 0x00 is a record without a value, and 0x01 and a value is the
//     usual case of a field that has one; a field that may have several values holds them all, in
//     the order that the field gives them. The values are in the form that the comparisons of the
//     field are written in, so that they can be checked on the encoded values (cond.Comparison).
//   - for a cover: the length of the encoded value as a uvarint, and then the value as the
//     codec of the cover encodes it. A cover has one value for every record.
//
// What the slots are, with the codecs of their values, is a part of the descriptor of the index,
// in its Encoding and its Covered: an index whose slots change is built again.

// slot is something that is kept in the value of every entry of an index: an included field, or
// a cover.
type slot[V any] struct {
	field field.Component[V] // set for an included field
	cover CoverItem[V]       // set for a cover
}

func (s slot[V]) path() string {
	if s.cover != nil {
		return s.cover.Path()
	}
	return s.field.Path()
}

func (s slot[V]) codecName() string {
	if s.cover != nil {
		return s.cover.CodecName()
	}
	return s.field.CodecName()
}

// describe returns the slot as it is in the encoding of the descriptor of the index.
func (s slot[V]) describe() string {
	if s.cover != nil {
		return "cover:" + s.path() + "=" + s.codecName()
	}
	return "inc:" + s.path() + "=" + s.codecName()
}

// holder is implemented by the indexers that keep slots in the values of their entries, and by the
// ones that wrap them.
type holder[V any] interface {
	// held returns the slots that the entries have, in the order that they are in.
	held() []slot[V]
}

// heldBy returns the slots that the entries of an indexer have.
func heldBy[V any](indexer Indexer[V]) []slot[V] {
	if h, ok := indexer.(holder[V]); ok {
		return h.held()
	}
	return nil
}

// Covered is a value that is computed from a record and that an index keeps with its entries
// (Covering), so that it can be read from the index without getting the record. It has a name,
// and it is selected by a query like a field is. The values are of the type C.
type Covered[V, C any] struct {
	name  string
	get   func(*V) C
	codec codec.ValueCodec[C]
}

// Cover defines a value that an index can keep, which is named name and computed by get from a
// record. Its codec is the default one of the type C, unless one is given. It panics if the name
// is empty, if get is nil, or if more than one codec is given.
func Cover[V, C any](name string, get func(*V) C, c ...codec.ValueCodec[C]) *Covered[V, C] {
	if name == "" {
		panic("index: a cover needs a name")
	}
	if get == nil {
		panic(fmt.Sprintf("index: the cover %q needs a function", name))
	}

	var vc codec.ValueCodec[C]
	switch len(c) {
	case 0:
	case 1:
		vc = c[0]
	default:
		panic(fmt.Sprintf("index: the cover %q has more than one codec", name))
	}
	if vc == nil {
		vc = codec.ValueFor[C](nil)
	}
	return &Covered[V, C]{name: name, get: get, codec: vc}
}

// Path implements the field.Item interface: it is the name of the cover.
func (c *Covered[V, C]) Path() string { return c.name }

// Multi implements the field.Item interface. A cover has one value for every record.
func (c *Covered[V, C]) Multi() bool { return false }

// CodecName returns the name of the codec of the values.
func (c *Covered[V, C]) CodecName() string { return c.codec.Name() }

// Values implements the field.Selectable interface: the value that the record has.
func (c *Covered[V, C]) Values(v *V) []C { return []C{c.get(v)} }

// AnyValues implements the field.Item interface.
func (c *Covered[V, C]) AnyValues(v *V) []any { return []any{c.get(v)} }

func (c *Covered[V, C]) encode(v *V) ([]byte, error) { return c.codec.Encode(c.get(v)) }

func (c *Covered[V, C]) decode(bz []byte) (any, error) {
	value, err := c.codec.Decode(bz)
	if err != nil {
		return nil, err
	}
	return value, nil
}

// CoverItem is a Covered of any type of values. It is what Covering takes.
type CoverItem[V any] interface {
	field.Item[V]
	// CodecName returns the name of the codec of the values.
	CodecName() string

	encode(v *V) ([]byte, error)
	decode(bz []byte) (any, error)
}

var _ field.Selectable[struct{}, int] = (*Covered[struct{}, int])(nil)
var _ CoverItem[struct{}] = (*Covered[struct{}, int])(nil)

// Covering stores the covers with every entry of the index, so that they can be read from the
// index without getting the record: with Iterator.Get, and by a query that selects them (package
// query). A cover is part of what the index is identified by, with its name and its codec: the
// index is built again when they change. It panics if a cover is given twice, or has the name of
// a field of the key or of an included field. The indexer must not store values of its own.
func Covering[V any](covers ...CoverItem[V]) Option[V] {
	slots := make([]slot[V], 0, len(covers))
	for _, c := range covers {
		if c == nil {
			panic("index: Covering is given a cover that is nil")
		}
		slots = append(slots, slot[V]{cover: c})
	}
	return func(indexer Indexer[V]) Indexer[V] {
		return valued(indexer, slots)
	}
}

// Including stores fields that are not in the key with every entry of the index, so that their
// values can be read from the index without getting the record, and so that conditions on them can
// be checked on the entries: by Iterator.Get, and by a query that selects them or has conditions
// on them (package query). Each value is stored encoded with the key codec of its field, and a
// field that may have several values for a record stores all of them. A field is part of what
// the index is identified by, with its path, its codec and its version: the index is built again
// when they change. It panics if a field is given twice, is a field of the key, or has the name
// of a cover. The indexer must not store values of its own.
func Including[V any](fields ...field.Component[V]) Option[V] {
	slots := make([]slot[V], 0, len(fields))
	for _, f := range fields {
		if f == nil {
			panic("index: Including is given a field that is nil")
		}
		slots = append(slots, slot[V]{field: f})
	}
	return func(indexer Indexer[V]) Indexer[V] {
		return valued(indexer, slots)
	}
}

// valuedIndexer wraps an indexer so that slots are stored with every entry that it returns.
type valuedIndexer[V any] struct {
	Indexer[V]
	slots []slot[V]
}

// valued wraps an indexer with slots, and panics for a name that is taken.
func valued[V any](indexer Indexer[V], slots []slot[V]) Indexer[V] {
	taken := map[string]string{}
	if f, ok := indexer.(fielded[V]); ok {
		for _, key := range f.Fields() {
			taken[key.Path()] = "a field of the key"
		}
	}
	for _, s := range heldBy(indexer) {
		taken[s.path()] = "held by the index"
	}
	for _, s := range slots {
		if what, ok := taken[s.path()]; ok {
			panic(fmt.Sprintf("index: %q is held twice: it is %s", s.path(), what))
		}
		taken[s.path()] = "held by the index"
	}
	return &valuedIndexer[V]{Indexer: indexer, slots: slices.Clone(slots)}
}

// Index implements the Indexer interface.
func (x *valuedIndexer[V]) Index(v *V) ([]Entry, error) {
	entries, err := x.Indexer.Index(v)
	if err != nil || len(entries) == 0 {
		return entries, err
	}

	value, err := x.encode(v)
	if err != nil {
		return nil, err
	}
	for i := range entries {
		entries[i].Value = slices.Concat(entries[i].Value, value)
	}
	return entries, nil
}

// encode returns the slots of a record, as they are in the value of an entry.
func (x *valuedIndexer[V]) encode(v *V) ([]byte, error) {
	var out []byte
	for _, s := range x.slots {
		if s.cover != nil {
			bz, err := s.cover.encode(v)
			if err != nil {
				return nil, fmt.Errorf("failed to encode the cover %s: %w", s.path(), err)
			}
			out = binary.AppendUvarint(out, uint64(len(bz)))
			out = append(out, bz...)
			continue
		}

		keys, err := s.field.Keys(v)
		if err != nil {
			return nil, err
		}
		out = binary.AppendUvarint(out, uint64(len(keys)))
		for _, key := range keys {
			if s.field.Descending() {
				// The values are held as they compare, whatever their order in the keys is.
				lex.Invert(key)
			}
			out = append(out, key...)
		}
	}
	return out, nil
}

// Descriptor implements the Indexer interface.
func (x *valuedIndexer[V]) Descriptor() Descriptor {
	desc := x.Indexer.Descriptor()
	covered := slices.Clip(desc.Covered)
	var versions []string
	for _, s := range x.slots {
		covered = append(covered, s.path())
		desc.Encoding += "+" + s.describe()
		if s.field != nil {
			if v, ok := s.field.(interface{ Version() string }); ok && v.Version() != "" {
				versions = append(versions, s.path()+"@"+v.Version())
			}
		}
	}
	desc.Covered = covered
	for _, v := range versions {
		if desc.Version != "" {
			desc.Version += ";"
		}
		desc.Version += v
	}
	return desc
}

// held implements the holder interface.
func (x *valuedIndexer[V]) held() []slot[V] {
	return slices.Concat(heldBy(x.Indexer), x.slots)
}

// Fields returns the fields of the wrapped indexer, if it is one of fields.
func (x *valuedIndexer[V]) Fields() []field.Component[V] {
	if f, ok := x.Indexer.(fielded[V]); ok {
		return f.Fields()
	}
	return nil
}

// Ranges implements the RangePlanner interface, if the wrapped indexer does.
func (x *valuedIndexer[V]) Ranges(conds ...cond.Cond[V]) ([]Bounds, []cond.Cond[V], error) {
	l, ok := x.Indexer.(RangePlanner[V])
	if !ok {
		return nil, nil, errors.New("the indexer cannot be looked up with conditions")
	}
	return l.Ranges(conds...)
}

// Access says where an index holds an item, to read it from the entries without looking it up
// again for every entry. It is made by Index.Access and Index.AccessField, and used with the
// iterators of that index.
type Access[V any] struct {
	owner any // the index
	// part is the position of the field in the key, or -1.
	part int
	// slot is the position of the slot in the values of the entries, or -1.
	slot int
	// field reads the values of a field, and cover the ones of a cover.
	field   field.Component[V]
	cover   CoverItem[V]
	path    string
	byEntry bool
}

// Path returns the path of the item, or the name of the cover.
func (a *Access[V]) Path() string { return a.path }

// ByEntry reports whether what the index holds is the value of the entry only, and not all the
// values of the record: it is so for a field of the key that may have several values, for which
// an entry has one of them. The values of any other item are the ones that the record has.
func (a *Access[V]) ByEntry() bool { return a.byEntry }

// IsCover reports whether the item is a cover. The values of anything else are the encoded
// values of a field.
func (a *Access[V]) IsCover() bool { return a.cover != nil }

// Access returns where the index holds an item: a field of its key or one that it includes, found
// by its path and the name of its key codec, or a cover, found by its name and the name of its
// codec. It reports whether the index holds the item.
func (x *Index[K, V]) Access(item field.Item[V]) (*Access[V], bool) {
	switch it := item.(type) {
	case CoverItem[V]:
		for i, s := range x.slots {
			if s.cover != nil && s.path() == it.Path() && s.codecName() == it.CodecName() {
				return &Access[V]{owner: x, part: -1, slot: i, cover: s.cover, path: s.path()}, true
			}
		}
	case field.Component[V]:
		return x.AccessField(it.Path(), it.CodecName())
	}
	return nil, false
}

// AccessField returns where the index holds the field of a path whose key codec has a name, which
// is how a condition names its field; see Access.
func (x *Index[K, V]) AccessField(path, codecName string) (*Access[V], bool) {
	for i, f := range x.fields {
		if f.Path() == path && f.CodecName() == codecName {
			return &Access[V]{owner: x, part: i, slot: -1, field: f, path: path, byEntry: f.Multi()}, true
		}
	}
	for i, s := range x.slots {
		if s.field != nil && s.path() == path && s.codecName() == codecName {
			return &Access[V]{owner: x, part: -1, slot: i, field: s.field, path: path}, true
		}
	}
	return nil, false
}

// entryView is what an entry holds, split but not decoded.
type entryView struct {
	// parts are the encoded fields of the key, each ascending: as Field.Encode gives them.
	parts [][]byte
	// held are the values of the slots: the encoded values of an included field, or the one
	// encoded value of a cover.
	held [][][]byte
}

// keyParts splits the key of the current entry into its fields.
func (it *Iterator[K, V]) keyParts() ([][]byte, error) {
	if it.viewed.parts != nil || len(it.ins.x.fields) == 0 {
		return it.viewed.parts, nil
	}
	key, err := it.cur.Key()
	if err != nil {
		return nil, err
	}

	fields := it.ins.x.fields
	parts := make([][]byte, len(fields))
	rest := key
	for i, f := range fields {
		var (
			whole = rest
			n     int
		)
		if f.Descending() {
			whole = lex.Invert(bytes.Clone(rest))
		}
		if _, n, err = f.ReadKey(whole); err != nil {
			return nil, fmt.Errorf("index %q: corrupt key: %w", it.ins.x.name, err)
		}
		parts[i] = whole[:n]
		rest = rest[n:]
	}
	it.viewed.parts = parts
	return parts, nil
}

// heldValues splits the value of the current entry into its slots.
func (it *Iterator[K, V]) heldValues() ([][][]byte, error) {
	slots := it.ins.x.slots
	if it.viewed.held != nil || len(slots) == 0 {
		return it.viewed.held, nil
	}
	value, err := it.cur.Value()
	if err != nil {
		return nil, err
	}

	corrupt := func(why string) error {
		return fmt.Errorf("index %q: corrupt value of an entry: %s", it.ins.x.name, why)
	}
	held := make([][][]byte, len(slots))
	rest := value
	for i, s := range slots {
		count, k := binary.Uvarint(rest)
		if k <= 0 {
			return nil, corrupt("no count for " + s.path())
		}
		rest = rest[k:]

		if s.cover != nil {
			if uint64(len(rest)) < count {
				return nil, corrupt("the cover " + s.path() + " is cut short")
			}
			held[i] = [][]byte{rest[:count]}
			rest = rest[count:]
			continue
		}

		held[i] = make([][]byte, 0, min(count, 16))
		for range count {
			_, n, err := s.field.ReadKey(rest)
			if err != nil {
				return nil, corrupt(err.Error())
			}
			held[i] = append(held[i], rest[:n])
			rest = rest[n:]
		}
	}
	if len(rest) != 0 {
		return nil, corrupt("there is more after the last slot")
	}
	it.viewed.held = held
	return held, nil
}

// check reports why an access cannot be used with the iterator, if it cannot.
func (it *Iterator[K, V]) check(a *Access[V]) error {
	if a == nil || a.owner != any(it.ins.x) {
		return errors.New("index: the access is of another index")
	}
	if it.cur == nil || it.err != nil {
		return errors.New("index: the iterator is not at an entry")
	}
	return nil
}

// Encoded returns the encoded values that the current entry holds for an item. For a field they
// are in the form that Field.Encode gives and the conditions are written in, ascending even if the
// field is descending in the key; for a cover the one encoded value is as its codec gives it. The
// slices must not be modified and are valid until the iterator moves. There are none for an
// included field that the record has no value for.
func (it *Iterator[K, V]) Encoded(a *Access[V]) ([][]byte, error) {
	if err := it.check(a); err != nil {
		return nil, err
	}
	if a.part >= 0 {
		parts, err := it.keyParts()
		if err != nil {
			return nil, err
		}
		return [][]byte{parts[a.part]}, nil
	}

	held, err := it.heldValues()
	if err != nil {
		return nil, err
	}
	return held[a.slot], nil
}

// Values returns the values that the current entry holds for an item, decoded: those of a field
// of the key or of an included field are of the type of the field, and the one of a cover is of
// the type of the cover. There are none for an included field that the record has no value for.
func (it *Iterator[K, V]) Values(a *Access[V]) ([]any, error) {
	encoded, err := it.Encoded(a)
	if err != nil {
		return nil, err
	}

	values := make([]any, 0, len(encoded))
	for _, bz := range encoded {
		var value any
		if a.cover != nil {
			value, err = a.cover.decode(bz)
			if err != nil {
				return nil, fmt.Errorf("index %q: failed to decode the cover %s: %w", it.ins.x.name, a.path, err)
			}
		} else if value, _, err = a.field.ReadKey(bz); err != nil {
			return nil, fmt.Errorf("index %q: %w", it.ins.x.name, err)
		}
		values = append(values, value)
	}
	return values, nil
}

// Get returns the first value that the current entry holds for an item, and whether there is one,
// without getting the record. The item is a field of the key of the index, a field that it
// includes (Including), or a cover (Covering), found as a query finds them: by its path and its
// codec. It fails with ErrNotHeld for any other item.
//
// A field of the key that may have several values has the one of the entry, which is not
// all the values of the record. An included field has all of them: GetAll returns them.
func (it *Iterator[K, V]) Get[F any](s field.Selectable[V, F]) (F, bool, error) {
	all, err := it.GetAll(s)
	if err != nil || len(all) == 0 {
		var zero F
		return zero, false, err
	}
	return all[0], true, nil
}

// GetAll returns all the values that the current entry holds for an item; see Get.
func (it *Iterator[K, V]) GetAll[F any](s field.Selectable[V, F]) ([]F, error) {
	a, ok := it.ins.x.Access(s)
	if !ok {
		return nil, fmt.Errorf("%w: %s is not in %q", ErrNotHeld, s.Path(), it.ins.x.name)
	}
	values, err := it.Values(a)
	if err != nil {
		return nil, err
	}

	result := make([]F, 0, len(values))
	for _, value := range values {
		f, ok := value.(F)
		if !ok {
			return nil, fmt.Errorf("index %q: %s holds a %T, not a %T", it.ins.x.name, s.Path(), value, f)
		}
		result = append(result, f)
	}
	return result, nil
}
