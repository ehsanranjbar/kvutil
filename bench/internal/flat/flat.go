package flat

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
)

// Name is the name of the codecs of this package, which a store records with its unit.
const Name = "flatbuffers"

// ErrCorrupt is wrapped by the error of a decode of bytes that are not a FlatBuffer of the table:
// the accessors of the Go runtime do not check the bounds and panic, which the codecs recover.
var ErrCorrupt = errors.New("flat: corrupt flatbuffer")

// Def is what makes the codec of a record T that is kept as a table of the schema: the functions
// that build and read it. They are small and are written by hand, field by field, as the object API
// of flatc cannot tell an absent string from an empty one.
//
// A function reads the table with the generated accessor, fbgen.GetRootAsOrder(bz, 0), in its
// own body and not through an argument: the accessor is a few words that stay on the stack that
// way, while one that is passed to a function value is allocated on the heap.
type Def[T any] struct {
	// Build builds the table into an empty builder and finishes it with the Finish function of
	// the table.
	Build func(b *flatbuffers.Builder, v T)
	// Read decodes every field of the table in the bytes. It copies out of them (the result
	// must not refer to them), and may panic on bytes that are corrupt.
	Read func(bz []byte) T
	// Fields decode one field of the table each into a zero record, by the name of the Go
	// struct field, with the same rules as Read.
	Fields map[string]func(bz []byte, v *T)
	// NotStored are the Go fields of the record that the table does not have, because the
	// value never has them: the id, which a store sets after it decodes. A partial decode of
	// one does nothing.
	NotStored []string
}

// Codec is the codec.ValueCodec of a record in FlatBuffers, with partial decoding
// (codec.PartialDecoder).
type Codec[T any] struct {
	def Def[T]
}

// New returns the codec of a record from its definition.
func New[T any](def Def[T]) *Codec[T] {
	if def.Build == nil || def.Read == nil {
		panic("flat: a definition needs Build and Read")
	}
	return &Codec[T]{def: def}
}

// builders are the builders of the encoders, reset between records, so that an encode does not
// allocate a buffer.
var builders = sync.Pool{New: func() any { return flatbuffers.NewBuilder(1024) }}

// maxPooled is the size of the buffer of a builder above which it is not kept.
const maxPooled = 1 << 20

// Name implements the codec.ValueCodec interface.
func (c *Codec[T]) Name() string { return Name }

// Encode implements the codec.ValueCodec interface.
func (c *Codec[T]) Encode(v T) ([]byte, error) {
	b := builders.Get().(*flatbuffers.Builder)
	b.Reset()
	c.def.Build(b, v)
	bz := bytes.Clone(b.FinishedBytes())
	if cap(b.Bytes) <= maxPooled {
		builders.Put(b)
	}
	return bz, nil
}

// Marshal is Encode of a record that is given by reference. It never fails.
func (c *Codec[T]) Marshal(v *T) []byte {
	bz, _ := c.Encode(*v)
	return bz
}

// Decode implements the codec.ValueCodec interface. The result does not refer to bz.
func (c *Codec[T]) Decode(bz []byte) (v T, err error) {
	defer recoverCorrupt(&v, &err)
	return c.def.Read(bz), nil
}

// PartialDecode implements the codec.PartialDecoder interface. The top-level field of each path
// is resolved once, to the function that reads it from the table. It is false for a name that the
// record does not have.
func (c *Codec[T]) PartialDecode(fields []string) (func(bz []byte, v *T) error, bool) {
	reads := make([]func(bz []byte, v *T), 0, len(fields))
	seen := make(map[string]bool, len(fields))
	for _, f := range fields {
		name, _, _ := strings.Cut(f, ".")
		if seen[name] {
			continue
		}
		seen[name] = true
		if read, ok := c.def.Fields[name]; ok {
			reads = append(reads, read)
			continue
		}
		if !contains(c.def.NotStored, name) {
			return nil, false
		}
	}
	return func(bz []byte, v *T) (err error) {
		defer recoverCorrupt(v, &err)
		for _, read := range reads {
			read(bz, v)
		}
		return nil
	}, true
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

// recoverCorrupt turns the panic of an accessor on a buffer that is corrupt into an error, and
// zeroes what was decoded.
func recoverCorrupt[T any](v *T, err *error) {
	if r := recover(); r != nil {
		var zero T
		*v = zero
		if e, ok := r.(error); ok && errors.Is(e, ErrCorrupt) {
			*err = e
			return
		}
		*err = fmt.Errorf("%w: %v", ErrCorrupt, r)
	}
}

// ---------------------------------------------------------------------------------------------
// The building blocks of the definitions.

// Time is the time of Unix nanoseconds, in UTC, as the generators make the times of the records.
// A time is kept in nanoseconds since the epoch: those of the years 1678 to 2262.
func Time(ns int64) time.Time { return time.Unix(0, ns).UTC() }

// CopyBytes returns a copy of the bytes, which is nil for none.
func CopyBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return append([]byte(nil), b...)
}

// Strings copies out a vector of n strings of the table in bz, which is nil for none; at is the
// accessor of the vector. It panics with ErrCorrupt if the vector is longer than the buffer: a
// length that is wrong must not allocate.
func Strings(bz []byte, n int, at func(int) []byte) []string {
	if n <= 0 {
		return nil
	}
	if n > len(bz) {
		panic(ErrCorrupt)
	}
	ss := make([]string, n)
	for i := range ss {
		ss[i] = string(at(i))
	}
	return ss
}

// Floats copies out a vector of n floats of the table in bz, which is nil for none. It panics with
// ErrCorrupt if the vector is longer than the buffer.
func Floats(bz []byte, n int, at func(int) float32) []float32 {
	if n <= 0 {
		return nil
	}
	if n > len(bz) {
		panic(ErrCorrupt)
	}
	fs := make([]float32, n)
	for i := range fs {
		fs[i] = at(i)
	}
	return fs
}

// CreateOptString creates a string that is optional: the offset of a present one, even an empty
// one, and 0, which adds no field, for nil.
func CreateOptString(b *flatbuffers.Builder, s *string) flatbuffers.UOffsetT {
	if s == nil {
		return 0
	}
	return b.CreateString(*s)
}

// OptString reads a string that is optional: nil if the field is absent.
func OptString(b []byte) *string {
	if b == nil {
		return nil
	}
	s := string(b)
	return &s
}

// CreateBytes creates a vector of bytes, or returns 0, which adds no field, for none.
func CreateBytes(b *flatbuffers.Builder, bs []byte) flatbuffers.UOffsetT {
	if len(bs) == 0 {
		return 0
	}
	return b.CreateByteVector(bs)
}

// CreateStrings creates a vector of strings with the Start function of the vector that flatc
// generates for the field, or returns 0, which adds no field, for none.
func CreateStrings(b *flatbuffers.Builder, ss []string, start func(*flatbuffers.Builder, int) flatbuffers.UOffsetT) flatbuffers.UOffsetT {
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

// CreateFloats creates a vector of floats with the Start function of the vector that flatc
// generates for the field, or returns 0, which adds no field, for none.
func CreateFloats(b *flatbuffers.Builder, fs []float32, start func(*flatbuffers.Builder, int) flatbuffers.UOffsetT) flatbuffers.UOffsetT {
	if len(fs) == 0 {
		return 0
	}
	start(b, len(fs))
	for i := len(fs) - 1; i >= 0; i-- {
		b.PrependFloat32(fs[i])
	}
	return b.EndVector(len(fs))
}
