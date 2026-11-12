package types

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/ehsanranjbar/kvutil/examples/petstore/types/fb"
)

// Status is the state of a pet in the store.
type Status int32

const (
	Status_UNKNOWN   Status = 0
	Status_AVAILABLE Status = 1
	Status_PENDING   Status = 2
	Status_SOLD      Status = 3
)

// Status_name is the name of each status.
var Status_name = map[int32]string{
	0: "UNKNOWN",
	1: "AVAILABLE",
	2: "PENDING",
	3: "SOLD",
}

// Status_value is the status of each name.
var Status_value = map[string]int32{
	"UNKNOWN":   0,
	"AVAILABLE": 1,
	"PENDING":   2,
	"SOLD":      3,
}

// String returns the name of the status, or its number if it has no name.
func (s Status) String() string {
	if name, ok := Status_name[int32(s)]; ok {
		return name
	}
	return strconv.Itoa(int(s))
}

// Category is a group of pets, such as dogs.
type Category struct {
	Id   int64
	Name string
}

// PetRecord is a pet as it is stored.
type PetRecord struct {
	Id         int64
	CategoryId int64
	Name       string
	Status     Status
	// CreatedAt is the time that the pet was added at, in seconds since the Unix epoch.
	CreatedAt int64
}

// Pet is a pet as the API has it: with its category, the addresses of its images and its tags.
// It is never stored.
type Pet struct {
	Id        int64
	Category  *Category
	Name      string
	PhotoUrls []string
	Tags      []*Tag
	Status    Status
	CreatedAt int64
}

// Tag is a label of a pet.
type Tag struct {
	Id   int64
	Name string
}

// The ids are not in the tables of FlatBuffers. The id of a record is its key in the store,
// which sets it on the record after it reads the record.

// GetId implements Record.
func (c *Category) GetId() int64 { return c.Id }

// SetId implements Record.
func (c *Category) SetId(id int64) { c.Id = id }

// GetId implements Record.
func (p *PetRecord) GetId() int64 { return p.Id }

// SetId implements Record.
func (p *PetRecord) SetId(id int64) { p.Id = id }

// builders are the builders of FlatBuffers that are not in use, which keep their buffers.
var builders = sync.Pool{New: func() any { return flatbuffers.NewBuilder(128) }}

// finish returns the bytes of the table that the builder was filled with, in a slice that the
// caller owns, and gives the builder back.
func finish(b *flatbuffers.Builder, table flatbuffers.UOffsetT) []byte {
	b.Finish(table)
	bz := bytes.Clone(b.FinishedBytes())
	builders.Put(b)
	return bz
}

// newBuilder returns an empty builder.
func newBuilder() *flatbuffers.Builder {
	b := builders.Get().(*flatbuffers.Builder)
	b.Reset()
	return b
}

// corrupt turns the panic of reading data that is not a table into an error. The tables are
// read without being checked, and the reads that go out of the data panic.
func corrupt(err *error) {
	if r := recover(); r != nil {
		*err = fmt.Errorf("types: the data is not a valid FlatBuffers table: %v", r)
	}
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (c *Category) MarshalBinary() ([]byte, error) {
	b := newBuilder()
	name := b.CreateString(c.Name)
	fb.CategoryStart(b)
	fb.CategoryAddName(b, name)
	return finish(b, fb.CategoryEnd(b)), nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler. The strings are copied, so the
// category does not refer to the data.
func (c *Category) UnmarshalBinary(data []byte) (err error) {
	defer corrupt(&err)
	t := fb.GetRootAsCategory(data, 0)
	*c = Category{Name: string(t.Name())}
	return nil
}

// UnmarshalPartial implements codec.PartialUnmarshaler. It reads the named fields from the data
// where they are, without decoding the others. A field that it does not know makes it read all.
func (c *Category) UnmarshalPartial(data []byte, fields []string) (err error) {
	defer corrupt(&err)
	t := fb.GetRootAsCategory(data, 0)
	for _, f := range fields {
		switch f {
		case "Id": // It is not in the data: the store sets it.
		case "Name":
			c.Name = string(t.Name())
		default:
			return c.UnmarshalBinary(data)
		}
	}
	return nil
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (p *PetRecord) MarshalBinary() ([]byte, error) {
	b := newBuilder()
	name := b.CreateString(p.Name)
	fb.PetRecordStart(b)
	fb.PetRecordAddCategoryId(b, p.CategoryId)
	fb.PetRecordAddName(b, name)
	fb.PetRecordAddStatus(b, fb.Status(p.Status))
	fb.PetRecordAddCreatedAt(b, p.CreatedAt)
	return finish(b, fb.PetRecordEnd(b)), nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler. The strings are copied, so the pet
// does not refer to the data.
func (p *PetRecord) UnmarshalBinary(data []byte) (err error) {
	defer corrupt(&err)
	t := fb.GetRootAsPetRecord(data, 0)
	*p = PetRecord{
		CategoryId: t.CategoryId(),
		Name:       string(t.Name()),
		Status:     Status(t.Status()),
		CreatedAt:  t.CreatedAt(),
	}
	return nil
}

// UnmarshalPartial implements codec.PartialUnmarshaler. It reads the named fields from the data
// where they are, without decoding the others. A field that it does not know makes it read all.
func (p *PetRecord) UnmarshalPartial(data []byte, fields []string) (err error) {
	defer corrupt(&err)
	t := fb.GetRootAsPetRecord(data, 0)
	for _, f := range fields {
		switch f {
		case "Id": // It is not in the data: the store sets it.
		case "CategoryId":
			p.CategoryId = t.CategoryId()
		case "Name":
			p.Name = string(t.Name())
		case "Status":
			p.Status = Status(t.Status())
		case "CreatedAt":
			p.CreatedAt = t.CreatedAt()
		default:
			return p.UnmarshalBinary(data)
		}
	}
	return nil
}
