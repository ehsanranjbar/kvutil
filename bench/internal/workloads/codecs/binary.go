package codecs

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
)

// OrderBin is a record that encodes itself with MarshalBinary and UnmarshalBinary: a format
// written by hand, which is what a program does that wants the smallest and fastest encoding.
// Being a type of its own, it is not the one that the msgpack variant stores.
type OrderBin dataset.Order

// GetId returns the id of the record.
func (o *OrderBin) GetId() int64 { return o.ID }

// SetId sets the id of the record.
func (o *OrderBin) SetId(id int64) { o.ID = id }

func appendString(buf []byte, s string) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(s)))
	return append(buf, s...)
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (o *OrderBin) MarshalBinary() ([]byte, error) {
	size := 64 + len(o.Code) + len(o.Customer) + len(o.Name) + len(o.Status) + len(o.Payload)
	if o.Note != nil {
		size += len(*o.Note)
	}
	for _, t := range o.Tags {
		size += len(t) + 2
	}
	buf := make([]byte, 0, size)
	buf = appendString(buf, o.Code)
	buf = appendString(buf, o.Customer)
	buf = appendString(buf, o.Name)
	buf = appendString(buf, o.Status)
	buf = binary.AppendVarint(buf, o.Total)
	buf = binary.AppendVarint(buf, int64(o.Qty))
	buf = binary.AppendVarint(buf, o.At.UnixNano())
	if o.Note == nil {
		buf = append(buf, 0)
	} else {
		buf = append(buf, 1)
		buf = appendString(buf, *o.Note)
	}
	buf = binary.AppendUvarint(buf, uint64(len(o.Tags)))
	for _, t := range o.Tags {
		buf = appendString(buf, t)
	}
	buf = binary.AppendUvarint(buf, uint64(len(o.Payload)))
	return append(buf, o.Payload...), nil
}

var errShort = errors.New("codecs: the binary record is truncated")

type reader struct {
	bz  []byte
	err error
}

func (r *reader) uvarint() uint64 {
	v, n := binary.Uvarint(r.bz)
	if n <= 0 {
		r.err = errShort
		return 0
	}
	r.bz = r.bz[n:]
	return v
}

func (r *reader) varint() int64 {
	v, n := binary.Varint(r.bz)
	if n <= 0 {
		r.err = errShort
		return 0
	}
	r.bz = r.bz[n:]
	return v
}

func (r *reader) bytes() []byte {
	n := r.uvarint()
	if r.err != nil || uint64(len(r.bz)) < n {
		r.err = errShort
		return nil
	}
	b := r.bz[:n]
	r.bz = r.bz[n:]
	return b
}

func (r *reader) string() string { return string(r.bytes()) }

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (o *OrderBin) UnmarshalBinary(bz []byte) error {
	r := &reader{bz: bz}
	o.Code = r.string()
	o.Customer = r.string()
	o.Name = r.string()
	o.Status = r.string()
	o.Total = r.varint()
	o.Qty = int32(r.varint())
	o.At = time.Unix(0, r.varint()).UTC()
	o.Note = nil
	if len(r.bz) > 0 {
		flag := r.bz[0]
		r.bz = r.bz[1:]
		if flag == 1 {
			note := r.string()
			o.Note = &note
		}
	} else {
		r.err = errShort
	}
	o.Tags = nil
	if n := r.uvarint(); r.err == nil && n > 0 {
		if n > uint64(len(r.bz)) {
			return errShort
		}
		o.Tags = make([]string, n)
		for i := range o.Tags {
			o.Tags[i] = r.string()
		}
	}
	o.Payload = nil
	if p := r.bytes(); len(p) > 0 {
		o.Payload = append([]byte(nil), p...)
	}
	return r.err
}
