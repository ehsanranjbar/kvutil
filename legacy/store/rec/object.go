package rec

import (
	"bytes"
	"fmt"

	msgpack "github.com/vmihailenco/msgpack/v5"
)

// Object is a generic object that can be stored in as a Record.
type Object[I comparable, D any] struct {
	Id       I              `json:"id,omitempty"`
	Data     D              `json:"data,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// NewObject creates a new object with the given data.
func NewObject[I comparable, D any](data D) *Object[I, D] {
	return &Object[I, D]{
		Data:     data,
		Metadata: make(map[string]any),
	}
}

// NewObjectWithId creates a new object with the given data and id.
func NewObjectWithId[I comparable, D any](id I, data D) *Object[I, D] {
	return &Object[I, D]{
		Id:       id,
		Data:     data,
		Metadata: make(map[string]any),
	}
}

// GetId implements the Record interface.
func (obj Object[I, D]) GetId() I {
	return obj.Id
}

// SetId implements the Record interface.
func (obj *Object[I, D]) SetId(id I) {
	obj.Id = id
}

// MarshalBinary implements the encoding.BinaryMarshaler interface.
func (obj Object[I, D]) MarshalBinary() ([]byte, error) {
	enc := msgpack.GetEncoder()
	var buf bytes.Buffer
	enc.Reset(&buf)
	defer msgpack.PutEncoder(enc)

	err := enc.Encode(&obj.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to encode object's data: %w", err)
	}

	err = enc.EncodeMap(obj.Metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to encode object's metadata: %w", err)
	}

	return buf.Bytes(), nil
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.
func (obj *Object[I, D]) UnmarshalBinary(data []byte) error {
	dec := msgpack.GetDecoder()
	dec.Reset(bytes.NewReader(data))
	defer msgpack.PutDecoder(dec)

	err := dec.Decode(&obj.Data)
	if err != nil {
		return fmt.Errorf("failed to decode object's data: %w", err)
	}

	obj.Metadata, err = dec.DecodeMap()
	if err != nil {
		return fmt.Errorf("failed to decode object's metadata: %w", err)
	}

	return nil
}
