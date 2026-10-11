package record

import (
	"bytes"

	msgpack "github.com/vmihailenco/msgpack/v5"
)

// Doc is a Record type that is schemaless.
// It is serialized entirely in msgpack format, without its id, which is the key of the record.
type Doc[I comparable] map[string]any

// GetId implements the Record interface.
// It returns the zero value if the document has no id yet.
func (doc Doc[I]) GetId() I {
	id, _ := doc["_id"].(I)
	return id
}

// SetId implements the Record interface.
func (doc Doc[I]) SetId(id I) {
	doc["_id"] = id
}

// MarshalBinary implements the encoding.BinaryMarshaler interface.
func (doc Doc[I]) MarshalBinary() ([]byte, error) {
	enc := msgpack.GetEncoder()
	var buf bytes.Buffer
	enc.Reset(&buf)
	defer msgpack.PutEncoder(enc)

	err := doc.encode(enc)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// encode encodes the document to the encoder.
func (doc Doc[I]) encode(enc *msgpack.Encoder) error {
	if doc == nil {
		return enc.EncodeNil()
	}
	// The id is serialized as the key of the record, so it is left out of the value.
	n := len(doc)
	if _, ok := doc["_id"]; ok {
		n--
	}
	if err := enc.EncodeMapLen(n); err != nil {
		return err
	}
	for k, v := range doc {
		if k == "_id" {
			continue
		}

		if err := enc.EncodeString(k); err != nil {
			return err
		}
		if err := enc.Encode(v); err != nil {
			return err
		}
	}
	return nil
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.
func (doc *Doc[I]) UnmarshalBinary(data []byte) error {
	dec := msgpack.GetDecoder()
	dec.Reset(bytes.NewReader(data))
	defer msgpack.PutDecoder(dec)

	// Decoding into doc itself would make msgpack call UnmarshalBinary again.
	m, err := dec.DecodeMap()
	if err != nil {
		return err
	}
	if m == nil {
		// So that the id can be set on the document.
		m = map[string]any{}
	}
	*doc = m
	return nil
}
