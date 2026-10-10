package lex

import (
	"encoding/binary"
	"math"
)

// EncodeInt8 returns the byte slice representation of the given int8 which is encoded in lexicographical order.
func EncodeInt8(v int8) []byte {
	return []byte{uint8(v) ^ (1 << 7)}
}

// DecodeInt8 returns the int8 representation of the given byte slice which is decoded from lexicographical order.
func DecodeInt8(b []byte) int8 {
	return int8(b[0] ^ (1 << 7))
}

// EncodeInt16 returns the byte slice representation of the given int16 which is encoded in lexicographical order.
func EncodeInt16(v int16) []byte {
	return EncodeUint16(uint16(v) ^ (1 << 15))
}

// DecodeInt16 returns the int16 representation of the given byte slice which is decoded from lexicographical order.
func DecodeInt16(b []byte) int16 {
	u := DecodeUint16(b)
	return int16(u ^ (1 << 15))
}

// EncodeUint16 returns the byte slice representation of the given uint16.
func EncodeUint16(v uint16) []byte {
	return binary.BigEndian.AppendUint16(nil, v)
}

// DecodeUint16 returns the uint16 representation of the given byte slice.
func DecodeUint16(b []byte) uint16 {
	return binary.BigEndian.Uint16(b)
}

// EncodeInt32 returns the byte slice representation of the given int32 which is encoded in lexicographical order.
func EncodeInt32(v int32) []byte {
	return EncodeUint32(uint32(v) ^ (1 << 31))
}

// DecodeInt32 returns the int32 representation of the given byte slice which is decoded from lexicographical order.
func DecodeInt32(b []byte) int32 {
	u := DecodeUint32(b)
	return int32(u ^ (1 << 31))
}

// EncodeUint32 returns the byte slice representation of the given uint32.
func EncodeUint32(v uint32) []byte {
	return binary.BigEndian.AppendUint32(nil, v)
}

// DecodeUint32 returns the uint32 representation of the given byte slice.
func DecodeUint32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

// EncodeInt64 returns the byte slice representation of the given int64 which is encoded in lexicographical order.
func EncodeInt64(v int64) []byte {
	return EncodeUint64(uint64(v) ^ (1 << 63))
}

// DecodeInt64 returns the int64 representation of the given byte slice which is decoded from lexicographical order.
func DecodeInt64(b []byte) int64 {
	u := DecodeUint64(b)
	return int64(u ^ (1 << 63))
}

// EncodeUint64 returns the byte slice representation of the given uint64.
func EncodeUint64(v uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, v)
}

// DecodeUint64 returns the uint64 representation of the given byte slice.
func DecodeUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// EncodeFloat32 returns the byte slice representation of the given float32 which is encoded in lexicographical order.
// NaN sorts before every other value. -0 and +0 are equal, so both are encoded as +0.
func EncodeFloat32(f float32) []byte {
	if math.IsNaN(float64(f)) {
		return EncodeUint32(0)
	}
	if f == 0 {
		return EncodeUint32(1 << 31)
	}

	u := math.Float32bits(f)
	if f < 0 {
		u = ^u
	} else {
		u ^= (1 << 31)
	}

	return EncodeUint32(u)
}

// DecodeFloat32 returns the float32 representation of the given byte slice which is decoded from lexicographical order.
func DecodeFloat32(b []byte) float32 {
	u := DecodeUint32(b)
	if u == 0 {
		return float32(math.NaN())
	}

	if u < (1 << 31) {
		u = ^u
	} else {
		u ^= (1 << 31)
	}

	return math.Float32frombits(u)
}

// EncodeFloat64 returns the byte slice representation of the given float64 which is encoded in lexicographical order.
// NaN sorts before every other value. -0 and +0 are equal, so both are encoded as +0.
func EncodeFloat64(f float64) []byte {
	if math.IsNaN(f) {
		return EncodeUint64(0)
	}
	if f == 0 {
		return EncodeUint64(1 << 63)
	}

	u := math.Float64bits(f)
	if f < 0 {
		u = ^u
	} else {
		u ^= (1 << 63)
	}

	return EncodeUint64(u)
}

// DecodeFloat64 returns the float64 representation of the given byte slice which is decoded from lexicographical order.
func DecodeFloat64(b []byte) float64 {
	u := DecodeUint64(b)
	if u == 0 {
		return math.NaN()
	}

	if u < (1 << 63) {
		u = ^u
	} else {
		u ^= (1 << 63)
	}

	return math.Float64frombits(u)
}
