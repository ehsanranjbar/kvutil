package lex

import (
	"bytes"
	"errors"
	"fmt"
)

// ErrUnterminated is returned when a value has no terminator.
var ErrUnterminated = errors.New("lex: value is not terminated")

// AppendTerminated appends b to dst in a form that marks where b ends and that still sorts
// as b does, so that it can be followed by other components in a key.
// Every 0x00 byte is written as 0x00 0xff, and the end is written as 0x00 0x01.
func AppendTerminated(dst, b []byte) []byte {
	for {
		i := bytes.IndexByte(b, 0x00)
		if i < 0 {
			break
		}
		dst = append(dst, b[:i]...)
		dst = append(dst, 0x00, 0xff)
		b = b[i+1:]
	}
	dst = append(dst, b...)
	return append(dst, 0x00, 0x01)
}

// ReadTerminated reads a value that was written by AppendTerminated from the front of b.
// It returns the value in a slice of its own and the number of bytes that it took in b.
func ReadTerminated(b []byte) (value []byte, n int, err error) {
	value = []byte{}
	for {
		i := bytes.IndexByte(b[n:], 0x00)
		if i < 0 || n+i+1 >= len(b) {
			return nil, 0, ErrUnterminated
		}
		value = append(value, b[n:n+i]...)

		switch b[n+i+1] {
		case 0x01:
			return value, n + i + 2, nil
		case 0xff:
			value = append(value, 0x00)
			n += i + 2
		default:
			return nil, 0, fmt.Errorf("lex: invalid escape 0x%02x", b[n+i+1])
		}
	}
}
