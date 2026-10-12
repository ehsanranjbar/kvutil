package codec

import "fmt"

// Pair is a key of two components, which sorts by the first one and then by the second one.
// A key of more components is a pair that has a pair as its second component.
type Pair[A, B any] struct {
	First  A
	Second B
}

// NewPair creates a new pair.
func NewPair[A, B any](first A, second B) Pair[A, B] {
	return Pair[A, B]{First: first, Second: second}
}

// keyCodec implements the keyCodecProvider interface.
func (Pair[A, B]) keyCodec() (any, error) {
	first, err := KeyFor[A]()
	if err != nil {
		return nil, err
	}
	second, err := KeyFor[B]()
	if err != nil {
		return nil, err
	}
	return PairKey(first, second), nil
}

// anyKeyCodec implements the keyCodecProvider interface.
func (p Pair[A, B]) anyKeyCodec() (KeyCodec[any], error) {
	c, err := p.keyCodec()
	if err != nil {
		return nil, err
	}
	return anyKeyOf(c.(KeyCodec[Pair[A, B]])), nil
}

// PairKey returns the key codec of the pairs of two types, given the key codecs of the two.
func PairKey[A, B any](first KeyCodec[A], second KeyCodec[B]) KeyCodec[Pair[A, B]] {
	return pairKey[A, B]{first: first, second: second}
}

type pairKey[A, B any] struct {
	first  KeyCodec[A]
	second KeyCodec[B]
}

// Name implements the KeyCodec interface.
func (c pairKey[A, B]) Name() string {
	return fmt.Sprintf("pair(%s,%s)", c.first.Name(), c.second.Name())
}

// Encode implements the KeyCodec interface.
func (c pairKey[A, B]) Encode(k Pair[A, B]) ([]byte, error) {
	bz, err := c.first.Append(nil, k.First)
	if err != nil {
		return nil, err
	}

	// The second component is the last one, so it does not have to mark where it ends.
	second, err := c.second.Encode(k.Second)
	if err != nil {
		return nil, err
	}
	return append(bz, second...), nil
}

// Decode implements the KeyCodec interface.
func (c pairKey[A, B]) Decode(bz []byte) (k Pair[A, B], err error) {
	var n int
	k.First, n, err = c.first.Read(bz)
	if err != nil {
		return k, err
	}

	k.Second, err = c.second.Decode(bz[n:])
	return k, err
}

// Append implements the KeyCodec interface.
func (c pairKey[A, B]) Append(dst []byte, k Pair[A, B]) ([]byte, error) {
	dst, err := c.first.Append(dst, k.First)
	if err != nil {
		return nil, err
	}
	return c.second.Append(dst, k.Second)
}

// Read implements the KeyCodec interface.
func (c pairKey[A, B]) Read(bz []byte) (k Pair[A, B], n int, err error) {
	k.First, n, err = c.first.Read(bz)
	if err != nil {
		return k, 0, err
	}

	var m int
	k.Second, m, err = c.second.Read(bz[n:])
	if err != nil {
		return k, 0, err
	}
	return k, n + m, nil
}
