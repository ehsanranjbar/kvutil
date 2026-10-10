package codec_test

import (
	"errors"
	"testing"

	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/stretchr/testify/require"
)

type plainValue struct {
	Name  string
	Count int
	Tags  []string
}

// selfValue encodes itself, with a value receiver to marshal and a pointer receiver to unmarshal.
type selfValue struct {
	Name string
}

func (v selfValue) MarshalBinary() ([]byte, error) {
	return []byte("self:" + v.Name), nil
}

func (v *selfValue) UnmarshalBinary(data []byte) error {
	if len(data) < 5 || string(data[:5]) != "self:" {
		return errors.New("not a self value")
	}
	v.Name = string(data[5:])
	return nil
}

// halfValue only marshals itself, so it does not count as a type that encodes itself.
type halfValue struct {
	Name string
}

func (v halfValue) MarshalBinary() ([]byte, error) {
	return nil, errors.New("must not be used")
}

func TestFormats(t *testing.T) {
	want := plainValue{Name: "foo", Count: 3, Tags: []string{"a", "b"}}

	for _, format := range []codec.Format{codec.Msgpack, codec.JSON} {
		t.Run(format.Name(), func(t *testing.T) {
			c := codec.From[plainValue](format)
			require.Equal(t, format.Name(), c.Name())

			bz, err := c.Encode(want)
			require.NoError(t, err)

			got, err := c.Decode(bz)
			require.NoError(t, err)
			require.Equal(t, want, got)

			_, err = c.Decode([]byte{0xc1})
			require.Error(t, err)
		})
	}

	bz, err := codec.From[plainValue](codec.JSON).Encode(want)
	require.NoError(t, err)
	require.JSONEq(t, `{"Name":"foo","Count":3,"Tags":["a","b"]}`, string(bz))
}

func TestBinary(t *testing.T) {
	c := codec.Binary[selfValue]()
	require.Equal(t, "binary", c.Name())

	bz, err := c.Encode(selfValue{Name: "foo"})
	require.NoError(t, err)
	require.Equal(t, []byte("self:foo"), bz)

	got, err := c.Decode(bz)
	require.NoError(t, err)
	require.Equal(t, selfValue{Name: "foo"}, got)

	_, err = c.Decode([]byte("other"))
	require.Error(t, err)
}

func TestValueFor(t *testing.T) {
	t.Run("SelfEncodingTypeIgnoresTheFormat", func(t *testing.T) {
		for _, format := range []codec.Format{nil, codec.JSON} {
			c := codec.ValueFor[selfValue](format)
			require.Equal(t, "binary", c.Name())

			bz, err := c.Encode(selfValue{Name: "foo"})
			require.NoError(t, err)
			require.Equal(t, []byte("self:foo"), bz)
		}
	})

	t.Run("GivenFormat", func(t *testing.T) {
		c := codec.ValueFor[plainValue](codec.JSON)
		require.Equal(t, "json", c.Name())
	})

	t.Run("MsgpackWithoutFormat", func(t *testing.T) {
		c := codec.ValueFor[plainValue](nil)
		require.Equal(t, "msgpack", c.Name())

		bz, err := c.Encode(plainValue{Name: "foo"})
		require.NoError(t, err)
		got, err := c.Decode(bz)
		require.NoError(t, err)
		require.Equal(t, plainValue{Name: "foo"}, got)
	})

	t.Run("HalfSelfEncodingTypeUsesTheFormat", func(t *testing.T) {
		c := codec.ValueFor[halfValue](codec.JSON)
		require.Equal(t, "json", c.Name())

		bz, err := c.Encode(halfValue{Name: "foo"})
		require.NoError(t, err)
		require.JSONEq(t, `{"Name":"foo"}`, string(bz))
	})

	t.Run("Scalars", func(t *testing.T) {
		c := codec.ValueFor[int64](nil)
		bz, err := c.Encode(42)
		require.NoError(t, err)
		got, err := c.Decode(bz)
		require.NoError(t, err)
		require.Equal(t, int64(42), got)
	})
}
