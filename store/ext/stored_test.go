package ext_test

import (
	"sync/atomic"
	"testing"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/kv"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/stretchr/testify/require"
)

type person struct {
	Name string
	Age  int
	City string
}

// countingCodec counts the full and the partial decodes of a value codec.
type countingCodec struct {
	codec.ValueCodec[person]
	full, partial *atomic.Int64
}

func (c countingCodec) Decode(bz []byte) (person, error) {
	c.full.Add(1)
	return c.ValueCodec.Decode(bz)
}

func (c countingCodec) PartialDecode(fields []string) (func([]byte, *person) error, bool) {
	decode, ok := codec.PartialOf(c.ValueCodec, fields)
	if !ok {
		return nil, false
	}
	return func(bz []byte, v *person) error {
		c.partial.Add(1)
		return decode(bz, v)
	}, true
}

var people = []*person{
	{Name: "ann", Age: 31, City: "oslo"},
	{Name: "bob", Age: 42, City: "rome"},
	{Name: "cy", Age: 53, City: "lima"},
}

func TestStoredDecodesFromOneRead(t *testing.T) {
	testkv.Each(t, func(t *testing.T, base kv.DB) {
		cc := countingCodec{
			ValueCodec: codec.ValueFor[person](nil),
			full:       &atomic.Int64{},
			partial:    &atomic.Int64{},
		}
		store := extstore.NewWithCodecs[int64, person]("people", nil, cc)
		db, counts := testkv.Count(base)
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			for i, p := range people {
				if err := store.Instantiate(tx).Set(int64(i+1), p); err != nil {
					return err
				}
			}
			return nil
		}))

		decode, ok := codec.PartialOf[person](cc, []string{"Name"})
		require.True(t, ok)
		reset := func() {
			counts.Reset()
			cc.full.Store(0)
			cc.partial.Store(0)
		}

		t.Run("getter", func(t *testing.T) {
			require.NoError(t, db.View(func(tx kv.Tx) error {
				reset()
				var getter extstore.Getter[int64, person] = store.Getter(tx)
				sg, ok := getter.(extstore.StoredGetter[int64, person])
				require.True(t, ok)

				st, err := sg.GetStored(2)
				require.NoError(t, err)

				// A value that is not zero is zeroed before the fields are decoded.
				got, err := st.Partial(decode, &person{Name: "dirty", Age: 7, City: "dirty"})
				require.NoError(t, err)
				require.Equal(t, &person{Name: "bob"}, got)

				full, err := st.Record()
				require.NoError(t, err)
				require.Equal(t, people[1], full)

				require.EqualValues(t, 1, counts.Gets.Load())
				require.EqualValues(t, 1, cc.partial.Load())
				require.EqualValues(t, 1, cc.full.Load())

				_, err = sg.GetStored(99)
				require.ErrorIs(t, err, kv.ErrNotFound)
				return nil
			}))
		})

		t.Run("iterator", func(t *testing.T) {
			require.NoError(t, db.View(func(tx kv.Tx) error {
				reset()
				it, err := store.Records(tx, kvutil.All[int64]())
				require.NoError(t, err)
				defer it.Close()
				si, ok := it.(extstore.StoredIterator[int64, person])
				require.True(t, ok)

				var into person
				n := 0
				for ; it.Valid(); it.Next() {
					st, err := si.Stored()
					require.NoError(t, err)
					got, err := st.Partial(decode, &into)
					require.NoError(t, err)
					require.Equal(t, &person{Name: people[n].Name}, got)

					// The stored record can be asked for again at the same position.
					st, err = si.Stored()
					require.NoError(t, err)
					full, err := st.Record()
					require.NoError(t, err)
					require.Equal(t, people[n], full)
					n++
				}
				require.Equal(t, len(people), n)
				require.EqualValues(t, len(people), counts.Values.Load(), "the value is read once at a position")
				require.EqualValues(t, len(people), cc.partial.Load())
				require.EqualValues(t, len(people), cc.full.Load())

				// The records are still there for whoever reads them without Stored.
				it2, err := store.Records(tx, kvutil.All[int64]())
				require.NoError(t, err)
				defer it2.Close()
				v, err := it2.Value()
				require.NoError(t, err)
				require.Equal(t, people[0], v)
				return nil
			}))
		})
	})
}

func TestStoredFixIsApplied(t *testing.T) {
	cc := codec.ValueFor[person](nil)
	bz, err := cc.Encode(*people[0])
	require.NoError(t, err)
	decode, ok := codec.PartialOf(cc, []string{"Name"})
	require.True(t, ok)

	fix := func(v *person, key int64) { v.Age = int(key) }
	st := extstore.NewStored(int64(5), bz, cc, fix)

	got, err := st.Partial(decode, &person{})
	require.NoError(t, err)
	require.Equal(t, &person{Name: "ann", Age: 5}, got)

	full, err := st.Record()
	require.NoError(t, err)
	require.Equal(t, &person{Name: "ann", Age: 5, City: "oslo"}, full)

	// Fixed replaces the key and the function.
	st = extstore.NewStored(int64(5), bz, cc, nil).Fixed(8, fix)
	full, err = st.Record()
	require.NoError(t, err)
	require.Equal(t, 8, full.Age)

	// A value that cannot be decoded is an error that says so.
	bad := extstore.NewStored(int64(1), []byte{0xc1}, cc, nil)
	_, err = bad.Record()
	require.ErrorContains(t, err, "failed to decode value")
	_, err = bad.Partial(decode, &person{})
	require.ErrorContains(t, err, "failed to decode value")
}

func TestStoredDecoded(t *testing.T) {
	record := &person{Name: "ann", Age: 31, City: "oslo"}
	st := extstore.Decoded(int64(1), record)

	decode, ok := codec.PartialOf(codec.ValueFor[person](nil), []string{"Name"})
	require.True(t, ok)

	got, err := st.Partial(decode, &person{})
	require.NoError(t, err)
	require.Same(t, record, got, "a record that is decoded has all the fields")

	full, err := st.Record()
	require.NoError(t, err)
	require.Same(t, record, full)
}
