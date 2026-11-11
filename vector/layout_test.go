package vector_test

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/ehsanranjbar/kvutil/vector"
	"github.com/stretchr/testify/require"
)

// vectorEntryKey returns the key of the vector of a record in a partition, as the index lays
// its keys out: the prefix, 'p', the partition and the primary key.
func vectorEntryKey(prefix []byte, pid uint32, rawKey []byte) []byte {
	k := append([]byte{}, prefix...)
	k = append(k, 'p')
	k = binary.BigEndian.AppendUint32(k, pid)
	return append(k, rawKey...)
}

// layout is what the keys of an index come to.
type layout struct {
	// Placed and Unplaced are the numbers of vectors in partitions and in the unplaced area.
	Placed, Unplaced int
	// Partitions is the number of centroids.
	Partitions int
	// Sizes are the numbers of vectors that the partitions have.
	Sizes map[uint32]int
}

// prepared returns the vector as the index stores it.
func prepared(metric vector.Metric, v []float32) []float32 {
	if metric != vector.Cosine {
		return v
	}
	var sum float64
	for _, c := range v {
		sum += float64(c) * float64(c)
	}
	out := make([]float32, len(v))
	for i, c := range v {
		out[i] = float32(float64(c) / math.Sqrt(sum))
	}
	return out
}

// checkLayout checks every key under the prefix of an index against the records of the store:
//
//   - a record that has a vector has exactly one vector in the index, which is the prepared one,
//     and nothing else is stored as a vector;
//   - a record has a place iff its vector is in a partition, and the place names it;
//   - every partition that has a vector has a centroid, of the right length;
//   - the state is there iff there is a centroid;
//   - there are no other keys.
func checkLayout(t *testing.T, db kv.DB, x *vector.Index[int64, doc], docs *extstore.Store[int64, doc]) layout {
	t.Helper()

	got := layout{Sizes: map[uint32]int{}}
	require.NoError(t, db.View(func(tx kv.Tx) error {
		want := map[string][]float32{}
		records, err := docs.Instantiate(tx).All()
		require.NoError(t, err)
		for id, d := range iterutil.Of(records).All(&err) {
			if len(d.Embedding) == 0 {
				continue
			}
			rawKey, encErr := docs.KeyCodec().Encode(id)
			require.NoError(t, encErr)
			want[string(rawKey)] = prepared(x.Metric(), d.Embedding)
		}
		require.NoError(t, err)

		var (
			vectors    = map[string]uint32{}
			places     = map[string]uint32{}
			centroids  = map[uint32]bool{}
			hasState   bool
			byteLength = 4 * x.Dims()
		)

		p := x.Prefix()
		it, err := tx.Iterator(p, kv.PrefixEnd(p))
		require.NoError(t, err)
		defer it.Close()
		for ; it.Valid(); it.Next() {
			key := it.Key()[len(p):]
			value, err := it.Value()
			require.NoError(t, err)
			require.NotEmpty(t, key)

			switch key[0] {
			case 'p':
				require.GreaterOrEqual(t, len(key), 5, "a vector key %x", key)
				pid, rawKey := binary.BigEndian.Uint32(key[1:5]), string(key[5:])
				expected, ok := want[rawKey]
				require.True(t, ok, "a vector of %x, which has no record with one", rawKey)
				_, twice := vectors[rawKey]
				require.False(t, twice, "two vectors of %x", rawKey)
				vectors[rawKey] = pid

				require.Len(t, value, byteLength)
				stored := make([]float32, x.Dims())
				for i := range stored {
					stored[i] = math.Float32frombits(binary.LittleEndian.Uint32(value[4*i:]))
				}
				require.InDeltaSlice(t, expected, stored, 1e-6, "the vector of %x", rawKey)
				got.Sizes[pid]++
			case 'k':
				require.Len(t, value, 4, "the place of %x", key[1:])
				places[string(key[1:])] = binary.BigEndian.Uint32(value)
			case 'c':
				require.Len(t, key, 5, "a centroid key %x", key)
				require.Len(t, value, byteLength)
				centroids[binary.BigEndian.Uint32(key[1:])] = true
			case 's':
				require.Len(t, key, 1)
				require.Len(t, value, 12)
				hasState = true
			default:
				require.Fail(t, "an unknown key", "%x", key)
			}
		}

		require.Len(t, vectors, len(want), "every record with a vector has one")
		for rawKey, pid := range vectors {
			place, placed := places[rawKey]
			if pid == 0 {
				require.False(t, placed, "%x is unplaced and has a place", rawKey)
				got.Unplaced++
				continue
			}
			require.True(t, placed, "%x is in the partition %d and has no place", rawKey, pid)
			require.Equal(t, pid, place, "the place of %x", rawKey)
			require.True(t, centroids[pid], "the partition %d has no centroid", pid)
			got.Placed++
		}
		require.Len(t, places, got.Placed, "a place with no vector in a partition")
		require.Equal(t, len(centroids) > 0, hasState, "the state is there iff there are centroids")
		got.Partitions = len(centroids)
		return nil
	}))
	return got
}
