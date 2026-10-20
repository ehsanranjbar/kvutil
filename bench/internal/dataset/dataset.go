// Package dataset generates the records that every workload writes, deterministically: a record
// is a pure function of the seed, its id and its version, so that both systems get the same
// records, a run can be repeated, and the verify command can compare what the systems return.
package dataset

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ValueSize is the size of the values that are stored.
type ValueSize string

const (
	// Small is a record of about 200 bytes once encoded.
	Small ValueSize = "small"
	// Large is a record of about 4 KB once encoded: the small one with a payload of 4096 bytes.
	Large ValueSize = "large"
)

// PayloadBytes is the size of the payload of a record of the size.
func (v ValueSize) PayloadBytes() int {
	if v == Large {
		return 4096
	}
	return 0
}

// Valid reports whether the size is one that is known.
func (v ValueSize) Valid() bool { return v == Small || v == Large }

// Order is the record of the benchmarks: some integers, strings, a time, an optional field and a
// list of tags, and a payload that makes the value large. The id is the key of the record and is
// not a part of the value.
type Order struct {
	ID       int64     `msgpack:"-" json:"-"`
	Code     string    // unique among the records, and different for every version of one
	Customer string    // one of Customers() values, so that each has some records
	Name     string    // mixed case words
	Status   string    // one of Statuses
	Total    int64     // 0 up to 99999
	Qty      int32     // 1 up to 20
	At       time.Time // second resolution, UTC, within two years
	Note     *string   // optional: nil for about half of the records
	Tags     []string  // 0 up to 4 distinct values of 50
	Payload  []byte    // random bytes, which no compression can shrink
}

// GetId returns the id of the record.
func (o *Order) GetId() int64 { return o.ID }

// SetId sets the id of the record.
func (o *Order) SetId(id int64) { o.ID = id }

// Statuses are the values that Status has.
var Statuses = []string{"new", "paid", "shipped", "late", "cancelled"}

// BigTotal is the least Total of the records that the conditional indexes keep: a tenth of them.
const BigTotal = 90000

// TagPool is the number of different tags.
const TagPool = 50

var (
	adjectives = []string{"Blue", "Red", "Green", "Grand", "Tiny", "Rapid", "Quiet", "Bold",
		"Sturdy", "Fancy", "Plain", "Deluxe", "Rustic", "Modern", "Classic", "Noble"}
	nouns = []string{"Widget", "Gadget", "Sprocket", "Bracket", "Lantern", "Kettle", "Compass", "Anchor",
		"Ladder", "Mirror", "Pillow", "Basket", "Hammer", "Pencil", "Teapot", "Violin"}
	words = []string{"deliver", "before", "noon", "gift", "wrap", "fragile", "call", "ahead", "leave",
		"at", "door", "express", "return", "label", "insured", "signature", "back", "order"}
)

// Spec is what a dataset is generated from.
type Spec struct {
	Seed   uint64
	Scale  int
	Values ValueSize
}

// Customers is the number of different customers: about ten records each.
func (s Spec) Customers() int {
	return max(10, s.Scale/10)
}

// Gen returns the record of an id in a version. The initial version of a record is 0.
func (s Spec) Gen(id int64, version uint32) *Order {
	o := &Order{}
	s.GenInto(o, id, version)
	return o
}

// GenInto is Gen into a record that is given, which reuses its payload.
func (s Spec) GenInto(o *Order, id int64, version uint32) {
	r := newRNG(s.Seed, uint64(id), uint64(version))

	o.ID = id
	o.Code = fmt.Sprintf("ORD-%08X-%04X", uint64(id), version)
	o.Customer = "cust-" + pad(r.intn(s.Customers()), 6)
	o.Name = adjectives[r.intn(len(adjectives))] + " " + nouns[r.intn(len(nouns))] + " " + strconv.Itoa(1+r.intn(99))
	o.Status = Statuses[r.intn(len(Statuses))]
	o.Total = int64(r.intn(100000))
	o.Qty = int32(1 + r.intn(20))
	o.At = time.Unix(1704067200+int64(r.intn(2*365*86400)), 0).UTC()

	o.Note = nil
	if r.intn(2) == 0 {
		var sb strings.Builder
		for i, n := 0, 3+r.intn(4); i < n; i++ {
			if i > 0 {
				sb.WriteByte(' ')
			}
			sb.WriteString(words[r.intn(len(words))])
		}
		note := sb.String()
		o.Note = &note
	}

	o.Tags = o.Tags[:0]
	for n := r.intn(5); len(o.Tags) < n; {
		tag := "tag-" + pad(r.intn(TagPool), 2)
		if !contains(o.Tags, tag) {
			o.Tags = append(o.Tags, tag)
		}
	}
	if len(o.Tags) == 0 {
		o.Tags = nil
	}

	if n := s.Values.PayloadBytes(); n > 0 {
		if cap(o.Payload) < n {
			o.Payload = make([]byte, n)
		}
		o.Payload = o.Payload[:n]
		for i := 0; i < n; i += 8 {
			v := r.next()
			for j := 0; j < 8 && i+j < n; j++ {
				o.Payload[i+j] = byte(v >> (8 * j))
			}
		}
	} else {
		o.Payload = nil
	}
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func pad(n, width int) string {
	s := strconv.Itoa(n)
	if len(s) >= width {
		return s
	}
	return strings.Repeat("0", width-len(s)) + s
}

// Canon returns a text that is the same for the records that are the same, whichever system or
// codec they went through: an empty list is no list, and the payload is its digest.
func Canon(o *Order) string {
	var sb strings.Builder
	note := "<nil>"
	if o.Note != nil {
		note = strconv.Quote(*o.Note)
	}
	sum := sha1.Sum(o.Payload)
	fmt.Fprintf(&sb, "id=%d code=%s customer=%s name=%q status=%s total=%d qty=%d at=%d note=%s tags=%s payload=%d:%s",
		o.ID, o.Code, o.Customer, o.Name, o.Status, o.Total, o.Qty, o.At.Unix(), note,
		strings.Join(o.Tags, ","), len(o.Payload), hex.EncodeToString(sum[:4]))
	return sb.String()
}

// rng is a splitmix64 generator: small, fast and without any state to allocate.
type rng struct{ s uint64 }

func newRNG(seed, id, version uint64) *rng {
	r := &rng{s: seed ^ 0x9e3779b97f4a7c15}
	r.s = r.next() ^ id
	r.s = r.next() ^ version
	r.next()
	return r
}

func (r *rng) next() uint64 {
	r.s += 0x9e3779b97f4a7c15
	z := r.s
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func (r *rng) intn(n int) int {
	return int(r.next() % uint64(n))
}
