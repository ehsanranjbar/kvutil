package relations

import (
	"math"
	"slices"
	"sort"
	"time"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
)

// The records of the relations. Both the parents and the children are stored under an int64 id
// that is a key and not a part of the value.
type parent struct {
	ID      int64 `msgpack:"-"`
	Name    string
	Region  string
	Created time.Time
}

func (p *parent) GetId() int64   { return p.ID }
func (p *parent) SetId(id int64) { p.ID = id }

// child belongs to a parent, which the association keeps (the child has no field for it).
type child struct {
	ID      int64 `msgpack:"-"`
	Title   string
	Qty     int64
	Status  string
	Payload []byte
}

func (c *child) GetId() int64   { return c.ID }
func (c *child) SetId(id int64) { c.ID = id }

// entity is a record of either side of the many-to-many relation: an account or a team.
type entity struct {
	ID   int64 `msgpack:"-"`
	Name string
	Kind string
}

func (e *entity) GetId() int64   { return e.ID }
func (e *entity) SetId(id int64) { e.ID = id }

// pairData is what a pair of the many-to-many relation carries.
type pairData struct {
	Role  string
	Since int64
}

var (
	regions  = []string{"north", "south", "east", "west", "central", "coastal", "alpine", "delta"}
	statuses = []string{"new", "paid", "shipped", "late", "cancelled"}
	roles    = []string{"owner", "admin", "member", "guest"}
)

// model is the data of the workload: how many records of each kind, and which child belongs to
// which parent, which pairs there are. Everything is a function of the seed and the scale.
//
// The children follow a Zipf law over the parents: the parent of rank k has about 1/(k+1) of the
// children of the first, so a few parents have thousands and most have a few. The pairs
// follow the same law over the left side; the right side gets the pairs evenly.
type model struct {
	seed    uint64
	payload int

	children, parents    int64
	lefts, rights, pairs int64

	// childCount is the number of children of each parent, from index 1; leftCount the number of
	// pairs of each left record; pairStart the number of pairs before each left record.
	childCount []int32
	leftCount  []int32
	pairStart  []int64
	stride     int64
}

func newModel(seed uint64, scale int, values dataset.ValueSize) *model {
	n := int64(scale)
	m := &model{
		seed:     seed,
		payload:  values.PayloadBytes(),
		children: n,
		parents:  max(20, n/10),
		lefts:    max(20, n/20),
		rights:   max(20, n/10),
		pairs:    n,
	}

	m.childCount = make([]int32, m.parents+1)
	for id := int64(1); id <= m.children; id++ {
		m.childCount[m.parentOf(id)]++
	}

	// The pairs of a left record by its rank, 1/(rank+1) of the first's, at least one and at most
	// as many as there are right records.
	m.leftCount = make([]int32, m.lefts+1)
	var total float64
	for l := int64(1); l <= m.lefts; l++ {
		total += 1 / float64(l)
	}
	for l := int64(1); l <= m.lefts; l++ {
		c := int64(math.Round(float64(m.pairs) / total / float64(l)))
		m.leftCount[l] = int32(min(max(c, 1), m.rights))
	}
	m.pairs = 0
	m.pairStart = make([]int64, m.lefts+2)
	for l := int64(1); l <= m.lefts; l++ {
		m.pairStart[l] = m.pairs
		m.pairs += int64(m.leftCount[l])
	}
	m.pairStart[m.lefts+1] = m.pairs

	// The step through the right records is coprime to their number, so that the pairs of a left
	// record are all different.
	m.stride = 7
	for gcd(m.stride, m.rights) != 1 {
		m.stride += 2
	}
	return m
}

func gcd(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

func mix(a, b, c uint64) uint64 {
	z := a*0x9e3779b97f4a7c15 ^ b*0xbf58476d1ce4e5b9 ^ c*0x94d049bb133111eb
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func unit(h uint64) float64 { return float64(h>>11) / (1 << 53) }

// rank maps a uniform number to a rank 0 to n-1 by a Zipf law of exponent 1.
func rank(u float64, n int64) int64 {
	r := int64(math.Exp(u*math.Log(float64(n)+1))) - 1
	return min(max(r, 0), n-1)
}

// parentOf is the parent that a child belongs to.
func (m *model) parentOf(childID int64) int64 {
	return 1 + rank(unit(mix(m.seed, uint64(childID), 1)), m.parents)
}

// ParentFromUniform is the parent that a number in [0, 1) leads to, by the same law: the parent
// of a child that is added.
func (m *model) parentFromUniform(u float64) int64 { return 1 + rank(u, m.parents) }

func (m *model) genParent(id int64, p *parent) {
	p.ID = id
	p.Name = "parent-" + pad(id, 7)
	p.Region = regions[mix(m.seed, uint64(id), 2)%uint64(len(regions))]
	p.Created = time.Unix(1704067200+int64(mix(m.seed, uint64(id), 3)%(2*365*86400)), 0).UTC()
}

func (m *model) genChild(id int64, c *child) {
	h := mix(m.seed, uint64(id), 4)
	c.ID = id
	c.Title = "child-" + pad(id, 9)
	c.Qty = int64(h % 1000)
	c.Status = statuses[(h>>20)%uint64(len(statuses))]
	if m.payload > 0 {
		if cap(c.Payload) < m.payload {
			c.Payload = make([]byte, m.payload)
		}
		c.Payload = c.Payload[:m.payload]
		x := h
		for i := 0; i < m.payload; i += 8 {
			x = mix(x, uint64(i), 5)
			for j := 0; j < 8 && i+j < m.payload; j++ {
				c.Payload[i+j] = byte(x >> (8 * j))
			}
		}
	} else {
		c.Payload = nil
	}
}

func (m *model) genEntity(side string, id int64, e *entity) {
	e.ID = id
	e.Name = side + "-" + pad(id, 7)
	e.Kind = regions[mix(m.seed, uint64(id), 6)%uint64(len(regions))]
}

// rightOf is the right record of the j-th pair of a left record (j from 0).
func (m *model) rightOf(left, j int64) int64 {
	return 1 + (left*1000003+j*m.stride)%m.rights
}

// pairAt returns the i-th of all the pairs, 0 to pairs-1, in the order of the left records.
func (m *model) pairAt(i int64) (left, right int64) {
	left = int64(sort.Search(int(m.lefts), func(k int) bool { return m.pairStart[k+2] > i })) + 1
	return left, m.rightOf(left, i-m.pairStart[left])
}

// data is the data of the pair.
func (m *model) data(left, right int64) pairData {
	return pairData{
		Role:  roles[(left+right)%int64(len(roles))],
		Since: 1_700_000_000 + (left*31+right)%1_000_000,
	}
}

func pad(n int64, width int) string {
	s := make([]byte, width)
	for i := width - 1; i >= 0; i-- {
		s[i] = byte('0' + n%10)
		n /= 10
	}
	return string(s)
}

// tier is a group of records picked by how many related records they have.
type tier struct {
	Name     string
	Lo, Hi   int32
	pool     []int64
	meanSize float64
}

// tierBounds are the sizes of the tiers: a few related records, a typical number, and many (a
// number that a cascade of kvutil can still delete in one transaction).
var tierBounds = []tier{
	{Name: "few", Lo: 1, Hi: 5},
	{Name: "typical", Lo: 10, Hi: 100},
	{Name: "many", Lo: 200, Hi: 5000},
}

const maxPool = 4096

// tiersOf sorts the records, from index 1, into the tiers by their counts. A pool is at most
// maxPool records, taken evenly from the ones that fit.
func tiersOf(counts []int32) []tier {
	var out []tier
	for _, t := range tierBounds {
		var all []int64
		var sum float64
		for id := int64(1); id < int64(len(counts)); id++ {
			if c := counts[id]; c >= t.Lo && c <= t.Hi {
				all = append(all, id)
				sum += float64(c)
			}
		}
		if len(all) == 0 {
			continue
		}
		t.meanSize = sum / float64(len(all))
		if len(all) > maxPool {
			picked := make([]int64, maxPool)
			for i := range picked {
				picked[i] = all[i*len(all)/maxPool]
			}
			all = picked
		}
		t.pool = all
		out = append(out, t)
	}
	return out
}

func (m *model) parentTiers() []tier { return tiersOf(m.childCount) }
func (m *model) leftTiers() []tier   { return tiersOf(m.leftCount) }

// slice returns the i-th of n parts of a pool, for the cases that use their records up.
func slice(pool []int64, i, n int) []int64 {
	return pool[i*len(pool)/n : (i+1)*len(pool)/n]
}

// sorted returns a sorted copy.
func sorted(ids []int64) []int64 {
	out := slices.Clone(ids)
	slices.Sort(out)
	return out
}
