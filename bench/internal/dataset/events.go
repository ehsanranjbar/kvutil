package dataset

import (
	"fmt"
	"math"
	"time"
)

// Event is the record of the aggregation benchmarks: something that happened to a customer at a
// time, with an amount in cents (integers, so that sums and maxima are exact on every system).
// The id is the key of the record and is not a part of the value.
type Event struct {
	ID       int64 `msgpack:"-" json:"-"`
	Customer string
	Amount   int64
	At       time.Time
	Kind     string
}

// GetId returns the id of the record.
func (e *Event) GetId() int64 { return e.ID }

// SetId sets the id of the record.
func (e *Event) SetId(id int64) { e.ID = id }

// EventKinds are the values that Kind has. No aggregation looks at it: it is there so that an
// event is more than the fields that are aggregated.
var EventKinds = []string{"sale", "refund", "fee", "tip"}

const (
	// MinAmount and MaxAmount bound the amount of an event in cents, both included.
	MinAmount = 100
	MaxAmount = 99_999
	// DefaultSpanDays is the span of the times of the events of a dataset, a year.
	DefaultSpanDays = 365
)

// EventSpec is what a dataset of events is generated from. An event is a pure function of the
// seed, its id and its version, so that every system gets the same ones.
//
// The times of the events of the ids 1 to Events are spread over the SpanDays days before End;
// the events of a stream (the new events that the write workloads add) are given the time that
// they are added at, by GenStream.
type EventSpec struct {
	Seed   uint64
	Events int
	// Customers is the number of customers; 0 is one for every hundred events, 10 at least.
	Customers int
	// End is the end of the span, exclusive: a midnight in UTC.
	End      time.Time
	SpanDays int
}

// NumCustomers is the number of different customers.
func (s EventSpec) NumCustomers() int {
	if s.Customers > 0 {
		return s.Customers
	}
	return max(10, s.Events/100)
}

// Start is the first instant of the span, a midnight in UTC.
func (s EventSpec) Start() time.Time {
	return s.End.AddDate(0, 0, -s.span())
}

func (s EventSpec) span() int {
	if s.SpanDays > 0 {
		return s.SpanDays
	}
	return DefaultSpanDays
}

// CustomerName is the customer of a rank: the most popular customer has rank 0.
func CustomerName(rank int) string {
	return "cust-" + pad(rank, 6)
}

// RankOfUniform maps a uniform number in [0, 1) to a rank of the customers, which follow a Zipf
// law of exponent 1: the customer of rank k has about 1/(k+1) of the events of the first, and the
// first has ln 2 / ln(C+1) of all of them, C being the number of customers. The rank is
// floor(exp(u * ln(C+1))) - 1, which has that distribution without a table.
func (s EventSpec) RankOfUniform(u float64) int {
	c := s.NumCustomers()
	rank := int(math.Exp(u*math.Log(float64(c)+1))) - 1
	return min(max(rank, 0), c-1)
}

func (s EventSpec) eventRNG(id uint64, stream uint64) *rng {
	// Constants apart from those of the other datasets, so that they are not correlated.
	return newRNG(s.Seed^0x7a5c0de7a5c0de01, id, stream)
}

// CustomerRank returns the rank of the customer of an event without generating it. The customer
// of an event does not change with its version.
func (s EventSpec) CustomerRank(id int64) int {
	r := s.eventRNG(uint64(id), 0)
	return s.RankOfUniform(float64(r.next()>>11) / (1 << 53))
}

// Gen returns the event of an id in a version. The initial version is 0. The customer, the time
// and the kind of an event are the same in every version; the amount changes.
func (s EventSpec) Gen(id int64, version uint32) *Event {
	e := &Event{}
	s.GenInto(e, id, version)
	return e
}

// GenInto is Gen into an event that is given.
func (s EventSpec) GenInto(e *Event, id int64, version uint32) {
	r := s.eventRNG(uint64(id), 0)
	e.ID = id
	e.Customer = CustomerName(s.RankOfUniform(float64(r.next()>>11) / (1 << 53)))
	e.At = s.Start().Add(time.Duration(r.intn(s.span()*86400)) * time.Second)
	e.Kind = EventKinds[r.intn(len(EventKinds))]
	e.Amount = s.amount(id, version)
}

func (s EventSpec) amount(id int64, version uint32) int64 {
	r := s.eventRNG(uint64(id), 1+uint64(version))
	return int64(MinAmount + r.intn(MaxAmount-MinAmount+1))
}

// GenMoved is GenInto for an event that is changed in every field: another customer, at another
// time of the span. It is for the verification, which moves events from a span of one aggregation
// to another.
func (s EventSpec) GenMoved(e *Event, id int64, version uint32) {
	s.GenInto(e, id, version)
	r := s.eventRNG(uint64(id), 1_000_000+uint64(version))
	e.Customer = CustomerName(s.RankOfUniform(float64(r.next()>>11) / (1 << 53)))
	e.At = s.Start().Add(time.Duration(r.intn(s.span()*86400)) * time.Second)
}

// GenStream returns a new event: the customer, the amount and the kind of the id, and the given
// time. Ids past the Events of the spec are the ones of a stream.
func (s EventSpec) GenStream(e *Event, id int64, at time.Time) {
	s.GenInto(e, id, 0)
	e.At = at
}

// CustomersOf returns the ids in [1, n] of the events that a customer has in version 0, which the
// workloads that change the old events of one customer pick from.
func (s EventSpec) CustomersOf(rank int, n int64) []int64 {
	var ids []int64
	for id := int64(1); id <= n; id++ {
		if s.CustomerRank(id) == rank {
			ids = append(ids, id)
		}
	}
	return ids
}

// Canon returns a text that is the same for the events that are the same.
func (e *Event) Canon() string {
	return fmt.Sprintf("id=%d customer=%s amount=%d at=%d kind=%s", e.ID, e.Customer, e.Amount, e.At.Unix(), e.Kind)
}
