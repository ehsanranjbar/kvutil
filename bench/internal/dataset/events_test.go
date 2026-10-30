package dataset

import (
	"testing"
	"time"
)

func eventSpec() EventSpec {
	return EventSpec{Seed: 1, Events: 20_000, End: time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), SpanDays: 365}
}

func TestEventsAreDeterministicAndVersionsChangeTheAmountOnly(t *testing.T) {
	s := eventSpec()
	a, b := s.Gen(77, 0), s.Gen(77, 0)
	if *a != *b {
		t.Fatalf("two generations differ: %+v %+v", a, b)
	}
	v := s.Gen(77, 5)
	if v.Customer != a.Customer || !v.At.Equal(a.At) || v.Kind != a.Kind || v.Amount == a.Amount {
		t.Errorf("version 5 of %+v is %+v: only the amount may differ", a, v)
	}
	if s.Gen(78, 0).Canon() == a.Canon() {
		t.Error("two ids give the same event")
	}

	// A move changes the customer and the time, and keeps the id.
	var moved Event
	for id := int64(1); id < 50; id++ {
		s.GenMoved(&moved, id, 2)
		old := s.Gen(id, 0)
		if moved.ID != id || (moved.Customer == old.Customer && moved.At.Equal(old.At)) {
			t.Fatalf("event %d was not moved: %+v from %+v", id, moved, old)
		}
	}
}

func TestEventsAreInTheSpanAtWholeSeconds(t *testing.T) {
	s := eventSpec()
	start := s.Start()
	if !start.Equal(time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("the span starts at %s", start)
	}
	for id := int64(1); id <= 5000; id++ {
		e := s.Gen(id, 0)
		if e.At.Before(start) || !e.At.Before(s.End) || e.At.Nanosecond() != 0 || e.At.Location() != time.UTC {
			t.Fatalf("event %d is at %s", id, e.At)
		}
		if e.Amount < MinAmount || e.Amount > MaxAmount {
			t.Fatalf("event %d has the amount %d", id, e.Amount)
		}
		if got := CustomerName(s.CustomerRank(id)); got != e.Customer {
			t.Fatalf("event %d: CustomerRank says %s, the event has %s", id, got, e.Customer)
		}
	}
}

func TestCustomersAreSkewed(t *testing.T) {
	s := eventSpec()
	if s.NumCustomers() != 200 {
		t.Fatalf("%d customers for 20,000 events", s.NumCustomers())
	}
	counts := make([]int, s.NumCustomers())
	for id := int64(1); id <= 20_000; id++ {
		counts[s.CustomerRank(id)]++
	}
	// The first has ln 2 / ln(C+1) of the events: 13%.
	if share := float64(counts[0]) / 20_000; share < 0.11 || share > 0.15 {
		t.Errorf("the first customer has %.1f%% of the events", share*100)
	}
	if counts[0] < 3*counts[3] || counts[3] < counts[30] {
		t.Errorf("not skewed: %v", counts[:8])
	}
	nonzero := 0
	for _, c := range counts {
		if c > 0 {
			nonzero++
		}
	}
	if nonzero < 150 {
		t.Errorf("only %d of the customers have events", nonzero)
	}
	if ids := s.CustomersOf(0, 20_000); len(ids) != counts[0] {
		t.Errorf("CustomersOf found %d events, %d counted", len(ids), counts[0])
	}
}

func TestStreamEventsTakeTheTimeGiven(t *testing.T) {
	s := eventSpec()
	at := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	var e Event
	s.GenStream(&e, 20_001, at)
	if !e.At.Equal(at) || e.ID != 20_001 || e.Customer == "" {
		t.Errorf("stream event %+v", e)
	}
}
