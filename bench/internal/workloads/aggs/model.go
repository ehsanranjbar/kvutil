package aggs

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"
)

// unit is a span of the calendar, in UTC, that the events are aggregated by.
type unit int

const (
	day unit = iota
	week
	month
)

func (u unit) String() string {
	switch u {
	case day:
		return "day"
	case week:
		return "week"
	}
	return "month"
}

// startOf returns the start of the span of the unit that a time is in: a midnight, a Monday
// midnight, the first of a month, all in UTC. It is written here and not taken from kvutil's
// buckets, so that the windows of the queries do not depend on what is being measured.
func startOf(u unit, t time.Time) time.Time {
	t = t.UTC()
	y, m, d := t.Date()
	switch u {
	case week:
		d -= (int(t.Weekday()) - int(time.Monday) + 7) % 7
	case month:
		d = 1
	}
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// nextOf returns the start of the span that follows the one that starts at the given time.
func nextOf(u unit, start time.Time) time.Time {
	y, m, d := start.UTC().Date()
	switch u {
	case day:
		d++
	case week:
		d += 7
	case month:
		m++
	}
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// alignUp returns the first start of a span that is at or after the time.
func alignUp(u unit, t time.Time) time.Time {
	s := startOf(u, t)
	if s.Equal(t.UTC()) {
		return s
	}
	return nextOf(u, s)
}

// qkind says what a query asks for.
type qkind int

const (
	// qSeries is the sum, count and maximum of a customer for each span of a unit in a window.
	qSeries qkind = iota
	// qTotal is the sum and count of a customer for all of time.
	qTotal
	// qTop is the 10 customers of the greatest sum in a month.
	qTop
)

// query is a read that every system answers.
type query struct {
	Kind     qkind
	Unit     unit
	Customer string
	// From and To bound the spans of a series: those that start in [From, To), both aligned to
	// the unit. For qTop, [From, To) is the month.
	From, To time.Time
}

// row is a part of an answer: a span of a series with its sum, count and maximum, the totals
// (the sum and count), or a customer of the top with its sum.
type row struct {
	Bucket   time.Time
	Customer string
	Sum      int64
	Count    int64
	Max      int64
}

// lines returns the answer as text, for the verification and for the tests: one line per row.
func lines(q query, rows []row) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		switch q.Kind {
		case qSeries:
			out[i] = fmt.Sprintf("%s sum=%d count=%d max=%d", r.Bucket.UTC().Format(time.RFC3339), r.Sum, r.Count, r.Max)
		case qTotal:
			out[i] = fmt.Sprintf("sum=%d count=%d", r.Sum, r.Count)
		default:
			out[i] = fmt.Sprintf("%s sum=%d", r.Customer, r.Sum)
		}
	}
	return out
}

// label names a query in a line of the verification.
func (q query) label(name string) string {
	switch q.Kind {
	case qSeries:
		return fmt.Sprintf("%s %s %s [%s,%s)", name, q.Unit, q.Customer, q.From.UTC().Format("2006-01-02"), q.To.UTC().Format("2006-01-02"))
	case qTotal:
		return fmt.Sprintf("%s %s", name, q.Customer)
	}
	return fmt.Sprintf("%s %s", name, q.From.UTC().Format("2006-01"))
}

// summarize returns the line of the verification for an answer: the number of rows, the sums
// that can be added up, and a digest of every row, so that any difference shows.
func summarize(q query, rows []row) string {
	var sum, count int64
	for _, r := range rows {
		sum += r.Sum
		count += r.Count
	}
	h := sha1.Sum([]byte(strings.Join(lines(q, rows), "\n")))
	s := fmt.Sprintf("rows=%d sum=%d", len(rows), sum)
	if q.Kind != qTop {
		s += fmt.Sprintf(" count=%d", count)
	}
	return s + " digest=" + hex.EncodeToString(h[:6])
}

// sortRows orders the rows of a series by the start of the span.
func sortRows(rows []row) {
	sort.Slice(rows, func(i, j int) bool { return rows[i].Bucket.Before(rows[j].Bucket) })
}

// topRows orders customers by the sum (the greatest first) and then by the name, and keeps 10.
func topRows(rows []row) []row {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Sum != rows[j].Sum {
			return rows[i].Sum > rows[j].Sum
		}
		return rows[i].Customer < rows[j].Customer
	})
	if len(rows) > 10 {
		rows = rows[:10]
	}
	return rows
}

// equalRows reports whether two answers are the same.
func equalRows(a, b []row) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !a[i].Bucket.Equal(b[i].Bucket) || a[i].Customer != b[i].Customer ||
			a[i].Sum != b[i].Sum || a[i].Count != b[i].Count || a[i].Max != b[i].Max {
			return false
		}
	}
	return true
}
