package agg

import (
	"fmt"
	"strings"
	"time"
)

// Bucket divides time into spans that records are aggregated in, by the times that they have.
type Bucket interface {
	// ID identifies the bucket among the buckets of an aggregation, in the keys that it stores.
	ID() byte
	// Name describes the bucket. An aggregation whose buckets change must be built again.
	Name() string
	// Start returns the start of the span that a time is in.
	Start(t time.Time) time.Time
	// Next returns the start of the span that follows the span that starts at the given time.
	Next(start time.Time) time.Time
}

type unit int

const (
	unitHour unit = iota + 1
	unitDay
	unitWeek
	unitMonth
	unitYear
)

// Calendar is a bucket of the spans of a calendar: hours, days, weeks, months or years.
type Calendar struct {
	unit      unit
	loc       *time.Location
	weekStart time.Weekday
}

var (
	// AllTime is the bucket of one span that all of time is in.
	AllTime Bucket = allTime{}
	// Hourly is the bucket of the hours, in UTC.
	Hourly = Calendar{unit: unitHour}
	// Daily is the bucket of the days, in UTC.
	Daily = Calendar{unit: unitDay}
	// Weekly is the bucket of the weeks that start on Monday, in UTC.
	Weekly = Calendar{unit: unitWeek, weekStart: time.Monday}
	// Monthly is the bucket of the months, in UTC.
	Monthly = Calendar{unit: unitMonth}
	// Yearly is the bucket of the years, in UTC.
	Yearly = Calendar{unit: unitYear}
)

// In returns the bucket of the same spans in another time zone.
func (c Calendar) In(loc *time.Location) Calendar {
	c.loc = loc
	return c
}

// StartingOn returns the bucket of the weeks that start on the given day.
// It only makes a difference for the weeks.
func (c Calendar) StartingOn(day time.Weekday) Calendar {
	c.weekStart = day
	return c
}

func (c Calendar) location() *time.Location {
	if c.loc == nil {
		return time.UTC
	}
	return c.loc
}

// ID implements the Bucket interface.
func (c Calendar) ID() byte {
	return byte(c.unit)
}

// Name implements the Bucket interface.
func (c Calendar) Name() string {
	var name string
	switch c.unit {
	case unitHour:
		name = "hourly"
	case unitDay:
		name = "daily"
	case unitWeek:
		name = "weekly(" + strings.ToLower(c.weekStart.String()) + ")"
	case unitMonth:
		name = "monthly"
	case unitYear:
		name = "yearly"
	}
	if loc := c.location(); loc != time.UTC {
		name += "@" + loc.String()
	}
	return name
}

// Start implements the Bucket interface.
func (c Calendar) Start(t time.Time) time.Time {
	loc := c.location()
	t = t.In(loc)
	y, m, d := t.Date()

	switch c.unit {
	case unitHour:
		// From the time itself, so that an hour that a day has twice stays two hours.
		return t.Add(-time.Duration(t.Minute())*time.Minute -
			time.Duration(t.Second())*time.Second - time.Duration(t.Nanosecond()))
	case unitWeek:
		d -= (int(t.Weekday()) - int(c.weekStart) + 7) % 7
	case unitMonth:
		d = 1
	case unitYear:
		m, d = 1, 1
	}
	return time.Date(y, m, d, 0, 0, 0, 0, loc)
}

// Next implements the Bucket interface.
func (c Calendar) Next(start time.Time) time.Time {
	loc := c.location()
	start = start.In(loc)
	y, m, d := start.Date()

	switch c.unit {
	case unitHour:
		return start.Add(time.Hour)
	case unitDay:
		d++
	case unitWeek:
		d += 7
	case unitMonth:
		m++
	case unitYear:
		y++
	}
	// By the calendar, as a day may have 23 or 25 hours.
	return time.Date(y, m, d, 0, 0, 0, 0, loc)
}

type allTime struct{}

// ID implements the Bucket interface.
func (allTime) ID() byte { return 0 }

// Name implements the Bucket interface.
func (allTime) Name() string { return "all" }

// Start implements the Bucket interface.
func (allTime) Start(time.Time) time.Time { return time.Time{} }

// Next implements the Bucket interface.
func (allTime) Next(time.Time) time.Time { return time.Time{} }

// everyID is the ID of the buckets of a fixed duration.
const everyID = 6

// Every returns the bucket of spans of a fixed duration, which are counted from the zero time.
// An aggregation can have one such bucket.
func Every(d time.Duration) Bucket {
	if d <= 0 {
		panic("agg: the duration of a bucket must be positive")
	}
	return every{d: d}
}

type every struct {
	d time.Duration
}

// ID implements the Bucket interface.
func (every) ID() byte { return everyID }

// Name implements the Bucket interface.
func (e every) Name() string { return fmt.Sprintf("every(%s)", e.d) }

// Start implements the Bucket interface.
func (e every) Start(t time.Time) time.Time { return t.Truncate(e.d).UTC() }

// Next implements the Bucket interface.
func (e every) Next(start time.Time) time.Time { return start.Add(e.d) }

// nests reports whether every span of the fine bucket is inside one span of the coarse bucket,
// which lets the spans of the coarse one be put together from the spans of the fine one.
func nests(fine, coarse Bucket) bool {
	if coarse == AllTime {
		return true
	}

	switch f := fine.(type) {
	case Calendar:
		c, ok := coarse.(Calendar)
		if !ok || f.location().String() != c.location().String() {
			return false
		}
		switch f.unit {
		case unitHour:
			return c.unit > unitHour
		case unitDay:
			return c.unit > unitDay
		case unitMonth:
			return c.unit == unitYear
		}
	case every:
		c, ok := coarse.(every)
		return ok && c.d >= f.d && c.d%f.d == 0
	}
	return false
}
