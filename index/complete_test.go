package index_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/kv"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

type reading struct {
	Sensor string
	Value  *int
	Unit   string
}

// TestLookupNeedsAValueOfTheFieldsThatRecordsMayLack checks that an index is looked up only for
// conditions that every record they are satisfied by has an entry for.
func TestLookupNeedsAValueOfTheFieldsThatRecordsMayLack(t *testing.T) {
	sensor := field.Path[reading, string]("Sensor")
	value := field.Path[reading, int]("Value") // a pointer: optional
	unit := field.Path[reading, string]("Unit")

	testkv.Each(t, func(t *testing.T, db kv.DB) {
		readings := extstore.New[int64, reading]("readings")
		byValue := index.Composite(readings, "by_sensor_value_unit", field.List(sensor, value, unit))
		require.NoError(t, readings.Check(db))

		one := 1
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			store := readings.Instantiate(tx)
			if err := store.Set(1, &reading{Sensor: "a", Value: &one, Unit: "C"}); err != nil {
				return err
			}
			// This one is not in the index.
			return store.Set(2, &reading{Sensor: "a", Unit: "C"})
		}))

		for name, test := range map[string]struct {
			conds []cond.Cond[reading]
			err   bool
		}{
			"the sensor alone":               {[]cond.Cond[reading]{sensor.Eq("a")}, true},
			"nothing":                        {nil, true},
			"a negation of the value":        {[]cond.Cond[reading]{sensor.Eq("a"), cond.Not(value.Eq(1))}, true},
			"a disjunction on the value":     {[]cond.Cond[reading]{cond.Or(value.Eq(1), value.Eq(2))}, true},
			"the value":                      {[]cond.Cond[reading]{value.Eq(1)}, false},
			"a range of the value":           {[]cond.Cond[reading]{value.Gte(0)}, false},
			"the values":                     {[]cond.Cond[reading]{value.In(1, 2)}, false},
			"the value inside a conjunction": {[]cond.Cond[reading]{cond.And(sensor.Eq("a"), value.Eq(1))}, false},
			"no values is no value":          {[]cond.Cond[reading]{value.In()}, true},
		} {
			t.Run(name, func(t *testing.T) {
				require.NoError(t, db.View(func(tx kv.Tx) error {
					ins := byValue.Instantiate(tx)

					require.Equal(t, test.err, byValue.Complete(test.conds...) != nil)
					_, _, err := ins.Plan(test.conds...)
					forward, errLookup := ins.Lookup(test.conds...)
					backward, errReverse := ins.ReverseLookup(test.conds...)
					for _, it := range []*index.Iterator[int64, reading]{forward, backward} {
						if it != nil {
							require.NoError(t, it.Close())
						}
					}
					for _, err := range []error{err, errLookup, errReverse} {
						if test.err {
							require.ErrorIs(t, err, index.ErrIncomplete)
						} else {
							require.NoError(t, err)
						}
					}
					return nil
				}))
			})
		}

		// A field that every record has is not needed, whatever the conditions say.
		bySensor := index.On(extstore.New[int64, reading]("sensors"), "by_sensor", sensor)
		require.NoError(t, bySensor.Complete())
	})
}
