package rectools_test

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/iters"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	recstore "github.com/ehsanranjbar/kvutil/store/rec"
	rectools "github.com/ehsanranjbar/kvutil/store/rec/rectools"
	"github.com/ehsanranjbar/kvutil/testutil"
	"github.com/stretchr/testify/suite"
)

type RelationSuite struct {
	suite.Suite
	txn        *badger.Txn
	leftStore  *recstore.Store[int64, testutil.SampleRecord, *testutil.SampleRecord]
	rightStore *recstore.Store[int64, testutil.SampleRecord, *testutil.SampleRecord]
	rel        *rectools.Relation[
		int64, testutil.SampleRecord, *testutil.SampleRecord,
		int64, testutil.SampleRecord, *testutil.SampleRecord,
		testutil.SampleStruct, *testutil.SampleStruct,
	]
	leftInstance  *recstore.Instance[int64, testutil.SampleRecord, *testutil.SampleRecord]
	rightInstance *recstore.Instance[int64, testutil.SampleRecord, *testutil.SampleRecord]
}

func TestRelationSuite(t *testing.T) {
	suite.Run(t, new(RelationSuite))
}

func (ts *RelationSuite) SetupTest() {
	ts.txn = testutil.PrepareTxn(ts.T(), true)

	ts.leftStore = testutil.NewRecordStore([]byte("left"))
	ts.rightStore = testutil.NewRecordStore([]byte("right"))

	ts.rel = rectools.NewRelation[
		int64, testutil.SampleRecord, *testutil.SampleRecord,
		int64, testutil.SampleRecord, *testutil.SampleRecord,
		testutil.SampleStruct,
	]("left-right", ts.leftStore, ts.rightStore)

	ts.leftInstance = ts.leftStore.Instantiate(ts.txn)
	ts.rightInstance = ts.rightStore.Instantiate(ts.txn)
}

func (ts *RelationSuite) TestStoreExtension() {
	l1 := testutil.NewSampleEntity("L1")
	r1 := testutil.NewSampleEntity("R1")
	r2 := testutil.NewSampleEntity("R2")

	err := ts.leftInstance.Set(l1)
	ts.NoError(err)
	err = ts.rightInstance.Set(r1, extstore.WithExtOption("left-right", l1.Id))
	ts.NoError(err)
	err = ts.rightInstance.Set(r2, extstore.WithExtOption("left-right", l1.Id))
	ts.NoError(err)

	ts.Equal(
		map[string]string{
			"leftd\x80\x00\x00\x00\x00\x00\x00\x01":                                            "{\"name\":\"L1\"}",
			"leftxleft-right\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00\x00\x00\x01":  "",
			"leftxleft-right\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00\x00\x00\x02":  "",
			"rightd\x80\x00\x00\x00\x00\x00\x00\x01":                                           "{\"name\":\"R1\"}",
			"rightd\x80\x00\x00\x00\x00\x00\x00\x02":                                           "{\"name\":\"R2\"}",
			"rightxleft-right\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00\x00\x00\x01": "",
			"rightxleft-right\x80\x00\x00\x00\x00\x00\x00\x02\x80\x00\x00\x00\x00\x00\x00\x01": "",
		},
		testutil.Dump(ts.txn),
	)

	err = ts.rightInstance.Delete(r1.Id)
	ts.NoError(err)

	ts.Equal(
		map[string]string{
			"leftd\x80\x00\x00\x00\x00\x00\x00\x01":                                            "{\"name\":\"L1\"}",
			"leftxleft-right\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00\x00\x00\x02":  "",
			"rightd\x80\x00\x00\x00\x00\x00\x00\x02":                                           "{\"name\":\"R2\"}",
			"rightxleft-right\x80\x00\x00\x00\x00\x00\x00\x02\x80\x00\x00\x00\x00\x00\x00\x01": "",
		},
		testutil.Dump(ts.txn),
	)

	err = ts.rightInstance.Set(r1, extstore.WithExtOption("left-right", int64(3)))
	ts.Error(err)
}

func (ts *RelationSuite) TestInstance() {
	l1 := testutil.NewSampleEntity("L1")
	r1 := testutil.NewSampleEntity("R1")
	r2 := testutil.NewSampleEntity("R2")

	err := ts.leftInstance.Set(l1)
	ts.NoError(err)
	err = ts.rightInstance.Set(r1)
	ts.NoError(err)
	err = ts.rightInstance.Set(r2)
	ts.NoError(err)

	rel := ts.rel.Instantiate(ts.txn)

	err = rel.Set(rectools.NewCompoundKey(l1.Id, r2.Id), &testutil.SampleStruct{A: 12})
	ts.NoError(err)

	ts.Equal(
		map[string]string{
			"leftd\x80\x00\x00\x00\x00\x00\x00\x01":                                            "{\"name\":\"L1\"}",
			"leftxleft-right\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00\x00\x00\x02":  "{\"a\":12}",
			"rightd\x80\x00\x00\x00\x00\x00\x00\x01":                                           "{\"name\":\"R1\"}",
			"rightd\x80\x00\x00\x00\x00\x00\x00\x02":                                           "{\"name\":\"R2\"}",
			"rightxleft-right\x80\x00\x00\x00\x00\x00\x00\x02\x80\x00\x00\x00\x00\x00\x00\x01": "",
		},
		testutil.Dump(ts.txn),
	)

	l1r2, err := rel.Get(rectools.NewCompoundKey(l1.Id, r2.Id))
	ts.NoError(err)
	ts.Equal(&testutil.SampleStruct{A: 12}, l1r2)

	it := rel.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	keys, err := iters.CollectKeys(it)
	ts.NoError(err)
	ts.Equal([]rectools.CompoundKey[int64, int64]{rectools.NewCompoundKey(l1.Id, r2.Id)}, keys)

	values, err := iters.Collect(it)
	ts.NoError(err)
	ts.Equal([]*testutil.SampleStruct{{A: 12}}, values)

	err = rel.Delete(rectools.NewCompoundKey(l1.Id, r2.Id))
	ts.NoError(err)

	ts.Equal(
		map[string]string{
			"leftd\x80\x00\x00\x00\x00\x00\x00\x01":  "{\"name\":\"L1\"}",
			"rightd\x80\x00\x00\x00\x00\x00\x00\x01": "{\"name\":\"R1\"}",
			"rightd\x80\x00\x00\x00\x00\x00\x00\x02": "{\"name\":\"R2\"}",
		},
		testutil.Dump(ts.txn),
	)
}
