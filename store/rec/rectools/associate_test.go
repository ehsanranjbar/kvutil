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

type AssociateSuite struct {
	suite.Suite
	txn *badger.Txn
	ps  *recstore.Store[int64, testutil.SampleRecord, *testutil.SampleRecord]
	cs  *recstore.Store[int64, testutil.SampleRecord, *testutil.SampleRecord]
	rel *rectools.Association[int64, testutil.SampleRecord, *testutil.SampleRecord, int64, testutil.SampleRecord, *testutil.SampleRecord]
	psi *recstore.Instance[int64, testutil.SampleRecord, *testutil.SampleRecord]
	csi *recstore.Instance[int64, testutil.SampleRecord, *testutil.SampleRecord]
}

func TestAssociateSuite(t *testing.T) {
	suite.Run(t, new(AssociateSuite))
}

func (ts *AssociateSuite) SetupTest() {
	ts.txn = testutil.PrepareTxn(ts.T(), true)

	ts.ps = testutil.NewRecordStore([]byte("p"))
	ts.cs = testutil.NewRecordStore([]byte("c"))

	ts.rel = rectools.Associate("p-c-rel", ts.ps, ts.cs)

	ts.psi = ts.ps.Instantiate(ts.txn)
	ts.csi = ts.cs.Instantiate(ts.txn)
}

func (ts *AssociateSuite) TestParentStore() {
	p1 := testutil.NewSampleEntity("P1")
	p2 := testutil.NewSampleEntity("P2")
	c1 := testutil.NewSampleEntity("C1")
	c2 := testutil.NewSampleEntity("C2")

	err := ts.psi.Set(p1, extstore.WithExtOption(ts.rel.Name(), c1))
	ts.NoError(err)
	err = ts.psi.Set(p2, extstore.WithExtOption(ts.rel.Name(), c2))
	ts.NoError(err)

	ts.Equal(
		map[string]string{
			"cd\x80\x00\x00\x00\x00\x00\x00\x01":                                        "{\"name\":\"C1\"}",
			"cd\x80\x00\x00\x00\x00\x00\x00\x02":                                        "{\"name\":\"C2\"}",
			"cxp-c-rel\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00\x00\x00\x01": "",
			"cxp-c-rel\x80\x00\x00\x00\x00\x00\x00\x02\x80\x00\x00\x00\x00\x00\x00\x02": "",
			"pd\x80\x00\x00\x00\x00\x00\x00\x01":                                        "{\"name\":\"P1\"}",
			"pd\x80\x00\x00\x00\x00\x00\x00\x02":                                        "{\"name\":\"P2\"}",
			"pxp-c-rel\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00\x00\x00\x01": "",
			"pxp-c-rel\x80\x00\x00\x00\x00\x00\x00\x02\x80\x00\x00\x00\x00\x00\x00\x02": "",
		},
		testutil.Dump(ts.txn),
	)

	err = ts.psi.Delete(p1.Id)
	ts.NoError(err)

	ts.Equal(
		map[string]string{
			"cd\x80\x00\x00\x00\x00\x00\x00\x02":                                        "{\"name\":\"C2\"}",
			"cxp-c-rel\x80\x00\x00\x00\x00\x00\x00\x02\x80\x00\x00\x00\x00\x00\x00\x02": "",
			"pd\x80\x00\x00\x00\x00\x00\x00\x02":                                        "{\"name\":\"P2\"}",
			"pxp-c-rel\x80\x00\x00\x00\x00\x00\x00\x02\x80\x00\x00\x00\x00\x00\x00\x02": "",
		},
		testutil.Dump(ts.txn),
	)
}

func (ts *AssociateSuite) TestChildStore() {
	p1 := testutil.NewSampleEntity("P1")
	c1 := testutil.NewSampleEntity("C1")
	c2 := testutil.NewSampleEntity("C2")

	err := ts.csi.Set(c1)
	ts.Error(err)

	err = ts.csi.Set(c1, extstore.WithExtOption(ts.rel.Name(), int64(2)))
	ts.Error(err)

	err = ts.psi.Set(p1)
	ts.NoError(err)

	err = ts.csi.Set(c1, extstore.WithExtOption(ts.rel.Name(), p1.Id))
	ts.NoError(err)

	err = ts.csi.Set(c2, extstore.WithExtOption(ts.rel.Name(), p1.Id))
	ts.NoError(err)

	ts.Equal(
		map[string]string{
			"cd\x80\x00\x00\x00\x00\x00\x00\x01":                                        "{\"name\":\"C1\"}",
			"cd\x80\x00\x00\x00\x00\x00\x00\x02":                                        "{\"name\":\"C2\"}",
			"cxp-c-rel\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00\x00\x00\x01": "",
			"cxp-c-rel\x80\x00\x00\x00\x00\x00\x00\x02\x80\x00\x00\x00\x00\x00\x00\x01": "",
			"pd\x80\x00\x00\x00\x00\x00\x00\x01":                                        "{\"name\":\"P1\"}",
			"pxp-c-rel\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00\x00\x00\x01": "",
			"pxp-c-rel\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00\x00\x00\x02": "",
		},
		testutil.Dump(ts.txn),
	)

	err = ts.csi.Delete(c1.Id)
	ts.NoError(err)

	ts.Equal(
		map[string]string{
			"cd\x80\x00\x00\x00\x00\x00\x00\x02":                                        "{\"name\":\"C2\"}",
			"cxp-c-rel\x80\x00\x00\x00\x00\x00\x00\x02\x80\x00\x00\x00\x00\x00\x00\x01": "",
			"pd\x80\x00\x00\x00\x00\x00\x00\x01":                                        "{\"name\":\"P1\"}",
			"pxp-c-rel\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00\x00\x00\x02": "",
		},
		testutil.Dump(ts.txn),
	)
}

func (ts *AssociateSuite) TestPIDFunc() {
	ps := testutil.NewRecordStore([]byte("g"))
	cs := testutil.NewRecordStore([]byte("f"))
	rectools.Associate("g-f-rel", ps, cs).WithPIDFunc(func(_ *testutil.SampleRecord) (int64, error) {
		return 1, nil
	})

	psi := ps.Instantiate(ts.txn)
	csi := cs.Instantiate(ts.txn)

	p1 := testutil.NewSampleEntity("P1")
	c1 := testutil.NewSampleEntity("C1")

	err := psi.Set(p1)
	ts.NoError(err)

	err = csi.Set(c1)
	ts.NoError(err)

	ts.Equal(
		map[string]string{
			"fd\x80\x00\x00\x00\x00\x00\x00\x01":                                        "{\"name\":\"C1\"}",
			"gd\x80\x00\x00\x00\x00\x00\x00\x01":                                        "{\"name\":\"P1\"}",
			"gxg-f-rel\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00\x00\x00\x01": "",
		},
		testutil.Dump(ts.txn),
	)

	err = csi.Delete(c1.Id)
	ts.NoError(err)

	ts.Equal(
		map[string]string{
			"gd\x80\x00\x00\x00\x00\x00\x00\x01": "{\"name\":\"P1\"}",
		},
		testutil.Dump(ts.txn),
	)
}

func (ts *AssociateSuite) TestPartial() {
	ps := testutil.NewRecordStore([]byte("g"))
	cs := testutil.NewRecordStore([]byte("f"))
	rectools.Associate("g-f-rel", ps, cs).Partial()

	c1 := testutil.NewSampleEntity("C1")

	csi := cs.Instantiate(ts.txn)

	err := csi.Set(c1)
	ts.NoError(err)
}

func (ts *AssociateSuite) TestInstance() {
	ps := testutil.NewRecordStore([]byte("g"))
	cs := testutil.NewRecordStore([]byte("f"))
	rel := rectools.Associate("g-f-rel", ps, cs).Partial()
	ins := rel.Instantiate(ts.txn)

	p1 := testutil.NewSampleEntity("P1")
	c1 := testutil.NewSampleEntity("C1")
	c2 := testutil.NewSampleEntity("C2")
	c3 := testutil.NewSampleEntity("C3")

	err := ins.Set(p1, c1, c2)
	ts.NoError(err)

	err = ins.Set(nil, c3)
	ts.NoError(err)

	p, err := ins.GetParent(c1.Id)
	ts.NoError(err)
	ts.Equal(p1, p)

	p, err = ins.GetParent(c3.Id)
	ts.NoError(err)
	ts.Nil(p)

	it, err := ins.GetChildrenIterator(p1.Id, badger.DefaultIteratorOptions)
	ts.NoError(err)
	defer it.Close()
	childs, err := iters.Collect(it)
	ts.NoError(err)

	ts.Equal([]*testutil.SampleRecord{c1, c2}, childs)
}
