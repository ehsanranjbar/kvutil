package testutil

import (
	"encoding/json"

	pstore "github.com/ehsanranjbar/kvutil/store/prefix"
	recstore "github.com/ehsanranjbar/kvutil/store/rec"
)

type SampleStruct struct {
	A int            `json:"a,omitempty"`
	B string         `json:"b,omitempty"`
	C bool           `json:"c,omitempty"`
	D []int          `json:"d,omitempty"`
	E map[string]int `json:"e,omitempty"`
	F *SampleStruct  `json:"f,omitempty"`
	G float32        `json:"g,omitempty"`
	H float64        `json:"h,omitempty"`
}

func (t SampleStruct) MarshalBinary() ([]byte, error) {
	return json.Marshal(t)
}

func (t *SampleStruct) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, t)
}

type SampleRecord struct {
	Id   int64  `json:"-"`
	Name string `json:"name,omitempty"`
}

func NewSampleEntity(name string) *SampleRecord {
	return &SampleRecord{
		Name: name,
	}
}

func (t SampleRecord) GetId() int64 {
	return t.Id
}

func (t *SampleRecord) SetId(id int64) {
	t.Id = id
}

func (t SampleRecord) MarshalBinary() ([]byte, error) {
	return json.Marshal(t)
}

func (t *SampleRecord) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, t)
}

func NewRecordStore(prefix []byte) *recstore.Store[int64, SampleRecord, *SampleRecord] {
	var i int64
	return recstore.New[int64, SampleRecord](pstore.New(nil, prefix)).
		WithIdFunc(func(_ *SampleRecord) (int64, error) {
			i++
			return i, nil
		})
}
