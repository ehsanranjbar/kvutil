package types

import (
	"encoding/json"
	"reflect"
	"testing"
)

// TestPetJSON pins the JSON of the API to what protojson wrote for the messages that the types
// were before.
func TestPetJSON(t *testing.T) {
	for _, tc := range []struct {
		name string
		pet  *Pet
		json string
	}{
		{
			name: "full",
			pet: &Pet{
				Id:        42,
				Category:  &Category{Id: 7, Name: "dogs"},
				Name:      `rex "the" <dog> & é`,
				PhotoUrls: []string{"http://localhost:8081/image/a", "http://localhost:8081/image/b"},
				Tags:      []*Tag{{Id: 1, Name: "good"}, {Id: 2, Name: "boy"}},
				Status:    Status_AVAILABLE,
				CreatedAt: 1700000000,
			},
			// Like encoding/json, protojson escapes the characters of HTML as unicode escapes.
			json: `{"id":"42","category":{"id":"7","name":"dogs"},"name":"rex \"the\" \u` + `003cdog\u` + `003e \u` + `0026 é",` +
				`"photoUrls":["http://localhost:8081/image/a","http://localhost:8081/image/b"],` +
				`"tags":[{"id":"1","name":"good"},{"id":"2","name":"boy"}],"status":"AVAILABLE","createdAt":"1700000000"}`,
		},
		{
			name: "minimal",
			pet:  &Pet{Name: "x", Category: &Category{Id: 3}},
			json: `{"category":{"id":"3"},"name":"x"}`,
		},
		{
			name: "zero",
			pet:  &Pet{},
			json: `{}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := json.Marshal(tc.pet)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.json {
				t.Fatalf("got  %s\nwant %s", got, tc.json)
			}

			var back Pet
			if err := json.Unmarshal([]byte(tc.json), &back); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(&back, tc.pet) {
				t.Fatalf("got %+v, want %+v", back, tc.pet)
			}
		})
	}
}

// TestPetUnmarshalJSON checks what protojson accepted besides what it wrote.
func TestPetUnmarshalJSON(t *testing.T) {
	var p Pet
	in := `{"name":"a","status":2,"category":{"id":5,"name":"c"},"id":"9","createdAt":12}`
	if err := json.Unmarshal([]byte(in), &p); err != nil {
		t.Fatal(err)
	}
	want := Pet{Id: 9, Category: &Category{Id: 5, Name: "c"}, Name: "a", Status: Status_PENDING, CreatedAt: 12}
	if !reflect.DeepEqual(p, want) {
		t.Fatalf("got %+v, want %+v", p, want)
	}

	for _, bad := range []string{
		`{"name":"a","extra":1}`,
		`{"name":"a","status":"BOGUS"}`,
		`{"name":"a","category":{"id":"x"}}`,
		`{"name":"a","category":{"other":1}}`,
	} {
		if err := json.Unmarshal([]byte(bad), new(Pet)); err == nil {
			t.Errorf("%s: got no error", bad)
		}
	}
}
