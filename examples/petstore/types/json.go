package types

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
)

// The JSON of the API is the one that protojson wrote for the messages that these types were
// before: the names of the fields in lowerCamelCase, the numbers of 64 bits as strings, a status
// as its name, and the fields that are zero or empty left out. It reads the numbers of 64 bits
// as numbers too, a status as its number too, and refuses a field that it does not know.

// number is an int64 in JSON as protojson has it: a string when written, and a string or a
// number when read.
type number int64

// MarshalJSON implements json.Marshaler.
func (n number) MarshalJSON() ([]byte, error) {
	return strconv.AppendQuote(nil, strconv.FormatInt(int64(n), 10)), nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (n *number) UnmarshalJSON(b []byte) error {
	text := string(b)
	if text == "null" {
		return nil
	}
	if len(text) > 0 && text[0] == '"' {
		unquoted, err := strconv.Unquote(text)
		if err != nil {
			return fmt.Errorf("invalid number %s", text)
		}
		text = unquoted
	}
	v, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid number %s", b)
	}
	*n = number(v)
	return nil
}

// statusName is a status in JSON as protojson has it: its name when written, and its name or
// its number when read.
type statusName Status

// MarshalJSON implements json.Marshaler.
func (s statusName) MarshalJSON() ([]byte, error) {
	if name, ok := Status_name[int32(s)]; ok {
		return json.Marshal(name)
	}
	return strconv.AppendInt(nil, int64(s), 10), nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (s *statusName) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		return nil
	}
	if len(b) > 0 && b[0] == '"' {
		var name string
		if err := json.Unmarshal(b, &name); err != nil {
			return err
		}
		v, ok := Status_value[name]
		if !ok {
			return fmt.Errorf("invalid status %s", b)
		}
		*s = statusName(v)
		return nil
	}
	v, err := strconv.ParseInt(string(b), 10, 32)
	if err != nil {
		return fmt.Errorf("invalid status %s", b)
	}
	*s = statusName(v)
	return nil
}

// strict decodes JSON into v and refuses the fields that v does not have.
func strict(b []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

type categoryJSON struct {
	Id   number `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

// MarshalJSON implements json.Marshaler.
func (c Category) MarshalJSON() ([]byte, error) {
	return json.Marshal(categoryJSON{Id: number(c.Id), Name: c.Name})
}

// UnmarshalJSON implements json.Unmarshaler.
func (c *Category) UnmarshalJSON(b []byte) error {
	var j categoryJSON
	if err := strict(b, &j); err != nil {
		return err
	}
	*c = Category{Id: int64(j.Id), Name: j.Name}
	return nil
}

type tagJSON struct {
	Id   number `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

// MarshalJSON implements json.Marshaler.
func (t Tag) MarshalJSON() ([]byte, error) {
	return json.Marshal(tagJSON{Id: number(t.Id), Name: t.Name})
}

// UnmarshalJSON implements json.Unmarshaler.
func (t *Tag) UnmarshalJSON(b []byte) error {
	var j tagJSON
	if err := strict(b, &j); err != nil {
		return err
	}
	*t = Tag{Id: int64(j.Id), Name: j.Name}
	return nil
}

type petJSON struct {
	Id        number     `json:"id,omitempty"`
	Category  *Category  `json:"category,omitempty"`
	Name      string     `json:"name,omitempty"`
	PhotoUrls []string   `json:"photoUrls,omitempty"`
	Tags      []*Tag     `json:"tags,omitempty"`
	Status    statusName `json:"status,omitempty"`
	CreatedAt number     `json:"createdAt,omitempty"`
}

// MarshalJSON implements json.Marshaler.
func (p Pet) MarshalJSON() ([]byte, error) {
	return json.Marshal(petJSON{
		Id:        number(p.Id),
		Category:  p.Category,
		Name:      p.Name,
		PhotoUrls: p.PhotoUrls,
		Tags:      p.Tags,
		Status:    statusName(p.Status),
		CreatedAt: number(p.CreatedAt),
	})
}

// UnmarshalJSON implements json.Unmarshaler.
func (p *Pet) UnmarshalJSON(b []byte) error {
	var j petJSON
	if err := strict(b, &j); err != nil {
		return err
	}
	*p = Pet{
		Id:        int64(j.Id),
		Category:  j.Category,
		Name:      j.Name,
		PhotoUrls: j.PhotoUrls,
		Tags:      j.Tags,
		Status:    Status(j.Status),
		CreatedAt: int64(j.CreatedAt),
	}
	return nil
}
