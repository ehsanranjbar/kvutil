package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/examples/petstore/types"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/badgerkv"
	"github.com/ehsanranjbar/kvutil/relation"
	"github.com/ehsanranjbar/kvutil/schema"
	"github.com/ehsanranjbar/kvutil/store/record"
	echo "github.com/labstack/echo/v4"
)

// client calls the server without a network.
type client struct {
	t *testing.T
	e *echo.Echo
}

func newClient(t *testing.T, db *DB) *client {
	e := echo.New()
	NewServer(db).Register(e)
	return &client{t: t, e: e}
}

// do sends a request and returns the status and the body of the response. A body that is not
// nil is sent as JSON.
func (c *client) do(method, path string, body any) (int, []byte) {
	c.t.Helper()

	var reader io.Reader
	if body != nil {
		bz, err := json.Marshal(body)
		if err != nil {
			c.t.Fatal(err)
		}
		reader = bytes.NewReader(bz)
	}
	req := httptest.NewRequest(method, path, reader)
	if body != nil {
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	}
	rec := httptest.NewRecorder()
	c.e.ServeHTTP(rec, req)
	return rec.Code, rec.Body.Bytes()
}

// get gets a path and decodes the JSON of the response.
func get[T any](c *client, path string) T {
	c.t.Helper()

	status, body := c.do(http.MethodGet, path, nil)
	if status != http.StatusOK {
		c.t.Fatalf("GET %s: status %d: %s", path, status, body)
	}
	var v T
	if err := json.Unmarshal(body, &v); err != nil {
		c.t.Fatalf("GET %s: %v: %s", path, err, body)
	}
	return v
}

type petJSON struct {
	Id       string `json:"id"`
	Name     string `json:"name"`
	Status   string `json:"status"`
	Category struct {
		Id   string `json:"id"`
		Name string `json:"name"`
	} `json:"category"`
	PhotoUrls []string `json:"photoUrls"`
	CreatedAt string   `json:"createdAt"`
}

func (c *client) addPet(name, status string, category map[string]any) int64 {
	c.t.Helper()

	code, body := c.do(http.MethodPost, "/pet", map[string]any{"name": name, "status": status, "category": category})
	if code != http.StatusCreated {
		c.t.Fatalf("POST /pet: status %d: %s", code, body)
	}
	var created struct{ Id int64 }
	if err := json.Unmarshal(body, &created); err != nil {
		c.t.Fatal(err)
	}
	return created.Id
}

func (c *client) upload(petId int64, data []byte) (int, string) {
	c.t.Helper()

	var form bytes.Buffer
	w := multipart.NewWriter(&form)
	part, err := w.CreateFormFile("file", "photo.png")
	if err != nil {
		c.t.Fatal(err)
	}
	part.Write(data)
	w.WriteField("additionalMetadata", "a photo")
	w.Close()

	req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/pet/%d/uploadImage", petId), &form)
	req.Header.Set(echo.HeaderContentType, w.FormDataContentType())
	rec := httptest.NewRecorder()
	c.e.ServeHTTP(rec, req)

	var uploaded struct{ Url string }
	json.Unmarshal(rec.Body.Bytes(), &uploaded)
	return rec.Code, strings.TrimPrefix(uploaded.Url, serverUrlBase)
}

func names(pets []petJSON) []string {
	names := []string{}
	for _, p := range pets {
		names = append(names, p.Name)
	}
	return names
}

func equal[T comparable](t *testing.T, what string, got, want []T) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("%s: got %v, want %v", what, got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("%s: got %v, want %v", what, got, want)
		}
	}
}

func TestPetStore(t *testing.T) {
	db, err := NewDB(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c := newClient(t, db)

	// The first pet makes its category, and the others are added to it by its id.
	rex := c.addPet("rex", "AVAILABLE", map[string]any{"name": "dogs"})
	p := get[petJSON](c, fmt.Sprintf("/pet/%d", rex))
	if p.Name != "rex" || p.Status != "AVAILABLE" || p.Category.Name != "dogs" || p.CreatedAt == "" {
		t.Fatalf("got %+v", p)
	}
	dogs := p.Category.Id
	c.addPet("fido", "SOLD", map[string]any{"id": dogs})
	tom := c.addPet("tom", "AVAILABLE", map[string]any{"name": "cats"})

	// A pet cannot be in a category that there is not.
	if code, body := c.do(http.MethodPost, "/pet", map[string]any{"name": "ghost", "category": map[string]any{"id": 999}}); code != http.StatusNotFound {
		t.Fatalf("status %d: %s", code, body)
	}
	if code, _ := c.do(http.MethodPost, "/pet", map[string]any{"name": "stray"}); code != http.StatusBadRequest {
		t.Fatalf("status %d", code)
	}

	// The list, a page at a time, and by the index of the statuses.
	equal(t, "all", names(get[[]petJSON](c, "/pet")), []string{"rex", "fido", "tom"})
	equal(t, "page", names(get[[]petJSON](c, "/pet?offset=1&limit=1")), []string{"fido"})
	equal(t, "available", names(get[[]petJSON](c, "/pet?status=available")), []string{"rex", "tom"})
	equal(t, "available page", names(get[[]petJSON](c, "/pet?status=available&offset=1")), []string{"tom"})
	equal(t, "sold", names(get[[]petJSON](c, "/pet?status=sold")), []string{"fido"})
	equal(t, "pending", names(get[[]petJSON](c, "/pet?status=pending")), []string{})
	if code, _ := c.do(http.MethodGet, "/pet?status=lost", nil); code != http.StatusBadRequest {
		t.Fatalf("status %d", code)
	}

	// The images of a pet.
	png := []byte("\x89PNG\r\n\x1a\n and the rest")
	code, image := c.upload(rex, png)
	if code != http.StatusCreated {
		t.Fatalf("status %d", code)
	}
	if code, _ := c.upload(999, png); code != http.StatusNotFound {
		t.Fatalf("status %d", code)
	}
	if code, body := c.do(http.MethodGet, image, nil); code != http.StatusOK || !bytes.Equal(body, png) {
		t.Fatalf("status %d: %q", code, body)
	}
	equal(t, "photos", get[petJSON](c, fmt.Sprintf("/pet/%d", rex)).PhotoUrls, []string{serverUrlBase + image})

	// The aggregations are exact right after the writes, before anything is collected.
	type count struct {
		Name string
		Pets int64
	}
	stats := func() []string {
		var counts []string
		for _, n := range get[[]count](c, "/stats/categories") {
			counts = append(counts, fmt.Sprint(n.Name, "=", n.Pets))
		}
		return counts
	}
	equal(t, "per category", stats(), []string{"dogs=2", "cats=1"})

	today := time.Now().UTC().Format(dateLayout)
	added := func(by string) int64 {
		t.Helper()

		points := get[[]AddedCount](c, "/stats/added?by="+by+"&from="+today+"&to="+today)
		if len(points) != 1 {
			t.Fatalf("by %s: got %+v", by, points)
		}
		return points[0].Pets
	}
	for _, by := range []string{"day", "week", "month"} {
		if n := added(by); n != 3 {
			t.Fatalf("added by %s: got %d", by, n)
		}
	}
	// A month of days by default, with the days that have no pets.
	if days := get[[]AddedCount](c, "/stats/added"); len(days) != 31 || days[30].Pets != 3 || days[0].Pets != 0 {
		t.Fatalf("got %+v", days)
	}

	// Deleting a pet deletes its images, and takes it out of the index and the aggregations.
	if code, body := c.do(http.MethodDelete, fmt.Sprintf("/pet/%d", rex), nil); code != http.StatusNoContent {
		t.Fatalf("status %d: %s", code, body)
	}
	if code, _ := c.do(http.MethodGet, fmt.Sprintf("/pet/%d", rex), nil); code != http.StatusNotFound {
		t.Fatalf("status %d", code)
	}
	if code, _ := c.do(http.MethodGet, image, nil); code != http.StatusNotFound {
		t.Fatalf("status %d", code)
	}
	equal(t, "available", names(get[[]petJSON](c, "/pet?status=available")), []string{"tom"})
	equal(t, "per category", stats(), []string{"dogs=1", "cats=1"})
	if n := added("day"); n != 2 {
		t.Fatalf("added: got %d", n)
	}

	// The same after the collector has folded the writes into the states.
	deadline := time.Now().Add(10 * time.Second)
	for {
		var collected int64
		err := db.View(func(tx kv.Tx) error {
			var err error
			collected, _, err = db.added.Instantiate(tx).AtCollected(db.added.Buckets()[0], struct{}{}, time.Now())
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		if collected == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the collector has not collected: got %d", collected)
		}
		time.Sleep(50 * time.Millisecond)
	}
	equal(t, "per category", stats(), []string{"dogs=1", "cats=1"})
	if n := added("month"); n != 2 {
		t.Fatalf("added: got %d", n)
	}
	_ = tom
}

// TestReopen checks that a database that is opened again has what was written to it, and that
// its definitions fit the ones that it was written with.
func TestReopen(t *testing.T) {
	opt := badger.DefaultOptions(t.TempDir()).WithLogger(nil)

	db, err := NewDB(opt)
	if err != nil {
		t.Fatal(err)
	}
	c := newClient(t, db)
	c.addPet("rex", "AVAILABLE", map[string]any{"name": "dogs"})
	c.addPet("tom", "PENDING", map[string]any{"name": "cats"})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = NewDB(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c = newClient(t, db)

	equal(t, "all", names(get[[]petJSON](c, "/pet")), []string{"rex", "tom"})
	equal(t, "pending", names(get[[]petJSON](c, "/pet?status=pending")), []string{"tom"})
	// The ids go on from where they were.
	if id := c.addPet("fido", "SOLD", map[string]any{"name": "dogs"}); id <= 2 {
		t.Fatalf("got the id %d", id)
	}
	if days := get[[]AddedCount](c, "/stats/added?by=month"); len(days) == 0 || days[len(days)-1].Pets != 3 {
		t.Fatalf("got %+v", days)
	}
	// A database of the version of the program has nothing to be done to.
	if report := db.Migrated.String(); report != "" {
		t.Fatalf("got the report %q", report)
	}
}

// writeFirstVersion writes a database as the first version of the program did: with pets that
// have no time that they were added at, and without the index and the aggregations.
func writeFirstVersion(t *testing.T, opt badger.Options) {
	t.Helper()

	base, err := badgerkv.Open(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()

	names, err := schema.NewNameRegistry(base)
	if err != nil {
		t.Fatal(err)
	}
	sch := schema.New(schema.WithPrefixAllocator(names))
	categories := record.New[int64, types.Category](sch.Namespace("categories"))
	pets := record.New[int64, types.PetRecord](sch.Namespace("pets"))
	relation.Associate(sch.Namespace("category_pets"), categories, pets).
		WithParentID(func(p *types.PetRecord) (int64, bool) { return p.CategoryId, true })
	if _, err := sch.Migrate(t.Context(), base); err != nil {
		t.Fatal(err)
	}

	err = base.Update(func(tx kv.Tx) error {
		if err := categories.Instantiate(tx).Set(&types.Category{Id: 100, Name: "dogs"}); err != nil {
			return err
		}
		for id, status := range map[int64]types.Status{100: types.Status_AVAILABLE, 101: types.Status_SOLD} {
			p := &types.PetRecord{Id: id, CategoryId: 100, Name: fmt.Sprint("pet", id), Status: status}
			if err := pets.Instantiate(tx).Set(p); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestUpgrade checks that a database of the version of the program before this one is brought
// to this one when it is opened: its pets are migrated, and the index and the aggregations
// that it did not have are built from them.
func TestUpgrade(t *testing.T) {
	opt := badger.DefaultOptions(t.TempDir()).WithLogger(nil)
	writeFirstVersion(t, opt)

	before := time.Now().Unix()
	db, err := NewDB(opt)
	if err != nil {
		t.Fatal(err)
	}
	report := db.Migrated.String()
	for _, line := range []string{
		`migrated unit "pets" from version 1 to 2`,
		`built index "pets/ext/by_status"`,
		`built agg "pets/ext/per_category"`,
		`built agg "pets/ext/added"`,
	} {
		if !strings.Contains(report, line) {
			t.Fatalf("the report has no %q:\n%s", line, report)
		}
	}
	c := newClient(t, db)

	// The pets have the time of the upgrade.
	pets := get[[]petJSON](c, "/pet")
	equal(t, "all", names(pets), []string{"pet100", "pet101"})
	for _, p := range pets {
		var createdAt int64
		fmt.Sscan(p.CreatedAt, &createdAt)
		if createdAt < before || createdAt > time.Now().Unix() {
			t.Fatalf("%s was added at %q", p.Name, p.CreatedAt)
		}
	}

	// The index and the aggregations know of the pets that were there before them.
	equal(t, "available", names(get[[]petJSON](c, "/pet?status=available")), []string{"pet100"})
	equal(t, "sold", names(get[[]petJSON](c, "/pet?status=sold")), []string{"pet101"})
	type count struct {
		Name string
		Pets int64
	}
	if counts := get[[]count](c, "/stats/categories"); len(counts) != 1 || counts[0] != (count{"dogs", 2}) {
		t.Fatalf("got %+v", counts)
	}
	today := time.Now().UTC().Format(dateLayout)
	if days := get[[]AddedCount](c, "/stats/added?from="+today+"&to="+today); len(days) != 1 || days[0].Pets != 2 {
		t.Fatalf("got %+v", days)
	}

	// And they are kept up to date from then on.
	c.addPet("new", "SOLD", map[string]any{"id": 100})
	equal(t, "sold", names(get[[]petJSON](c, "/pet?status=sold")), []string{"new", "pet101"})
	if counts := get[[]count](c, "/stats/categories"); counts[0].Pets != 3 {
		t.Fatalf("got %+v", counts)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// The next time there is nothing to do.
	db, err = NewDB(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if report := db.Migrated.String(); report != "" {
		t.Fatalf("got the report %q", report)
	}
	equal(t, "all", names(get[[]petJSON](newClient(t, db), "/pet")), []string{"new", "pet100", "pet101"})
}
