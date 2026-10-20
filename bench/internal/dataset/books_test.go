package dataset

import (
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func TestBooksAreDeterministic(t *testing.T) {
	spec := Spec{Seed: 7, Scale: 5000, Values: Large}
	for _, id := range []int64{1, 2, 999, 123456} {
		a, b := spec.GenBook(id), spec.GenBook(id)
		if !reflect.DeepEqual(a, b) || BookCanon(a) != BookCanon(b) {
			t.Fatalf("id %d: two generations differ", id)
		}
	}
	b := spec.GenBook(5)
	spec.GenBookInto(b, 77)
	if want := spec.GenBook(77); !reflect.DeepEqual(b, want) {
		t.Fatalf("GenBookInto over a used book differs:\n%+v\n%+v", b, want)
	}
	other := Spec{Seed: 8, Scale: 5000, Values: Large}
	if BookCanon(other.GenBook(10)) == BookCanon(spec.GenBook(10)) {
		t.Error("a different seed gives the same book")
	}
}

func TestBookCodesAreUnique(t *testing.T) {
	seen := map[string]int64{}
	for id := int64(1); id <= 200_000; id++ {
		code := BookCode(id)
		if other, ok := seen[code]; ok {
			t.Fatalf("the ids %d and %d have the code %s", other, id, code)
		}
		seen[code] = id
	}
	if BookCode(1) >= BookCode(2) && BookCode(2) >= BookCode(3) && BookCode(3) >= BookCode(4) {
		t.Error("the codes are in the order of the ids")
	}
}

func TestAuthorsAreSkewed(t *testing.T) {
	spec := Spec{Seed: 1, Scale: 200_000, Values: Small}
	n := int64(spec.Scale)
	counts := spec.AuthorCounts(n)
	if len(counts) != spec.Authors() {
		t.Fatalf("%d counts for %d authors", len(counts), spec.Authors())
	}

	var total int64
	for _, c := range counts {
		total += int64(c)
	}
	if total != n {
		t.Fatalf("%d books counted of %d", total, n)
	}

	// The first author has ln 2 / ln(A+1) of the books, and the author of rank k about 1/(k+1)
	// of the first.
	want := math.Ln2 / math.Log(float64(spec.Authors())+1)
	if got := float64(counts[0]) / float64(n); math.Abs(got-want) > want*0.1 {
		t.Errorf("the first author has %.4f of the books, want about %.4f", got, want)
	}
	for _, k := range []int{9, 29, 99} {
		ratio := float64(counts[k]) / float64(counts[0])
		if wantRatio := 1 / float64(k+1); ratio < wantRatio*0.6 || ratio > wantRatio*1.6 {
			t.Errorf("rank %d has %.4f of the first, want about %.4f", k, ratio, wantRatio)
		}
	}

	// A tail of authors with a few books each, which the cases of few matches pick from.
	few := 0
	for _, c := range counts {
		if c >= 1 && c <= 4 {
			few++
		}
	}
	if few < 100 {
		t.Errorf("only %d authors have 1 to 4 books", few)
	}

	// AuthorRank agrees with the book.
	for id := int64(1); id <= 500; id++ {
		if got := AuthorName(spec.AuthorRank(id)); got != spec.GenBook(id).Author {
			t.Fatalf("book %d: AuthorRank says %s, the book has %s", id, got, spec.GenBook(id).Author)
		}
	}
}

func TestShapeOfBooks(t *testing.T) {
	spec := Spec{Seed: 3, Scale: 20_000, Values: Small}
	var inPrint, subtitled, tagged int
	years := map[int64]int{}
	titles := map[string]int{}
	var tagCount [5]int
	for id := int64(1); id <= 20_000; id++ {
		b := spec.GenBook(id)
		if b.ID != id || !strings.HasPrefix(b.Code, "BK-") || b.Payload != nil {
			t.Fatalf("book %d: %+v", id, b)
		}
		if b.Year < FirstYear || b.Year > LastYear || b.Price < MinPrice || b.Price > MaxPrice ||
			b.Score < 0 || b.Score >= ScoreRange {
			t.Fatalf("book %d out of range: %+v", id, b)
		}
		if b.InPrint {
			inPrint++
		}
		if b.Subtitle != nil {
			subtitled++
		}
		if len(b.Tags) > 4 {
			t.Fatalf("book %d has %d tags", id, len(b.Tags))
		}
		if b.Tags != nil {
			tagged++
		}
		tagCount[len(b.Tags)]++
		seen := map[string]bool{}
		for _, tag := range b.Tags {
			if seen[tag] {
				t.Fatalf("book %d has the tag %s twice", id, tag)
			}
			seen[tag] = true
		}
		years[b.Year]++
		titles[b.Title]++
		if !strings.HasPrefix(b.Title, TitleWords(b.Title)+" ") {
			t.Fatalf("book %d: TitleWords of %q is %q", id, b.Title, TitleWords(b.Title))
		}
	}
	if inPrint < 1600 || inPrint > 2400 {
		t.Errorf("%d books in print of 20000, want about 2000", inPrint)
	}
	if subtitled < 9000 || subtitled > 11000 {
		t.Errorf("%d books with a subtitle of 20000, want about 10000", subtitled)
	}
	if tagged < 15000 {
		t.Errorf("%d books with tags of 20000", tagged)
	}
	for n, c := range tagCount {
		if c == 0 {
			t.Errorf("no book has %d tags", n)
		}
	}
	if len(years) != LastYear-FirstYear+1 {
		t.Errorf("%d different years, want %d", len(years), LastYear-FirstYear+1)
	}
	// About four books have each title, and the titles that a lower-case comparison would find
	// are the ones that are equal: no two titles differ in case only.
	lower := map[string]string{}
	for title := range titles {
		if other, ok := lower[strings.ToLower(title)]; ok && other != title {
			t.Fatalf("%q and %q differ in case only", title, other)
		}
		lower[strings.ToLower(title)] = title
	}
	if avg := 20000 / float64(len(titles)); avg < 2 || avg > 8 {
		t.Errorf("%.1f books per title, want about 4", avg)
	}
}

func TestBooksEncodeAndLargeOnesHaveAPayload(t *testing.T) {
	spec := Spec{Seed: 1, Scale: 1000, Values: Large}
	b := spec.GenBook(3)
	if len(b.Payload) != 4096 {
		t.Fatalf("payload of %d bytes", len(b.Payload))
	}
	bz, err := msgpack.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	var back Book
	if err := msgpack.Unmarshal(bz, &back); err != nil {
		t.Fatal(err)
	}
	back.ID = 3
	if BookCanon(&back) != BookCanon(b) {
		t.Errorf("the book changed through msgpack:\n%s\n%s", BookCanon(&back), BookCanon(b))
	}
}
