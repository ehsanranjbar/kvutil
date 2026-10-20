package dataset

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Book is the record of the query benchmarks: a catalogue of books, with a skewed author, a few
// fields to filter and sort by, tags, a title, an optional field and, for large values, a payload.
// The id is the key of the record and is not a part of the value.
type Book struct {
	ID       int64 `msgpack:"-" json:"-"`
	Code     string
	Author   string
	Year     int64
	Price    int64
	InPrint  bool
	Tags     []string
	Title    string
	Subtitle *string
	Score    int64
	Payload  []byte
}

// GetId returns the id of the record.
func (b *Book) GetId() int64 { return b.ID }

// SetId sets the id of the record.
func (b *Book) SetId(id int64) { b.ID = id }

const (
	// FirstYear and LastYear are the least and the greatest Year of a book: 576 of them.
	FirstYear = 1450
	LastYear  = 2025
	// MinPrice and MaxPrice bound Price, in cents, both included.
	MinPrice = 100
	MaxPrice = 9999
	// InPrintPercent is the share of the books that are in print.
	InPrintPercent = 10
	// ScoreRange is the number of different values of Score: from 0 up to, and not including, this.
	ScoreRange = 1 << 30
)

var subtitles = []string{"A Novel", "A Memoir", "Second Edition", "Illustrated", "Collected Essays",
	"Revised and Expanded", "A History", "Volume One", "The Complete Guide", "Annotated"}

// Authors is the number of different authors: about twenty books each, on average, but not
// evenly: see AuthorRank.
func (s Spec) Authors() int {
	return max(50, s.Scale/20)
}

// AuthorName is the author of a rank: the most popular author has rank 0.
func AuthorName(rank int) string {
	return "author-" + pad(rank, 6)
}

// AuthorRank returns the rank of the author of a book without generating the book. The ranks
// follow a Zipf law of exponent 1: the author of rank k has about 1/(k+1) of the books of the
// first, and the first has ln 2 / ln(A+1) of all of them, A being the number of authors. The rank
// is drawn as floor(exp(u * ln(A+1))) - 1 for a uniform u, which has that distribution without a
// table.
func (s Spec) AuthorRank(id int64) int {
	r := bookRNG(s.Seed, uint64(id))
	return s.authorRank(r)
}

func (s Spec) authorRank(r *rng) int {
	a := s.Authors()
	u := float64(r.next()>>11) / (1 << 53)
	rank := int(math.Exp(u*math.Log(float64(a)+1))) - 1
	return min(max(rank, 0), a-1)
}

func bookRNG(seed, id uint64) *rng {
	// A constant apart from the one of the orders, so that the two datasets are not correlated.
	return newRNG(seed^0x5bd1e9955bd1e995, id, 0)
}

// BookCode is the unique code of the book of an id: a bijective scramble of the id, so that the
// order of the codes is not the order of the ids.
func BookCode(id int64) string {
	x := uint32(id)
	x *= 0x9e3779b1
	x ^= x >> 15
	x *= 0x85ebca77
	x ^= x >> 13
	return fmt.Sprintf("BK-%08x", x)
}

// TitleSuffixes is the number of different numbers that end a title: about four books have
// each title.
func (s Spec) TitleSuffixes() int {
	return max(4, s.Scale/1024)
}

// GenBook returns the book of an id.
func (s Spec) GenBook(id int64) *Book {
	b := &Book{}
	s.GenBookInto(b, id)
	return b
}

// GenBookInto is GenBook into a book that is given, which reuses its payload.
func (s Spec) GenBookInto(b *Book, id int64) {
	r := bookRNG(s.Seed, uint64(id))

	b.ID = id
	b.Code = BookCode(id)
	b.Author = AuthorName(s.authorRank(r))
	b.Year = int64(FirstYear + r.intn(LastYear-FirstYear+1))
	b.Price = int64(MinPrice + r.intn(MaxPrice-MinPrice+1))
	b.InPrint = r.intn(100) < InPrintPercent
	b.Title = adjectives[r.intn(len(adjectives))] + " " + nouns[r.intn(len(nouns))] + " " + strconv.Itoa(1+r.intn(s.TitleSuffixes()))

	b.Subtitle = nil
	if r.intn(2) == 0 {
		sub := subtitles[r.intn(len(subtitles))]
		b.Subtitle = &sub
	}

	b.Tags = b.Tags[:0]
	for n := r.intn(5); len(b.Tags) < n; {
		tag := "tag-" + pad(r.intn(TagPool), 2)
		if !contains(b.Tags, tag) {
			b.Tags = append(b.Tags, tag)
		}
	}
	if len(b.Tags) == 0 {
		b.Tags = nil
	}

	b.Score = int64(r.next() >> 34)

	if n := s.Values.PayloadBytes(); n > 0 {
		if cap(b.Payload) < n {
			b.Payload = make([]byte, n)
		}
		b.Payload = b.Payload[:n]
		for i := 0; i < n; i += 8 {
			v := r.next()
			for j := 0; j < 8 && i+j < n; j++ {
				b.Payload[i+j] = byte(v >> (8 * j))
			}
		}
	} else {
		b.Payload = nil
	}
}

// TitleWords returns the first two words of a title (the adjective and the noun), which every
// title starts with.
func TitleWords(title string) string {
	parts := strings.SplitN(title, " ", 3)
	if len(parts) < 3 {
		return title
	}
	return parts[0] + " " + parts[1]
}

// AuthorCounts returns the number of books that each author has among the ids [1, n]: indexed by
// rank. It reads the author of every book, and nothing else.
func (s Spec) AuthorCounts(n int64) []int32 {
	counts := make([]int32, s.Authors())
	for id := int64(1); id <= n; id++ {
		counts[s.AuthorRank(id)]++
	}
	return counts
}

// BookCanon returns a text that is the same for the books that are the same, whichever system or
// codec they went through: an empty list is no list, and the payload is its digest.
func BookCanon(b *Book) string {
	var sb strings.Builder
	sub := "<nil>"
	if b.Subtitle != nil {
		sub = strconv.Quote(*b.Subtitle)
	}
	sum := sha1.Sum(b.Payload)
	fmt.Fprintf(&sb, "id=%d code=%s author=%s year=%d price=%d in_print=%t tags=%s title=%q subtitle=%s score=%d payload=%d:%s",
		b.ID, b.Code, b.Author, b.Year, b.Price, b.InPrint, strings.Join(b.Tags, ","), b.Title, sub, b.Score,
		len(b.Payload), hex.EncodeToString(sum[:4]))
	return sb.String()
}
