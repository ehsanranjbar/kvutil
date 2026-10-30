package harness

import (
	"math"
	"testing"
)

func TestKeysUniformStaysInRange(t *testing.T) {
	r := NewRand(1, 1)
	k := NewKeys(Uniform, 50)
	seen := map[int64]bool{}
	for range 5000 {
		v := k.Next(r)
		if v < 0 || v >= 50 {
			t.Fatalf("key %d out of range", v)
		}
		seen[v] = true
	}
	if len(seen) != 50 {
		t.Fatalf("uniform keys drew %d of 50 distinct keys", len(seen))
	}
}

func TestKeysZipfIsSkewedAndScrambled(t *testing.T) {
	const n = 10_000
	k := NewKeys(Zipf, n)
	r := NewRand(2, 1)
	counts := map[int64]int{}
	const draws = 400_000
	for range draws {
		v := k.Next(r)
		if v < 0 || v >= n {
			t.Fatalf("key %d out of range", v)
		}
		counts[v]++
	}

	// The most popular key has ln 2 / ln(n+1) of the draws: 7.5% for 10,000 keys.
	hot := k.Scramble(0)
	want := math.Ln2 / math.Log(n+1)
	got := float64(counts[hot]) / draws
	if math.Abs(got-want) > 0.01 {
		t.Errorf("the hottest key has %.4f of the draws, want about %.4f", got, want)
	}
	// And the one of rank 9 has about a tenth of that.
	got9 := float64(counts[k.Scramble(9)]) / draws
	if ratio := got9 / got; ratio < 0.07 || ratio > 0.14 {
		t.Errorf("rank 9 has %.3f of the draws of rank 0, want about 0.1", ratio)
	}
	// The popular keys are not the first keys.
	if hot == 0 {
		t.Errorf("the hottest key is key 0: the ranks were not scrambled")
	}
}

func TestKeysScrambleIsABijection(t *testing.T) {
	for _, n := range []int64{1, 2, 7, 1000, 2654435761 % 100000, 99991} {
		k := NewKeys(Zipf, n)
		seen := make(map[int64]bool, n)
		for rank := range n {
			key := k.Scramble(rank)
			if key < 0 || key >= n || seen[key] {
				t.Fatalf("n=%d: rank %d maps to %d, which is out of range or taken", n, rank, key)
			}
			seen[key] = true
		}
	}
}

func TestKeysOfOne(t *testing.T) {
	r := NewRand(3, 1)
	for _, d := range []Dist{Uniform, Zipf} {
		k := NewKeys(d, 1)
		for range 20 {
			if v := k.Next(r); v != 0 {
				t.Fatalf("%s: a single key drew %d", d, v)
			}
		}
	}
}
