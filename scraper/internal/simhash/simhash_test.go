package simhash

import "testing"

func TestFingerprint_IdenticalTextsMatchExactly(t *testing.T) {
	text := "the quick brown fox jumps over the lazy dog"
	first, second := Fingerprint(text), Fingerprint(text)
	if first != second {
		t.Error("identical text should produce identical fingerprints")
	}
}

func TestFingerprint_NearDuplicateTextsAreClose(t *testing.T) {
	// A realistic article-length body shared by both, differing only by a
	// timestamp and an ad slot -- exactly the case exact content-hash
	// dedup misses but simhash should catch. Simhash's near-duplicate
	// property relies on shared words dominating the bit vote, which needs
	// a body long enough that a couple of differing tokens can't flip many
	// bits -- a two-word difference in a two-word document says nothing,
	// but the same difference in a paragraph-length shared body does.
	const sharedBody = `the quarterly earnings report released this morning showed the
company beat analyst expectations on both revenue and profit margins
the stock price rallied in early trading as investors reacted
positively to the strong guidance provided by the chief financial
officer during the call analysts noted that the growth was broad
based across all major product lines and geographic regions the
company reiterated its full year outlook and announced an increase
to its share buyback program`

	a := sharedBody + " published 2024-01-01 10:00am sponsored by acme ads"
	b := sharedBody + " published 2024-01-01 11:30am sponsored by other ads"

	d := HammingDistance(Fingerprint(a), Fingerprint(b))
	if d > 3 {
		t.Errorf("Hamming distance = %d, want <= 3 for near-duplicate text (fingerprints should be close)", d)
	}
}

func TestFingerprint_UnrelatedTextsAreFar(t *testing.T) {
	a := "breaking news today the market rallied on strong earnings from tech companies"
	b := "recipe for chocolate chip cookies requires flour sugar butter eggs and vanilla extract"

	d := HammingDistance(Fingerprint(a), Fingerprint(b))
	if d < 10 {
		t.Errorf("Hamming distance = %d, want a large distance for unrelated text", d)
	}
}

func TestHammingDistance_Symmetric(t *testing.T) {
	a := Fingerprint("some sample text for hashing")
	b := Fingerprint("some different sample text for hashing purposes")
	if HammingDistance(a, b) != HammingDistance(b, a) {
		t.Error("HammingDistance should be symmetric")
	}
}

func TestHammingDistance_SameValueIsZero(t *testing.T) {
	h := Fingerprint("anything at all")
	if HammingDistance(h, h) != 0 {
		t.Errorf("HammingDistance(h, h) = %d, want 0", HammingDistance(h, h))
	}
}

func TestAnyWithin(t *testing.T) {
	base := Fingerprint("the quick brown fox jumps over the lazy dog every single morning")
	near := Fingerprint("the quick brown fox jumps over the lazy dog every single evening")
	far := Fingerprint("completely unrelated text about a totally different topic entirely")

	hashes := []uint64{base}

	if !AnyWithin(hashes, near, 3) {
		t.Error("near-duplicate fingerprint should be within threshold of an existing hash")
	}
	if AnyWithin(hashes, far, 3) {
		t.Error("unrelated fingerprint should not be within threshold")
	}
	if !AnyWithin(hashes, base, 0) {
		t.Error("identical fingerprint should be within distance 0 of itself")
	}
}

func TestAnyWithin_EmptySliceIsAlwaysFalse(t *testing.T) {
	if AnyWithin(nil, Fingerprint("anything"), 64) {
		t.Error("AnyWithin against an empty slice should always be false")
	}
}
