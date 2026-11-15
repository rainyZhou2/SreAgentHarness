package memory

import "testing"

func TestEmbeddingComparableRequiresExactDescriptor(t *testing.T) {
	a := EmbeddingDescriptor{Model: "http:text-embedding-3", Version: "v1", Dimensions: 256}
	for _, b := range []EmbeddingDescriptor{a,
		{Model: a.Model, Version: "v2", Dimensions: 256},
		{Model: a.Model, Version: "v1", Dimensions: 128},
		{Model: "other", Version: "v1", Dimensions: 256}} {
		want := b == a
		if got := a.Comparable(b); got != want {
			t.Fatalf("compare %+v with %+v: got %v", a, b, got)
		}
	}
	if (HashingEmbedder{Dims: 64}).Descriptor() != (EmbeddingDescriptor{Model: "hash-uni-bigram", Version: "v2", Dimensions: 64}) {
		t.Fatal("hash descriptor must expose stable model, version and dimensions")
	}
}

func TestEmbeddingKeySeparatesVersionAndDimensions(t *testing.T) {
	a := EmbeddingDescriptor{Model: "model", Version: "v1", Dimensions: 64}
	b := a
	b.Version = "v2"
	c := a
	c.Dimensions = 128
	if a.Key() == b.Key() || a.Key() == c.Key() || b.Key() == c.Key() {
		t.Fatal("embedding identities must not share a vector key")
	}
}
