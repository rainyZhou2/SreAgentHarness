package cpkit

import (
	"encoding/json"
	"testing"
	"time"
)

func TestCanonicalJSONOrderIndependent(t *testing.T) {
	a := json.RawMessage(`{"b":1,"a":{"y":[1,2],"x":"s"}}`)
	b := json.RawMessage(`{"a":{"x":"s","y":[1,2]},"b":1}`)
	ha, hb := MustHash(a), MustHash(b)
	if ha != hb {
		t.Fatalf("hash differs: %s vs %s", ha, hb)
	}
	if MustHash(json.RawMessage(`{"b":2}`)) == ha {
		t.Fatal("different values must hash differently")
	}
}

func TestCanonicalJSONKeepsLargeNumbers(t *testing.T) {
	b, err := CanonicalJSON(json.RawMessage(`{"n":12345678901234567890}`))
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"n":12345678901234567890}` {
		t.Fatalf("got %s", b)
	}
}

func TestNormalizeRef(t *testing.T) {
	good := map[string]string{
		"service://prod/search-api":       "service://prod/search-api",
		"SERVICE://prod//search-api/":     "service://prod/search-api",
		"k8s://c1/ns/Deployment/search":   "k8s://c1/ns/Deployment/search",
		" gpu://node-1/GPU-1234-abcd ":    "gpu://node-1/GPU-1234-abcd",
		"model://sglang/llama-3.1-70b@v2": "model://sglang/llama-3.1-70b@v2",
	}
	for in, want := range good {
		got, err := NormalizeRef(in)
		if err != nil || got != want {
			t.Errorf("NormalizeRef(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	bad := []string{"", "search-api", "service://", "service://a/../b", "service://a b", "ser vice://a",
		"service://a?x=1", "service://a#f", "service://a%2Fb"}
	for _, in := range bad {
		if _, err := NormalizeRef(in); err == nil {
			t.Errorf("NormalizeRef(%q) accepted", in)
		}
	}
}

func TestRefMatches(t *testing.T) {
	cases := []struct {
		p, r string
		want bool
	}{
		{"service://prod", "service://prod/search", true},
		{"service://prod", "service://production", false},
		{"service://prod/*", "service://prod/search", true},
		{"service://prod/*", "service://prod/search/x", false},
		{"service://prod/search", "service://prod/search", true},
	}
	for _, c := range cases {
		if got := RefMatches(c.p, c.r); got != c.want {
			t.Errorf("RefMatches(%q,%q)=%v", c.p, c.r, got)
		}
	}
}

func TestDuration(t *testing.T) {
	var d struct {
		A Duration `json:"a"`
		B Duration `json:"b"`
	}
	if err := json.Unmarshal([]byte(`{"a":"5m","b":30}`), &d); err != nil {
		t.Fatal(err)
	}
	if d.A.D() != 5*time.Minute || d.B.D() != 30*time.Second {
		t.Fatalf("got %v %v", d.A.D(), d.B.D())
	}
	if err := json.Unmarshal([]byte(`{"a":"five"}`), &d); err == nil {
		t.Fatal("bad duration accepted")
	}
}

func TestTruncateRuneBoundary(t *testing.T) {
	s := "故障排查"
	if got := Truncate(s, 4); got != "故" {
		t.Fatalf("got %q", got)
	}
}
