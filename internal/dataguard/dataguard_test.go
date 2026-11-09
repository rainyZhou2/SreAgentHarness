package dataguard

import (
	"errors"
	"strings"
	"testing"
)

func TestRedact(t *testing.T) {
	in := "owner alice@example.com (13812345678) paid with 4111 1111 1111 1111; id 11010519491231002X; " +
		"token=abcd1234secret; host 10.0.3.7 svc checkout; build 1234567890123"
	out, rep := Redact(in, Internal)
	for _, gone := range []string{"alice@example.com", "13812345678", "4111 1111 1111 1111", "11010519491231002X", "abcd1234secret"} {
		if strings.Contains(out, gone) {
			t.Fatalf("%q not redacted: %s", gone, out)
		}
	}
	// Infrastructure identifiers and non-Luhn numbers stay.
	for _, kept := range []string{"10.0.3.7", "checkout", "1234567890123"} {
		if !strings.Contains(out, kept) {
			t.Fatalf("%q must be kept: %s", kept, out)
		}
	}
	if rep.Secrets != 1 || rep.PII != 4 {
		t.Fatalf("report: %+v (%s)", rep, out)
	}
	pub, rep := Redact("mail ops@example.com token=abcd1234secret", Public)
	if !strings.Contains(pub, "ops@example.com") || strings.Contains(pub, "abcd1234secret") || rep.Secrets != 1 {
		t.Fatalf("public: %s %+v", pub, rep)
	}
}

func TestForModel(t *testing.T) {
	if _, _, err := ForModel("x", Restricted, Confidential); !errors.Is(err, ErrTooSensitive) {
		t.Fatalf("restricted data must be refused: %v", err)
	}
	out, _, err := ForModel("call 13812345678", Confidential, Confidential)
	if err != nil || strings.Contains(out, "13812345678") {
		t.Fatalf("confidential: %q %v", out, err)
	}
	for _, s := range []string{"", "public", "internal", "confidential", "restricted"} {
		if _, err := Parse(s); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := Parse("secret"); err == nil {
		t.Fatal("unknown class")
	}
}
