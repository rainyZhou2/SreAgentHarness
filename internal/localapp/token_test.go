package localapp

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"go-sre-harness/internal/auth"
)

func TestEnsureTokenCreatesAndReuses(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	file, tok, err := EnsureToken(dir)
	if err != nil || len(tok) != 64 {
		t.Fatalf("EnsureToken: %q %v", tok, err)
	}
	ts, err := auth.LoadStaticTokens(file)
	if err != nil {
		t.Fatalf("server cannot load token file: %v", err)
	}
	_ = ts
	if runtime.GOOS != "windows" {
		for _, n := range []string{"token", "token.json"} {
			st, err := os.Stat(filepath.Join(dir, n))
			if err != nil || st.Mode().Perm() != 0o600 {
				t.Errorf("%s: %v mode %v", n, err, st)
			}
		}
	}
	file2, tok2, err := EnsureToken(dir)
	if err != nil || tok2 != tok || file2 != file {
		t.Fatalf("restart must reuse the token: %q vs %q (%v)", tok2, tok, err)
	}
}

func TestEnsureTokenReplacesMismatchedPair(t *testing.T) {
	dir := t.TempDir()
	_, tok, err := EnsureToken(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "token"), []byte("tampered"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, tok2, err := EnsureToken(dir)
	if err != nil || tok2 == tok || tok2 == "tampered" {
		t.Fatalf("mismatched pair must be regenerated: %q %v", tok2, err)
	}
	if _, _, err := EnsureToken(""); err == nil {
		t.Fatal("empty data dir must fail")
	}
}
