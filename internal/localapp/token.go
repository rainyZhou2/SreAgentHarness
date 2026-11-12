package localapp

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go-sre-harness/internal/auth"
)

// Names of the files EnsureToken keeps in the data directory.
const (
	tokenFileName = "token.json" // SHA-256 only: what the server loads
	tokenPlain    = "token"      // the bearer token, for the local user's CLI
)

// DataDirFromEnv resolves the local data directory: HARNESS_LOCAL_DATA, else
// the user config directory, else the system temp directory.
func DataDirFromEnv(getenv func(string) string) string {
	if d := getenv("HARNESS_LOCAL_DATA"); d != "" {
		return d
	}
	if dir, err := os.UserConfigDir(); err == nil {
		return filepath.Join(dir, "sre-harness-desktop", "local")
	}
	return filepath.Join(os.TempDir(), "sre-harness-local")
}

// EnsureToken makes sure the data directory holds a local bearer token and
// returns the token file the server loads and the plaintext token. A valid
// existing pair is reused so a restart keeps clients working; a missing or
// mismatched pair is replaced. Both files are private to the user (0600).
func EnsureToken(dataDir string) (tokenFile, token string, err error) {
	if dataDir == "" {
		return "", "", errors.New("local data directory is required")
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return "", "", err
	}
	jsonPath := filepath.Join(dataDir, tokenFileName)
	plainPath := filepath.Join(dataDir, tokenPlain)
	if t, ok := readValidPair(jsonPath, plainPath); ok {
		return jsonPath, t, nil
	}
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", "", err
	}
	token = hex.EncodeToString(b)
	entry := []auth.StaticToken{{SHA256: auth.HashToken(token), Tenant: tenant, Subject: "local-user",
		Roles: []auth.Role{"operator", "approver", "admin"}}}
	data, err := json.Marshal(entry)
	if err != nil {
		return "", "", err
	}
	// The hash file goes last: a crash between the two leaves a mismatched
	// pair, which the next start detects and replaces.
	if err := writePrivate(plainPath, []byte(token)); err != nil {
		return "", "", err
	}
	if err := writePrivate(jsonPath, data); err != nil {
		return "", "", err
	}
	return jsonPath, token, nil
}

func readValidPair(jsonPath, plainPath string) (string, bool) {
	plain, err := os.ReadFile(plainPath)
	if err != nil {
		return "", false
	}
	token := strings.TrimSpace(string(plain))
	if token == "" {
		return "", false
	}
	var ts []auth.StaticToken
	raw, err := os.ReadFile(jsonPath)
	if err != nil || json.Unmarshal(raw, &ts) != nil || len(ts) != 1 {
		return "", false
	}
	want := []byte(strings.ToLower(ts[0].SHA256))
	if subtle.ConstantTimeCompare(want, []byte(auth.HashToken(token))) != 1 {
		return "", false
	}
	if _, err := auth.LoadStaticTokens(jsonPath); err != nil {
		return "", false
	}
	return token, true
}

// writePrivate writes a 0600 file atomically (temp file, then rename).
func writePrivate(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}
