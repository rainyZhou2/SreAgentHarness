package localruntime

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
)

// newToken returns a random bearer token and the token-file payload. The file
// contains only the SHA-256 of the token, matching the harness token
// authenticator format.
func newToken() (string, []byte, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", nil, err
	}
	plain := hex.EncodeToString(b)
	entry := struct {
		SHA256  string   `json:"sha256"`
		Tenant  string   `json:"tenant"`
		Subject string   `json:"subject"`
		Roles   []string `json:"roles"`
	}{tokenSHA256(plain), "local", "desktop", []string{"operator", "approver", "admin"}}
	data, err := json.Marshal([]any{entry})
	return plain, data, err
}

func tokenSHA256(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func writeTokenFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}
